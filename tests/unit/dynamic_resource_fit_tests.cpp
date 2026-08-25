#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <utility>

#include "radahn/coordinator/in_memory_coordinator.hpp"

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/scheduler/least_loaded_policy.hpp"

/*
 * Milestone 4C — dynamic resource-fit checking.
 *
 * Eligibility must be judged against a worker's currently
 * *available* capacity, not its total capacity: a 16-core worker
 * with 14 cores already reserved cannot take a 4-core job even
 * though 4 <= 16. These tests drive that scenario through the
 * real coordinator dispatch path (submit -> dispatch -> reserve),
 * not a hand-constructed snapshot, so they also exercise the 4B
 * reservation wiring that produces the "available" figure being
 * checked.
 */

namespace {

int failure_count = 0;

constexpr std::uint64_t gibibyte =
    1024ULL * 1024ULL * 1024ULL;

void expect(
    bool condition,
    std::string_view description
) {
    if (condition) {
        std::cout
            << "[PASS] "
            << description
            << '\n';

        return;
    }

    std::cerr
        << "[FAIL] "
        << description
        << '\n';

    ++failure_count;
}

radahn::domain::ResourceRequirements
make_requirements(double cpu_cores) {
    return radahn::domain::ResourceRequirements{
        cpu_cores,
        1ULL * gibibyte,
        1ULL * gibibyte,
        false,
        {"linux"}
    };
}

radahn::domain::Job make_job(
    std::string id,
    double cpu_cores,
    int priority = 50
) {
    return radahn::domain::Job{
        radahn::domain::JobId{std::move(id)},
        "Dynamic resource-fit test job",
        priority,
        make_requirements(cpu_cores)
    };
}

radahn::domain::WorkerRecord make_worker(
    std::string id,
    double total_cpu,
    double available_cpu,
    std::size_t running_jobs = 0
) {
    using radahn::domain::WorkerId;
    using radahn::domain::WorkerRecord;
    using radahn::domain::WorkerResources;
    using radahn::domain::WorkerSnapshot;
    using radahn::domain::WorkerState;

    return WorkerRecord{
        WorkerSnapshot{
            WorkerId{std::move(id)},
            WorkerState::online,
            WorkerResources{
                total_cpu,
                available_cpu,
                16ULL * gibibyte,
                16ULL * gibibyte,
                100ULL * gibibyte,
                100ULL * gibibyte,
                false
            },
            running_jobs,
            10,
            {"linux", "x86_64"}
        }
    };
}

void test_job_waits_for_available_not_total_capacity() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;
    using radahn::scheduler::LeastLoadedPolicy;

    const WorkerId worker_id{"capacity-worker"};

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    /*
     * Total capacity is 16 cores. Two 7-core jobs are dispatched
     * first, leaving only 2 cores available.
     */
    coordinator.register_worker(
        make_worker("capacity-worker", 16.0, 16.0)
    );

    coordinator.submit_job(
        make_job("filler-1", 7.0, 90)
    );

    coordinator.submit_job(
        make_job("filler-2", 7.0, 80)
    );

    expect(
        coordinator.dispatch_once().has_value(),
        "First filler job dispatches"
    );

    expect(
        coordinator.dispatch_once().has_value(),
        "Second filler job dispatches"
    );

    const auto loaded_snapshot =
        coordinator.worker_snapshot(worker_id);

    expect(
        loaded_snapshot.has_value() &&
        loaded_snapshot->resources()
                .available_cpu_cores() == 2.0,
        "Worker has 2 of 16 total cores available"
    );

    /*
     * A 4-core job fits the worker's 16-core total but not its
     * 2-core current availability. It must not be dispatched.
     */
    coordinator.submit_job(
        make_job("big-job", 4.0, 100)
    );

    const auto blocked_decision =
        coordinator.dispatch_once();

    expect(
        !blocked_decision.has_value(),
        "4-core job is not dispatched with only 2 cores available"
    );

    expect(
        coordinator.job_state(
            JobId{"big-job"}
        ) == JobState::queued,
        "4-core job remains queued despite fitting total capacity"
    );

    /*
     * Once a filler job completes and releases its reservation,
     * the previously-blocked job becomes dispatchable.
     */
    coordinator.mark_running(
        JobId{"filler-1"}
    );

    coordinator.mark_succeeded(
        JobId{"filler-1"}
    );

    const auto freed_snapshot =
        coordinator.worker_snapshot(worker_id);

    expect(
        freed_snapshot.has_value() &&
        freed_snapshot->resources()
                .available_cpu_cores() == 9.0,
        "Completing a filler job frees its reserved cores"
    );

    const auto unblocked_decision =
        coordinator.dispatch_once();

    expect(
        unblocked_decision.has_value() &&
        unblocked_decision->job_id.value() ==
            "big-job",
        "4-core job dispatches once enough capacity frees up"
    );
}

void test_scheduler_prefers_availability_over_total_capacity() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::WorkerId;
    using radahn::scheduler::LeastLoadedPolicy;

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    /*
     * "big-worker" has far more total capacity than "small-worker"
     * but almost none of it is currently available. The scheduler
     * must route to "small-worker" instead of rejecting the job
     * outright or naively favoring the larger total.
     */
    coordinator.register_worker(
        make_worker(
            "big-worker",
            32.0,
            2.0,
            9
        )
    );

    coordinator.register_worker(
        make_worker(
            "small-worker",
            8.0,
            8.0,
            0
        )
    );

    coordinator.submit_job(
        make_job("routed-job", 4.0)
    );

    const auto decision =
        coordinator.dispatch_once();

    expect(
        decision.has_value() &&
        decision->worker_id.value() ==
            "small-worker",
        "Job routes to the worker with sufficient availability, "
        "not the one with more total capacity"
    );

    expect(
        decision.has_value() &&
        decision->eligible_worker_count == 1,
        "The heavily-reserved worker is excluded as ineligible, "
        "not merely deprioritized"
    );
}

}  // namespace

int main() {
    test_job_waits_for_available_not_total_capacity();
    test_scheduler_prefers_availability_over_total_capacity();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " dynamic resource-fit assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll dynamic resource-fit tests passed\n";

    return EXIT_SUCCESS;
}
