#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/scheduler/dispatch_planner.hpp"
#include "radahn/scheduler/job_queue.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"
#include "radahn/scheduler/round_robin_policy.hpp"

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
make_cpu_requirements() {
    return radahn::domain::ResourceRequirements{
        1.0,
        1ULL * gibibyte,
        1ULL * gibibyte,
        false,
        {"linux"}
    };
}

radahn::domain::ResourceRequirements
make_gpu_requirements() {
    return radahn::domain::ResourceRequirements{
        2.0,
        2ULL * gibibyte,
        1ULL * gibibyte,
        true,
        {"linux", "gpu"}
    };
}

radahn::domain::Job make_job(
    std::string id,
    int priority,
    radahn::domain::ResourceRequirements requirements
) {
    return radahn::domain::Job{
        radahn::domain::JobId{std::move(id)},
        "Test job",
        priority,
        std::move(requirements)
    };
}

radahn::domain::WorkerSnapshot make_worker(
    std::string id,
    double available_cpu,
    bool gpu_available,
    std::size_t running_jobs,
    std::size_t max_concurrent_jobs,
    std::vector<std::string> tags
) {
    using radahn::domain::WorkerId;
    using radahn::domain::WorkerResources;
    using radahn::domain::WorkerSnapshot;
    using radahn::domain::WorkerState;

    return WorkerSnapshot{
        WorkerId{std::move(id)},
        WorkerState::online,
        WorkerResources{
            8.0,
            available_cpu,
            16ULL * gibibyte,
            12ULL * gibibyte,
            500ULL * gibibyte,
            300ULL * gibibyte,
            gpu_available
        },
        running_jobs,
        max_concurrent_jobs,
        std::move(tags)
    };
}

void test_highest_priority_schedulable_job_selected() {
    using radahn::scheduler::DispatchPlanner;
    using radahn::scheduler::InMemoryJobQueue;
    using radahn::scheduler::LeastLoadedPolicy;

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "low-priority",
            10,
            make_cpu_requirements()
        )
    );

    queue.enqueue(
        make_job(
            "high-priority",
            100,
            make_cpu_requirements()
        )
    );

    const std::vector workers{
        make_worker(
            "worker-a",
            6.0,
            false,
            0,
            4,
            {"linux"}
        )
    };

    LeastLoadedPolicy policy;
    DispatchPlanner planner{policy};

    const auto decision = planner.plan(
        queue,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        decision.has_value() &&
        decision->job_id.value() == "high-priority",
        "Highest-priority schedulable job is selected"
    );

    expect(
        decision.has_value() &&
        decision->worker_id.value() == "worker-a",
        "Dispatch decision contains selected worker"
    );
}

void test_unschedulable_job_does_not_block_queue() {
    using radahn::scheduler::DispatchPlanner;
    using radahn::scheduler::InMemoryJobQueue;
    using radahn::scheduler::LeastLoadedPolicy;

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "gpu-job",
            100,
            make_gpu_requirements()
        )
    );

    queue.enqueue(
        make_job(
            "cpu-job",
            50,
            make_cpu_requirements()
        )
    );

    const std::vector workers{
        make_worker(
            "cpu-worker",
            6.0,
            false,
            0,
            4,
            {"linux"}
        )
    };

    LeastLoadedPolicy policy;
    DispatchPlanner planner{policy};

    const auto decision = planner.plan(
        queue,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        decision.has_value() &&
        decision->job_id.value() == "cpu-job",
        "Lower-priority schedulable job bypasses blocked job"
    );

    expect(
        queue.contains(
            radahn::domain::JobId{"gpu-job"}
        ),
        "Unschedulable high-priority job remains queued"
    );

    expect(
        queue.contains(
            radahn::domain::JobId{"cpu-job"}
        ),
        "Planning does not remove selected job"
    );

    expect(
        queue.size() == 2,
        "Planning does not modify queue size"
    );
}

void test_no_decision_when_nothing_can_run() {
    using radahn::scheduler::DispatchPlanner;
    using radahn::scheduler::InMemoryJobQueue;
    using radahn::scheduler::LeastLoadedPolicy;

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "gpu-job",
            100,
            make_gpu_requirements()
        )
    );

    const std::vector workers{
        make_worker(
            "cpu-worker",
            6.0,
            false,
            0,
            4,
            {"linux"}
        )
    };

    LeastLoadedPolicy policy;
    DispatchPlanner planner{policy};

    const auto decision = planner.plan(
        queue,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        !decision.has_value(),
        "Planner returns no decision when nothing can run"
    );

    expect(
        queue.size() == 1,
        "Unscheduled job remains in queue"
    );
}

void test_least_loaded_worker_is_used() {
    using radahn::scheduler::DispatchPlanner;
    using radahn::scheduler::InMemoryJobQueue;
    using radahn::scheduler::LeastLoadedPolicy;

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "job-1",
            50,
            make_cpu_requirements()
        )
    );

    const std::vector workers{
        make_worker(
            "busy-worker",
            6.0,
            false,
            3,
            4,
            {"linux"}
        ),
        make_worker(
            "idle-worker",
            6.0,
            false,
            0,
            4,
            {"linux"}
        )
    };

    LeastLoadedPolicy policy;
    DispatchPlanner planner{policy};

    const auto decision = planner.plan(
        queue,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        decision.has_value() &&
        decision->worker_id.value() == "idle-worker",
        "Dispatch planner uses configured worker policy"
    );

    expect(
        decision.has_value() &&
        decision->policy_name == "least-loaded",
        "Dispatch decision records policy name"
    );

    expect(
        decision.has_value() &&
        decision->eligible_worker_count == 2,
        "Dispatch decision records eligible worker count"
    );
}

void test_blocked_high_priority_job_dispatches_first_once_eligible() {
    using radahn::scheduler::DispatchPlanner;
    using radahn::scheduler::InMemoryJobQueue;
    using radahn::scheduler::LeastLoadedPolicy;

    InMemoryJobQueue queue;

    // High-priority job that cannot run on the only
    // available worker yet (no GPU).
    queue.enqueue(
        make_job(
            "gpu-job",
            100,
            make_gpu_requirements()
        )
    );

    queue.enqueue(
        make_job(
            "cpu-job-1",
            50,
            make_cpu_requirements()
        )
    );

    queue.enqueue(
        make_job(
            "cpu-job-2",
            40,
            make_cpu_requirements()
        )
    );

    LeastLoadedPolicy policy;
    DispatchPlanner planner{policy};

    {
        const std::vector workers{
            make_worker(
                "cpu-worker",
                6.0,
                false,
                0,
                4,
                {"linux"}
            )
        };

        // While no GPU worker exists, lower-priority CPU
        // jobs are dispatched around the blocked job instead
        // of the queue stalling.
        const auto first_decision = planner.plan(
            queue,
            std::span<
                const radahn::domain::WorkerSnapshot
            >{workers}
        );

        expect(
            first_decision.has_value() &&
            first_decision->job_id.value() == "cpu-job-1",
            "Lower-priority job dispatches while high-priority "
            "job is blocked"
        );

        static_cast<void>(
            queue.take(
                radahn::domain::JobId{"cpu-job-1"}
            )
        );

        const auto second_decision = planner.plan(
            queue,
            std::span<
                const radahn::domain::WorkerSnapshot
            >{workers}
        );

        expect(
            second_decision.has_value() &&
            second_decision->job_id.value() == "cpu-job-2",
            "Next-highest-priority schedulable job dispatches "
            "next while blocked job keeps waiting"
        );

        static_cast<void>(
            queue.take(
                radahn::domain::JobId{"cpu-job-2"}
            )
        );
    }

    expect(
        queue.contains(
            radahn::domain::JobId{"gpu-job"}
        ),
        "Blocked high-priority job was never lost or "
        "silently dropped while waiting"
    );

    // A GPU worker now shows up alongside a fresh, newly
    // enqueued job of lower priority.
    queue.enqueue(
        make_job(
            "late-low-priority",
            10,
            make_cpu_requirements()
        )
    );

    const std::vector workers_with_gpu{
        make_worker(
            "gpu-worker",
            6.0,
            true,
            0,
            4,
            {"linux", "gpu"}
        )
    };

    const auto decision = planner.plan(
        queue,
        std::span<
            const radahn::domain::WorkerSnapshot
        >{workers_with_gpu}
    );

    expect(
        decision.has_value() &&
        decision->job_id.value() == "gpu-job",
        "Previously-blocked high-priority job dispatches "
        "immediately once an eligible worker appears, ahead "
        "of any newer lower-priority job — no permanent "
        "starvation"
    );
}

void test_empty_queue_returns_no_decision() {
    using radahn::scheduler::DispatchPlanner;
    using radahn::scheduler::InMemoryJobQueue;
    using radahn::scheduler::RoundRobinPolicy;

    InMemoryJobQueue queue;

    const std::vector workers{
        make_worker(
            "worker-a",
            6.0,
            false,
            0,
            4,
            {"linux"}
        )
    };

    RoundRobinPolicy policy;
    DispatchPlanner planner{policy};

    const auto decision = planner.plan(
        queue,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        !decision.has_value(),
        "Empty queue produces no dispatch decision"
    );
}

}  // namespace

int main() {
    test_highest_priority_schedulable_job_selected();
    test_unschedulable_job_does_not_block_queue();
    test_no_decision_when_nothing_can_run();
    test_least_loaded_worker_is_used();
    test_blocked_high_priority_job_dispatches_first_once_eligible();
    test_empty_queue_returns_no_decision();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " test assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll dispatch planner tests passed\n";

    return EXIT_SUCCESS;
}