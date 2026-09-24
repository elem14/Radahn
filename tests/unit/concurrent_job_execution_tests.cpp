#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "radahn/coordinator/in_memory_coordinator.hpp"

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/in_memory_worker_repository.hpp"

#include "radahn/scheduler/least_loaded_policy.hpp"

/*
 * Milestone 4F — multiple simultaneous jobs per worker.
 *
 * The coordinator's resource/eligibility accounting for multiple
 * concurrent jobs on one worker (running_jobs vs max_concurrent_jobs,
 * per-job resource reservation) already existed going into 4F, built
 * during 4A-4D. What was actually missing was independent per-job
 * lease renewal — a lease was only ever set once, at mark_running(),
 * and nothing extended it, so any job running longer than the lease
 * duration would be reclaimed out from under a worker that was still
 * legitimately executing it. These tests lock in the fix
 * (renew_job_leases_for_worker) and the multi-job dispatch/eligibility
 * behavior it relies on.
 */

namespace {

int failure_count = 0;

constexpr std::uint64_t mebibyte =
    1024ULL * 1024ULL;

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
make_requirements() {
    return radahn::domain::ResourceRequirements{
        2.0,
        2ULL * gibibyte,
        5ULL * mebibyte,
        false,
        {"linux"}
    };
}

radahn::domain::Job make_job(
    std::string id,
    int priority = 50
) {
    return radahn::domain::Job{
        radahn::domain::JobId{std::move(id)},
        "Concurrent execution test job",
        priority,
        make_requirements()
    };
}

radahn::domain::WorkerRecord make_worker(
    std::string id,
    std::size_t max_concurrent_jobs = 4
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
                8.0,
                8.0,
                16ULL * gibibyte,
                16ULL * gibibyte,
                100ULL * gibibyte,
                100ULL * gibibyte,
                false
            },
            0,
            max_concurrent_jobs,
            {"linux", "x86_64"}
        }
    };
}

void test_renewed_lease_survives_past_base_duration() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;

    using radahn::persistence::
        InMemoryJobRepository;

    using radahn::persistence::
        InMemoryWorkerRepository;

    using radahn::scheduler::
        LeastLoadedPolicy;

    constexpr auto lease_duration =
        std::chrono::seconds{5};

    const JobId job_id{"long-running-job"};
    const WorkerId worker_id{"renewal-worker"};

    InMemoryJobRepository job_repository;
    InMemoryWorkerRepository worker_repository;

    LeastLoadedPolicy policy;

    InMemoryCoordinator coordinator{
        policy,
        job_repository,
        worker_repository,
        lease_duration
    };

    coordinator.register_worker(
        make_worker("renewal-worker")
    );

    coordinator.submit_job(
        make_job("long-running-job")
    );

    const auto decision =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        decision.has_value(),
        "Job dispatches before running"
    );

    coordinator.mark_running(job_id);

    const auto record_before =
        job_repository.get(job_id);

    expect(
        record_before.has_value() &&
        record_before->lease_expires_at.has_value(),
        "Running job carries an initial lease deadline"
    );

    const auto base_deadline =
        *record_before->lease_expires_at;

    /*
     * A heartbeat arrives partway through the lease reporting
     * this job as still active, well before the original
     * deadline would have passed.
     */
    const auto heartbeat_time =
        base_deadline -
        std::chrono::seconds{2};

    const auto renewed_count =
        coordinator.renew_job_leases_for_worker(
            worker_id,
            std::vector<JobId>{job_id},
            heartbeat_time
        );

    expect(
        renewed_count == 1,
        "Heartbeat renews the active job's lease"
    );

    const auto record_after =
        job_repository.get(job_id);

    expect(
        record_after.has_value() &&
        record_after->lease_expires_at.has_value() &&
        *record_after->lease_expires_at >
            base_deadline,
        "Renewal pushes the lease deadline forward"
    );

    /*
     * Time now passes the ORIGINAL deadline. Without renewal
     * this job would have been reclaimed; with renewal it must
     * still be running.
     */
    const auto expired_at_original_deadline =
        coordinator.mark_expired_job_leases(
            base_deadline +
            std::chrono::milliseconds{1}
        );

    expect(
        expired_at_original_deadline == 0,
        "Renewed job is not reclaimed at its original deadline"
    );

    expect(
        coordinator.job_state(job_id) ==
            JobState::running,
        "Renewed job remains RUNNING past its original deadline"
    );
}

void test_lease_renewal_is_independent_per_job() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;

    using radahn::persistence::
        InMemoryJobRepository;

    using radahn::persistence::
        InMemoryWorkerRepository;

    using radahn::scheduler::
        LeastLoadedPolicy;

    constexpr auto lease_duration =
        std::chrono::seconds{5};

    const JobId healthy_job_id{"healthy-job"};
    const JobId stuck_job_id{"stuck-job"};
    const WorkerId worker_id{"independence-worker"};

    InMemoryJobRepository job_repository;
    InMemoryWorkerRepository worker_repository;

    LeastLoadedPolicy policy;

    InMemoryCoordinator coordinator{
        policy,
        job_repository,
        worker_repository,
        lease_duration
    };

    coordinator.register_worker(
        make_worker("independence-worker", 2)
    );

    coordinator.submit_job(
        make_job("healthy-job", 100)
    );

    coordinator.submit_job(
        make_job("stuck-job", 50)
    );

    expect(
        coordinator.dispatch_once_for_worker(
            worker_id
        ).has_value(),
        "Healthy job dispatches onto the shared worker"
    );

    expect(
        coordinator.dispatch_once_for_worker(
            worker_id
        ).has_value(),
        "Stuck job dispatches onto the same worker "
        "(bounded concurrency allows both)"
    );

    coordinator.mark_running(healthy_job_id);
    coordinator.mark_running(stuck_job_id);

    const auto base_record =
        job_repository.get(healthy_job_id);

    expect(
        base_record.has_value() &&
        base_record->lease_expires_at.has_value(),
        "Healthy job carries a lease deadline"
    );

    const auto base_deadline =
        *base_record->lease_expires_at;

    /*
     * A heartbeat reports only the healthy job as active. The
     * stuck job — hung, crashed, or otherwise not reported — is
     * deliberately left out.
     */
    const auto renewed_count =
        coordinator.renew_job_leases_for_worker(
            worker_id,
            std::vector<JobId>{healthy_job_id},
            base_deadline -
                std::chrono::seconds{1}
        );

    expect(
        renewed_count == 1,
        "Only the reported job's lease is renewed"
    );

    const auto expired_count =
        coordinator.mark_expired_job_leases(
            base_deadline +
            std::chrono::milliseconds{1}
        );

    expect(
        expired_count == 1,
        "Exactly the unreported job's lease expires"
    );

    expect(
        coordinator.job_state(healthy_job_id) ==
            JobState::running,
        "Renewed job is unaffected by the other job's expiry"
    );

    expect(
        coordinator.job_state(stuck_job_id) ==
            JobState::retry_wait,
        "Un-renewed job is reclaimed independently, "
        "not kept alive by the other job's heartbeat"
    );
}

void test_renew_job_leases_ignores_invalid_targets() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobId;
    using radahn::domain::WorkerId;

    using radahn::persistence::
        InMemoryJobRepository;

    using radahn::persistence::
        InMemoryWorkerRepository;

    using radahn::persistence::JobLeaseClock;

    using radahn::scheduler::
        LeastLoadedPolicy;

    const JobId leased_job_id{"still-leased-job"};
    const WorkerId worker_id{"guard-worker"};
    const WorkerId other_worker_id{"other-guard-worker"};

    InMemoryJobRepository job_repository;
    InMemoryWorkerRepository worker_repository;

    LeastLoadedPolicy policy;

    InMemoryCoordinator coordinator{
        policy,
        job_repository,
        worker_repository
    };

    coordinator.register_worker(
        make_worker("guard-worker")
    );

    coordinator.register_worker(
        make_worker("other-guard-worker")
    );

    coordinator.submit_job(
        make_job("still-leased-job")
    );

    expect(
        coordinator.dispatch_once_for_worker(
            worker_id
        ).has_value(),
        "Job dispatches into LEASED (never started running)"
    );

    const auto now =
        JobLeaseClock::now();

    const auto renewed_for_wrong_worker =
        coordinator.renew_job_leases_for_worker(
            other_worker_id,
            std::vector<JobId>{leased_job_id},
            now
        );

    expect(
        renewed_for_wrong_worker == 0,
        "A job assigned to a different worker is not renewed"
    );

    const auto renewed_while_leased =
        coordinator.renew_job_leases_for_worker(
            worker_id,
            std::vector<JobId>{leased_job_id},
            now
        );

    expect(
        renewed_while_leased == 0,
        "A LEASED-but-not-yet-RUNNING job is not renewed"
    );

    const auto renewed_unknown_job =
        coordinator.renew_job_leases_for_worker(
            worker_id,
            std::vector<JobId>{JobId{"no-such-job"}},
            now
        );

    expect(
        renewed_unknown_job == 0,
        "An unknown job ID is silently ignored"
    );
}

void test_worker_runs_multiple_jobs_concurrently() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;
    using radahn::scheduler::LeastLoadedPolicy;

    const WorkerId worker_id{"multi-job-worker"};

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    coordinator.register_worker(
        make_worker("multi-job-worker", 2)
    );

    coordinator.submit_job(
        make_job("first-job")
    );

    coordinator.submit_job(
        make_job("second-job")
    );

    coordinator.submit_job(
        make_job("third-job")
    );

    expect(
        coordinator.dispatch_once_for_worker(
            worker_id
        ).has_value(),
        "First job dispatches"
    );

    expect(
        coordinator.dispatch_once_for_worker(
            worker_id
        ).has_value(),
        "Second job dispatches alongside the first"
    );

    const auto full_snapshot =
        coordinator.worker_snapshot(worker_id);

    expect(
        full_snapshot.has_value() &&
        full_snapshot->running_jobs() == 2,
        "Worker reports two occupied execution slots"
    );

    expect(
        full_snapshot.has_value() &&
        full_snapshot->resources()
                .available_cpu_cores() == 4.0,
        "Both jobs' CPU reservations are held simultaneously"
    );

    const auto blocked_decision =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        !blocked_decision.has_value(),
        "A worker at max_concurrent_jobs rejects a third job"
    );

    coordinator.mark_running(
        radahn::domain::JobId{"first-job"}
    );

    coordinator.mark_succeeded(
        radahn::domain::JobId{"first-job"}
    );

    const auto freed_decision =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        freed_decision.has_value() &&
        freed_decision->job_id.value() ==
            "third-job",
        "Freeing one slot lets the third job dispatch"
    );
}

}  // namespace

int main() {
    try {
        test_renewed_lease_survives_past_base_duration();
        test_lease_renewal_is_independent_per_job();
        test_renew_job_leases_ignores_invalid_targets();
        test_worker_runs_multiple_jobs_concurrently();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected concurrent job execution exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " concurrent job execution assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll concurrent job execution tests passed\n";

    return EXIT_SUCCESS;
}
