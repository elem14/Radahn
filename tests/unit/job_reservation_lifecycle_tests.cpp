#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <utility>

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
 * Milestone 4B — reservation lifecycle.
 *
 * These tests lock in behavior not already covered elsewhere:
 * that a reservation survives the LEASED -> RUNNING transition
 * (it is not accidentally re-reserved or released in between),
 * that cancellation reached via lease expiry releases resources
 * the same way retry-driven expiry does, and that a coordinator
 * restart never leaves a worker's resources over- or
 * under-reserved relative to the jobs actually left assigned to
 * it afterward.
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
        "Reservation lifecycle test job",
        priority,
        make_requirements()
    };
}

radahn::domain::WorkerRecord make_worker(
    std::string id
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
            4,
            {"linux", "x86_64"}
        }
    };
}

void test_reservation_holds_through_running_state() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;
    using radahn::scheduler::LeastLoadedPolicy;

    const JobId job_id{"held-job"};
    const WorkerId worker_id{"held-worker"};

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    coordinator.register_worker(
        make_worker("held-worker")
    );

    coordinator.submit_job(
        make_job("held-job")
    );

    const auto decision =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        decision.has_value(),
        "Job dispatches into LEASED"
    );

    const auto leased_snapshot =
        coordinator.worker_snapshot(worker_id);

    expect(
        leased_snapshot.has_value() &&
        leased_snapshot->resources()
                .available_cpu_cores() == 6.0,
        "LEASED reserves worker CPU"
    );

    coordinator.mark_running(job_id);

    expect(
        coordinator.job_state(job_id) ==
            JobState::running,
        "Job transitions to RUNNING"
    );

    const auto running_snapshot =
        coordinator.worker_snapshot(worker_id);

    expect(
        running_snapshot.has_value() &&
        running_snapshot->resources()
                .available_cpu_cores() == 6.0,
        "RUNNING still holds the same reservation"
    );

    expect(
        running_snapshot.has_value() &&
        running_snapshot->running_jobs() == 1,
        "RUNNING still occupies one execution slot"
    );

    coordinator.mark_succeeded(job_id);

    const auto finished_snapshot =
        coordinator.worker_snapshot(worker_id);

    expect(
        finished_snapshot.has_value() &&
        finished_snapshot->resources()
                .available_cpu_cores() == 8.0,
        "SUCCEEDED releases the reservation"
    );
}

void test_cancelled_via_lease_expiry_releases_resources() {
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

    const JobId job_id{"cancelled-job"};
    const WorkerId worker_id{"cancelled-worker"};

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
        make_worker("cancelled-worker")
    );

    coordinator.submit_job(
        make_job("cancelled-job")
    );

    const auto decision =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        decision.has_value(),
        "Job dispatches before cancellation"
    );

    coordinator.mark_running(job_id);

    auto record = job_repository.get(job_id);

    expect(
        record.has_value() &&
        record->lease_expires_at.has_value(),
        "Running job carries a lease deadline"
    );

    if (
        !record.has_value() ||
        !record->lease_expires_at.has_value()
    ) {
        return;
    }

    const auto lease_deadline =
        *record->lease_expires_at;

    /*
     * Simulate a cancellation request landing while the job is
     * RUNNING. There is no coordinator-level API for this yet
     * (cancellation is only reachable via lease expiry today),
     * so the persisted record is moved into
     * CANCELLATION_REQUESTED directly, preserving its existing
     * worker assignment and lease deadline.
     */
    radahn::domain::Job cancellation_requested_job =
        radahn::domain::Job::restore(
            record->job.id(),
            record->job.name(),
            record->job.priority(),
            record->job.requirements(),
            record->job.workload(),
            JobState::cancellation_requested,
            record->job.created_at(),
            record->job.attempt_count(),
            record->job.max_attempts()
        );

    record->job = std::move(cancellation_requested_job);

    job_repository.update(*record);

    const auto expiration_count =
        coordinator.mark_expired_job_leases(
            lease_deadline
        );

    expect(
        expiration_count == 1,
        "Expired cancellation request is processed"
    );

    expect(
        coordinator.job_state(job_id) ==
            JobState::cancelled,
        "Cancellation-requested job becomes CANCELLED on expiry"
    );

    const auto worker =
        worker_repository.get(worker_id);

    expect(
        worker.has_value() &&
        worker->snapshot().running_jobs() == 0,
        "Cancelled job releases the worker's execution slot"
    );

    expect(
        worker.has_value() &&
        worker->snapshot()
            .resources()
            .available_cpu_cores() == 8.0,
        "Cancelled job releases the worker's reserved CPU"
    );
}

void test_restart_reconstructs_worker_resources_without_leaking() {
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

    const WorkerId worker_a_id{"restart-worker-a"};
    const WorkerId worker_b_id{"restart-worker-b"};

    /*
     * These repositories outlive both coordinator instances,
     * standing in for durable storage across a process restart.
     */
    InMemoryJobRepository job_repository;
    InMemoryWorkerRepository worker_repository;

    {
        LeastLoadedPolicy policy;

        InMemoryCoordinator coordinator{
            policy,
            job_repository,
            worker_repository
        };

        coordinator.register_worker(
            make_worker("restart-worker-a")
        );

        coordinator.register_worker(
            make_worker("restart-worker-b")
        );

        coordinator.submit_job(
            make_job("leased-before-restart")
        );

        const auto leased_decision =
            coordinator.dispatch_once_for_worker(
                worker_a_id
            );

        expect(
            leased_decision.has_value(),
            "LEASED job dispatches before restart"
        );

        coordinator.submit_job(
            make_job("running-before-restart")
        );

        const auto running_decision =
            coordinator.dispatch_once_for_worker(
                worker_b_id
            );

        expect(
            running_decision.has_value(),
            "RUNNING job dispatches before restart"
        );

        if (running_decision.has_value()) {
            coordinator.mark_running(
                running_decision->job_id
            );
        }

        coordinator.submit_job(
            make_job("queued-before-restart")
        );

        const auto worker_a_before =
            coordinator.worker_snapshot(worker_a_id);

        expect(
            worker_a_before.has_value() &&
            worker_a_before->resources()
                    .available_cpu_cores() == 6.0,
            "Worker A holds a reservation for its LEASED job"
        );

        const auto worker_b_before =
            coordinator.worker_snapshot(worker_b_id);

        expect(
            worker_b_before.has_value() &&
            worker_b_before->resources()
                    .available_cpu_cores() == 6.0,
            "Worker B holds a reservation for its RUNNING job"
        );

        expect(
            coordinator.active_job_count() == 2,
            "Two jobs are active before restart"
        );
    }

    /*
     * "Restart": a fresh coordinator is constructed over the
     * same repositories, exactly as a new coordinator process
     * would reopen its persisted state.
     */
    {
        LeastLoadedPolicy policy;

        InMemoryCoordinator coordinator{
            policy,
            job_repository,
            worker_repository
        };

        expect(
            coordinator.active_job_count() == 0,
            "No job remains active immediately after restart"
        );

        expect(
            coordinator.queued_job_count() == 3,
            "LEASED, RUNNING, and QUEUED jobs are all requeued"
        );

        const auto worker_a_after =
            coordinator.worker_snapshot(worker_a_id);

        expect(
            worker_a_after.has_value() &&
            worker_a_after->resources()
                    .available_cpu_cores() == 8.0,
            "Worker A's reservation is not leaked past restart"
        );

        expect(
            worker_a_after.has_value() &&
            worker_a_after->running_jobs() == 0,
            "Worker A's execution slot is freed past restart"
        );

        const auto worker_b_after =
            coordinator.worker_snapshot(worker_b_id);

        expect(
            worker_b_after.has_value() &&
            worker_b_after->resources()
                    .available_cpu_cores() == 8.0,
            "Worker B's reservation is not leaked past restart"
        );

        expect(
            worker_b_after.has_value() &&
            worker_b_after->running_jobs() == 0,
            "Worker B's execution slot is freed past restart"
        );

        /*
         * With both workers correctly showing full capacity,
         * every requeued job should be dispatchable again
         * without ever exceeding a worker's true capacity
         * (reserve() would throw if the coordinator thought a
         * worker had capacity it did not actually have).
         */
        std::size_t dispatched = 0;

        while (
            coordinator.dispatch_once().has_value()
        ) {
            ++dispatched;
        }

        expect(
            dispatched == 3,
            "All three requeued jobs redispatch cleanly"
        );

        expect(
            coordinator.active_job_count() == 3,
            "All redispatched jobs are active after restart"
        );
    }
}

}  // namespace

int main() {
    try {
        test_reservation_holds_through_running_state();
        test_cancelled_via_lease_expiry_releases_resources();
        test_restart_reconstructs_worker_resources_without_leaking();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected reservation lifecycle exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " reservation lifecycle assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll job reservation lifecycle tests passed\n";

    return EXIT_SUCCESS;
}
