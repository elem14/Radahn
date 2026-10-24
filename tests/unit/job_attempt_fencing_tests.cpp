#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>

#include "radahn/coordinator/in_memory_coordinator.hpp"
#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/in_memory_worker_repository.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"

namespace {

namespace domain = radahn::domain;
namespace persistence = radahn::persistence;

using radahn::coordinator::InMemoryCoordinator;
using radahn::coordinator::JobAttempt;

void require(bool condition, const char* description) {
    if (!condition) {
        throw std::runtime_error{description};
    }

    std::cout << "[PASS] " << description << '\n';
}

struct Fixture {
    radahn::scheduler::LeastLoadedPolicy policy;
    persistence::InMemoryJobRepository jobs;
    persistence::InMemoryWorkerRepository workers;

    InMemoryCoordinator coordinator{
        policy,
        jobs,
        workers,
        std::chrono::seconds{30}
    };

    domain::WorkerId worker_id{"worker-a"};
    domain::JobId job_id{"job-a"};

    Fixture() {
        coordinator.register_worker(
            domain::WorkerRecord{
                domain::WorkerSnapshot{
                    worker_id,
                    domain::WorkerState::online,
                    domain::WorkerResources{
                        4.0, 4.0,
                        4096, 4096,
                        4096, 4096,
                        false
                    },
                    0,
                    2,
                    {}
                }
            }
        );

        coordinator.submit_job(
            domain::Job{
                job_id,
                "Attempt fencing test",
                1,
                domain::ResourceRequirements{
                    1.0, 128, 128, false, {}
                }
            }
        );

        require(
            coordinator.dispatch_once_for_worker(worker_id).has_value(),
            "initial attempt dispatched"
        );
    }

    JobAttempt attempt() const {
        return JobAttempt{
            job_id,
            static_cast<std::uint64_t>(
                jobs.get(job_id)->job.attempt_count()
            )
        };
    }
};

domain::ExecutionResult result_with_exit_code(int code) {
    return domain::ExecutionResult{
        domain::ExecutionOutcome::exited,
        code,
        std::nullopt,
        {}
    };
}

void test_stale_attempt_and_duplicate_finish() {
    Fixture fixture;
    auto& coordinator = fixture.coordinator;

    const auto first = fixture.attempt();

    require(
        coordinator.start_job_attempt(
            fixture.worker_id,
            first,
            persistence::JobLeaseClock::now()
        ),
        "first attempt starts"
    );

    const auto expiration =
        *fixture.jobs.get(fixture.job_id)->lease_expires_at;

    require(
        coordinator.mark_expired_job_leases(expiration) == 1,
        "first attempt expires"
    );

    require(
        coordinator.requeue_retry_wait_jobs() == 1,
        "expired attempt requeues"
    );

    require(
        coordinator.dispatch_once_for_worker(
            fixture.worker_id
        ).has_value(),
        "same worker receives the next attempt"
    );

    const auto second = fixture.attempt();

    require(
        second.attempt_number == first.attempt_number + 1,
        "new dispatch increments attempt number"
    );

    const auto now = persistence::JobLeaseClock::now();

    require(
        !coordinator.start_job_attempt(
            fixture.worker_id, first, now
        ),
        "old attempt cannot start the new assignment"
    );

    require(
        coordinator.start_job_attempt(
            fixture.worker_id, second, now
        ),
        "current attempt starts"
    );

    const auto lease_before =
        fixture.jobs.get(fixture.job_id)->lease_expires_at;

    require(
        coordinator.renew_job_attempt_leases(
            fixture.worker_id, {first}, now
        ) == 0,
        "old attempt cannot renew current lease"
    );

    require(
        fixture.jobs.get(fixture.job_id)->lease_expires_at ==
            lease_before,
        "stale renewal leaves lease unchanged"
    );

    const auto success = result_with_exit_code(0);

    require(
        !coordinator.finish_job_attempt(
            fixture.worker_id, first, success, now
        ),
        "old completion is rejected"
    );

    require(
        !coordinator.finish_job_attempt(
            domain::WorkerId{"wrong-worker"},
            second,
            success,
            now
        ),
        "wrong worker completion is rejected"
    );

    require(
        coordinator.job_state(fixture.job_id) ==
            domain::JobState::running,
        "rejected completions leave current attempt running"
    );

    require(
        fixture.workers.get(fixture.worker_id)
            ->allocated_resources().cpu_cores() == 1.0,
        "rejected completions retain CPU reservation"
    );

    require(
        coordinator.renew_job_attempt_leases(
            fixture.worker_id,
            {second, second},
            now + std::chrono::seconds{1}
        ) == 1,
        "current lease renews once despite duplicate entries"
    );

    require(
        coordinator.finish_job_attempt(
            fixture.worker_id, second, success, now
        ),
        "current completion succeeds"
    );

    require(
        coordinator.finish_job_attempt(
            fixture.worker_id, second, success, now
        ),
        "identical completion succeeds again"
    );

    require(
        fixture.workers.get(fixture.worker_id)
            ->allocated_resources().cpu_cores() == 0.0,
        "duplicate completion does not double-release CPU"
    );

    require(
        !coordinator.finish_job_attempt(
            fixture.worker_id,
            second,
            result_with_exit_code(7),
            now
        ),
        "conflicting completion is rejected"
    );

    require(
        fixture.jobs.get(fixture.job_id)
            ->job.execution_result()->exit_code == 0,
        "conflicting completion leaves stored result unchanged"
    );
}

void test_expiration_without_sweep() {
    Fixture fixture;
    auto& coordinator = fixture.coordinator;
    const auto attempt = fixture.attempt();

    const auto leased_expiration =
        *fixture.jobs.get(fixture.job_id)->lease_expires_at;

    require(
        !coordinator.start_job_attempt(
            fixture.worker_id, attempt, leased_expiration
        ),
        "expired assignment cannot start before sweep"
    );

    // The rejected call made no change; use the real current
    // time to start the still-unswept fixture deterministically.
    require(
        coordinator.start_job_attempt(
            fixture.worker_id,
            attempt,
            persistence::JobLeaseClock::now()
        ),
        "fixture starts with a valid lease"
    );

    const auto running_expiration =
        *fixture.jobs.get(fixture.job_id)->lease_expires_at;

    require(
        coordinator.renew_job_attempt_leases(
            fixture.worker_id, {attempt}, running_expiration
        ) == 0,
        "expired running lease cannot be revived"
    );

    require(
        !coordinator.finish_job_attempt(
            fixture.worker_id,
            attempt,
            result_with_exit_code(0),
            running_expiration
        ),
        "expired running attempt cannot finish before sweep"
    );

    require(
        coordinator.job_state(fixture.job_id) ==
            domain::JobState::running,
        "rejection leaves reclamation to the sweep"
    );
}

}  // namespace

int main() {
    try {
        test_stale_attempt_and_duplicate_finish();
        test_expiration_without_sweep();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}