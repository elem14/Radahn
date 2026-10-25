/*
 * Verifies the structured "event=..." lines InMemoryCoordinator
 * writes to stdout at the important state transitions (see
 * radahn::coordinator::log_event, libs/coordinator/src/event_log.cpp).
 *
 * Exercises the coordinator layer directly (no gRPC server), the
 * same way job_attempt_fencing_tests.cpp does, and captures stdout
 * by swapping std::cout's streambuf for the duration of each
 * operation under test. worker_registered, job_submitted,
 * log_upload_failed and the shutdown events are logged one layer up,
 * in CoordinatorServiceImpl - they are exercised (and visible in its
 * own stdout) by tests/unit/coordinator_rpc_integration_tests.cpp
 * instead.
 */

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "radahn/coordinator/in_memory_coordinator.hpp"
#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/in_memory_worker_repository.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"

namespace {

namespace domain = radahn::domain;
namespace persistence = radahn::persistence;

using radahn::coordinator::InMemoryCoordinator;
using radahn::coordinator::JobAttempt;

void require(bool condition, std::string_view description) {
    if (!condition) {
        throw std::runtime_error{std::string{description}};
    }

    std::cout << "[PASS] " << description << '\n';
}

/*
 * Swaps std::cout's streambuf for the duration of its lifetime, so
 * event lines written during a scenario can be captured and asserted
 * on afterward instead of going to the test binary's real stdout.
 */
class StdoutCapture {
public:
    StdoutCapture()
        : previous_buffer_{
              std::cout.rdbuf(buffer_.rdbuf())
          } {
    }

    ~StdoutCapture() {
        std::cout.rdbuf(previous_buffer_);
    }

    StdoutCapture(const StdoutCapture&) = delete;
    StdoutCapture& operator=(const StdoutCapture&) = delete;

    [[nodiscard]]
    std::string str() const {
        return buffer_.str();
    }

private:
    std::ostringstream buffer_;
    std::streambuf* previous_buffer_;
};

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

    void register_and_submit() {
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
                "Event logging test",
                1,
                domain::ResourceRequirements{
                    1.0, 128, 128, false, {}
                }
            }
        );
    }

    void dispatch_and_start() {
        if (
            !coordinator
                .dispatch_once_for_worker(worker_id)
                .has_value()
        ) {
            throw std::runtime_error{
                "Setup: dispatch_once_for_worker did not dispatch"
            };
        }

        if (
            !coordinator.start_job_attempt(
                worker_id,
                attempt(),
                persistence::JobLeaseClock::now()
            )
        ) {
            throw std::runtime_error{
                "Setup: start_job_attempt did not start"
            };
        }
    }

    [[nodiscard]]
    JobAttempt attempt() const {
        return JobAttempt{
            job_id,
            static_cast<std::uint64_t>(
                jobs.get(job_id)->job.attempt_count()
            )
        };
    }
};

[[nodiscard]]
domain::ExecutionResult succeeded_result() {
    return domain::ExecutionResult{
        domain::ExecutionOutcome::exited,
        0,
        std::nullopt,
        {}
    };
}

[[nodiscard]]
domain::ExecutionResult failed_result() {
    return domain::ExecutionResult{
        domain::ExecutionOutcome::exited,
        1,
        std::nullopt,
        {}
    };
}

void test_attempt_dispatched_and_started() {
    Fixture fixture;
    fixture.register_and_submit();

    bool dispatched = false;
    std::string dispatch_log;

    {
        StdoutCapture capture;

        dispatched =
            fixture.coordinator
                .dispatch_once_for_worker(fixture.worker_id)
                .has_value();

        dispatch_log = capture.str();
    }

    require(
        dispatched,
        "The job dispatches to the registered worker"
    );

    require(
        dispatch_log.find(
            "event=attempt_dispatched job_id=job-a attempt=1 "
            "worker_id=worker-a"
        ) != std::string::npos,
        "attempt_dispatched is logged with job_id, attempt, "
        "and worker_id"
    );

    bool started = false;
    std::string start_log;

    {
        StdoutCapture capture;

        started = fixture.coordinator.start_job_attempt(
            fixture.worker_id,
            fixture.attempt(),
            persistence::JobLeaseClock::now()
        );

        start_log = capture.str();
    }

    require(
        started,
        "StartJob transitions the leased attempt to running"
    );

    require(
        start_log.find(
            "event=attempt_started job_id=job-a attempt=1 "
            "worker_id=worker-a"
        ) != std::string::npos,
        "attempt_started is logged when the attempt actually "
        "transitions to running"
    );
}

void test_lease_rejected_for_stale_attempt() {
    Fixture fixture;
    fixture.register_and_submit();
    fixture.dispatch_and_start();

    const JobAttempt stale_attempt{
        fixture.job_id,
        99
    };

    bool rejected = false;
    std::string log;

    {
        StdoutCapture capture;

        rejected = !fixture.coordinator.start_job_attempt(
            fixture.worker_id,
            stale_attempt,
            persistence::JobLeaseClock::now()
        );

        log = capture.str();
    }

    require(
        rejected,
        "An attempt number that does not match the current "
        "attempt is rejected"
    );

    require(
        log.find(
            "event=lease_rejected job_id=job-a attempt=99 "
            "worker_id=worker-a reason=stale_attempt"
        ) != std::string::npos,
        "lease_rejected is logged with the rejection reason"
    );
}

void test_completion_accepted_duplicate_and_rejected() {
    Fixture fixture;
    fixture.register_and_submit();
    fixture.dispatch_and_start();

    bool accepted = false;
    std::string accepted_log;

    {
        StdoutCapture capture;

        accepted = fixture.coordinator.finish_job_attempt(
            fixture.worker_id,
            fixture.attempt(),
            succeeded_result(),
            persistence::JobLeaseClock::now()
        );

        accepted_log = capture.str();
    }

    require(
        accepted,
        "A valid completion is accepted"
    );

    require(
        accepted_log.find(
            "event=completion_accepted job_id=job-a attempt=1 "
            "worker_id=worker-a"
        ) != std::string::npos,
        "completion_accepted is logged for a fresh acceptance"
    );

    bool duplicate_accepted = false;
    std::string duplicate_log;

    {
        StdoutCapture capture;

        duplicate_accepted = fixture.coordinator.finish_job_attempt(
            fixture.worker_id,
            fixture.attempt(),
            succeeded_result(),
            persistence::JobLeaseClock::now()
        );

        duplicate_log = capture.str();
    }

    require(
        duplicate_accepted,
        "An identical repeated completion is still accepted"
    );

    require(
        duplicate_log.find(
            "event=duplicate_completion_accepted job_id=job-a "
            "attempt=1 worker_id=worker-a"
        ) != std::string::npos,
        "duplicate_completion_accepted is logged for an "
        "idempotent retry"
    );

    bool rejected = false;
    std::string rejected_log;

    {
        StdoutCapture capture;

        rejected = !fixture.coordinator.finish_job_attempt(
            fixture.worker_id,
            fixture.attempt(),
            failed_result(),
            persistence::JobLeaseClock::now()
        );

        rejected_log = capture.str();
    }

    require(
        rejected,
        "A conflicting repeated completion is rejected"
    );

    require(
        rejected_log.find(
            "event=completion_rejected job_id=job-a attempt=1 "
            "worker_id=worker-a reason=conflicting_result"
        ) != std::string::npos,
        "completion_rejected is logged with reason=conflicting_result"
    );
}

void test_worker_offline_lease_expired_and_requeued() {
    Fixture fixture;
    fixture.register_and_submit();
    fixture.dispatch_and_start();

    bool went_offline = false;
    std::string offline_log;

    {
        StdoutCapture capture;

        const auto far_future =
            persistence::WorkerHeartbeatClock::now() +
            std::chrono::hours{1};

        went_offline =
            fixture.coordinator.mark_stale_workers_offline(
                far_future,
                std::chrono::seconds{1}
            ) == 1;

        offline_log = capture.str();
    }

    require(
        went_offline,
        "The worker is marked offline once its heartbeat is stale"
    );

    require(
        offline_log.find(
            "event=worker_offline worker_id=worker-a"
        ) != std::string::npos,
        "worker_offline is logged for the stale worker"
    );

    bool lease_expired = false;
    std::string expired_log;

    {
        StdoutCapture capture;

        const auto far_future_lease =
            persistence::JobLeaseClock::now() +
            std::chrono::hours{1};

        lease_expired =
            fixture.coordinator.mark_expired_job_leases(
                far_future_lease
            ) == 1;

        expired_log = capture.str();
    }

    require(
        lease_expired,
        "The running attempt's lease is reclaimed as abandoned"
    );

    require(
        expired_log.find(
            "event=lease_expired job_id=job-a attempt=1 "
            "worker_id=worker-a"
        ) != std::string::npos,
        "lease_expired is logged for the abandoned attempt"
    );

    bool requeued = false;
    std::string requeued_log;

    {
        StdoutCapture capture;

        requeued =
            fixture.coordinator.requeue_retry_wait_jobs() == 1;

        requeued_log = capture.str();
    }

    require(
        requeued,
        "The abandoned job is requeued for another attempt"
    );

    require(
        requeued_log.find(
            "event=attempt_requeued job_id=job-a attempt=1"
        ) != std::string::npos,
        "attempt_requeued is logged when a job returns to QUEUED"
    );
}

}  // namespace

int main() {
    try {
        test_attempt_dispatched_and_started();
        test_lease_rejected_for_stale_attempt();
        test_completion_accepted_duplicate_and_rejected();
        test_worker_offline_lease_expired_and_requeued();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
