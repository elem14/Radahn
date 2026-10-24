#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "completion_reporter.hpp"

#include "radahn/coordinator/in_memory_coordinator.hpp"
#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/in_memory_worker_repository.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"

namespace {

namespace domain = radahn::domain;
namespace persistence = radahn::persistence;

using radahn::coordinator::InMemoryCoordinator;
using radahn::coordinator::JobAttempt;
using radahn::worker_app::CompletionRecord;
using radahn::worker_app::CompletionReporter;
using radahn::worker_app::CompletionReportResult;
using radahn::worker_app::ICompletionTransport;

void require(bool condition, std::string_view description) {
    if (!condition) {
        throw std::runtime_error{std::string{description}};
    }

    std::cout << "[PASS] " << description << '\n';
}

/*
 * Commits each completion for real against a live coordinator, but
 * deterministically loses the first response - simulating the case
 * where the coordinator accepted a completion and the acknowledging
 * reply never reached the worker.
 */
class LossThenAcceptTransport final : public ICompletionTransport {
public:
    LossThenAcceptTransport(
        InMemoryCoordinator& coordinator,
        domain::WorkerId worker_id,
        persistence::JobLeaseTimePoint now
    )
        : coordinator_{coordinator},
          worker_id_{std::move(worker_id)},
          now_{now} {
    }

    [[nodiscard]]
    grpc::Status finish_job(
        const CompletionRecord& record,
        radahn::rpc::v1::FinishJobResponse& response
    ) override {
        static_cast<void>(response);

        ++call_count;

        const JobAttempt attempt{
            domain::JobId{record.job_id},
            record.attempt_number
        };

        const bool accepted =
            coordinator_.finish_job_attempt(
                worker_id_,
                attempt,
                record.result,
                now_
            );

        if (!accepted) {
            return grpc::Status{
                grpc::StatusCode::FAILED_PRECONDITION,
                "rejected"
            };
        }

        if (call_count == 1) {
            return grpc::Status{
                grpc::StatusCode::UNAVAILABLE,
                "simulated response loss"
            };
        }

        return grpc::Status::OK;
    }

    int call_count = 0;

private:
    InMemoryCoordinator& coordinator_;
    domain::WorkerId worker_id_;
    persistence::JobLeaseTimePoint now_;
};

void test_retry_after_lost_response() {
    radahn::scheduler::LeastLoadedPolicy policy;
    persistence::InMemoryJobRepository jobs;
    persistence::InMemoryWorkerRepository workers;

    InMemoryCoordinator coordinator{
        policy,
        jobs,
        workers,
        std::chrono::seconds{30}
    };

    const domain::WorkerId worker_id{"worker-a"};
    const domain::JobId job_id{"job-a"};

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
            "Completion retry test",
            1,
            domain::ResourceRequirements{
                1.0, 128, 128, false, {}
            }
        }
    );

    require(
        coordinator.dispatch_once_for_worker(worker_id).has_value(),
        "job dispatched"
    );

    const auto now = persistence::JobLeaseClock::now();

    const auto attempt_number =
        static_cast<std::uint64_t>(
            jobs.get(job_id)->job.attempt_count()
        );

    require(
        coordinator.start_job_attempt(
            worker_id,
            JobAttempt{job_id, attempt_number},
            now
        ),
        "attempt starts"
    );

    const domain::ExecutionResult result{
        domain::ExecutionOutcome::exited,
        0,
        std::nullopt,
        {}
    };

    const CompletionRecord record{
        worker_id.value(),
        job_id.value(),
        attempt_number,
        result
    };

    LossThenAcceptTransport transport{
        coordinator,
        worker_id,
        now
    };

    const CompletionReporter reporter{
        transport,
        std::chrono::seconds{10}
    };

    const auto report_result = reporter.report(record);

    require(
        report_result == CompletionReportResult::accepted,
        "Retry after a lost response still reports acceptance"
    );

    require(
        transport.call_count == 2,
        "Exactly one retry was needed after the simulated loss"
    );

    require(
        coordinator.job_state(job_id) ==
            domain::JobState::succeeded,
        "The coordinator holds the committed success"
    );

    require(
        workers.get(worker_id)
            ->allocated_resources().cpu_cores() == 0.0,
        "The CPU reservation was released exactly once, "
        "not once per retry"
    );
}

/*
 * A FAILED_PRECONDITION response means this worker has lost lease
 * authority over the attempt; the reporter must stop immediately
 * rather than retry forever.
 */
class AlwaysRejectTransport final : public ICompletionTransport {
public:
    [[nodiscard]]
    grpc::Status finish_job(
        const CompletionRecord&,
        radahn::rpc::v1::FinishJobResponse&
    ) override {
        ++call_count;

        return grpc::Status{
            grpc::StatusCode::FAILED_PRECONDITION,
            "stale attempt"
        };
    }

    int call_count = 0;
};

void test_rejected_attempt_stops_immediately() {
    AlwaysRejectTransport transport;

    const CompletionReporter reporter{
        transport,
        std::chrono::seconds{10}
    };

    const CompletionRecord record{
        "worker-a",
        "job-a",
        1,
        domain::ExecutionResult{
            domain::ExecutionOutcome::exited,
            0,
            std::nullopt,
            {}
        }
    };

    const auto result = reporter.report(record);

    require(
        result == CompletionReportResult::rejected,
        "A FAILED_PRECONDITION response is reported as rejected"
    );

    require(
        transport.call_count == 1,
        "A rejected attempt is not retried"
    );
}

}  // namespace

int main() {
    try {
        test_retry_after_lost_response();
        test_rejected_attempt_stops_immediately();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
