#include "completion_reporter.hpp"

#include <algorithm>
#include <thread>

#include "workload_codec.hpp"

namespace radahn::worker_app {

namespace {

namespace LocalRpc = radahn::rpc::v1;

[[nodiscard]]
LocalRpc::FinishJobRequest to_request(
    const CompletionRecord& record
) {
    LocalRpc::FinishJobRequest request;

    request.set_worker_id(record.worker_id);
    request.set_job_id(record.job_id);
    request.set_attempt_number(record.attempt_number);

    request.set_outcome(
        record.result.succeeded()
            ? LocalRpc::JOB_OUTCOME_SUCCEEDED
            : LocalRpc::JOB_OUTCOME_FAILED
    );

    radahn::rpc_codec::encode_execution_result(
        record.result,
        *request.mutable_execution_result()
    );

    return request;
}

[[nodiscard]]
bool is_transient(
    grpc::StatusCode code
) noexcept {
    return
        code == grpc::StatusCode::UNAVAILABLE ||
        code == grpc::StatusCode::DEADLINE_EXCEEDED;
}

}  // namespace

GrpcCompletionTransport::GrpcCompletionTransport(
    rpc::v1::WorkerService::Stub& stub
)
    : stub_{stub} {
}

grpc::Status GrpcCompletionTransport::finish_job(
    const CompletionRecord& record,
    rpc::v1::FinishJobResponse& response
) {
    const auto request = to_request(record);

    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds{5}
    );

    return stub_.FinishJob(
        &context,
        request,
        &response
    );
}

CompletionReporter::CompletionReporter(
    ICompletionTransport& transport,
    std::chrono::milliseconds reporting_deadline
)
    : transport_{transport},
      reporting_deadline_{reporting_deadline} {
}

CompletionReportResult CompletionReporter::report(
    const CompletionRecord& record,
    const std::function<bool()>& keep_retrying
) const {
    const auto deadline =
        std::chrono::steady_clock::now() +
        reporting_deadline_;

    std::chrono::milliseconds backoff{250};

    constexpr std::chrono::milliseconds
        max_backoff{2000};

    while (true) {
        rpc::v1::FinishJobResponse response;

        const grpc::Status status =
            transport_.finish_job(
                record,
                response
            );

        if (status.ok()) {
            return CompletionReportResult::accepted;
        }

        if (
            status.error_code() ==
            grpc::StatusCode::FAILED_PRECONDITION
        ) {
            return CompletionReportResult::rejected;
        }

        if (
            status.error_code() ==
            grpc::StatusCode::INVALID_ARGUMENT
        ) {
            return CompletionReportResult::invalid;
        }

        static_cast<void>(
            is_transient(status.error_code())
        );

        if (
            !keep_retrying() ||
            std::chrono::steady_clock::now() >= deadline
        ) {
            return CompletionReportResult::deadline_exceeded;
        }

        const auto remaining =
            deadline - std::chrono::steady_clock::now();

        std::this_thread::sleep_for(
            std::min(
                backoff,
                std::chrono::duration_cast<
                    std::chrono::milliseconds
                >(remaining)
            )
        );

        backoff = std::min(
            backoff * 2,
            max_backoff
        );
    }
}

}  // namespace radahn::worker_app
