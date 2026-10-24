#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

#include <grpcpp/grpcpp.h>

#include "radahn/domain/execution_result.hpp"
#include "worker_service.grpc.pb.h"

namespace radahn::worker_app {

/*
 * One immutable completion record. Every retry sends exactly this
 * same record, so a lost response and a genuine retry are
 * indistinguishable to the coordinator - and its finish_job_attempt
 * is idempotent for an exact repeat.
 */
struct CompletionRecord {
    std::string worker_id;
    std::string job_id;
    std::uint64_t attempt_number;
    domain::ExecutionResult result;
};

/*
 * The attempt's local phase while completion reporting is underway.
 *
 * executing: the job is still running or has not yet finished.
 * reporting: execution is done and a completion record is pending
 *   acknowledgement; the attempt stays registered for heartbeat
 *   renewal, but the worker must not execute it again.
 *
 * Deliberately a plain two-state flag so a later milestone can
 * extend it (e.g. folding it into a richer per-attempt control
 * struct) without having to discard this type.
 */
enum class CompletionPhase {
    executing,
    reporting
};

/*
 * Tracks one attempt's completion record together with its local
 * phase, for the duration that a finished attempt's result is being
 * reported to the coordinator. A later milestone is expected to
 * fold this into a richer per-attempt control structure (adding
 * local lease-deadline tracking alongside the phase) rather than
 * replace it outright.
 */
struct PendingCompletion {
    CompletionRecord record;
    CompletionPhase phase = CompletionPhase::executing;
};

/*
 * Outcome of attempting to report one completion record, bounded by
 * the reporter's configured deadline.
 */
enum class CompletionReportResult {
    // The coordinator accepted the completion (either freshly, or
    // as an exact repeat of a completion it already held).
    accepted,

    // The coordinator rejected this attempt as no longer current
    // (FAILED_PRECONDITION) - the worker has lost lease authority
    // over this attempt and must not retry it further.
    rejected,

    // The coordinator reported the request itself was malformed.
    invalid,

    // The configured reporting deadline elapsed before either of
    // the above was observed.
    deadline_exceeded
};

/*
 * Narrow seam over the FinishJob RPC so tests can simulate response
 * loss deterministically without a real network round trip.
 */
class ICompletionTransport {
public:
    virtual ~ICompletionTransport() = default;

    [[nodiscard]]
    virtual grpc::Status finish_job(
        const CompletionRecord& record,
        rpc::v1::FinishJobResponse& response
    ) = 0;
};

class GrpcCompletionTransport final : public ICompletionTransport {
public:
    explicit GrpcCompletionTransport(
        rpc::v1::WorkerService::Stub& stub
    );

    [[nodiscard]]
    grpc::Status finish_job(
        const CompletionRecord& record,
        rpc::v1::FinishJobResponse& response
    ) override;

private:
    rpc::v1::WorkerService::Stub& stub_;
};

/*
 * Retries a single completion record with bounded exponential
 * backoff (250ms -> 500ms -> 1s -> 2s -> 2s -> ...) until the
 * coordinator acknowledges or definitively rejects it, or the
 * reporting deadline expires.
 */
class CompletionReporter {
public:
    explicit CompletionReporter(
        ICompletionTransport& transport,
        std::chrono::milliseconds reporting_deadline =
            std::chrono::seconds{60}
    );

    /*
     * `keep_retrying` is polled between attempts (e.g. to react to
     * a shutdown request) - returning false stops retrying early
     * and reports deadline_exceeded.
     */
    [[nodiscard]]
    CompletionReportResult report(
        const CompletionRecord& record,
        const std::function<bool()>& keep_retrying =
            [] { return true; }
    ) const;

private:
    ICompletionTransport& transport_;
    std::chrono::milliseconds reporting_deadline_;
};

}  // namespace radahn::worker_app
