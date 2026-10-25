#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "radahn/domain/execution_result.hpp"

namespace radahn::worker_app {

/*
 * Generates a fresh, safely-named directory under `root` for one
 * execution's captured output. The name is built only from this
 * process's pid, a monotonic timestamp, and an atomic sequence
 * counter - never from the job ID - so an attacker- or
 * operator-controlled job ID can never be concatenated into a
 * filesystem path. The sequence counter alone already guarantees two
 * calls from the same process never collide, so two attempts of the
 * same job (or of two different jobs) always get distinct
 * directories.
 */
[[nodiscard]]
std::filesystem::path next_execution_output_directory(
    const std::filesystem::path& root
);

/*
 * One execution's local bookkeeping: what was run, where its captured
 * output lives on local disk, and how it finished. Retained for the
 * lifetime of the worker process so the attempt's completion-handling
 * and log-upload paths can still find the right directory after
 * execution returns control to the caller.
 *
 * MVP retention policy: entries - and the underlying log files on
 * disk - are never deleted by the worker itself. They remain until an
 * operator removes them manually. A long-running worker executing
 * many commands with large stdout/stderr can therefore accumulate
 * unbounded local disk usage over time; the 1 MiB per-stream limit
 * enforced on upload (see persistence::JobLogRepository) bounds only
 * what the coordinator stores, not what a worker keeps locally.
 */
struct ExecutionLogRecord {
    std::string job_id;
    std::uint64_t attempt_number;
    std::string worker_id;

    std::filesystem::path output_directory;

    // Absent for workloads that produce no captured output (e.g. the
    // internal sleep workload has no subprocess and therefore no
    // stdout.log/stderr.log at all).
    std::optional<std::filesystem::path> stdout_path;
    std::optional<std::filesystem::path> stderr_path;

    domain::ExecutionResult execution_result;
};

/*
 * Thread-safe collection of ExecutionLogRecord entries, keyed by
 * (job_id, attempt_number). Recorded once per finished execution by
 * the execution thread, and later read back by the completion/upload
 * path to locate the captured output for that same attempt.
 */
class ExecutionLogRegistry final {
public:
    void record(
        ExecutionLogRecord entry
    );

    [[nodiscard]]
    std::optional<ExecutionLogRecord> find(
        const std::string& job_id,
        std::uint64_t attempt_number
    ) const;

    [[nodiscard]]
    std::size_t size() const;

private:
    mutable std::mutex mutex_;
    std::vector<ExecutionLogRecord> records_;
};

}  // namespace radahn::worker_app
