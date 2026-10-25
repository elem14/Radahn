#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "radahn/domain/id.hpp"

namespace radahn::persistence {

/*
 * Maximum number of bytes of one stream's content the coordinator
 * will ever store for one attempt. The worker is responsible for
 * capping what it uploads to this same limit and setting `truncated`
 * when the original local output exceeded it - the coordinator
 * additionally rejects any upload that arrives over this limit
 * regardless (see CoordinatorServiceImpl::UploadJobLog), so no single
 * misbehaving worker can make the coordinator's log storage grow
 * without bound.
 */
inline constexpr std::uint64_t max_job_log_bytes =
    1024ULL * 1024ULL;

enum class JobLogStream {
    standard_output,
    standard_error
};

/*
 * One uploaded stream's content for one job attempt.
 *
 * Keyed by (job_id, attempt_number, stream). Immutable once stored -
 * there is deliberately no `update`: a log record is only ever
 * inserted once for a given key (an identical repeated upload is
 * accepted as a no-op by the caller, and a conflicting one is
 * rejected before it ever reaches the repository).
 */
struct JobLogRecord {
    domain::JobId job_id;
    std::uint64_t attempt_number;
    JobLogStream stream;
    domain::WorkerId worker_id;

    // Raw bytes, up to max_job_log_bytes long. May contain embedded
    // NUL bytes - this is a byte buffer, not display text.
    std::string content;

    std::uint64_t original_byte_count;
    bool truncated;
};

class IJobLogRepository {
public:
    virtual ~IJobLogRepository() = default;

    /*
     * Insert a brand-new log record.
     *
     * Implementations must reject a duplicate
     * (job_id, attempt_number, stream) key.
     */
    virtual void insert(
        JobLogRecord record
    ) = 0;

    /*
     * Retrieve one stored log by its key.
     *
     * Returns std::nullopt when no log was ever stored for this key -
     * distinct from a stored, empty log (content.empty() but present).
     */
    [[nodiscard]]
    virtual std::optional<JobLogRecord> get(
        const domain::JobId& job_id,
        std::uint64_t attempt_number,
        JobLogStream stream
    ) const = 0;

    [[nodiscard]]
    virtual bool contains(
        const domain::JobId& job_id,
        std::uint64_t attempt_number,
        JobLogStream stream
    ) const = 0;

    /*
     * Delete a stored log.
     *
     * Implementations must reject an unknown key.
     */
    virtual void erase(
        const domain::JobId& job_id,
        std::uint64_t attempt_number,
        JobLogStream stream
    ) = 0;

    [[nodiscard]]
    virtual std::size_t size() const = 0;
};

}  // namespace radahn::persistence
