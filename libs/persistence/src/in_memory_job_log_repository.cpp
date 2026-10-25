#include "radahn/persistence/in_memory_job_log_repository.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace radahn::persistence {

namespace {

[[nodiscard]]
bool matches_key(
    const JobLogRecord& record,
    const domain::JobId& job_id,
    std::uint64_t attempt_number,
    JobLogStream stream
) {
    return
        record.job_id == job_id &&
        record.attempt_number == attempt_number &&
        record.stream == stream;
}

}  // namespace

void InMemoryJobLogRepository::insert(
    JobLogRecord record
) {
    if (
        contains(
            record.job_id,
            record.attempt_number,
            record.stream
        )
    ) {
        throw std::invalid_argument{
            "A log already exists for this "
            "(job_id, attempt_number, stream) key"
        };
    }

    records_.push_back(
        std::move(record)
    );
}

std::optional<JobLogRecord>
InMemoryJobLogRepository::get(
    const domain::JobId& job_id,
    std::uint64_t attempt_number,
    JobLogStream stream
) const {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&](const JobLogRecord& record) {
                return matches_key(
                    record,
                    job_id,
                    attempt_number,
                    stream
                );
            }
        );

    if (iterator == records_.end()) {
        return std::nullopt;
    }

    return *iterator;
}

bool InMemoryJobLogRepository::contains(
    const domain::JobId& job_id,
    std::uint64_t attempt_number,
    JobLogStream stream
) const {
    return std::any_of(
        records_.begin(),
        records_.end(),
        [&](const JobLogRecord& record) {
            return matches_key(
                record,
                job_id,
                attempt_number,
                stream
            );
        }
    );
}

void InMemoryJobLogRepository::erase(
    const domain::JobId& job_id,
    std::uint64_t attempt_number,
    JobLogStream stream
) {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&](const JobLogRecord& record) {
                return matches_key(
                    record,
                    job_id,
                    attempt_number,
                    stream
                );
            }
        );

    if (iterator == records_.end()) {
        throw std::invalid_argument{
            "Cannot erase an unknown job log"
        };
    }

    records_.erase(iterator);
}

std::size_t
InMemoryJobLogRepository::size() const {
    return records_.size();
}

}  // namespace radahn::persistence
