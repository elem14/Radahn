#include "radahn/persistence/in_memory_job_repository.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace radahn::persistence {

void InMemoryJobRepository::insert(
    JobRecord record
) {
    if (contains(record.job.id())) {
        throw std::invalid_argument{
            "A job with this ID already exists"
        };
    }

    records_.push_back(
        std::move(record)
    );
}

void InMemoryJobRepository::update(
    JobRecord record
) {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&record](
                const JobRecord& stored_record
            ) {
                return
                    stored_record.job.id() ==
                    record.job.id();
            }
        );

    if (iterator == records_.end()) {
        throw std::invalid_argument{
            "Cannot update an unknown job"
        };
    }

    *iterator = std::move(record);
}

std::optional<JobRecord>
InMemoryJobRepository::get(
    const domain::JobId& job_id
) const {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&job_id](
                const JobRecord& record
            ) {
                return
                    record.job.id() ==
                    job_id;
            }
        );

    if (iterator == records_.end()) {
        return std::nullopt;
    }

    return *iterator;
}

std::vector<JobRecord>
InMemoryJobRepository::list() const {
    return records_;
}

bool InMemoryJobRepository::contains(
    const domain::JobId& job_id
) const {
    return std::any_of(
        records_.begin(),
        records_.end(),
        [&job_id](
            const JobRecord& record
        ) {
            return
                record.job.id() ==
                job_id;
        }
    );
}

void InMemoryJobRepository::erase(
    const domain::JobId& job_id
) {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&job_id](
                const JobRecord& record
            ) {
                return
                    record.job.id() ==
                    job_id;
            }
        );

    if (iterator == records_.end()) {
        throw std::invalid_argument{
            "Cannot erase an unknown job"
        };
    }

    records_.erase(iterator);
}

std::size_t
InMemoryJobRepository::size() const noexcept {
    return records_.size();
}

}  // namespace radahn::persistence