#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/persistence/job_record.hpp"

namespace radahn::persistence {

class IJobRepository {
public:
    virtual ~IJobRepository() = default;

    /*
     * Insert a completely new job.
     *
     * Implementations must reject duplicate job IDs.
     */
    virtual void insert(
        JobRecord record
    ) = 0;

    /*
     * Replace the stored version of an existing job.
     *
     * Implementations must reject unknown job IDs.
     */
    virtual void update(
        JobRecord record
    ) = 0;

    /*
     * Retrieve one job by ID.
     *
     * Returns std::nullopt when the job does not exist.
     */
    [[nodiscard]]
    virtual std::optional<JobRecord> get(
        const domain::JobId& job_id
    ) const = 0;

    /*
     * Return every stored job.
     *
     * Repository order is not scheduling order.
     * Scheduling policy remains outside the repository.
     */
    [[nodiscard]]
    virtual std::vector<JobRecord>
    list() const = 0;

    [[nodiscard]]
    virtual bool contains(
        const domain::JobId& job_id
    ) const = 0;

    /*
     * Delete a stored job.
     *
     * Implementations must reject unknown job IDs.
     */
    virtual void erase(
        const domain::JobId& job_id
    ) = 0;

    [[nodiscard]]
    virtual std::size_t size() const noexcept = 0;
};

}  // namespace radahn::persistence