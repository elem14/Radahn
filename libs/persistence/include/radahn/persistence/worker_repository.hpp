#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/worker_record.hpp"

namespace radahn::persistence {

class IWorkerRepository {
public:
    virtual ~IWorkerRepository() = default;

    /*
     * Insert a newly registered worker.
     *
     * Implementations must reject duplicate worker IDs.
     */
    virtual void insert(
        domain::WorkerRecord worker
    ) = 0;

    /*
     * Replace the current record for an existing worker.
     *
     * This is how future persistent implementations will
     * save updated resources, state, and running-job counts.
     */
    virtual void update(
        domain::WorkerRecord worker
    ) = 0;

    [[nodiscard]]
    virtual std::optional<domain::WorkerRecord>
    get(
        const domain::WorkerId& worker_id
    ) const = 0;

    [[nodiscard]]
    virtual std::vector<domain::WorkerRecord>
    list() const = 0;

    [[nodiscard]]
    virtual bool contains(
        const domain::WorkerId& worker_id
    ) const = 0;

    virtual void erase(
        const domain::WorkerId& worker_id
    ) = 0;

    [[nodiscard]]
    virtual std::size_t size() const noexcept = 0;
};

}  // namespace radahn::persistence