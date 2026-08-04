#pragma once

#include <chrono>
#include <cstddef>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/worker_record.hpp"

namespace radahn::persistence {

using WorkerHeartbeatClock =
    std::chrono::system_clock;

using WorkerHeartbeatTimePoint =
    WorkerHeartbeatClock::time_point;

class IWorkerRepository {
public:
    virtual ~IWorkerRepository() = default;

    virtual void insert(
        domain::WorkerRecord worker
    ) = 0;

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

    virtual void record_heartbeat(
        const domain::WorkerId& worker_id,
        WorkerHeartbeatTimePoint heartbeat_time
    ) = 0;

    [[nodiscard]]
    virtual std::optional<WorkerHeartbeatTimePoint>
    last_heartbeat(
        const domain::WorkerId& worker_id
    ) const = 0;

    virtual void erase(
        const domain::WorkerId& worker_id
    ) = 0;

    [[nodiscard]]
    virtual std::size_t size() const = 0;
};

}  // namespace radahn::persistence