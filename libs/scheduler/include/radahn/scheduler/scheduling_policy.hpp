#pragma once

#include <optional>
#include <span>
#include <string_view>

#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/scheduler/scheduling_decision.hpp"

namespace radahn::scheduler {

class ISchedulingPolicy {
public:
    virtual ~ISchedulingPolicy() = default;

    [[nodiscard]] virtual std::string_view
    name() const noexcept = 0;

    [[nodiscard]] virtual std::optional<SchedulingDecision>
    select_worker(
        const domain::ResourceRequirements& requirements,
        std::span<const domain::WorkerSnapshot> workers
    ) = 0;
};

}  // namespace radahn::scheduler