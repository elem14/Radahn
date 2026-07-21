#pragma once

#include "radahn/scheduler/scheduling_policy.hpp"

namespace radahn::scheduler {

class LeastLoadedPolicy final : public ISchedulingPolicy {
public:
    [[nodiscard]] std::string_view
    name() const noexcept override;

    [[nodiscard]] std::optional<SchedulingDecision>
    select_worker(
        const domain::ResourceRequirements& requirements,
        std::span<const domain::WorkerSnapshot> workers
    ) override;
};

}  // namespace radahn::scheduler