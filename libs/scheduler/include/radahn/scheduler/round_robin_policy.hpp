#pragma once

#include <cstddef>

#include "radahn/scheduler/scheduling_policy.hpp"

namespace radahn::scheduler {

class RoundRobinPolicy final : public ISchedulingPolicy {
public:
    [[nodiscard]] std::string_view
    name() const noexcept override;

    [[nodiscard]] std::optional<SchedulingDecision>
    select_worker(
        const domain::ResourceRequirements& requirements,
        std::span<const domain::WorkerSnapshot> workers
    ) override;

private:
    std::size_t next_index_{0};
};

}  // namespace radahn::scheduler