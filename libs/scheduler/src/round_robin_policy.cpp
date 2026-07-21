#include "radahn/scheduler/round_robin_policy.hpp"

#include <string>

#include "radahn/scheduler/worker_filter.hpp"

namespace radahn::scheduler {

std::string_view RoundRobinPolicy::name() const noexcept {
    return "round-robin";
}

std::optional<SchedulingDecision>
RoundRobinPolicy::select_worker(
    const domain::ResourceRequirements& requirements,
    std::span<const domain::WorkerSnapshot> workers
) {
    const auto candidates = find_eligible_workers(
        requirements,
        workers
    );

    if (candidates.empty()) {
        next_index_ = 0;
        return std::nullopt;
    }

    const std::size_t selected_index =
        next_index_ % candidates.size();

    const auto* selected_worker =
        candidates.at(selected_index);

    next_index_ =
        (selected_index + 1) % candidates.size();

    return SchedulingDecision{
        selected_worker->id(),
        std::string{name()},
        candidates.size(),
        std::nullopt
    };
}

}  // namespace radahn::scheduler