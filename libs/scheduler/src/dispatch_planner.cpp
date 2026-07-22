#include "radahn/scheduler/dispatch_planner.hpp"

namespace radahn::scheduler {

DispatchPlanner::DispatchPlanner(
    ISchedulingPolicy& policy
) noexcept
    : policy_{policy} {
}

std::optional<DispatchDecision>
DispatchPlanner::plan(
    const InMemoryJobQueue& queue,
    std::span<const domain::WorkerSnapshot> workers
) {
    const auto ordered_jobs = queue.ordered_jobs();

    for (const auto* job : ordered_jobs) {
        const auto scheduling_decision =
            policy_.select_worker(
                job->requirements(),
                workers
            );

        if (!scheduling_decision.has_value()) {
            continue;
        }

        return DispatchDecision{
            job->id(),
            scheduling_decision->worker_id,
            scheduling_decision->policy_name,
            scheduling_decision->eligible_worker_count,
            scheduling_decision->score
        };
    }

    return std::nullopt;
}

}  // namespace radahn::scheduler