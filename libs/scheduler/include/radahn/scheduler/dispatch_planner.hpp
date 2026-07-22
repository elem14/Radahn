#pragma once

#include <optional>
#include <span>

#include "radahn/domain/worker.hpp"
#include "radahn/scheduler/dispatch_decision.hpp"
#include "radahn/scheduler/job_queue.hpp"
#include "radahn/scheduler/scheduling_policy.hpp"

namespace radahn::scheduler {

class DispatchPlanner {
public:
    explicit DispatchPlanner(
        ISchedulingPolicy& policy
    ) noexcept;

    [[nodiscard]] std::optional<DispatchDecision>
    plan(
        const InMemoryJobQueue& queue,
        std::span<const domain::WorkerSnapshot> workers
    );

private:
    ISchedulingPolicy& policy_;
};

}  // namespace radahn::scheduler