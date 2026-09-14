#pragma once

#include "radahn/scheduler/scheduling_policy.hpp"

namespace radahn::scheduler {

/*
 * Places a job on the eligible worker that would be left with the
 * least normalized capacity afterward (CPU/memory/disk averaged
 * as fractions of each worker's total capacity). This packs jobs
 * tightly onto already-loaded workers instead of spreading them
 * out, the opposite tie-break direction from LeastLoadedPolicy.
 */
class BestFitPolicy final : public ISchedulingPolicy {
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
