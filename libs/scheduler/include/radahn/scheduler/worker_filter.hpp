#pragma once

#include <span>
#include <vector>

#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"

namespace radahn::scheduler {

using WorkerCandidates =
    std::vector<const domain::WorkerSnapshot*>;

[[nodiscard]] WorkerCandidates find_eligible_workers(
    const domain::ResourceRequirements& requirements,
    std::span<const domain::WorkerSnapshot> workers
);

}  // namespace radahn::scheduler