#include "radahn/scheduler/worker_filter.hpp"

#include <algorithm>

#include "radahn/domain/worker_eligibility.hpp"

namespace radahn::scheduler {

WorkerCandidates find_eligible_workers(
    const domain::ResourceRequirements& requirements,
    std::span<const domain::WorkerSnapshot> workers
) {
    WorkerCandidates eligible_workers;

    eligible_workers.reserve(workers.size());

    for (const auto& worker : workers) {
        const auto result = domain::evaluate_worker(
            requirements,
            worker
        );

        if (result.eligible()) {
            eligible_workers.push_back(&worker);
        }
    }

    std::sort(
        eligible_workers.begin(),
        eligible_workers.end(),
        [](const auto* left, const auto* right) {
            return left->id().value() <
                   right->id().value();
        }
    );

    return eligible_workers;
}

}  // namespace radahn::scheduler