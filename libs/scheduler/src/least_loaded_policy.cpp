// normalized load = running jobs / max concurrent jobs
// policy will choose the worker with the least load
// if two workers have equal load, policy prefers
// 1. greater cpu
// 2. greater memory
// 3. lexicographically smaller worker ID


#include "radahn/scheduler/least_loaded_policy.hpp"

#include <algorithm>
#include <string>

#include "radahn/scheduler/worker_filter.hpp"

namespace radahn::scheduler {

namespace {

[[nodiscard]] double normalized_load(
    const domain::WorkerSnapshot& worker
) noexcept {
    return static_cast<double>(worker.running_jobs()) /
           static_cast<double>(
               worker.max_concurrent_jobs()
           );
}

[[nodiscard]] bool is_better_candidate(
    const domain::WorkerSnapshot& left,
    const domain::WorkerSnapshot& right
) {
    const double left_load = normalized_load(left);
    const double right_load = normalized_load(right);

    if (left_load != right_load) {
        return left_load < right_load;
    }

    const auto& left_resources = left.resources();
    const auto& right_resources = right.resources();

    if (left_resources.available_cpu_cores() !=
        right_resources.available_cpu_cores()) {
        return left_resources.available_cpu_cores() >
               right_resources.available_cpu_cores();
    }

    if (left_resources.available_memory_bytes() !=
        right_resources.available_memory_bytes()) {
        return left_resources.available_memory_bytes() >
               right_resources.available_memory_bytes();
    }

    return left.id().value() < right.id().value();
}

}  // namespace

std::string_view LeastLoadedPolicy::name() const noexcept {
    return "least-loaded";
}

std::optional<SchedulingDecision>
LeastLoadedPolicy::select_worker(
    const domain::ResourceRequirements& requirements,
    std::span<const domain::WorkerSnapshot> workers
) {
    const auto candidates = find_eligible_workers(
        requirements,
        workers
    );

    if (candidates.empty()) {
        return std::nullopt;
    }

    const auto selected_iterator = std::min_element(
        candidates.begin(),
        candidates.end(),
        [](const auto* left, const auto* right) {
            return is_better_candidate(
                *left,
                *right
            );
        }
    );

    const auto* selected_worker =
        *selected_iterator;

    return SchedulingDecision{
        selected_worker->id(),
        std::string{name()},
        candidates.size(),
        normalized_load(*selected_worker)
    };
}

}  // namespace radahn::scheduler