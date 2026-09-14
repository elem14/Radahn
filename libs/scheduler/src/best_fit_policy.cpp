#include "radahn/scheduler/best_fit_policy.hpp"

#include <algorithm>
#include <cmath>

#include "radahn/scheduler/worker_filter.hpp"

namespace radahn::scheduler {

namespace {

/*
 * Fraction of each resource dimension that would remain free
 * after placing this job on this worker, averaged across CPU,
 * memory, and disk. Lower means a tighter fit.
 */
[[nodiscard]] double remaining_capacity_score(
    const domain::ResourceRequirements& requirements,
    const domain::WorkerSnapshot& worker
) noexcept {
    const auto& resources = worker.resources();

    const double remaining_cpu_ratio =
        (resources.available_cpu_cores() -
         requirements.cpu_cores()) /
        resources.total_cpu_cores();

    const double remaining_memory_ratio =
        static_cast<double>(
            resources.available_memory_bytes() -
            requirements.memory_bytes()
        ) /
        static_cast<double>(
            resources.total_memory_bytes()
        );

    const double remaining_disk_ratio =
        static_cast<double>(
            resources.available_disk_bytes() -
            requirements.disk_bytes()
        ) /
        static_cast<double>(
            resources.total_disk_bytes()
        );

    return (
        remaining_cpu_ratio +
        remaining_memory_ratio +
        remaining_disk_ratio
    ) / 3.0;
}

[[nodiscard]] bool is_tighter_fit(
    const domain::ResourceRequirements& requirements,
    const domain::WorkerSnapshot& left,
    const domain::WorkerSnapshot& right
) {
    const double left_score =
        remaining_capacity_score(requirements, left);

    const double right_score =
        remaining_capacity_score(requirements, right);

    constexpr double tolerance = 0.000001;

    if (
        std::abs(left_score - right_score) >
        tolerance
    ) {
        return left_score < right_score;
    }

    return left.id().value() < right.id().value();
}

}  // namespace

std::string_view BestFitPolicy::name() const noexcept {
    return "best-fit";
}

std::optional<SchedulingDecision>
BestFitPolicy::select_worker(
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
        [&requirements](
            const auto* left,
            const auto* right
        ) {
            return is_tighter_fit(
                requirements,
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
        remaining_capacity_score(
            requirements,
            *selected_worker
        )
    };
}

}  // namespace radahn::scheduler
