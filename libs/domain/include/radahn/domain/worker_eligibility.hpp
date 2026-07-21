#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"

namespace radahn::domain {

enum class EligibilityFailure {
    worker_not_online,
    worker_draining,
    no_execution_slot,
    insufficient_cpu,
    insufficient_memory,
    insufficient_disk,
    gpu_unavailable,
    missing_required_tag
};

class EligibilityResult {
public:
    void add_failure(EligibilityFailure failure);
    void add_missing_tag(std::string tag);

    [[nodiscard]] bool eligible() const noexcept;

    [[nodiscard]] const std::vector<EligibilityFailure>&
    failures() const noexcept;

    [[nodiscard]] const std::vector<std::string>&
    missing_tags() const noexcept;

private:
    std::vector<EligibilityFailure> failures_;
    std::vector<std::string> missing_tags_;
};

[[nodiscard]] EligibilityResult evaluate_worker(
    const ResourceRequirements& requirements,
    const WorkerSnapshot& worker
);

[[nodiscard]] std::string_view to_string(
    EligibilityFailure failure
) noexcept;

}  // namespace radahn::domain