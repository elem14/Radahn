#include "radahn/domain/worker_eligibility.hpp"

#include <utility>

namespace radahn::domain {

void EligibilityResult::add_failure(
    EligibilityFailure failure
) {
    failures_.push_back(failure);
}

void EligibilityResult::add_missing_tag(
    std::string tag
) {
    missing_tags_.push_back(std::move(tag));
}

bool EligibilityResult::eligible() const noexcept {
    return failures_.empty();
}

const std::vector<EligibilityFailure>&
EligibilityResult::failures() const noexcept {
    return failures_;
}

const std::vector<std::string>&
EligibilityResult::missing_tags() const noexcept {
    return missing_tags_;
}

EligibilityResult evaluate_worker(
    const ResourceRequirements& requirements,
    const WorkerSnapshot& worker
) {
    EligibilityResult result;

    switch (worker.state()) {
        case WorkerState::online:
            break;

        case WorkerState::draining:
            result.add_failure(
                EligibilityFailure::worker_draining
            );
            break;

        case WorkerState::offline:
        case WorkerState::disabled:
            result.add_failure(
                EligibilityFailure::worker_not_online
            );
            break;
    }

    if (!worker.has_execution_slot()) {
        result.add_failure(
            EligibilityFailure::no_execution_slot
        );
    }

    const auto& resources = worker.resources();

    if (resources.available_cpu_cores() <
        requirements.cpu_cores()) {
        result.add_failure(
            EligibilityFailure::insufficient_cpu
        );
    }

    if (resources.available_memory_bytes() <
        requirements.memory_bytes()) {
        result.add_failure(
            EligibilityFailure::insufficient_memory
        );
    }

    if (resources.available_disk_bytes() <
        requirements.disk_bytes()) {
        result.add_failure(
            EligibilityFailure::insufficient_disk
        );
    }

    if (requirements.requires_gpu() &&
        !resources.gpu_available()) {
        result.add_failure(
            EligibilityFailure::gpu_unavailable
        );
    }

    bool missing_any_tag = false;

    for (const auto& required_tag :
         requirements.required_tags()) {
        if (!worker.has_tag(required_tag)) {
            result.add_missing_tag(required_tag);
            missing_any_tag = true;
        }
    }

    if (missing_any_tag) {
        result.add_failure(
            EligibilityFailure::missing_required_tag
        );
    }

    return result;
}

std::string_view to_string(
    EligibilityFailure failure
) noexcept {
    switch (failure) {
        case EligibilityFailure::worker_not_online:
            return "WORKER_NOT_ONLINE";

        case EligibilityFailure::worker_draining:
            return "WORKER_DRAINING";

        case EligibilityFailure::no_execution_slot:
            return "NO_EXECUTION_SLOT";

        case EligibilityFailure::insufficient_cpu:
            return "INSUFFICIENT_CPU";

        case EligibilityFailure::insufficient_memory:
            return "INSUFFICIENT_MEMORY";

        case EligibilityFailure::insufficient_disk:
            return "INSUFFICIENT_DISK";

        case EligibilityFailure::gpu_unavailable:
            return "GPU_UNAVAILABLE";

        case EligibilityFailure::missing_required_tag:
            return "MISSING_REQUIRED_TAG";
    }

    return "UNKNOWN";
}

}  // namespace radahn::domain