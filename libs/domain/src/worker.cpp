#include "radahn/domain/worker.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace radahn::domain {

WorkerSnapshot::WorkerSnapshot(
    WorkerId id,
    WorkerState state,
    WorkerResources resources,
    std::size_t running_jobs,
    std::size_t max_concurrent_jobs,
    std::vector<std::string> tags
)
    : id_{std::move(id)},
      state_{state},
      resources_{std::move(resources)},
      running_jobs_{running_jobs},
      max_concurrent_jobs_{max_concurrent_jobs},
      tags_{std::move(tags)} {
    if (max_concurrent_jobs_ == 0) {
        throw std::invalid_argument{
            "Worker must support at least one concurrent job"
        };
    }

    if (running_jobs_ > max_concurrent_jobs_) {
        throw std::invalid_argument{
            "Running jobs cannot exceed the worker concurrency limit"
        };
    }

    for (const auto& tag : tags_) {
        if (tag.empty()) {
            throw std::invalid_argument{
                "Worker tags cannot be empty"
            };
        }
    }
}

const WorkerId& WorkerSnapshot::id() const noexcept {
    return id_;
}

WorkerState WorkerSnapshot::state() const noexcept {
    return state_;
}

const WorkerResources& WorkerSnapshot::resources() const noexcept {
    return resources_;
}

std::size_t WorkerSnapshot::running_jobs() const noexcept {
    return running_jobs_;
}

std::size_t WorkerSnapshot::max_concurrent_jobs() const noexcept {
    return max_concurrent_jobs_;
}

const std::vector<std::string>&
WorkerSnapshot::tags() const noexcept {
    return tags_;
}

bool WorkerSnapshot::has_execution_slot() const noexcept {
    return running_jobs_ < max_concurrent_jobs_;
}

bool WorkerSnapshot::has_tag(const std::string& tag) const {
    return std::find(
        tags_.begin(),
        tags_.end(),
        tag
    ) != tags_.end();
}

}  // namespace radahn::domain