#include "active_job_registry.hpp"

#include <algorithm>

namespace radahn::worker_app {

void ActiveJobRegistry::add(
    const std::string& job_id
) {
    const std::lock_guard lock{mutex_};

    job_ids_.push_back(job_id);
}

void ActiveJobRegistry::remove(
    const std::string& job_id
) {
    const std::lock_guard lock{mutex_};

    const auto iterator = std::find(
        job_ids_.begin(),
        job_ids_.end(),
        job_id
    );

    if (iterator != job_ids_.end()) {
        job_ids_.erase(iterator);
    }
}

std::vector<std::string>
ActiveJobRegistry::snapshot() const {
    const std::lock_guard lock{mutex_};

    return job_ids_;
}

std::size_t ActiveJobRegistry::size() const {
    const std::lock_guard lock{mutex_};

    return job_ids_.size();
}

}  // namespace radahn::worker_app
