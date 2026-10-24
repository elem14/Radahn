#include "active_job_registry.hpp"

#include <algorithm>

namespace radahn::worker_app {

void ActiveJobRegistry::add(
    const ActiveAttempt& attempt
) {
    const std::lock_guard lock{mutex_};

    const auto iterator = std::find(
        attempts_.begin(),
        attempts_.end(),
        attempt
    );

    // Reject an exact duplicate add rather than tracking it twice.
    if (iterator != attempts_.end()) {
        return;
    }

    attempts_.push_back(attempt);
}

void ActiveJobRegistry::remove(
    const ActiveAttempt& attempt
) {
    const std::lock_guard lock{mutex_};

    const auto iterator = std::find(
        attempts_.begin(),
        attempts_.end(),
        attempt
    );

    if (iterator != attempts_.end()) {
        attempts_.erase(iterator);
    }
}

std::vector<ActiveAttempt>
ActiveJobRegistry::snapshot() const {
    const std::lock_guard lock{mutex_};

    return attempts_;
}

std::size_t ActiveJobRegistry::size() const {
    const std::lock_guard lock{mutex_};

    return attempts_.size();
}

}  // namespace radahn::worker_app
