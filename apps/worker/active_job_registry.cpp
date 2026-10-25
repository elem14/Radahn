#include "active_job_registry.hpp"

#include <algorithm>

namespace radahn::worker_app {

AttemptControlPtr ActiveJobRegistry::add(
    std::string job_id,
    std::uint64_t attempt_number
) {
    auto control =
        std::make_shared<AttemptControl>(
            std::move(job_id),
            attempt_number
        );

    const std::lock_guard lock{mutex_};

    attempts_.push_back(control);

    return control;
}

void ActiveJobRegistry::remove(
    const AttemptControlPtr& control
) {
    const std::lock_guard lock{mutex_};

    const auto iterator = std::find(
        attempts_.begin(),
        attempts_.end(),
        control
    );

    if (iterator != attempts_.end()) {
        attempts_.erase(iterator);
    }
}

std::vector<AttemptControlPtr>
ActiveJobRegistry::snapshot() const {
    const std::lock_guard lock{mutex_};

    return attempts_;
}

std::size_t ActiveJobRegistry::size() const {
    const std::lock_guard lock{mutex_};

    return attempts_.size();
}

}  // namespace radahn::worker_app
