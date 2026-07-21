#include "radahn/domain/job.hpp"

#include <stdexcept>
#include <utility>

namespace radahn::domain {

Job::Job(
    JobId id,
    std::string name,
    int priority,
    ResourceRequirements requirements,
    TimePoint created_at
)
    : id_{std::move(id)},
      name_{std::move(name)},
      priority_{priority},
      requirements_{std::move(requirements)},
      created_at_{created_at} {
    if (name_.empty()) {
        throw std::invalid_argument{
            "Job name cannot be empty"
        };
    }
}

const JobId& Job::id() const noexcept {
    return id_;
}

const std::string& Job::name() const noexcept {
    return name_;
}

int Job::priority() const noexcept {
    return priority_;
}

const ResourceRequirements&
Job::requirements() const noexcept {
    return requirements_;
}

JobState Job::state() const noexcept {
    return state_;
}

Job::TimePoint Job::created_at() const noexcept {
    return created_at_;
}

void Job::transition_to(JobState next_state) {
    JobStateMachine::validate_transition(
        state_,
        next_state
    );

    state_ = next_state;
}

}  // namespace radahn::domain