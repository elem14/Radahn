#include "radahn/domain/job.hpp"

#include <chrono>
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
    : Job{
          std::move(id),
          std::move(name),
          priority,
          std::move(requirements),
          WorkloadSpec::sleep(
              std::chrono::seconds{1}
          ),
          JobState::queued,
          created_at,
          0,
          default_max_attempts
      } {
}

Job::Job(
    JobId id,
    std::string name,
    int priority,
    ResourceRequirements requirements,
    WorkloadSpec workload,
    TimePoint created_at
)
    : Job{
          std::move(id),
          std::move(name),
          priority,
          std::move(requirements),
          std::move(workload),
          JobState::queued,
          created_at,
          0,
          default_max_attempts
      } {
}

Job::Job(
    JobId id,
    std::string name,
    int priority,
    ResourceRequirements requirements,
    WorkloadSpec workload,
    std::size_t max_attempts,
    TimePoint created_at
)
    : Job{
          std::move(id),
          std::move(name),
          priority,
          std::move(requirements),
          std::move(workload),
          JobState::queued,
          created_at,
          0,
          max_attempts
      } {
}

Job Job::restore(
    JobId id,
    std::string name,
    int priority,
    ResourceRequirements requirements,
    WorkloadSpec workload,
    JobState state,
    TimePoint created_at,
    std::size_t attempt_count,
    std::size_t max_attempts
) {
    return Job{
        std::move(id),
        std::move(name),
        priority,
        std::move(requirements),
        std::move(workload),
        state,
        created_at,
        attempt_count,
        max_attempts
    };
}

Job::Job(
    JobId id,
    std::string name,
    int priority,
    ResourceRequirements requirements,
    WorkloadSpec workload,
    JobState state,
    TimePoint created_at,
    std::size_t attempt_count,
    std::size_t max_attempts
)
    : id_{std::move(id)},
      name_{std::move(name)},
      priority_{priority},
      requirements_{std::move(requirements)},
      workload_{std::move(workload)},
      state_{state},
      created_at_{created_at},
      attempt_count_{attempt_count},
      max_attempts_{max_attempts} {
    if (name_.empty()) {
        throw std::invalid_argument{
            "Job name cannot be empty"
        };
    }

    if (max_attempts_ == 0) {
        throw std::invalid_argument{
            "Job max attempts must be positive"
        };
    }

    if (attempt_count_ > max_attempts_) {
        throw std::invalid_argument{
            "Job attempt count cannot exceed max attempts"
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

const WorkloadSpec&
Job::workload() const noexcept {
    return workload_;
}

JobState Job::state() const noexcept {
    return state_;
}

Job::TimePoint Job::created_at() const noexcept {
    return created_at_;
}

std::size_t
Job::attempt_count() const noexcept {
    return attempt_count_;
}

std::size_t
Job::max_attempts() const noexcept {
    return max_attempts_;
}

bool Job::can_attempt() const noexcept {
    return
        attempt_count_ <
        max_attempts_;
}

void Job::record_attempt() {
    if (state_ != JobState::queued) {
        throw std::logic_error{
            "Only a queued job can begin an attempt"
        };
    }

    if (!can_attempt()) {
        throw std::logic_error{
            "Job has exhausted its retry limit"
        };
    }

    ++attempt_count_;
}

void Job::transition_to(
    JobState next_state
) {
    JobStateMachine::validate_transition(
        state_,
        next_state
    );

    state_ = next_state;
}

}  // namespace radahn::domain