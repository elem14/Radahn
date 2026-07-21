#pragma once

#include <chrono>
#include <string>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"

namespace radahn::domain {

class Job {
public:
    using Clock = std::chrono::system_clock;
    using TimePoint = Clock::time_point;

    Job(
        JobId id,
        std::string name,
        int priority,
        ResourceRequirements requirements,
        TimePoint created_at = Clock::now()
    );

    [[nodiscard]] const JobId& id() const noexcept;

    [[nodiscard]] const std::string&
    name() const noexcept;

    [[nodiscard]] int priority() const noexcept;

    [[nodiscard]] const ResourceRequirements&
    requirements() const noexcept;

    [[nodiscard]] JobState state() const noexcept;

    [[nodiscard]] TimePoint created_at() const noexcept;

    void transition_to(JobState next_state);

private:
    JobId id_;
    std::string name_;
    int priority_;
    ResourceRequirements requirements_;
    JobState state_{JobState::queued};
    TimePoint created_at_;
};

}  // namespace radahn::domain