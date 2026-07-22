// select jobs in order of highest priority
// for equal priority -> older job first -> smallest job ID


#include "radahn/scheduler/job_queue.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "radahn/domain/job_state.hpp"

namespace radahn::scheduler {

namespace {

[[nodiscard]] bool should_run_before(
    const domain::Job& left,
    const domain::Job& right
) {
    if (left.priority() != right.priority()) {
        return left.priority() > right.priority();
    }

    if (left.created_at() != right.created_at()) {
        return left.created_at() < right.created_at();
    }

    return left.id().value() < right.id().value();
}

}  // namespace

void InMemoryJobQueue::enqueue(domain::Job job) {
    if (job.state() != domain::JobState::queued) {
        throw std::invalid_argument{
            "Only queued jobs may be added to the job queue"
        };
    }

    if (contains(job.id())) {
        throw std::invalid_argument{
            "A job with this ID is already queued"
        };
    }

    jobs_.push_back(std::move(job));
}

std::optional<domain::Job>
InMemoryJobQueue::pop_next() {
    if (jobs_.empty()) {
        return std::nullopt;
    }

    auto best_job = jobs_.begin();

    for (auto iterator = jobs_.begin() + 1;
         iterator != jobs_.end();
         ++iterator) {
        if (should_run_before(*iterator, *best_job)) {
            best_job = iterator;
        }
    }

    domain::Job selected_job = std::move(*best_job);

    jobs_.erase(best_job);

    return selected_job;
}

std::optional<domain::Job>
InMemoryJobQueue::take(
    const domain::JobId& job_id
) {
    const auto iterator = std::find_if(
        jobs_.begin(),
        jobs_.end(),
        [&job_id](const domain::Job& job) {
            return job.id() == job_id;
        }
    );

    if (iterator == jobs_.end()) {
        return std::nullopt;
    }

    domain::Job selected_job =
        std::move(*iterator);

    jobs_.erase(iterator);

    return selected_job;
}

OrderedJobs InMemoryJobQueue::ordered_jobs() const {
    OrderedJobs ordered;

    ordered.reserve(jobs_.size());

    for (const auto& job : jobs_) {
        ordered.push_back(&job);
    }

    std::sort(
        ordered.begin(),
        ordered.end(),
        [](const auto* left, const auto* right) {
            return should_run_before(
                *left,
                *right
            );
        }
    );

    return ordered;
}

bool InMemoryJobQueue::contains(
    const domain::JobId& job_id
) const {
    return std::any_of(
        jobs_.begin(),
        jobs_.end(),
        [&job_id](const domain::Job& job) {
            return job.id() == job_id;
        }
    );
}

std::size_t InMemoryJobQueue::size() const noexcept {
    return jobs_.size();
}

bool InMemoryJobQueue::empty() const noexcept {
    return jobs_.empty();
}

}  // namespace radahn::scheduler