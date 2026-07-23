#include "radahn/coordinator/in_memory_coordinator.hpp"

#include <algorithm>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace radahn::coordinator {

InMemoryCoordinator::InMemoryCoordinator(
    scheduler::ISchedulingPolicy& policy
) noexcept
    : planner_{policy} {
}

void InMemoryCoordinator::submit_job(
    domain::Job job
) {
    if (job_exists(job.id())) {
        throw std::invalid_argument{
            "A job with this ID already exists"
        };
    }

    queue_.enqueue(std::move(job));
}

void InMemoryCoordinator::register_worker(
    domain::WorkerRecord worker
) {
    if (find_worker(worker.id()) != nullptr) {
        throw std::invalid_argument{
            "A worker with this ID is already registered"
        };
    }

    workers_.push_back(std::move(worker));
}

std::optional<scheduler::DispatchDecision>
InMemoryCoordinator::dispatch_once() {
    std::vector<domain::WorkerSnapshot> snapshots;

    snapshots.reserve(workers_.size());

    for (const auto& worker : workers_) {
        snapshots.push_back(worker.snapshot());
    }

    const auto decision = planner_.plan(
        queue_,
        std::span<const domain::WorkerSnapshot>{
            snapshots
        }
    );

    if (!decision.has_value()) {
        return std::nullopt;
    }

    auto* selected_worker = find_worker(
        decision->worker_id
    );

    if (selected_worker == nullptr) {
        throw std::logic_error{
            "Dispatch planner selected an unknown worker"
        };
    }

    auto selected_job = queue_.take(
        decision->job_id
    );

    if (!selected_job.has_value()) {
        throw std::logic_error{
            "Dispatch planner selected an unknown job"
        };
    }

    // Preserve the original queued job for rollback if applying
    // the dispatch decision unexpectedly fails.
    domain::Job rollback_job = *selected_job;

    try {
        selected_worker->reserve(
            selected_job->requirements()
        );

        selected_job->transition_to(
            domain::JobState::leased
        );

        active_jobs_.push_back(
            ActiveJob{
                std::move(*selected_job),
                decision->worker_id
            }
        );
    } catch (...) {
        const auto current_snapshot =
            selected_worker->snapshot();

        if (current_snapshot.running_jobs() >
            rollback_job.requirements().cpu_cores() * 0.0) {
            // release() itself verifies that resources were
            // actually reserved before restoring them.
            bool resources_reserved = false;

            try {
                selected_worker->reserve(
                    selected_job->requirements()
                );

                resources_reserved = true;

                selected_job->transition_to(
                    domain::JobState::leased
                );

                active_jobs_.push_back(
                    ActiveJob{
                        std::move(*selected_job),
                        decision->worker_id
                    }
                );
            } catch (...) {
                if (resources_reserved) {
                    try {
                        selected_worker->release(
                            rollback_job.requirements()
                        );
                    } catch (...) {
                        // Preserve the original exception.
                    }
                }

                if (!queue_.contains(rollback_job.id())) {
                    queue_.enqueue(std::move(rollback_job));
                }

                throw;
            }
        }

        if (!queue_.contains(rollback_job.id())) {
            queue_.enqueue(std::move(rollback_job));
        }

        throw;
    }

    return decision;
}

void InMemoryCoordinator::mark_running(
    const domain::JobId& job_id
) {
    auto* active_job = find_active_job(job_id);

    if (active_job == nullptr) {
        throw std::invalid_argument{
            "Cannot start an unknown active job"
        };
    }

    active_job->job.transition_to(
        domain::JobState::running
    );
}

void InMemoryCoordinator::mark_succeeded(
    const domain::JobId& job_id
) {
    finish_job(
        job_id,
        domain::JobState::succeeded
    );
}

void InMemoryCoordinator::mark_failed(
    const domain::JobId& job_id
) {
    finish_job(
        job_id,
        domain::JobState::failed
    );
}

std::size_t
InMemoryCoordinator::queued_job_count() const noexcept {
    return queue_.size();
}

std::size_t
InMemoryCoordinator::active_job_count() const noexcept {
    return active_jobs_.size();
}

std::size_t
InMemoryCoordinator::finished_job_count() const noexcept {
    return finished_jobs_.size();
}

std::optional<domain::JobState>
InMemoryCoordinator::job_state(
    const domain::JobId& job_id
) const {
    for (const auto* queued_job : queue_.ordered_jobs()) {
        if (queued_job->id() == job_id) {
            return queued_job->state();
        }
    }

    const auto* active_job = find_active_job(job_id);

    if (active_job != nullptr) {
        return active_job->job.state();
    }

    const auto finished_iterator = std::find_if(
        finished_jobs_.begin(),
        finished_jobs_.end(),
        [&job_id](const domain::Job& job) {
            return job.id() == job_id;
        }
    );

    if (finished_iterator != finished_jobs_.end()) {
        return finished_iterator->state();
    }

    return std::nullopt;
}

std::optional<domain::Job>
InMemoryCoordinator::get_job(
    const domain::JobId& job_id
) const {
    for (const auto* queued_job : queue_.ordered_jobs()) {
        if (queued_job->id() == job_id) {
            return *queued_job;
        }
    }

    const auto* active_job = find_active_job(job_id);

    if (active_job != nullptr) {
        return active_job->job;
    }

    const auto finished_iterator = std::find_if(
        finished_jobs_.begin(),
        finished_jobs_.end(),
        [&job_id](const domain::Job& job) {
            return job.id() == job_id;
        }
    );

    if (finished_iterator != finished_jobs_.end()) {
        return *finished_iterator;
    }

    return std::nullopt;
}

std::vector<domain::Job>
InMemoryCoordinator::list_jobs() const {
    std::vector<domain::Job> jobs;

    jobs.reserve(
        queue_.size() +
        active_jobs_.size() +
        finished_jobs_.size()
    );

    for (const auto* queued_job : queue_.ordered_jobs()) {
        jobs.push_back(*queued_job);
    }

    for (const auto& active_job : active_jobs_) {
        jobs.push_back(active_job.job);
    }

    for (const auto& finished_job : finished_jobs_) {
        jobs.push_back(finished_job);
    }

    std::sort(
        jobs.begin(),
        jobs.end(),
        [](const domain::Job& left,
           const domain::Job& right) {
            if (left.created_at() != right.created_at()) {
                return left.created_at() < right.created_at();
            }

            return left.id().value() <
                   right.id().value();
        }
    );

    return jobs;
}

std::optional<domain::Job>
InMemoryCoordinator::leased_job_for_worker(
    const domain::WorkerId& worker_id
) const {
    const auto iterator = std::find_if(
        active_jobs_.begin(),
        active_jobs_.end(),
        [&worker_id](const ActiveJob& active_job) {
            return
                active_job.worker_id == worker_id &&
                active_job.job.state() ==
                    domain::JobState::leased;
        }
    );

    if (iterator == active_jobs_.end()) {
        return std::nullopt;
    }

    return iterator->job;
}

std::optional<domain::WorkerSnapshot>
InMemoryCoordinator::worker_snapshot(
    const domain::WorkerId& worker_id
) const {
    const auto* worker = find_worker(worker_id);

    if (worker == nullptr) {
        return std::nullopt;
    }

    return worker->snapshot();
}

domain::WorkerRecord*
InMemoryCoordinator::find_worker(
    const domain::WorkerId& worker_id
) {
    const auto iterator = std::find_if(
        workers_.begin(),
        workers_.end(),
        [&worker_id](const domain::WorkerRecord& worker) {
            return worker.id() == worker_id;
        }
    );

    if (iterator == workers_.end()) {
        return nullptr;
    }

    return &*iterator;
}

const domain::WorkerRecord*
InMemoryCoordinator::find_worker(
    const domain::WorkerId& worker_id
) const {
    const auto iterator = std::find_if(
        workers_.begin(),
        workers_.end(),
        [&worker_id](const domain::WorkerRecord& worker) {
            return worker.id() == worker_id;
        }
    );

    if (iterator == workers_.end()) {
        return nullptr;
    }

    return &*iterator;
}

InMemoryCoordinator::ActiveJob*
InMemoryCoordinator::find_active_job(
    const domain::JobId& job_id
) {
    const auto iterator = std::find_if(
        active_jobs_.begin(),
        active_jobs_.end(),
        [&job_id](const ActiveJob& active_job) {
            return active_job.job.id() == job_id;
        }
    );

    if (iterator == active_jobs_.end()) {
        return nullptr;
    }

    return &*iterator;
}

const InMemoryCoordinator::ActiveJob*
InMemoryCoordinator::find_active_job(
    const domain::JobId& job_id
) const {
    const auto iterator = std::find_if(
        active_jobs_.begin(),
        active_jobs_.end(),
        [&job_id](const ActiveJob& active_job) {
            return active_job.job.id() == job_id;
        }
    );

    if (iterator == active_jobs_.end()) {
        return nullptr;
    }

    return &*iterator;
}

bool InMemoryCoordinator::job_exists(
    const domain::JobId& job_id
) const {
    if (queue_.contains(job_id)) {
        return true;
    }

    if (find_active_job(job_id) != nullptr) {
        return true;
    }

    return std::any_of(
        finished_jobs_.begin(),
        finished_jobs_.end(),
        [&job_id](const domain::Job& job) {
            return job.id() == job_id;
        }
    );
}

void InMemoryCoordinator::finish_job(
    const domain::JobId& job_id,
    domain::JobState terminal_state
) {
    const auto active_iterator = std::find_if(
        active_jobs_.begin(),
        active_jobs_.end(),
        [&job_id](const ActiveJob& active_job) {
            return active_job.job.id() == job_id;
        }
    );

    if (active_iterator == active_jobs_.end()) {
        throw std::invalid_argument{
            "Cannot finish an unknown active job"
        };
    }

    domain::JobStateMachine::validate_transition(
        active_iterator->job.state(),
        terminal_state
    );

    auto* worker = find_worker(
        active_iterator->worker_id
    );

    if (worker == nullptr) {
        throw std::logic_error{
            "Active job references an unknown worker"
        };
    }

    worker->release(
        active_iterator->job.requirements()
    );

    active_iterator->job.transition_to(
        terminal_state
    );

    finished_jobs_.push_back(
        std::move(active_iterator->job)
    );

    active_jobs_.erase(active_iterator);
}

}  // namespace radahn::coordinator