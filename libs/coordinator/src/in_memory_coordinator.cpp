#include "radahn/coordinator/in_memory_coordinator.hpp"

#include <chrono>
#include <algorithm>
#include <array>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "radahn/domain/job_state.hpp"

#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/in_memory_worker_repository.hpp"
#include "radahn/persistence/job_record.hpp"

namespace radahn::coordinator {

InMemoryCoordinator::InMemoryCoordinator(
    scheduler::ISchedulingPolicy& policy,
    std::chrono::milliseconds lease_duration
)
    : owned_job_repository_{
          std::make_unique<
              persistence::InMemoryJobRepository
          >()
      },
      owned_worker_repository_{
          std::make_unique<
              persistence::InMemoryWorkerRepository
          >()
      },
      job_repository_{
          *owned_job_repository_
      },
      worker_repository_{
          *owned_worker_repository_
      },
      lease_duration_{
          lease_duration
      },
      planner_{policy} {
    if (
        lease_duration_ <=
        std::chrono::milliseconds::zero()
    ) {
        throw std::invalid_argument{
            "Job lease duration must be positive"
        };
    }

    recover_persisted_state();
}

InMemoryCoordinator::InMemoryCoordinator(
    scheduler::ISchedulingPolicy& policy,
    persistence::IJobRepository& job_repository,
    persistence::IWorkerRepository& worker_repository,
    std::chrono::milliseconds lease_duration
)
    : job_repository_{job_repository},
      worker_repository_{worker_repository},
      lease_duration_{
          lease_duration
      },
      planner_{policy} {
    if (
        lease_duration_ <=
        std::chrono::milliseconds::zero()
    ) {
        throw std::invalid_argument{
            "Job lease duration must be positive"
        };
    }

    recover_persisted_state();
}

void InMemoryCoordinator::submit_job(
    domain::Job job
) {
    const domain::JobId job_id{
        job.id()
    };

    if (job_exists(job_id)) {
        throw std::invalid_argument{
            "A job with this ID already exists"
        };
    }

    persistence::JobRecord record{
        job,
        std::nullopt,
        std::nullopt
    };

    job_repository_.insert(
        std::move(record)
    );

    try {
        queue_.enqueue(
            std::move(job)
        );
    } catch (...) {
        /*
         * Keep repository and queue state consistent if queue
         * insertion unexpectedly fails.
         */
        try {
            job_repository_.erase(job_id);
        } catch (...) {
            // Preserve the original exception.
        }

        throw;
    }
}

void InMemoryCoordinator::register_worker(
    domain::WorkerRecord worker
) {
    worker_repository_.insert(
        std::move(worker)
    );
}

void InMemoryCoordinator::record_worker_heartbeat(
    const domain::WorkerId& worker_id,
    persistence::WorkerHeartbeatTimePoint
        heartbeat_time
) {
    auto worker =
        worker_repository_.get(
            worker_id
        );

    if (!worker.has_value()) {
        throw std::invalid_argument{
            "Cannot record a heartbeat for an unknown worker"
        };
    }

    const auto snapshot =
        worker->snapshot();

    if (
        snapshot.state() ==
        domain::WorkerState::offline
    ) {
        domain::WorkerSnapshot online_snapshot{
            worker->id(),
            domain::WorkerState::online,
            snapshot.resources(),
            snapshot.running_jobs(),
            snapshot.max_concurrent_jobs(),
            snapshot.tags()
        };

        worker_repository_.update(
            domain::WorkerRecord{
                std::move(online_snapshot)
            }
        );
    }

    worker_repository_.record_heartbeat(
        worker_id,
        heartbeat_time
    );

    auto job_records =
        job_repository_.list();

    for (auto& record : job_records) {
        if (
            !record.assigned_worker_id.has_value() ||
            *record.assigned_worker_id !=
                worker_id
        ) {
            continue;
        }

        if (
            !is_active_state(
                record.job.state()
            )
        ) {
            continue;
        }

        record.lease_expires_at =
            heartbeat_time +
            lease_duration_;

        job_repository_.update(
            std::move(record)
        );
    }
}

std::size_t
InMemoryCoordinator::mark_stale_workers_offline(
    persistence::WorkerHeartbeatTimePoint now,
    std::chrono::milliseconds timeout
) {
    if (
        timeout <=
        std::chrono::milliseconds::zero()
    ) {
        throw std::invalid_argument{
            "Worker heartbeat timeout must be positive"
        };
    }

    const auto workers =
        worker_repository_.list();

    std::size_t marked_offline = 0;

    for (const auto& worker : workers) {
        const auto snapshot =
            worker.snapshot();

        if (
            snapshot.state() !=
            domain::WorkerState::online
        ) {
            continue;
        }

        const auto last_heartbeat =
            worker_repository_.last_heartbeat(
                worker.id()
            );

        if (!last_heartbeat.has_value()) {
            throw std::logic_error{
                "Stored worker has no heartbeat timestamp"
            };
        }

        if (now < *last_heartbeat) {
            continue;
        }

        const auto heartbeat_age =
            now - *last_heartbeat;

        if (heartbeat_age < timeout) {
            continue;
        }

        domain::WorkerSnapshot offline_snapshot{
            worker.id(),
            domain::WorkerState::offline,
            snapshot.resources(),
            snapshot.running_jobs(),
            snapshot.max_concurrent_jobs(),
            snapshot.tags()
        };

        worker_repository_.update(
            domain::WorkerRecord{
                std::move(offline_snapshot)
            }
        );

        ++marked_offline;
    }

    return marked_offline;
}

std::optional<scheduler::DispatchDecision>
InMemoryCoordinator::dispatch_once() {
    const auto workers =
        worker_repository_.list();

    std::vector<domain::WorkerSnapshot> snapshots;

    snapshots.reserve(
        workers.size()
    );

    for (const auto& worker : workers) {
        snapshots.push_back(
            worker.snapshot()
        );
    }

    const auto decision =
        planner_.plan(
            queue_,
            std::span<
                const domain::WorkerSnapshot
            >{
                snapshots
            }
        );

    if (!decision.has_value()) {
        return std::nullopt;
    }

    apply_dispatch_decision(
        *decision
    );

    return decision;
}

std::optional<scheduler::DispatchDecision>
InMemoryCoordinator::dispatch_once_for_worker(
    const domain::WorkerId& worker_id
) {
    const auto worker =
        worker_repository_.get(
            worker_id
        );

    if (!worker.has_value()) {
        throw std::invalid_argument{
            "Cannot dispatch to an unknown worker"
        };
    }

    const std::array<
        domain::WorkerSnapshot,
        1
    > snapshots{
        worker->snapshot()
    };

    const auto decision =
        planner_.plan(
            queue_,
            std::span<
                const domain::WorkerSnapshot
            >{
                snapshots
            }
        );

    if (!decision.has_value()) {
        return std::nullopt;
    }

    apply_dispatch_decision(
        *decision
    );

    return decision;
}

void InMemoryCoordinator::mark_running(
    const domain::JobId& job_id
) {
    auto record =
        job_repository_.get(
            job_id
        );

    if (!record.has_value()) {
        throw std::invalid_argument{
            "Cannot start an unknown job"
        };
    }

    if (
        !record->assigned_worker_id.has_value()
    ) {
        throw std::invalid_argument{
            "Cannot start an unassigned job"
        };
    }

    record->job.transition_to(
        domain::JobState::running
    );

    record->lease_expires_at = 
        persistence::JobLeaseClock::now() +
        lease_duration_;

    job_repository_.update(
        std::move(*record)
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
InMemoryCoordinator::queued_job_count() const {
    const auto records =
        job_repository_.list();

    return static_cast<std::size_t>(
        std::count_if(
            records.begin(),
            records.end(),
            [](
                const persistence::JobRecord& record
            ) {
                return
                    record.job.state() ==
                    domain::JobState::queued;
            }
        )
    );
}

std::size_t
InMemoryCoordinator::active_job_count() const {
    const auto records =
        job_repository_.list();

    return static_cast<std::size_t>(
        std::count_if(
            records.begin(),
            records.end(),
            [](
                const persistence::JobRecord& record
            ) {
                return is_active_state(
                    record.job.state()
                );
            }
        )
    );
}

std::size_t
InMemoryCoordinator::finished_job_count() const {
    const auto records =
        job_repository_.list();

    return static_cast<std::size_t>(
        std::count_if(
            records.begin(),
            records.end(),
            [](
                const persistence::JobRecord& record
            ) {
                return is_finished_state(
                    record.job.state()
                );
            }
        )
    );
}

std::optional<domain::JobState>
InMemoryCoordinator::job_state(
    const domain::JobId& job_id
) const {
    const auto record =
        job_repository_.get(
            job_id
        );

    if (!record.has_value()) {
        return std::nullopt;
    }

    return record->job.state();
}

std::optional<domain::Job>
InMemoryCoordinator::get_job(
    const domain::JobId& job_id
) const {
    const auto record =
        job_repository_.get(
            job_id
        );

    if (!record.has_value()) {
        return std::nullopt;
    }

    return record->job;
}

std::vector<domain::Job>
InMemoryCoordinator::list_jobs() const {
    const auto records =
        job_repository_.list();

    std::vector<domain::Job> jobs;

    jobs.reserve(
        records.size()
    );

    for (const auto& record : records) {
        jobs.push_back(
            record.job
        );
    }

    std::sort(
        jobs.begin(),
        jobs.end(),
        [](
            const domain::Job& left,
            const domain::Job& right
        ) {
            if (
                left.created_at() !=
                right.created_at()
            ) {
                return
                    left.created_at() <
                    right.created_at();
            }

            return
                left.id().value() <
                right.id().value();
        }
    );

    return jobs;
}

std::optional<domain::Job>
InMemoryCoordinator::leased_job_for_worker(
    const domain::WorkerId& worker_id
) const {
    const auto records =
        job_repository_.list();

    const auto iterator =
        std::find_if(
            records.begin(),
            records.end(),
            [&worker_id](
                const persistence::JobRecord& record
            ) {
                return
                    record.assigned_worker_id
                        .has_value() &&
                    *record.assigned_worker_id ==
                        worker_id &&
                    record.job.state() ==
                        domain::JobState::leased;
            }
        );

    if (iterator == records.end()) {
        return std::nullopt;
    }

    return iterator->job;
}

bool
InMemoryCoordinator::is_job_assigned_to_worker(
    const domain::JobId& job_id,
    const domain::WorkerId& worker_id
) const {
    const auto record =
        job_repository_.get(
            job_id
        );

    return
        record.has_value() &&
        record->assigned_worker_id.has_value() &&
        *record->assigned_worker_id ==
            worker_id;
}

std::optional<domain::WorkerSnapshot>
InMemoryCoordinator::worker_snapshot(
    const domain::WorkerId& worker_id
) const {
    const auto worker =
        worker_repository_.get(
            worker_id
        );

    if (!worker.has_value()) {
        return std::nullopt;
    }

    return worker->snapshot();
}

bool InMemoryCoordinator::is_active_state(
    domain::JobState state
) noexcept {
    return
        state == domain::JobState::leased ||
        state == domain::JobState::running ||
        state ==
            domain::JobState::
                cancellation_requested;
}

bool InMemoryCoordinator::is_finished_state(
    domain::JobState state
) noexcept {
    return
        state == domain::JobState::succeeded ||
        state == domain::JobState::failed ||
        state == domain::JobState::cancelled;
}

bool InMemoryCoordinator::job_exists(
    const domain::JobId& job_id
) const {
    return job_repository_.contains(
        job_id
    );
}

void InMemoryCoordinator::apply_dispatch_decision(
    const scheduler::DispatchDecision& decision
) {
    auto worker =
        worker_repository_.get(
            decision.worker_id
        );

    if (!worker.has_value()) {
        throw std::logic_error{
            "Dispatch planner selected an unknown worker"
        };
    }

    auto selected_job =
        queue_.take(
            decision.job_id
        );

    if (!selected_job.has_value()) {
        throw std::logic_error{
            "Dispatch planner selected an unknown job"
        };
    }

    auto stored_record =
        job_repository_.get(
            decision.job_id
        );

    if (!stored_record.has_value()) {
        queue_.enqueue(
            std::move(*selected_job)
        );

        throw std::logic_error{
            "Queued job does not exist in repository"
        };
    }

    const domain::WorkerRecord original_worker{
        *worker
    };

    const persistence::JobRecord original_record{
        *stored_record
    };

    bool worker_repository_updated = false;
    bool job_repository_updated = false;

    try {
        worker->reserve(
            selected_job->requirements()
        );

        selected_job->transition_to(
            domain::JobState::leased
        );

        stored_record->job =
            std::move(*selected_job);

        stored_record->assigned_worker_id =
            decision.worker_id;

        stored_record->lease_expires_at = 
            persistence::JobLeaseClock::now() +
            lease_duration_;

        worker_repository_.update(
            std::move(*worker)
        );

        worker_repository_updated = true;

        job_repository_.update(
            std::move(*stored_record)
        );

        job_repository_updated = true;
    } catch (...) {
        /*
         * These compensating updates are sufficient for the
         * in-memory implementation. SQLite will later use a real
         * database transaction for this operation.
         */
        if (worker_repository_updated) {
            try {
                worker_repository_.update(
                    original_worker
                );
            } catch (...) {
                // Preserve the original exception.
            }
        }

        if (job_repository_updated) {
            try {
                job_repository_.update(
                    original_record
                );
            } catch (...) {
                // Preserve the original exception.
            }
        }

        if (
            !queue_.contains(
                original_record.job.id()
            )
        ) {
            try {
                queue_.enqueue(
                    original_record.job
                );
            } catch (...) {
                // Preserve the original exception.
            }
        }

        throw;
    }
}

void InMemoryCoordinator::finish_job(
    const domain::JobId& job_id,
    domain::JobState terminal_state
) {
    auto record =
        job_repository_.get(
            job_id
        );

    if (!record.has_value()) {
        throw std::invalid_argument{
            "Cannot finish an unknown job"
        };
    }

    if (
        !record->assigned_worker_id.has_value()
    ) {
        throw std::invalid_argument{
            "Cannot finish an unassigned job"
        };
    }

    domain::JobStateMachine::validate_transition(
        record->job.state(),
        terminal_state
    );

    auto worker =
        worker_repository_.get(
            *record->assigned_worker_id
        );

    if (!worker.has_value()) {
        throw std::logic_error{
            "Assigned job references an unknown worker"
        };
    }

    const domain::WorkerRecord original_worker{
        *worker
    };

    const persistence::JobRecord original_record{
        *record
    };

    bool worker_repository_updated = false;
    bool job_repository_updated = false;

    try {
        worker->release(
            record->job.requirements()
        );

        record->job.transition_to(
            terminal_state
        );

        record->lease_expires_at.reset();

        worker_repository_.update(
            std::move(*worker)
        );

        worker_repository_updated = true;

        job_repository_.update(
            std::move(*record)
        );

        job_repository_updated = true;
    } catch (...) {
        if (worker_repository_updated) {
            try {
                worker_repository_.update(
                    original_worker
                );
            } catch (...) {
                // Preserve the original exception.
            }
        }

        if (job_repository_updated) {
            try {
                job_repository_.update(
                    original_record
                );
            } catch (...) {
                // Preserve the original exception.
            }
        }

        throw;
    }
}

void InMemoryCoordinator::recover_persisted_state() {

    auto workers = 
        worker_repository_.list();

    for (auto& worker : workers) {
        const auto snapshot = worker.snapshot();
        const auto& resources = snapshot.resources();
        domain::WorkerResources
            recovered_resources{
                resources.total_cpu_cores(),
                resources.total_cpu_cores(),
                resources.total_memory_bytes(),
                resources.total_memory_bytes(),
                resources.total_disk_bytes(),
                resources.total_disk_bytes(),
                resources.gpu_available()
            };

        domain::WorkerSnapshot 
            recovered_snapshot{
                worker.id(),
                snapshot.state(),
                std::move(recovered_resources),
                0,
                snapshot.max_concurrent_jobs(),
                snapshot.tags()
            };

        worker_repository_.update(
            domain::WorkerRecord{
                std::move(recovered_snapshot)
            }
        );
    }

    auto records = job_repository_.list();

    for (auto& record : records) {
        switch (record.job.state()) {
            case domain::JobState::queued:
                
                if (
                    record.assigned_worker_id.has_value() ||
                    record.lease_expires_at.has_value()
                ) {
                    record.assigned_worker_id.reset();
                    record.lease_expires_at.reset();

                    job_repository_.update(
                        record
                    );
                }

                queue_.enqueue(
                    record.job
                );

                break;

            case domain::JobState::leased:
            case domain::JobState::running:
            case domain::JobState::retry_wait: {
                
                domain::Job recovered_job =
                    domain::Job::restore(
                        record.job.id(),
                        record.job.name(),
                        record.job.priority(),
                        record.job.requirements(),
                        record.job.workload(),
                        domain::JobState::queued,
                        record.job.created_at()
                    );

                record.job =
                    recovered_job;

                record.assigned_worker_id.reset();
                record.lease_expires_at.reset();

                job_repository_.update(
                    record
                );

                queue_.enqueue(
                    std::move(recovered_job)
                );

                break;
            }

            case domain::JobState::
                cancellation_requested: {
                domain::Job cancelled_job =
                    domain::Job::restore(
                        record.job.id(),
                        record.job.name(),
                        record.job.priority(),
                        record.job.requirements(),
                        record.job.workload(),
                        domain::JobState::cancelled,
                        record.job.created_at()
                    );

                record.job =
                    std::move(cancelled_job);

                record.assigned_worker_id.reset();
                record.lease_expires_at.reset();

                job_repository_.update(
                    std::move(record)
                );

                break;
            }

            case domain::JobState::succeeded:
            case domain::JobState::failed:
            case domain::JobState::cancelled:
                
                if (record.lease_expires_at.has_value()) {
                    record.lease_expires_at.reset();

                    job_repository_.update(
                        std::move(record)
                    );
                }

                break;
        }
    }
}

}  // namespace radahn::coordinator