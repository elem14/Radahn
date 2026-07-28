#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/persistence/job_repository.hpp"
#include "radahn/persistence/worker_repository.hpp"

#include "radahn/scheduler/dispatch_planner.hpp"
#include "radahn/scheduler/job_queue.hpp"
#include "radahn/scheduler/scheduling_decision.hpp"
#include "radahn/scheduler/scheduling_policy.hpp"

namespace radahn::coordinator {

class InMemoryCoordinator {
public:
    /*
     * Convenience constructor.
     *
     * This creates private in-memory repositories so existing
     * tests and callers can continue constructing the coordinator
     * with only a scheduling policy.
     */
    explicit InMemoryCoordinator(
        scheduler::ISchedulingPolicy& policy
    );

    /*
     * Dependency-injected constructor.
     *
     * This is the important Milestone 3A constructor. It lets the
     * caller decide which repository implementations are used.
     */
    InMemoryCoordinator(
        scheduler::ISchedulingPolicy& policy,
        persistence::IJobRepository& job_repository,
        persistence::IWorkerRepository& worker_repository
    );

    void submit_job(
        domain::Job job
    );

    void register_worker(
        domain::WorkerRecord worker
    );

    [[nodiscard]]
    std::optional<scheduler::DispatchDecision>
    dispatch_once();

    [[nodiscard]]
    std::optional<scheduler::DispatchDecision>
    dispatch_once_for_worker(
        const domain::WorkerId& worker_id
    );

    void mark_running(
        const domain::JobId& job_id
    );

    void mark_succeeded(
        const domain::JobId& job_id
    );

    void mark_failed(
        const domain::JobId& job_id
    );

    [[nodiscard]]
    std::size_t queued_job_count() const;

    [[nodiscard]]
    std::size_t active_job_count() const;

    [[nodiscard]]
    std::size_t finished_job_count() const;

    [[nodiscard]]
    std::optional<domain::JobState>
    job_state(
        const domain::JobId& job_id
    ) const;

    [[nodiscard]]
    std::optional<domain::Job>
    get_job(
        const domain::JobId& job_id
    ) const;

    [[nodiscard]]
    std::vector<domain::Job>
    list_jobs() const;

    [[nodiscard]]
    std::optional<domain::Job>
    leased_job_for_worker(
        const domain::WorkerId& worker_id
    ) const;

    [[nodiscard]]
    bool is_job_assigned_to_worker(
        const domain::JobId& job_id,
        const domain::WorkerId& worker_id
    ) const;

    [[nodiscard]]
    std::optional<domain::WorkerSnapshot>
    worker_snapshot(
        const domain::WorkerId& worker_id
    ) const;

private:
    [[nodiscard]]
    static bool is_active_state(
        domain::JobState state
    ) noexcept;

    [[nodiscard]]
    static bool is_finished_state(
        domain::JobState state
    ) noexcept;

    [[nodiscard]]
    bool job_exists(
        const domain::JobId& job_id
    ) const;

    void apply_dispatch_decision(
        const scheduler::DispatchDecision& decision
    );

    void finish_job(
        const domain::JobId& job_id,
        domain::JobState terminal_state
    );

    /*
     * These are populated only by the convenience constructor.
     * The injected constructor leaves them empty.
     */
    std::unique_ptr<persistence::IJobRepository>
        owned_job_repository_;

    std::unique_ptr<persistence::IWorkerRepository>
        owned_worker_repository_;

    /*
     * All coordinator operations use these interfaces, regardless
     * of whether the repositories are owned or injected.
     */
    persistence::IJobRepository&
        job_repository_;

    persistence::IWorkerRepository&
        worker_repository_;

    /*
     * The queue is still transient scheduling state.
     * Persistent queue restoration comes in Milestone 3C.
     */
    scheduler::InMemoryJobQueue queue_;

    scheduler::DispatchPlanner planner_;
};

}  // namespace radahn::coordinator