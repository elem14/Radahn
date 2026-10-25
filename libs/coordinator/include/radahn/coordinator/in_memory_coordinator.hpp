#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <vector>
#include <cstdint>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/persistence/job_repository.hpp"
#include "radahn/persistence/transaction_runner.hpp"
#include "radahn/persistence/worker_repository.hpp"

#include "radahn/scheduler/dispatch_planner.hpp"
#include "radahn/scheduler/job_queue.hpp"
#include "radahn/scheduler/scheduling_decision.hpp"
#include "radahn/scheduler/scheduling_policy.hpp"

namespace radahn::coordinator {

struct JobAttempt {
    domain::JobId job_id;
    std::uint64_t attempt_number;
};

class InMemoryCoordinator {
public:
    explicit InMemoryCoordinator(
        scheduler::ISchedulingPolicy& policy,
        std::chrono::milliseconds lease_duration =
            std::chrono::seconds{15},
        persistence::ITransactionRunner* transaction_runner =
            nullptr
    );

    // Dependency-injected constructor
    InMemoryCoordinator(
        scheduler::ISchedulingPolicy& policy,
        persistence::IJobRepository& job_repository,
        persistence::IWorkerRepository& worker_repository,
        std::chrono::milliseconds lease_duration =
            std::chrono::seconds{15},
        /*
         * nullptr (the default) uses a no-op runner, which is
         * correct for the in-memory repositories tests normally
         * inject here - they have no separate commit step, so
         * apply_dispatch_decision / finish_job / lease reclamation
         * already keep themselves consistent via their own
         * compensating updates on exception. Production code (see
         * apps/coordinator/main.cpp) passes a SqliteTransactionRunner
         * so those same code paths commit or roll back both the job
         * and worker repository writes as one durable unit.
         */
        persistence::ITransactionRunner* transaction_runner =
            nullptr
    );

    void submit_job(
        domain::Job job
    );

    void register_worker(
        domain::WorkerRecord worker
    );

    void record_worker_heartbeat(
        const domain::WorkerId& worker_id,
        persistence::WorkerHeartbeatTimePoint heartbeat_time
    );

    [[nodiscard]]
    std::size_t mark_stale_workers_offline(
        persistence::WorkerHeartbeatTimePoint now,
        std::chrono::milliseconds timeout
    );

    [[nodiscard]]
    std::size_t mark_expired_job_leases(
        persistence::JobLeaseTimePoint now
    );

    [[nodiscard]]
    std::size_t renew_job_leases_for_worker(
        const domain::WorkerId& worker_id,
        const std::vector<domain::JobId>& active_job_ids,
        persistence::JobLeaseTimePoint now
    );

    [[nodiscard]]
    std::size_t requeue_retry_wait_jobs();

    [[nodiscard]]
    std::optional<scheduler::DispatchDecision>
    dispatch_once();

    [[nodiscard]]
    std::optional<scheduler::DispatchDecision>
    dispatch_once_for_worker(
        const domain::WorkerId& worker_id
    );

    [[nodiscard]]
    bool start_job_attempt(
        const domain::WorkerId& worker_id,
        const JobAttempt& attempt,
        persistence::JobLeaseTimePoint now
    );

    [[nodiscard]]
    bool finish_job_attempt(
        const domain::WorkerId& worker_id,
        const JobAttempt& attempt,
        const domain::ExecutionResult& result,
        persistence::JobLeaseTimePoint now
    );

    [[nodiscard]]
    std::size_t renew_job_attempt_leases(
        const domain::WorkerId& worker_id,
        const std::vector<JobAttempt>& attempts,
        persistence::JobLeaseTimePoint now
    );

    // Same renewal, returning the identities actually renewed so a
    // caller (e.g. the Heartbeat RPC handler) never has to guess
    // them back out of the request it sent in.
    [[nodiscard]]
    std::vector<JobAttempt> renew_job_attempt_leases_detailed(
        const domain::WorkerId& worker_id,
        const std::vector<JobAttempt>& attempts,
        persistence::JobLeaseTimePoint now
    );

    void mark_running(
        const domain::JobId& job_id
    );

    void mark_succeeded(
        const domain::JobId& job_id,
        std::optional<domain::ExecutionResult> result = 
            std::nullopt
    );

    void mark_failed(
        const domain::JobId& job_id,
        std::optional<domain::ExecutionResult> result = std::nullopt
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

    [[nodiscard]]
    std::chrono::milliseconds
    lease_duration() const noexcept;

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
        domain::JobState terminal_state,
        std::optional<domain::ExecutionResult> result
    );

    void recover_persisted_state();

    /*
     * These are populated only by the convenience constructor
     * The injected constructor leaves them empty
     */
    std::unique_ptr<persistence::IJobRepository>
        owned_job_repository_;

    std::unique_ptr<persistence::IWorkerRepository>
        owned_worker_repository_;

    /*
     * All coordinator operations use these interfaces regardless
     * of whether the repositories are owned or injected
     */
    persistence::IJobRepository&
        job_repository_;

    persistence::IWorkerRepository&
        worker_repository_;

    std::chrono::milliseconds
        lease_duration_;

    persistence::ITransactionRunner&
        transaction_runner_;

    /*
     * The queue is still transient scheduling state.
     * Persistent queue restoration comes in Milestone 3C.
     */
    scheduler::InMemoryJobQueue queue_;

    scheduler::DispatchPlanner planner_;
};

}  // namespace radahn::coordinator