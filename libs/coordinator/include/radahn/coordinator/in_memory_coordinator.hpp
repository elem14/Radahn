#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"
#include "radahn/scheduler/dispatch_decision.hpp"
#include "radahn/scheduler/dispatch_planner.hpp"
#include "radahn/scheduler/job_queue.hpp"
#include "radahn/scheduler/scheduling_policy.hpp"

namespace radahn::coordinator {

class InMemoryCoordinator {
public:
    explicit InMemoryCoordinator(
        scheduler::ISchedulingPolicy& policy
    ) noexcept;

    void submit_job(domain::Job job);

    void register_worker(
        domain::WorkerRecord worker
    );

    [[nodiscard]]
    std::optional<scheduler::DispatchDecision>
    dispatch_once();

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
    std::size_t queued_job_count() const noexcept;

    [[nodiscard]]
    std::size_t active_job_count() const noexcept;

    [[nodiscard]]
    std::size_t finished_job_count() const noexcept;

    [[nodiscard]]
    std::optional<domain::JobState> job_state(
        const domain::JobId& job_id
    ) const;

    [[nodiscard]]
    std::optional<domain::Job> get_job(
        const domain::JobId& job_id
    ) const;

    [[nodiscard]]
    std::vector<domain::Job> list_jobs() const;

    [[nodiscard]]
    std::optional<domain::Job>
    leased_job_for_worker(
        const domain::WorkerId& worker_id
    ) const;

    [[nodiscard]]
    std::optional<domain::WorkerSnapshot>
    worker_snapshot(
        const domain::WorkerId& worker_id
    ) const;

private:
    struct ActiveJob {
        domain::Job job;
        domain::WorkerId worker_id;
    };

    [[nodiscard]]
    domain::WorkerRecord* find_worker(
        const domain::WorkerId& worker_id
    );

    [[nodiscard]]
    const domain::WorkerRecord* find_worker(
        const domain::WorkerId& worker_id
    ) const;

    [[nodiscard]]
    ActiveJob* find_active_job(
        const domain::JobId& job_id
    );

    [[nodiscard]]
    const ActiveJob* find_active_job(
        const domain::JobId& job_id
    ) const;

    [[nodiscard]]
    bool job_exists(
        const domain::JobId& job_id
    ) const;

    void finish_job(
        const domain::JobId& job_id,
        domain::JobState terminal_state
    );

    scheduler::InMemoryJobQueue queue_;

    std::vector<domain::WorkerRecord> workers_;
    std::vector<ActiveJob> active_jobs_;
    std::vector<domain::Job> finished_jobs_;

    scheduler::DispatchPlanner planner_;
};

}  // namespace radahn::coordinator