#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace radahn::worker_app {

/*
 * Local safety margin subtracted from an acknowledged lease so a
 * small amount of clock/scheduling jitter between "the coordinator
 * computed this deadline" and "this worker notices it has passed"
 * can never make a worker believe it still holds authority for
 * slightly longer than it actually does. Shared by main.cpp (for
 * the StartJob-acknowledged deadline) and heartbeat_loop.cpp (for
 * each Heartbeat-renewed deadline).
 */
inline constexpr std::chrono::milliseconds
    lease_safety_margin{500};

/*
 * A single job attempt this worker process currently considers
 * actively RUNNING. The attempt number disambiguates two different
 * assignments of the same job ID, so an old attempt's cleanup can
 * never remove a newer attempt that happens to share a job ID.
 */
struct ActiveAttempt {
    std::string job_id;
    std::uint64_t attempt_number;
};

[[nodiscard]] inline bool operator==(
    const ActiveAttempt& left,
    const ActiveAttempt& right
) noexcept {
    return
        left.job_id == right.job_id &&
        left.attempt_number == right.attempt_number;
}

/*
 * This attempt's local phase.
 *
 * executing: the job is still running (or sleeping) locally, or
 *   execution has not yet started.
 * reporting: execution is done and a completion record is pending
 *   acknowledgement; the attempt stays registered for heartbeat
 *   renewal, but there is no child left to stop - the bounded
 *   completion-retry rules (CompletionReporter) govern it instead.
 */
enum class AttemptPhase {
    executing,
    reporting
};

/*
 * Lock-free holder for a steady_clock::time_point, stored as the
 * clock's raw duration count so the execution thread, the heartbeat
 * loop, and the main acquisition loop can all read or write it
 * concurrently without a mutex.
 */
class AtomicSteadyDeadline {
public:
    explicit AtomicSteadyDeadline(
        std::chrono::steady_clock::time_point deadline =
            std::chrono::steady_clock::time_point::max()
    ) noexcept
        : value_{deadline.time_since_epoch().count()} {
    }

    void store(
        std::chrono::steady_clock::time_point deadline
    ) noexcept {
        value_.store(
            deadline.time_since_epoch().count()
        );
    }

    [[nodiscard]]
    std::chrono::steady_clock::time_point load() const noexcept {
        return std::chrono::steady_clock::time_point{
            std::chrono::steady_clock::duration{
                value_.load()
            }
        };
    }

private:
    std::atomic<std::chrono::steady_clock::rep> value_;
};

/*
 * Shared control state for one job attempt this worker process
 * currently considers actively RUNNING or REPORTING.
 *
 * Shared (via shared_ptr) between the execution thread, the
 * completion-reporting path, the main acquisition loop, and the
 * heartbeat loop, so any of them can observe this attempt's state or
 * request it stop without owning its lifetime. Nothing here proves
 * the job is still allowed to run locally merely because it is
 * running - `lease_deadline` is the only thing that does, and it is
 * only ever extended from an acknowledged RPC (see heartbeat_loop.cpp
 * and main.cpp), never from local execution progress.
 */
struct AttemptControl {
    const std::string job_id;
    const std::uint64_t attempt_number;

    std::atomic<AttemptPhase> phase{
        AttemptPhase::executing
    };

    // Flipped to ask the executing command (or interruptible sleep)
    // to stop as soon as possible - either because the coordinator
    // explicitly declined to renew this attempt, or because its
    // locally tracked lease deadline has passed.
    std::atomic<bool> stop_requested{false};

    // Conservative local estimate of when this attempt's lease
    // authority expires. Only ever set from the send time of an
    // acknowledged StartJob/Heartbeat RPC plus the coordinator's
    // reported lease duration, minus a safety margin - never from
    // response-arrival time (which would overestimate remaining
    // authority after network delay) and never extended on a failed
    // RPC.
    AtomicSteadyDeadline lease_deadline;

    AttemptControl(
        std::string job_id_in,
        std::uint64_t attempt_number_in
    )
        : job_id{std::move(job_id_in)},
          attempt_number{attempt_number_in} {
    }

    void request_stop() noexcept {
        stop_requested.store(true);
    }

    [[nodiscard]]
    bool stop_was_requested() const noexcept {
        return stop_requested.load();
    }
};

using AttemptControlPtr = std::shared_ptr<AttemptControl>;

/*
 * Thread-safe set of job attempts this worker process currently
 * considers actively RUNNING or REPORTING.
 *
 * Shared between the job execution threads (which add themselves as
 * they start and remove themselves once their completion is
 * resolved) and the heartbeat loop (which reads a snapshot to report
 * to the coordinator, renew each attempt's local lease deadline on
 * success, and request a stop for any attempt the coordinator
 * declines to renew).
 */
class ActiveJobRegistry final {
public:
    /*
     * Registers a new attempt and returns its shared control block.
     * The caller keeps this pointer (e.g. to hand its stop flag to
     * the command executor as a stop token, and to update its phase)
     * for as long as the attempt is active.
     */
    [[nodiscard]]
    AttemptControlPtr add(
        std::string job_id,
        std::uint64_t attempt_number
    );

    void remove(
        const AttemptControlPtr& control
    );

    [[nodiscard]]
    std::vector<AttemptControlPtr>
    snapshot() const;

    [[nodiscard]]
    std::size_t size() const;

private:
    mutable std::mutex mutex_;

    std::vector<AttemptControlPtr> attempts_;
};

}  // namespace radahn::worker_app
