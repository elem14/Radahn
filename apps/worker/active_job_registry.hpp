#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace radahn::worker_app {

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
 * Thread-safe set of job attempts this worker process currently
 * considers actively RUNNING.
 *
 * Shared between the job execution threads (which add/remove
 * themselves as they start and finish) and the heartbeat loop
 * (which reads a snapshot to report to the coordinator so each
 * attempt's lease can be renewed independently).
 */
class ActiveJobRegistry final {
public:
    void add(const ActiveAttempt& attempt);

    void remove(const ActiveAttempt& attempt);

    [[nodiscard]] std::vector<ActiveAttempt>
    snapshot() const;

    [[nodiscard]] std::size_t size() const;

private:
    mutable std::mutex mutex_;

    std::vector<ActiveAttempt> attempts_;
};

}  // namespace radahn::worker_app
