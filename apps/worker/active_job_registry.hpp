#pragma once

#include <mutex>
#include <string>
#include <vector>

namespace radahn::worker_app {

/*
 * Thread-safe set of job IDs this worker process currently
 * considers actively RUNNING.
 *
 * Shared between the job execution threads (which add/remove
 * themselves as they start and finish) and the heartbeat loop
 * (which reads a snapshot to report to the coordinator so each
 * job's lease can be renewed independently).
 */
class ActiveJobRegistry final {
public:
    void add(const std::string& job_id);

    void remove(const std::string& job_id);

    [[nodiscard]] std::vector<std::string>
    snapshot() const;

    [[nodiscard]] std::size_t size() const;

private:
    mutable std::mutex mutex_;

    std::vector<std::string> job_ids_;
};

}  // namespace radahn::worker_app
