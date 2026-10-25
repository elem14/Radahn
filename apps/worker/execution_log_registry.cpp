#include "execution_log_registry.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>

#include <unistd.h>

namespace radahn::worker_app {

std::filesystem::path next_execution_output_directory(
    const std::filesystem::path& root
) {
    static std::atomic<std::uint64_t> sequence{0};

    const auto absolute_root =
        std::filesystem::absolute(root);

    std::filesystem::create_directories(
        absolute_root
    );

    const auto timestamp =
        std::chrono::steady_clock::now()
            .time_since_epoch()
            .count();

    return absolute_root / (
        "execution-" +
        std::to_string(::getpid()) +
        "-" +
        std::to_string(timestamp) +
        "-" +
        std::to_string(
            sequence.fetch_add(
                1,
                std::memory_order_relaxed
            )
        )
    );
}

void ExecutionLogRegistry::record(
    ExecutionLogRecord entry
) {
    const std::lock_guard lock{mutex_};

    records_.push_back(
        std::move(entry)
    );
}

std::optional<ExecutionLogRecord>
ExecutionLogRegistry::find(
    const std::string& job_id,
    std::uint64_t attempt_number
) const {
    const std::lock_guard lock{mutex_};

    const auto iterator = std::find_if(
        records_.begin(),
        records_.end(),
        [&](const ExecutionLogRecord& entry) {
            return
                entry.job_id == job_id &&
                entry.attempt_number == attempt_number;
        }
    );

    if (iterator == records_.end()) {
        return std::nullopt;
    }

    return *iterator;
}

std::size_t ExecutionLogRegistry::size() const {
    const std::lock_guard lock{mutex_};

    return records_.size();
}

}  // namespace radahn::worker_app
