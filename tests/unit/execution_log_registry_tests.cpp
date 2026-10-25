#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>

#include "execution_log_registry.hpp"

namespace {

using radahn::worker_app::ExecutionLogRecord;
using radahn::worker_app::ExecutionLogRegistry;
using radahn::worker_app::next_execution_output_directory;

void require(bool condition, std::string_view description) {
    if (!condition) {
        throw std::runtime_error{std::string{description}};
    }

    std::cout << "[PASS] " << description << '\n';
}

[[nodiscard]]
radahn::domain::ExecutionResult succeeded_result() {
    return radahn::domain::ExecutionResult{
        radahn::domain::ExecutionOutcome::exited,
        0,
        std::nullopt,
        {}
    };
}

void test_output_directories_are_unique_and_absolute() {
    const auto root =
        std::filesystem::temp_directory_path() /
        "radahn-execution-log-registry-test";

    const auto first =
        next_execution_output_directory(root);

    const auto second =
        next_execution_output_directory(root);

    require(
        first.is_absolute() && second.is_absolute(),
        "Generated output directories are always absolute paths"
    );

    require(
        first != second,
        "Two directories generated back-to-back never collide"
    );

    require(
        first.parent_path() == std::filesystem::absolute(root) &&
        second.parent_path() == std::filesystem::absolute(root),
        "Generated directories live directly under the configured root"
    );

    std::filesystem::remove_all(root);
}

void test_two_attempts_of_same_job_have_separate_logs() {
    ExecutionLogRegistry registry;

    const ExecutionLogRecord first_attempt{
        "job-a",
        1,
        "worker-1",
        "/tmp/radahn-logs/execution-a-1",
        std::filesystem::path{"/tmp/radahn-logs/execution-a-1/stdout.log"},
        std::filesystem::path{"/tmp/radahn-logs/execution-a-1/stderr.log"},
        succeeded_result()
    };

    const ExecutionLogRecord second_attempt{
        "job-a",
        2,
        "worker-1",
        "/tmp/radahn-logs/execution-a-2",
        std::filesystem::path{"/tmp/radahn-logs/execution-a-2/stdout.log"},
        std::filesystem::path{"/tmp/radahn-logs/execution-a-2/stderr.log"},
        succeeded_result()
    };

    registry.record(first_attempt);
    registry.record(second_attempt);

    require(
        registry.size() == 2,
        "Both attempts of the same job are retained independently"
    );

    const auto looked_up_first = registry.find("job-a", 1);
    const auto looked_up_second = registry.find("job-a", 2);

    require(
        looked_up_first.has_value() && looked_up_second.has_value(),
        "Both attempts can be looked up by (job_id, attempt_number)"
    );

    require(
        looked_up_first->output_directory !=
            looked_up_second->output_directory,
        "The two attempts' output directories are distinct"
    );

    require(
        looked_up_first->stdout_path != looked_up_second->stdout_path &&
        looked_up_first->stderr_path != looked_up_second->stderr_path,
        "Neither attempt's stdout/stderr path overwrites the other's"
    );

    require(
        looked_up_first->attempt_number == 1 &&
        looked_up_second->attempt_number == 2,
        "Each looked-up record reports its own attempt number"
    );
}

void test_unknown_attempt_is_absent() {
    ExecutionLogRegistry registry;

    registry.record(
        ExecutionLogRecord{
            "job-b",
            1,
            "worker-1",
            "/tmp/radahn-logs/execution-b-1",
            std::nullopt,
            std::nullopt,
            succeeded_result()
        }
    );

    require(
        !registry.find("job-b", 2).has_value(),
        "A different attempt number of a known job is not found"
    );

    require(
        !registry.find("job-c", 1).has_value(),
        "An entirely unknown job is not found"
    );
}

void test_sleep_workload_has_no_log_paths() {
    ExecutionLogRegistry registry;

    registry.record(
        ExecutionLogRecord{
            "job-sleep",
            1,
            "worker-1",
            std::filesystem::path{},
            std::nullopt,
            std::nullopt,
            succeeded_result()
        }
    );

    const auto record = registry.find("job-sleep", 1);

    require(
        record.has_value() &&
        !record->stdout_path.has_value() &&
        !record->stderr_path.has_value(),
        "A workload with no captured output retains absent log paths, "
        "not fabricated ones"
    );
}

}  // namespace

int main() {
    try {
        test_output_directories_are_unique_and_absolute();
        test_two_attempts_of_same_job_have_separate_logs();
        test_unknown_attempt_is_absent();
        test_sleep_workload_has_no_log_paths();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
