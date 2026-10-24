#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

#include <sys/types.h>

#include "radahn/execution/command_executor.hpp"

namespace {

using namespace std::chrono_literals;

using radahn::domain::WorkloadSpec;
using radahn::execution::ExecutionControl;
using radahn::execution::ExecutionOutcome;
using radahn::execution::execute_command;

void require(
    bool condition,
    std::string_view description
) {
    if (!condition) {
        throw std::runtime_error{
            std::string{description}
        };
    }

    std::cout << "[PASS] " << description << '\n';
}

class TemporaryDirectory {
public:
    TemporaryDirectory()
        : path{
            std::filesystem::temp_directory_path() /
            (
                "radahn-executor-test-" +
                std::to_string(
                    std::chrono::steady_clock::now()
                        .time_since_epoch()
                        .count()
                )
            )
        } {
        if (!std::filesystem::create_directory(path)) {
            throw std::runtime_error{
                "Could not create temporary directory"
            };
        }
    }

    ~TemporaryDirectory() {
        std::error_code ignored;

        std::filesystem::remove_all(
            path,
            ignored
        );
    }

    std::filesystem::path path;
};

std::string read_file(
    const std::filesystem::path& path
) {
    std::ifstream input{
        path,
        std::ios::binary
    };

    if (!input) {
        throw std::runtime_error{
            "Could not open captured output"
        };
    }

    return std::string{
        std::istreambuf_iterator<char>{input},
        std::istreambuf_iterator<char>{}
    };
}

void test_executor() {
    TemporaryDirectory temporary;

    const auto success_directory =
        temporary.path / "success";

    const auto success = execute_command(
        WorkloadSpec::command({
            "/usr/bin/printf",
            {"<%s>\\n", "hello world", "", "$(not-expanded)"},
            std::nullopt
        }),
        success_directory
    );

    require(
        success.succeeded(),
        "Real executable exits successfully"
    );

    require(
        read_file(success_directory / "stdout.log") ==
            "<hello world>\n<>\n<$(not-expanded)>\n",
        "Arguments preserve spaces and empties without shell expansion"
    );

    require(
        read_file(success_directory / "stderr.log").empty(),
        "Successful command has empty stderr"
    );

    const auto failure_directory =
        temporary.path / "failure";

    const auto failure = execute_command(
        WorkloadSpec::command({
            "/bin/sh",
            {"-c", "printf 'problem\\n' >&2; exit 7"},
            std::nullopt
        }),
        failure_directory
    );

    require(
        failure.outcome == ExecutionOutcome::exited &&
        failure.exit_code == 7 &&
        !failure.succeeded(),
        "Nonzero program exit is reported accurately"
    );

    require(
        read_file(failure_directory / "stderr.log") ==
            "problem\n",
        "Stderr is captured separately"
    );

    const auto signaled = execute_command(
        WorkloadSpec::command({
            "/bin/sh",
            {"-c", "kill -TERM $$"},
            std::nullopt
        }),
        temporary.path / "signal"
    );

    require(
        signaled.outcome == ExecutionOutcome::signaled &&
        signaled.terminating_signal == SIGTERM &&
        !signaled.exit_code.has_value(),
        "Signal termination is distinct from normal exit"
    );

    const auto missing = execute_command(
        WorkloadSpec::command({
            (temporary.path / "missing-executable").string(),
            {},
            std::nullopt
        }),
        temporary.path / "missing"
    );

    require(
        missing.outcome == ExecutionOutcome::spawn_failed ||
        (
            missing.outcome == ExecutionOutcome::exited &&
            missing.exit_code == 127
        ),
        "Missing executable reports a launch failure"
    );

    bool reuse_rejected = false;

    try {
        (void)execute_command(
            WorkloadSpec::command({
                "/usr/bin/printf",
                {"replacement"},
                std::nullopt
            }),
            success_directory
        );
    } catch (const std::invalid_argument&) {
        reuse_rejected = true;
    }

    require(
        reuse_rejected,
        "Existing execution directory cannot be reused"
    );

    require(
        read_file(success_directory / "stdout.log") ==
            "<hello world>\n<>\n<$(not-expanded)>\n",
        "Rejected directory reuse preserves previous output"
    );
}

[[nodiscard]]
bool process_exists(pid_t pid) {
    return ::kill(pid, 0) == 0;
}

/*
 * Polls up to a short bound for a condition that should become
 * true shortly after a signal is delivered (e.g. a reparented
 * descendant being reaped by its new parent), rather than assuming
 * it holds the instant our own call returns.
 */
void wait_until(
    const std::function<bool()>& condition,
    std::chrono::milliseconds bound
) {
    const auto deadline =
        std::chrono::steady_clock::now() + bound;

    while (
        !condition() &&
        std::chrono::steady_clock::now() < deadline
    ) {
        std::this_thread::sleep_for(10ms);
    }
}

[[nodiscard]]
pid_t read_pid_file(
    const std::filesystem::path& path
) {
    wait_until(
        [&path] {
            return std::filesystem::exists(path);
        },
        2s
    );

    std::ifstream input{path};

    if (!input) {
        throw std::runtime_error{
            "Could not open pid file"
        };
    }

    long long value = 0;
    input >> value;

    return static_cast<pid_t>(value);
}

void test_timeout_enforcement() {
    TemporaryDirectory temporary;

    const auto start = std::chrono::steady_clock::now();

    const auto result = execute_command(
        WorkloadSpec::command({
            "/bin/sleep",
            {"5"},
            200ms
        }),
        temporary.path / "timeout"
    );

    const auto elapsed =
        std::chrono::steady_clock::now() - start;

    require(
        result.outcome == ExecutionOutcome::timed_out &&
        !result.exit_code.has_value() &&
        !result.terminating_signal.has_value(),
        "A command exceeding its timeout is reported as TIMED_OUT"
    );

    require(
        elapsed < 4s,
        "Timeout enforcement stops the process well before it "
        "would finish on its own"
    );
}

void test_sigterm_ignoring_process_is_killed_after_grace() {
    TemporaryDirectory temporary;
    const auto pid_file =
        temporary.path / "ignoring-pid";

    const auto start = std::chrono::steady_clock::now();

    const auto result = execute_command(
        WorkloadSpec::command({
            "/bin/sh",
            {
                "-c",
                "trap '' TERM; echo $$ > '" +
                    pid_file.string() +
                    "'; sleep 5"
            },
            150ms
        }),
        temporary.path / "ignoring",
        ExecutionControl{
            {},
            std::chrono::milliseconds{300}
        }
    );

    const auto elapsed =
        std::chrono::steady_clock::now() - start;

    require(
        result.outcome == ExecutionOutcome::timed_out,
        "A SIGTERM-ignoring process still reports TIMED_OUT"
    );

    require(
        elapsed >= 150ms && elapsed < 4s,
        "The ignoring process survives until escalation, not "
        "until it would exit on its own"
    );

    const auto child_pid = read_pid_file(pid_file);

    require(
        !process_exists(child_pid),
        "The SIGTERM-ignoring process is gone (SIGKILL escalated) "
        "and left no zombie behind"
    );
}

void test_descendant_in_process_group_is_terminated() {
    TemporaryDirectory temporary;
    const auto pid_file =
        temporary.path / "descendant-pid";

    const auto result = execute_command(
        WorkloadSpec::command({
            "/bin/sh",
            {
                "-c",
                "sleep 5 & echo $! > '" +
                    pid_file.string() +
                    "'; wait"
            },
            150ms
        }),
        temporary.path / "descendant"
    );

    require(
        result.outcome == ExecutionOutcome::timed_out,
        "The parent shell reports TIMED_OUT"
    );

    const auto descendant_pid = read_pid_file(pid_file);

    wait_until(
        [descendant_pid] {
            return !process_exists(descendant_pid);
        },
        2s
    );

    require(
        !process_exists(descendant_pid),
        "An ordinary child left in the same process group is "
        "also terminated, not merely the direct child"
    );
}

void test_stop_token_triggers_worker_stopped() {
    TemporaryDirectory temporary;

    std::atomic<bool> stop_requested{false};

    std::thread requester{[&stop_requested] {
        std::this_thread::sleep_for(150ms);
        stop_requested.store(true);
    }};

    const auto result = execute_command(
        WorkloadSpec::command({
            "/bin/sleep",
            {"5"},
            std::nullopt
        }),
        temporary.path / "stop-token",
        ExecutionControl{
            radahn::execution::StopToken{&stop_requested},
            std::chrono::milliseconds{300}
        }
    );

    requester.join();

    require(
        result.outcome == ExecutionOutcome::worker_stopped &&
        !result.exit_code.has_value() &&
        !result.terminating_signal.has_value(),
        "A requested stop reports WORKER_STOPPED, "
        "independent of any configured timeout"
    );
}

}  // namespace

int main() {
    try {
        test_executor();
        test_timeout_enforcement();
        test_stop_token_triggers_worker_stopped();
        test_sigterm_ignoring_process_is_killed_after_grace();
        test_descendant_in_process_group_is_terminated();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr
            << "[FAIL] "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }
}