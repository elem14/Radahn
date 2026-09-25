#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include "radahn/execution/command_executor.hpp"

namespace {

using namespace std::chrono_literals;

using radahn::domain::WorkloadSpec;
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

    bool timeout_rejected = false;

    try {
        (void)execute_command(
            WorkloadSpec::command({
                "/usr/bin/printf",
                {"hello"},
                1s
            }),
            temporary.path / "timeout"
        );
    } catch (const std::invalid_argument&) {
        timeout_rejected = true;
    }

    require(
        timeout_rejected,
        "Unsupported timeout is rejected rather than ignored"
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

}  // namespace

int main() {
    try {
        test_executor();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr
            << "[FAIL] "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }
}