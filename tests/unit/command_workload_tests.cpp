#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "radahn/domain/workload.hpp"

namespace {

using namespace std::chrono_literals;
using radahn::domain::WorkloadKind;
using radahn::domain::WorkloadSpec;

int failure_count = 0;

void expect(
    bool condition,
    std::string_view description
) {
    if (condition) {
        std::cout << "[PASS] " << description << '\n';
    } else {
        std::cerr << "[FAIL] " << description << '\n';
        ++failure_count;
    }
}

template<class ExpectedException, class Action>
void expect_throws(
    Action action,
    std::string_view description
) {
    try {
        action();
    } catch (const ExpectedException&) {
        expect(true, description);
        return;
    } catch (...) {
        expect(false, description);
        return;
    }

    expect(false, description);
}

void test_valid_command() {
    const std::vector<std::string> args{
        "simulation.py",
        "--label",
        "trial one",
        "",
        "repeat",
        "repeat"
    };

    const auto workload = WorkloadSpec::command({
        "python3",
        args,
        30s
    });

    expect(
        workload.kind() == WorkloadKind::command,
        "Command has the correct workload kind"
    );

    const auto& command = workload.command_spec();

    expect(
        command.executable == "python3",
        "Executable is preserved"
    );

    expect(
        command.args == args,
        "Argument order, spaces, empty values and duplicates are preserved"
    );

    expect(
        command.timeout == 30s,
        "Timeout is preserved"
    );

    expect(
        workload.sleep_duration() == 0ms,
        "Command has no sleep duration"
    );

    const auto without_timeout = WorkloadSpec::command({
        "hostname",
        {},
        std::nullopt
    });

    expect(
        !without_timeout.command_spec().timeout.has_value(),
        "Command can omit its timeout"
    );

    expect(
        without_timeout.command_spec().args.empty(),
        "Command can have no arguments"
    );
}

void test_invalid_commands() {
    expect_throws<std::invalid_argument>(
        [] {
            (void)WorkloadSpec::command({
                "",
                {},
                std::nullopt
            });
        },
        "Empty executable is rejected"
    );

    expect_throws<std::invalid_argument>(
        [] {
            (void)WorkloadSpec::command({
                std::string{"py\0thon", 7},
                {},
                std::nullopt
            });
        },
        "Embedded NUL in executable is rejected"
    );

    expect_throws<std::invalid_argument>(
        [] {
            (void)WorkloadSpec::command({
                "python3",
                {std::string{"a\0b", 3}},
                std::nullopt
            });
        },
        "Embedded NUL in argument is rejected"
    );

    expect_throws<std::invalid_argument>(
        [] {
            (void)WorkloadSpec::command({
                "python3",
                {},
                0ms
            });
        },
        "Zero timeout is rejected"
    );

    expect_throws<std::invalid_argument>(
        [] {
            (void)WorkloadSpec::command({
                "python3",
                {},
                -1ms
            });
        },
        "Negative timeout is rejected"
    );
}

void test_sleep_compatibility() {
    const auto workload = WorkloadSpec::sleep(250ms);

    expect(
        workload.kind() == WorkloadKind::sleep,
        "Sleep has the correct workload kind"
    );

    expect(
        workload.sleep_duration() == 250ms,
        "Sleep duration is preserved"
    );

    expect_throws<std::logic_error>(
        [&workload] {
            (void)workload.command_spec();
        },
        "Reading command settings from sleep is rejected"
    );

    expect_throws<std::invalid_argument>(
        [] {
            (void)WorkloadSpec::sleep(0ms);
        },
        "Zero sleep duration remains invalid"
    );
}

}  // namespace

int main() {
    try {
        test_valid_command();
        test_invalid_commands();
        test_sleep_compatibility();
    } catch (const std::exception& error) {
        std::cerr
            << "[FAIL] Unexpected exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    return failure_count == 0
        ? EXIT_SUCCESS
        : EXIT_FAILURE;
}