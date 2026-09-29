#pragma once

#include <optional>
#include <stdexcept>
#include <string>

namespace radahn::domain {

// Explicit values are used by persistence and protobuf.
// Keep existing values stable.
enum class ExecutionOutcome {
    exited = 1,
    signaled = 2,
    spawn_failed = 3,
    worker_error = 4
};

struct ExecutionResult {
    ExecutionOutcome outcome;

    std::optional<int> exit_code;
    std::optional<int> terminating_signal;

    std::string diagnostic;

    [[nodiscard]]
    bool succeeded() const noexcept {
        return
            outcome == ExecutionOutcome::exited &&
            exit_code == 0;
    }

    void validate() const {
        switch (outcome) {
            case ExecutionOutcome::exited:
                if (
                    !exit_code.has_value() ||
                    *exit_code < 0 ||
                    *exit_code > 255 ||
                    terminating_signal.has_value()
                ) {
                    throw std::invalid_argument{
                        "Exited result requires an exit code "
                        "from 0 to 255 and no signal"
                    };
                }
                break;

            case ExecutionOutcome::signaled:
                if (
                    exit_code.has_value() ||
                    !terminating_signal.has_value() ||
                    *terminating_signal <= 0
                ) {
                    throw std::invalid_argument{
                        "Signaled result requires a positive "
                        "signal number and no exit code"
                    };
                }
                break;

            case ExecutionOutcome::spawn_failed:
            case ExecutionOutcome::worker_error:
                if (
                    exit_code.has_value() ||
                    terminating_signal.has_value()
                ) {
                    throw std::invalid_argument{
                        "Execution error cannot contain "
                        "an exit code or terminating signal"
                    };
                }
                break;

            default:
                throw std::invalid_argument{
                    "Unknown execution outcome"
                };
        }
    }
};

}  // namespace radahn::domain