#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include "radahn/domain/workload.hpp"

namespace radahn::execution {

enum class ExecutionOutcome {
    exited,
    signaled,
    spawn_failed
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
};

// output_directory must be a new directory for this execution.
// Its parent directory must already exist.
[[nodiscard]]
ExecutionResult execute_command(
    const domain::WorkloadSpec& workload,
    const std::filesystem::path& output_directory
);

}  // namespace radahn::execution