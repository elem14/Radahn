#pragma once

#include <filesystem>

#include "radahn/domain/execution_result.hpp"
#include "radahn/domain/workload.hpp"

namespace radahn::execution {

// Preserve the existing executor-facing names.
using ExecutionOutcome = domain::ExecutionOutcome;
using ExecutionResult = domain::ExecutionResult;

[[nodiscard]]
ExecutionResult execute_command(
    const domain::WorkloadSpec& workload,
    const std::filesystem::path& output_directory
);

}  // namespace radahn::execution