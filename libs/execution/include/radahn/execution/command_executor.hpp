#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>

#include "radahn/domain/execution_result.hpp"
#include "radahn/domain/workload.hpp"

namespace radahn::execution {

// Preserve the existing executor-facing names.
using ExecutionOutcome = domain::ExecutionOutcome;
using ExecutionResult = domain::ExecutionResult;

/*
 * A minimal, non-owning stand-in for std::stop_token.
 *
 * The standard facility (std::stop_token / std::jthread) is not yet
 * implemented by the libc++ shipped with this toolchain, so this
 * narrow substitute covers exactly what execute_command needs: a
 * cheap, copyable "has someone asked me to stop?" check backed by a
 * caller-owned atomic flag. A default-constructed token is bound to
 * no flag at all, so stop_requested() is always false for it - the
 * default control therefore never asks for an early stop, leaving
 * an un-timed-out execution to simply run to completion as before.
 */
class StopToken {
public:
    StopToken() noexcept = default;

    explicit StopToken(
        const std::atomic<bool>* requested
    ) noexcept
        : requested_{requested} {
    }

    [[nodiscard]]
    bool stop_requested() const noexcept {
        return
            requested_ != nullptr &&
            requested_->load();
    }

private:
    const std::atomic<bool>* requested_ = nullptr;
};

/*
 * Input controlling how an in-flight execution may be interrupted.
 */
struct ExecutionControl {
    StopToken stop_token{};

    // How long a terminated process (and any descendants left in
    // its process group) is given to exit after SIGTERM before
    // SIGKILL is sent.
    std::chrono::milliseconds termination_grace{2000};
};

[[nodiscard]]
ExecutionResult execute_command(
    const domain::WorkloadSpec& workload,
    const std::filesystem::path& output_directory,
    ExecutionControl control = ExecutionControl{}
);

}  // namespace radahn::execution