#pragma once

#include <chrono>
#include <optional>
#include <string>
#include <vector>

namespace radahn::domain {

enum class WorkloadKind {
    sleep,
    command
};

struct CommandWorkload {
    std::string executable;
    std::vector<std::string> args;
    std::optional<std::chrono::milliseconds> timeout;
};

class WorkloadSpec {
public:
    [[nodiscard]]
    static WorkloadSpec sleep(
        std::chrono::milliseconds duration
    );

    [[nodiscard]]
    static WorkloadSpec command(
        CommandWorkload command
    );

    [[nodiscard]]
    WorkloadKind kind() const noexcept;

    [[nodiscard]]
    std::chrono::milliseconds
    sleep_duration() const noexcept;

    [[nodiscard]]
    const CommandWorkload& command_spec() const;

private:
    explicit WorkloadSpec(
        std::chrono::milliseconds duration
    );

    explicit WorkloadSpec(
        CommandWorkload command
    );

    WorkloadKind kind_;

    std::chrono::milliseconds sleep_duration_{0};

    std::optional<CommandWorkload> command_;
};

}  // namespace radahn::domain