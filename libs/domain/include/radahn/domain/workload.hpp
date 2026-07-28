#pragma once

#include <chrono>

namespace radahn::domain {

enum class WorkloadKind {
    sleep
};

class WorkloadSpec {
public:
    [[nodiscard]] static WorkloadSpec sleep(
        std::chrono::milliseconds duration
    );

    [[nodiscard]] WorkloadKind kind() const noexcept;

    [[nodiscard]] std::chrono::milliseconds
    sleep_duration() const noexcept;

private:
    WorkloadSpec(
        WorkloadKind kind,
        std::chrono::milliseconds sleep_duration
    );

    WorkloadKind kind_;
    std::chrono::milliseconds sleep_duration_;
};

}  // namespace radahn::domain