#pragma once

#include <string_view>

namespace radahn::domain {

enum class JobState {
    queued,
    leased,
    running,
    retry_wait,
    cancellation_requested,
    succeeded,
    failed,
    cancelled
};

class JobStateMachine {
public:
    [[nodiscard]] static bool can_transition(
        JobState from,
        JobState to
    ) noexcept;

    static void validate_transition(
        JobState from,
        JobState to
    );
};

[[nodiscard]] std::string_view to_string(
    JobState state
) noexcept;

[[nodiscard]] bool is_terminal(
    JobState state
) noexcept;

} // namespace radahn::domain