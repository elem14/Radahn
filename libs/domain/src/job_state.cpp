#include "radahn/domain/job_state.hpp"

#include <stdexcept>
#include <string>

namespace radahn::domain {

bool JobStateMachine::can_transition(
    JobState from,
    JobState to
) noexcept {
    switch (from) {
        case JobState::queued:
            return to == JobState::leased ||
                   to == JobState::cancelled;

        case JobState::leased:
            return to == JobState::running ||
                   to == JobState::retry_wait ||
                   to == JobState::cancelled;

        case JobState::running:
            return to == JobState::succeeded ||
                   to == JobState::failed ||
                   to == JobState::retry_wait ||
                   to == JobState::cancellation_requested;

        case JobState::retry_wait:
            return to == JobState::queued ||
                   to == JobState::failed ||
                   to == JobState::cancelled;

        case JobState::cancellation_requested:
            return to == JobState::cancelled ||
                   to == JobState::succeeded ||
                   to == JobState::failed;

        case JobState::succeeded:
        case JobState::failed:
        case JobState::cancelled:
            return false;
    }

    return false;
}

void JobStateMachine::validate_transition(
    JobState from,
    JobState to
) {
    if (can_transition(from, to)) {
        return;
    }

    throw std::logic_error{
        "Illegal job transition from " +
        std::string{to_string(from)} +
        " to " +
        std::string{to_string(to)}
    };
}

std::string_view to_string(JobState state) noexcept {
    switch (state) {
        case JobState::queued:
            return "QUEUED";

        case JobState::leased:
            return "LEASED";

        case JobState::running:
            return "RUNNING";

        case JobState::retry_wait:
            return "RETRY_WAIT";

        case JobState::cancellation_requested:
            return "CANCELLATION_REQUESTED";

        case JobState::succeeded:
            return "SUCCEEDED";

        case JobState::failed:
            return "FAILED";

        case JobState::cancelled:
            return "CANCELLED";
    }

    return "UNKNOWN";
}

bool is_terminal(JobState state) noexcept {
    return state == JobState::succeeded ||
           state == JobState::failed ||
           state == JobState::cancelled;
}

}  // namespace radahn::domain