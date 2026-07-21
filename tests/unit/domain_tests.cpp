#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string_view>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job_state.hpp"

namespace {

int failure_count = 0;

void expect(
    bool condition,
    std::string_view description
) {
    if (condition) {
        std::cout << "[PASS] " << description << '\n';
        return;
    }

    std::cerr << "[FAIL] " << description << '\n';
    ++failure_count;
}

void test_strong_ids() {
    using radahn::domain::JobId;
    using radahn::domain::WorkerId;

    const JobId first_job{"job-123"};
    const JobId same_job{"job-123"};
    const JobId different_job{"job-456"};
    const WorkerId worker{"worker-123"};

    expect(
        first_job.value() == "job-123",
        "JobId preserves its underlying value"
    );

    expect(
        first_job == same_job,
        "Equivalent JobIds compare as equal"
    );

    expect(
        first_job != different_job,
        "Different JobIds compare as unequal"
    );

    expect(
        worker.value() == "worker-123",
        "WorkerId preserves its underlying value"
    );

    bool empty_id_rejected = false;

    try {
        const JobId invalid{""};
        static_cast<void>(invalid);
    } catch (const std::invalid_argument&) {
        empty_id_rejected = true;
    }

    expect(
        empty_id_rejected,
        "Empty identifiers are rejected"
    );
}

void test_valid_job_transitions() {
    using radahn::domain::JobState;
    using radahn::domain::JobStateMachine;

    expect(
        JobStateMachine::can_transition(
            JobState::queued,
            JobState::leased
        ),
        "Queued job may become leased"
    );

    expect(
        JobStateMachine::can_transition(
            JobState::leased,
            JobState::running
        ),
        "Leased job may begin running"
    );

    expect(
        JobStateMachine::can_transition(
            JobState::running,
            JobState::succeeded
        ),
        "Running job may succeed"
    );

    expect(
        JobStateMachine::can_transition(
            JobState::running,
            JobState::retry_wait
        ),
        "Running job may wait for retry"
    );

    expect(
        JobStateMachine::can_transition(
            JobState::retry_wait,
            JobState::queued
        ),
        "Retrying job may return to queue"
    );
}

void test_invalid_job_transitions() {
    using radahn::domain::JobState;
    using radahn::domain::JobStateMachine;

    expect(
        !JobStateMachine::can_transition(
            JobState::queued,
            JobState::succeeded
        ),
        "Queued job cannot skip directly to succeeded"
    );

    expect(
        !JobStateMachine::can_transition(
            JobState::succeeded,
            JobState::running
        ),
        "Succeeded job cannot return to running"
    );

    expect(
        !JobStateMachine::can_transition(
            JobState::failed,
            JobState::queued
        ),
        "Failed job cannot be automatically requeued"
    );

    bool invalid_transition_rejected = false;

    try {
        JobStateMachine::validate_transition(
            JobState::succeeded,
            JobState::running
        );
    } catch (const std::logic_error&) {
        invalid_transition_rejected = true;
    }

    expect(
        invalid_transition_rejected,
        "Validator rejects illegal transitions"
    );
}

void test_terminal_states() {
    using radahn::domain::JobState;
    using radahn::domain::is_terminal;

    expect(
        is_terminal(JobState::succeeded),
        "Succeeded is terminal"
    );

    expect(
        is_terminal(JobState::failed),
        "Failed is terminal"
    );

    expect(
        is_terminal(JobState::cancelled),
        "Cancelled is terminal"
    );

    expect(
        !is_terminal(JobState::running),
        "Running is not terminal"
    );
}

}  // namespace

int main() {
    test_strong_ids();
    test_valid_job_transitions();
    test_invalid_job_transitions();
    test_terminal_states();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " test assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout << "\nAll domain tests passed\n";
    return EXIT_SUCCESS;
}