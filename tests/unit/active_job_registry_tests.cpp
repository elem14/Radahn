#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string_view>

#include "active_job_registry.hpp"

namespace {

using radahn::worker_app::ActiveJobRegistry;
using radahn::worker_app::AttemptControlPtr;
using radahn::worker_app::AttemptPhase;

void require(bool condition, std::string_view description) {
    if (!condition) {
        throw std::runtime_error{std::string{description}};
    }

    std::cout << "[PASS] " << description << '\n';
}

[[nodiscard]] bool contains(
    const std::vector<AttemptControlPtr>& attempts,
    const AttemptControlPtr& needle
) {
    for (const auto& attempt : attempts) {
        if (attempt == needle) {
            return true;
        }
    }

    return false;
}

void test_attempt_identity_and_removal() {
    ActiveJobRegistry registry;

    const auto first_attempt = registry.add("job-a", 1);
    const auto second_attempt = registry.add("job-a", 2);

    require(
        registry.size() == 2,
        "Two distinct attempts of the same job are both tracked"
    );

    registry.remove(first_attempt);

    const auto after_removal = registry.snapshot();

    require(
        after_removal.size() == 1,
        "Removing one attempt leaves exactly one behind"
    );

    require(
        contains(after_removal, second_attempt),
        "The surviving attempt is the newer one, not the removed one"
    );

    require(
        !contains(after_removal, first_attempt),
        "An old attempt's removal does not linger"
    );
}

void test_two_attempts_have_independent_identity() {
    ActiveJobRegistry registry;

    const auto attempt_one = registry.add("job-c", 1);
    const auto attempt_two = registry.add("job-c", 2);

    require(
        attempt_one->job_id == attempt_two->job_id &&
        attempt_one->attempt_number !=
            attempt_two->attempt_number,
        "Two attempts of the same job carry distinct attempt numbers"
    );

    // Removing a different attempt's control block must not remove
    // the one actually registered.
    registry.remove(attempt_two);

    require(
        registry.size() == 1,
        "Removing a non-matching attempt leaves the registered "
        "attempt untouched"
    );

    registry.remove(attempt_one);

    require(
        registry.size() == 0,
        "Removing the exact matching attempt empties the registry"
    );
}

void test_attempt_control_defaults() {
    ActiveJobRegistry registry;

    const auto control = registry.add("job-d", 1);

    require(
        control->phase.load() == AttemptPhase::executing,
        "A newly added attempt starts in the executing phase"
    );

    require(
        !control->stop_was_requested(),
        "A newly added attempt has not been asked to stop"
    );

    control->request_stop();

    require(
        control->stop_was_requested(),
        "request_stop() is observable through stop_was_requested()"
    );

    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds{5};

    control->lease_deadline.store(deadline);

    require(
        control->lease_deadline.load() == deadline,
        "The lease deadline round-trips through the atomic holder"
    );
}

void test_attempt_control_isolation() {
    // A response concerning one attempt must never extend or stop a
    // different attempt, even of the same job.
    ActiveJobRegistry registry;

    const auto attempt_one = registry.add("job-e", 1);
    const auto attempt_two = registry.add("job-e", 2);

    attempt_one->request_stop();

    require(
        attempt_one->stop_was_requested() &&
        !attempt_two->stop_was_requested(),
        "Stopping one attempt does not stop a sibling attempt"
    );

    const auto deadline =
        std::chrono::steady_clock::now() +
        std::chrono::seconds{10};

    attempt_two->lease_deadline.store(deadline);

    require(
        attempt_two->lease_deadline.load() == deadline &&
        attempt_one->lease_deadline.load() != deadline,
        "Extending one attempt's deadline does not extend a "
        "sibling attempt's deadline"
    );
}

}  // namespace

int main() {
    try {
        test_attempt_identity_and_removal();
        test_two_attempts_have_independent_identity();
        test_attempt_control_defaults();
        test_attempt_control_isolation();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
