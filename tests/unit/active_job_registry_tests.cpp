#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string_view>

#include "active_job_registry.hpp"

namespace {

using radahn::worker_app::ActiveAttempt;
using radahn::worker_app::ActiveJobRegistry;

void require(bool condition, std::string_view description) {
    if (!condition) {
        throw std::runtime_error{std::string{description}};
    }

    std::cout << "[PASS] " << description << '\n';
}

[[nodiscard]] bool contains(
    const std::vector<ActiveAttempt>& attempts,
    const ActiveAttempt& needle
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

    const ActiveAttempt first_attempt{"job-a", 1};
    const ActiveAttempt second_attempt{"job-a", 2};

    registry.add(first_attempt);
    registry.add(second_attempt);

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

void test_duplicate_add_is_rejected() {
    ActiveJobRegistry registry;

    const ActiveAttempt attempt{"job-b", 1};

    registry.add(attempt);
    registry.add(attempt);

    require(
        registry.size() == 1,
        "An exact duplicate add is rejected rather than tracked twice"
    );
}

void test_remove_matches_exact_identity_only() {
    ActiveJobRegistry registry;

    const ActiveAttempt attempt_one{"job-c", 1};
    const ActiveAttempt attempt_two{"job-c", 2};

    registry.add(attempt_one);

    // Removing a different attempt number of the same job ID must
    // not remove the attempt that is actually registered.
    registry.remove(attempt_two);

    require(
        registry.size() == 1,
        "Removing a non-matching attempt number leaves the "
        "registered attempt untouched"
    );

    registry.remove(attempt_one);

    require(
        registry.size() == 0,
        "Removing the exact matching attempt empties the registry"
    );
}

}  // namespace

int main() {
    try {
        test_attempt_identity_and_removal();
        test_duplicate_add_is_rejected();
        test_remove_matches_exact_identity_only();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
