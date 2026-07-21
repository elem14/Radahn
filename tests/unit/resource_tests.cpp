#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <vector>
#include <utility>

#include "radahn/domain/id.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_eligibility.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t gibibyte =
    1024ULL * 1024ULL * 1024ULL;

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

template <typename T>
bool contains(
    const std::vector<T>& values,
    const T& target
) {
    return std::find(
        values.begin(),
        values.end(),
        target
    ) != values.end();
}

radahn::domain::WorkerSnapshot make_worker(
    radahn::domain::WorkerState state,
    double available_cpu,
    std::uint64_t available_memory,
    std::uint64_t available_disk,
    bool gpu_available,
    std::size_t running_jobs,
    std::size_t max_concurrent_jobs,
    std::vector<std::string> tags
) {
    using radahn::domain::WorkerId;
    using radahn::domain::WorkerResources;
    using radahn::domain::WorkerSnapshot;

    return WorkerSnapshot{
        WorkerId{"worker-1"},
        state,
        WorkerResources{
            8.0,
            available_cpu,
            16ULL * gibibyte,
            available_memory,
            500ULL * gibibyte,
            available_disk,
            gpu_available
        },
        running_jobs,
        max_concurrent_jobs,
        std::move(tags)
    };
}

void test_eligible_worker() {
    using radahn::domain::ResourceRequirements;
    using radahn::domain::WorkerState;
    using radahn::domain::evaluate_worker;

    const ResourceRequirements requirements{
        2.0,
        2ULL * gibibyte,
        5ULL * gibibyte,
        false,
        {"linux", "x86_64"}
    };

    const auto worker = make_worker(
        WorkerState::online,
        6.0,
        12ULL * gibibyte,
        300ULL * gibibyte,
        false,
        1,
        4,
        {"linux", "x86_64", "high-cpu"}
    );

    const auto result = evaluate_worker(
        requirements,
        worker
    );

    expect(
        result.eligible(),
        "Eligible worker is accepted"
    );

    expect(
        result.failures().empty(),
        "Eligible worker has no failure reasons"
    );
}

void test_insufficient_resources() {
    using radahn::domain::EligibilityFailure;
    using radahn::domain::ResourceRequirements;
    using radahn::domain::WorkerState;
    using radahn::domain::evaluate_worker;

    const ResourceRequirements requirements{
        6.0,
        8ULL * gibibyte,
        100ULL * gibibyte,
        false
    };

    const auto worker = make_worker(
        WorkerState::online,
        2.0,
        4ULL * gibibyte,
        20ULL * gibibyte,
        false,
        0,
        4,
        {"linux"}
    );

    const auto result = evaluate_worker(
        requirements,
        worker
    );

    expect(
        !result.eligible(),
        "Worker with insufficient resources is rejected"
    );

    expect(
        contains(
            result.failures(),
            EligibilityFailure::insufficient_cpu
        ),
        "Insufficient CPU is reported"
    );

    expect(
        contains(
            result.failures(),
            EligibilityFailure::insufficient_memory
        ),
        "Insufficient memory is reported"
    );

    expect(
        contains(
            result.failures(),
            EligibilityFailure::insufficient_disk
        ),
        "Insufficient disk is reported"
    );
}

void test_gpu_requirement() {
    using radahn::domain::EligibilityFailure;
    using radahn::domain::ResourceRequirements;
    using radahn::domain::WorkerState;
    using radahn::domain::evaluate_worker;

    const ResourceRequirements requirements{
        2.0,
        1ULL * gibibyte,
        1ULL * gibibyte,
        true
    };

    const auto worker = make_worker(
        WorkerState::online,
        6.0,
        12ULL * gibibyte,
        300ULL * gibibyte,
        false,
        0,
        4,
        {"linux"}
    );

    const auto result = evaluate_worker(
        requirements,
        worker
    );

    expect(
        contains(
            result.failures(),
            EligibilityFailure::gpu_unavailable
        ),
        "Missing GPU is reported"
    );
}

void test_required_tags() {
    using radahn::domain::EligibilityFailure;
    using radahn::domain::ResourceRequirements;
    using radahn::domain::WorkerState;
    using radahn::domain::evaluate_worker;

    const ResourceRequirements requirements{
        1.0,
        1ULL * gibibyte,
        1ULL * gibibyte,
        false,
        {"linux", "arm64"}
    };

    const auto worker = make_worker(
        WorkerState::online,
        6.0,
        12ULL * gibibyte,
        300ULL * gibibyte,
        false,
        0,
        4,
        {"linux", "x86_64"}
    );

    const auto result = evaluate_worker(
        requirements,
        worker
    );

    expect(
        contains(
            result.failures(),
            EligibilityFailure::missing_required_tag
        ),
        "Missing required tag is reported"
    );

    expect(
        result.missing_tags().size() == 1,
        "Exactly one tag is missing"
    );

    expect(
        result.missing_tags().front() == "arm64",
        "Missing tag name is preserved"
    );
}

void test_worker_state_and_capacity() {
    using radahn::domain::EligibilityFailure;
    using radahn::domain::ResourceRequirements;
    using radahn::domain::WorkerState;
    using radahn::domain::evaluate_worker;

    const ResourceRequirements requirements{
        1.0,
        1ULL * gibibyte,
        1ULL * gibibyte,
        false
    };

    const auto draining_worker = make_worker(
        WorkerState::draining,
        6.0,
        12ULL * gibibyte,
        300ULL * gibibyte,
        false,
        0,
        4,
        {"linux"}
    );

    const auto full_worker = make_worker(
        WorkerState::online,
        6.0,
        12ULL * gibibyte,
        300ULL * gibibyte,
        false,
        4,
        4,
        {"linux"}
    );

    const auto draining_result = evaluate_worker(
        requirements,
        draining_worker
    );

    const auto full_result = evaluate_worker(
        requirements,
        full_worker
    );

    expect(
        contains(
            draining_result.failures(),
            EligibilityFailure::worker_draining
        ),
        "Draining worker is rejected"
    );

    expect(
        contains(
            full_result.failures(),
            EligibilityFailure::no_execution_slot
        ),
        "Worker at concurrency limit is rejected"
    );
}

void test_invalid_resource_models() {
    using radahn::domain::ResourceRequirements;
    using radahn::domain::WorkerResources;

    bool invalid_requirement_rejected = false;

    try {
        const ResourceRequirements invalid{
            0.0,
            0,
            0,
            false
        };

        static_cast<void>(invalid);
    } catch (const std::invalid_argument&) {
        invalid_requirement_rejected = true;
    }

    expect(
        invalid_requirement_rejected,
        "Zero required CPU is rejected"
    );

    bool invalid_worker_resources_rejected = false;

    try {
        const WorkerResources invalid{
            8.0,
            10.0,
            16ULL * gibibyte,
            8ULL * gibibyte,
            100ULL * gibibyte,
            50ULL * gibibyte,
            false
        };

        static_cast<void>(invalid);
    } catch (const std::invalid_argument&) {
        invalid_worker_resources_rejected = true;
    }

    expect(
        invalid_worker_resources_rejected,
        "Available CPU exceeding total CPU is rejected"
    );
}

}  // namespace

int main() {
    test_eligible_worker();
    test_insufficient_resources();
    test_gpu_requirement();
    test_required_tags();
    test_worker_state_and_capacity();
    test_invalid_resource_models();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " test assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout << "\nAll resource tests passed\n";
    return EXIT_SUCCESS;
}