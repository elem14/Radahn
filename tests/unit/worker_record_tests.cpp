#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string_view>

#include "radahn/domain/id.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t gibibyte =
    1024ULL * 1024ULL * 1024ULL;

void expect(
    bool condition,
    std::string_view description
) {
    if (condition) {
        std::cout
            << "[PASS] "
            << description
            << '\n';

        return;
    }

    std::cerr
        << "[FAIL] "
        << description
        << '\n';

    ++failure_count;
}

radahn::domain::WorkerRecord make_worker() {
    using radahn::domain::WorkerId;
    using radahn::domain::WorkerRecord;
    using radahn::domain::WorkerResources;
    using radahn::domain::WorkerSnapshot;
    using radahn::domain::WorkerState;

    return WorkerRecord{
        WorkerSnapshot{
            WorkerId{"worker-1"},
            WorkerState::online,
            WorkerResources{
                8.0,
                6.0,
                16ULL * gibibyte,
                12ULL * gibibyte,
                500ULL * gibibyte,
                300ULL * gibibyte,
                false
            },
            1,
            4,
            {"linux", "x86_64"}
        }
    };
}

radahn::domain::ResourceRequirements
make_requirements() {
    return radahn::domain::ResourceRequirements{
        2.0,
        2ULL * gibibyte,
        5ULL * gibibyte,
        false,
        {"linux"}
    };
}

void test_reservation_reduces_resources() {
    auto worker = make_worker();

    worker.reserve(make_requirements());

    const auto snapshot = worker.snapshot();

    expect(
        snapshot.resources().available_cpu_cores() ==
            4.0,
        "Reservation reduces available CPU"
    );

    expect(
        snapshot.resources().available_memory_bytes() ==
            10ULL * gibibyte,
        "Reservation reduces available memory"
    );

    expect(
        snapshot.resources().available_disk_bytes() ==
            295ULL * gibibyte,
        "Reservation reduces available disk"
    );

    expect(
        snapshot.running_jobs() == 2,
        "Reservation increases running job count"
    );
}

void test_release_restores_resources() {
    auto worker = make_worker();

    const auto requirements = make_requirements();

    worker.reserve(requirements);
    worker.release(requirements);

    const auto snapshot = worker.snapshot();

    expect(
        snapshot.resources().available_cpu_cores() ==
            6.0,
        "Release restores available CPU"
    );

    expect(
        snapshot.resources().available_memory_bytes() ==
            12ULL * gibibyte,
        "Release restores available memory"
    );

    expect(
        snapshot.resources().available_disk_bytes() ==
            300ULL * gibibyte,
        "Release restores available disk"
    );

    expect(
        snapshot.running_jobs() == 1,
        "Release decreases running job count"
    );
}

void test_failed_reservation_is_non_destructive() {
    auto worker = make_worker();

    const radahn::domain::ResourceRequirements excessive{
        7.0,
        14ULL * gibibyte,
        400ULL * gibibyte,
        false,
        {"linux"}
    };

    const auto before = worker.snapshot();

    bool rejected = false;

    try {
        worker.reserve(excessive);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    const auto after = worker.snapshot();

    expect(
        rejected,
        "Insufficient worker reservation is rejected"
    );

    expect(
        after.resources().available_cpu_cores() ==
            before.resources().available_cpu_cores(),
        "Failed reservation does not change CPU"
    );

    expect(
        after.resources().available_memory_bytes() ==
            before.resources().available_memory_bytes(),
        "Failed reservation does not change memory"
    );

    expect(
        after.running_jobs() ==
            before.running_jobs(),
        "Failed reservation does not change job count"
    );
}

void test_invalid_release_is_rejected() {
    auto worker = make_worker();

    const radahn::domain::ResourceRequirements excessive{
        7.0,
        1ULL * gibibyte,
        1ULL * gibibyte,
        false
    };

    bool rejected = false;

    try {
        worker.release(excessive);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    expect(
        rejected,
        "Releasing more resources than reserved is rejected"
    );
}

}  // namespace

int main() {
    test_reservation_reduces_resources();
    test_release_restores_resources();
    test_failed_reservation_is_non_destructive();
    test_invalid_release_is_rejected();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " test assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll worker record tests passed\n";

    return EXIT_SUCCESS;
}