#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "radahn/coordinator/in_memory_coordinator.hpp"

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"
#include "radahn/domain/workload.hpp"

#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/in_memory_worker_repository.hpp"
#include "radahn/persistence/job_record.hpp"

#include "radahn/scheduler/least_loaded_policy.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t mebibyte =
    1024ULL * 1024ULL;

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

radahn::domain::Job make_job() {
    radahn::domain::ResourceRequirements
        requirements{
            1.0,
            512ULL * mebibyte,
            1024ULL * mebibyte,
            false,
            {"macos", "arm64"}
        };

    return radahn::domain::Job{
        radahn::domain::JobId{
            "lease-lifecycle-job"
        },
        "Lease lifecycle test job",
        50,
        std::move(requirements),
        radahn::domain::WorkloadSpec::sleep(
            std::chrono::milliseconds{
                100
            }
        )
    };
}

radahn::domain::WorkerRecord make_worker() {
    radahn::domain::WorkerResources resources{
        8.0,
        8.0,
        16ULL * gibibyte,
        16ULL * gibibyte,
        100ULL * gibibyte,
        100ULL * gibibyte,
        false
    };

    radahn::domain::WorkerSnapshot snapshot{
        radahn::domain::WorkerId{
            "lease-lifecycle-worker"
        },
        radahn::domain::WorkerState::online,
        std::move(resources),
        0,
        4,
        {"macos", "arm64"}
    };

    return radahn::domain::WorkerRecord{
        std::move(snapshot)
    };
}

void test_job_lease_lifecycle() {
    using radahn::coordinator::
        InMemoryCoordinator;

    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;

    using radahn::persistence::
        InMemoryJobRepository;

    using radahn::persistence::
        InMemoryWorkerRepository;

    using radahn::persistence::
        JobLeaseClock;

    using radahn::scheduler::
        LeastLoadedPolicy;

    constexpr auto lease_duration =
        std::chrono::seconds{30};

    InMemoryJobRepository
        job_repository;

    InMemoryWorkerRepository
        worker_repository;

    LeastLoadedPolicy policy;

    InMemoryCoordinator coordinator{
        policy,
        job_repository,
        worker_repository,
        lease_duration
    };

    const JobId job_id{
        "lease-lifecycle-job"
    };

    const WorkerId worker_id{
        "lease-lifecycle-worker"
    };

    coordinator.register_worker(
        make_worker()
    );

    coordinator.submit_job(
        make_job()
    );

    const auto before_dispatch =
        JobLeaseClock::now();

    const auto decision =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    const auto after_dispatch =
        JobLeaseClock::now();

    expect(
        decision.has_value(),
        "Dispatch succeeds"
    );

    const auto leased_record =
        job_repository.get(
            job_id
        );

    expect(
        leased_record.has_value() &&
        leased_record->job.state() ==
            JobState::leased,
        "Dispatched job enters LEASED state"
    );

    expect(
        leased_record.has_value() &&
        leased_record->lease_expires_at
            .has_value(),
        "Dispatch creates a lease expiration"
    );

    expect(
        leased_record.has_value() &&
        leased_record->lease_expires_at
            .has_value() &&
        *leased_record->lease_expires_at >=
            before_dispatch +
                lease_duration &&
        *leased_record->lease_expires_at <=
            after_dispatch +
                lease_duration,
        "Dispatch lease uses configured duration"
    );

    coordinator.mark_running(
        job_id
    );

    const auto running_record =
        job_repository.get(
            job_id
        );

    expect(
        running_record.has_value() &&
        running_record->job.state() ==
            JobState::running,
        "Started job enters RUNNING state"
    );

    expect(
        running_record.has_value() &&
        running_record->lease_expires_at
            .has_value(),
        "Starting job preserves an active lease"
    );

    /*
     * Use a fixed future heartbeat so the expected renewed
     * deadline can be compared exactly.
     */
    const auto heartbeat_time =
        JobLeaseClock::now() +
        std::chrono::hours{1};

    coordinator.record_worker_heartbeat(
        worker_id,
        heartbeat_time
    );

    const auto renewed_record =
        job_repository.get(
            job_id
        );

    expect(
        renewed_record.has_value() &&
        renewed_record->lease_expires_at
            .has_value() &&
        *renewed_record->lease_expires_at ==
            heartbeat_time +
                lease_duration,
        "Worker heartbeat renews active job lease"
    );

    coordinator.mark_succeeded(
        job_id
    );

    const auto finished_record =
        job_repository.get(
            job_id
        );

    expect(
        finished_record.has_value() &&
        finished_record->job.state() ==
            JobState::succeeded,
        "Finished job enters SUCCEEDED state"
    );

    expect(
        finished_record.has_value() &&
        !finished_record->lease_expires_at
             .has_value(),
        "Finishing job clears its lease"
    );
}

void test_invalid_lease_duration() {
    using radahn::coordinator::
        InMemoryCoordinator;

    using radahn::persistence::
        InMemoryJobRepository;

    using radahn::persistence::
        InMemoryWorkerRepository;

    using radahn::scheduler::
        LeastLoadedPolicy;

    InMemoryJobRepository
        job_repository;

    InMemoryWorkerRepository
        worker_repository;

    LeastLoadedPolicy policy;

    bool invalid_duration_rejected = false;

    try {
        InMemoryCoordinator coordinator{
            policy,
            job_repository,
            worker_repository,
            std::chrono::milliseconds{0}
        };

        static_cast<void>(
            coordinator
        );
    } catch (
        const std::invalid_argument&
    ) {
        invalid_duration_rejected = true;
    }

    expect(
        invalid_duration_rejected,
        "Non-positive lease duration is rejected"
    );
}

}  // namespace

int main() {
    try {
        test_job_lease_lifecycle();
        test_invalid_lease_duration();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected job lease lifecycle exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " lease lifecycle assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll job lease lifecycle tests passed\n";

    return EXIT_SUCCESS;
}