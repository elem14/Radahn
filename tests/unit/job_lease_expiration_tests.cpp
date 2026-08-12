#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
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
            2.0,
            512ULL * mebibyte,
            1024ULL * mebibyte,
            false,
            {"macos", "arm64"}
        };

    return radahn::domain::Job{
        radahn::domain::JobId{
            "lease-expiration-job"
        },
        "Lease expiration test job",
        50,
        std::move(requirements),
        radahn::domain::WorkloadSpec::sleep(
            std::chrono::seconds{30}
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
            "lease-expiration-worker"
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

void test_expired_running_job() {
    using radahn::coordinator::
        InMemoryCoordinator;

    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;

    using radahn::persistence::
        InMemoryJobRepository;

    using radahn::persistence::
        InMemoryWorkerRepository;

    using radahn::scheduler::
        LeastLoadedPolicy;

    constexpr auto lease_duration =
        std::chrono::seconds{5};

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
        "lease-expiration-job"
    };

    const WorkerId worker_id{
        "lease-expiration-worker"
    };

    coordinator.register_worker(
        make_worker()
    );

    coordinator.submit_job(
        make_job()
    );

    const auto decision =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        decision.has_value(),
        "Job is dispatched before expiration"
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
        "Job is RUNNING before lease expiration"
    );

    expect(
        running_record.has_value() &&
        running_record->lease_expires_at
            .has_value(),
        "Running job has a lease deadline"
    );

    if (
        !running_record.has_value() ||
        !running_record->lease_expires_at
             .has_value()
    ) {
        return;
    }

    const auto lease_deadline =
        *running_record->lease_expires_at;

    const auto early_expiration_count =
        coordinator.mark_expired_job_leases(
            lease_deadline -
            std::chrono::milliseconds{1}
        );

    expect(
        early_expiration_count == 0,
        "Lease does not expire before its deadline"
    );

    expect(
        coordinator.job_state(
            job_id
        ) == JobState::running,
        "Job remains RUNNING before deadline"
    );

    const auto expiration_count =
        coordinator.mark_expired_job_leases(
            lease_deadline
        );

    expect(
        expiration_count == 1,
        "Expired lease is detected"
    );

    const auto expired_record =
        job_repository.get(
            job_id
        );

    expect(
        expired_record.has_value() &&
        expired_record->job.state() ==
            JobState::retry_wait,
        "Expired job enters RETRY_WAIT"
    );

    expect(
        expired_record.has_value() &&
        !expired_record
             ->assigned_worker_id
             .has_value(),
        "Expired assignment is cleared"
    );

    expect(
        expired_record.has_value() &&
        !expired_record
             ->lease_expires_at
             .has_value(),
        "Expired lease timestamp is cleared"
    );

    const auto worker =
        worker_repository.get(
            worker_id
        );

    expect(
        worker.has_value() &&
        worker->snapshot()
            .running_jobs() == 0,
        "Expired job releases worker slot"
    );

    expect(
        worker.has_value() &&
        worker->snapshot()
            .resources()
            .available_cpu_cores() == 8.0,
        "Expired job releases worker CPU"
    );

    expect(
        worker.has_value() &&
        worker->snapshot()
            .resources()
            .available_memory_bytes() ==
            16ULL * gibibyte,
        "Expired job releases worker memory"
    );

    const auto second_dispatch =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        !second_dispatch.has_value(),
        "Expired job is not automatically requeued yet"
    );

    const auto repeated_expiration_count =
        coordinator.mark_expired_job_leases(
            lease_deadline +
            std::chrono::minutes{1}
        );

    expect(
        repeated_expiration_count == 0,
        "Expired assignment is processed only once"
    );
}

}  // namespace

int main() {
    try {
        test_expired_running_job();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected lease expiration exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " lease expiration assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll job lease expiration tests passed\n";

    return EXIT_SUCCESS;
}