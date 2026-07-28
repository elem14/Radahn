#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

#include "radahn/coordinator/in_memory_coordinator.hpp"

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

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
            1.0,
            512ULL * mebibyte,
            1024ULL * mebibyte,
            false,
            {"macos", "arm64"}
        };

    return radahn::domain::Job{
        radahn::domain::JobId{
            "repository-job"
        },
        "Repository-backed coordinator job",
        50,
        std::move(requirements)
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
            "repository-worker"
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

void test_repository_backed_job_lifecycle() {
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

    LeastLoadedPolicy policy;

    InMemoryJobRepository
        job_repository;

    InMemoryWorkerRepository
        worker_repository;

    InMemoryCoordinator coordinator{
        policy,
        job_repository,
        worker_repository
    };

    coordinator.register_worker(
        make_worker()
    );

    coordinator.submit_job(
        make_job()
    );

    expect(
        job_repository.contains(
            JobId{"repository-job"}
        ),
        "Coordinator stores submitted job in repository"
    );

    expect(
        worker_repository.contains(
            WorkerId{"repository-worker"}
        ),
        "Coordinator stores registered worker in repository"
    );

    const auto decision =
        coordinator.dispatch_once_for_worker(
            WorkerId{"repository-worker"}
        );

    expect(
        decision.has_value(),
        "Coordinator dispatches repository-backed job"
    );

    const auto leased_record =
        job_repository.get(
            JobId{"repository-job"}
        );

    expect(
        leased_record.has_value() &&
        leased_record->job.state() ==
            JobState::leased,
        "Repository records LEASED state"
    );

    expect(
        leased_record.has_value() &&
        leased_record->assigned_worker_id
            .has_value() &&
        leased_record->assigned_worker_id
            ->value() ==
            "repository-worker",
        "Repository records assigned worker"
    );

    coordinator.mark_running(
        JobId{"repository-job"}
    );

    const auto running_record =
        job_repository.get(
            JobId{"repository-job"}
        );

    expect(
        running_record.has_value() &&
        running_record->job.state() ==
            JobState::running,
        "Repository records RUNNING state"
    );

    coordinator.mark_succeeded(
        JobId{"repository-job"}
    );

    const auto finished_record =
        job_repository.get(
            JobId{"repository-job"}
        );

    expect(
        finished_record.has_value() &&
        finished_record->job.state() ==
            JobState::succeeded,
        "Repository records SUCCEEDED state"
    );

    expect(
        coordinator.finished_job_count() == 1,
        "Coordinator counts finished repository job"
    );
}

}  // namespace

int main() {
    test_repository_backed_job_lifecycle();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " coordinator repository assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll coordinator repository tests passed\n";

    return EXIT_SUCCESS;
}