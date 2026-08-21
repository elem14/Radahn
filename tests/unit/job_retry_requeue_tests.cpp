#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <stdexcept>
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
            1.0,
            512ULL * mebibyte,
            1024ULL * mebibyte,
            false,
            {"macos", "arm64"}
        };

    return radahn::domain::Job{
        radahn::domain::JobId{
            "retry-requeue-job"
        },
        "Automatic retry test job",
        50,
        std::move(requirements),
        radahn::domain::WorkloadSpec::sleep(
            std::chrono::seconds{30}
        )
    };
}

radahn::domain::WorkerRecord make_worker(
    std::string id
) {
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
            std::move(id)
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

void test_abandoned_job_is_requeued() {
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
        "retry-requeue-job"
    };

    const WorkerId crashed_worker_id{
        "crashed-worker"
    };

    const WorkerId replacement_worker_id{
        "replacement-worker"
    };

    coordinator.register_worker(
        make_worker(
            crashed_worker_id.value()
        )
    );

    coordinator.register_worker(
        make_worker(
            replacement_worker_id.value()
        )
    );

    coordinator.submit_job(
        make_job()
    );

    const auto first_dispatch =
        coordinator.dispatch_once_for_worker(
            crashed_worker_id
        );

    expect(
        first_dispatch.has_value(),
        "Original worker receives job"
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
        "Original job reaches RUNNING"
    );

    if (
        !running_record.has_value() ||
        !running_record->lease_expires_at
             .has_value()
    ) {
        expect(
            false,
            "Running job has a lease deadline"
        );

        return;
    }

    const auto lease_deadline =
        *running_record->lease_expires_at;

    const auto expired_count =
        coordinator.mark_expired_job_leases(
            lease_deadline
        );

    expect(
        expired_count == 1,
        "Crashed worker's lease expires"
    );

    const auto retry_record =
        job_repository.get(
            job_id
        );

    expect(
        retry_record.has_value() &&
        retry_record->job.state() ==
            JobState::retry_wait,
        "Abandoned job enters RETRY_WAIT"
    );

    expect(
        retry_record.has_value() &&
        !retry_record->assigned_worker_id
             .has_value(),
        "Abandoned job has no assigned worker"
    );

    const auto requeued_count =
        coordinator.requeue_retry_wait_jobs();

    expect(
        requeued_count == 1,
        "RETRY_WAIT job is requeued"
    );

    const auto queued_record =
        job_repository.get(
            job_id
        );

    expect(
        queued_record.has_value() &&
        queued_record->job.state() ==
            JobState::queued,
        "Recovered job returns to QUEUED"
    );

    expect(
        coordinator.queued_job_count() == 1,
        "Recovered job appears in queued-job count"
    );

    const auto replacement_dispatch =
        coordinator.dispatch_once_for_worker(
            replacement_worker_id
        );

    expect(
        replacement_dispatch.has_value(),
        "Replacement worker acquires recovered job"
    );

    expect(
        replacement_dispatch.has_value() &&
        replacement_dispatch->job_id ==
            job_id,
        "Replacement worker receives same logical job"
    );

    const auto replacement_leased_record =
        job_repository.get(
            job_id
        );

    expect(
        replacement_leased_record.has_value() &&
        replacement_leased_record
            ->job.state() ==
            JobState::leased,
        "Recovered job receives a new lease"
    );

    expect(
        replacement_leased_record.has_value() &&
        replacement_leased_record
            ->assigned_worker_id
            .has_value() &&
        *replacement_leased_record
             ->assigned_worker_id ==
            replacement_worker_id,
        "Recovered job is assigned to replacement worker"
    );

    expect(
        replacement_leased_record.has_value() &&
        replacement_leased_record
            ->lease_expires_at
            .has_value(),
        "Replacement assignment has a new lease deadline"
    );

    coordinator.mark_running(
        job_id
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
        "Recovered job can complete successfully"
    );

    expect(
        finished_record.has_value() &&
        !finished_record->lease_expires_at
             .has_value(),
        "Successful retry clears replacement lease"
    );

    const auto repeated_requeue =
        coordinator.requeue_retry_wait_jobs();

    expect(
        repeated_requeue == 0,
        "Completed job is not requeued again"
    );
}

}  // namespace

int main() {
    try {
        test_abandoned_job_is_requeued();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected retry requeue exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " retry requeue assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll job retry requeue tests passed\n";

    return EXIT_SUCCESS;
}