#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <optional>
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
#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"

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

radahn::domain::Job make_job(
    std::string id,
    std::size_t max_attempts
) {
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
            std::move(id)
        },
        "Retry limit test job",
        50,
        std::move(requirements),
        radahn::domain::WorkloadSpec::sleep(
            std::chrono::seconds{30}
        ),
        max_attempts
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

void test_retry_limit() {
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

    InMemoryJobRepository job_repository;
    InMemoryWorkerRepository worker_repository;
    LeastLoadedPolicy policy;

    InMemoryCoordinator coordinator{
        policy,
        job_repository,
        worker_repository,
        lease_duration
    };

    const JobId job_id{
        "bounded-retry-job"
    };

    const WorkerId worker_a{
        "retry-worker-a"
    };

    const WorkerId worker_b{
        "retry-worker-b"
    };

    const WorkerId worker_c{
        "retry-worker-c"
    };

    coordinator.register_worker(
        make_worker(
            worker_a.value()
        )
    );

    coordinator.register_worker(
        make_worker(
            worker_b.value()
        )
    );

    coordinator.register_worker(
        make_worker(
            worker_c.value()
        )
    );

    coordinator.submit_job(
        make_job(
            job_id.value(),
            2
        )
    );

    /*
     * ATTEMPT 1
     */
    const auto first_dispatch =
        coordinator.dispatch_once_for_worker(
            worker_a
        );

    expect(
        first_dispatch.has_value(),
        "First worker receives attempt one"
    );

    auto record =
        job_repository.get(job_id);

    expect(
        record.has_value() &&
        record->job.attempt_count() == 1,
        "First dispatch increments attempt count"
    );

    expect(
        record.has_value() &&
        record->job.max_attempts() == 2,
        "Job preserves configured retry limit"
    );

    coordinator.mark_running(job_id);

    record = job_repository.get(job_id);

    if (
        !record.has_value() ||
        !record->lease_expires_at.has_value()
    ) {
        expect(
            false,
            "Attempt one has a lease"
        );

        return;
    }

    coordinator.mark_expired_job_leases(
        *record->lease_expires_at
    );

    expect(
        coordinator.requeue_retry_wait_jobs() == 1,
        "Job is requeued after first abandoned attempt"
    );

    /*
     * ATTEMPT 2
     */
    const auto second_dispatch =
        coordinator.dispatch_once_for_worker(
            worker_b
        );

    expect(
        second_dispatch.has_value(),
        "Second worker receives attempt two"
    );

    record = job_repository.get(job_id);

    expect(
        record.has_value() &&
        record->job.attempt_count() == 2,
        "Second dispatch increments attempt count"
    );

    coordinator.mark_running(job_id);

    record = job_repository.get(job_id);

    if (
        !record.has_value() ||
        !record->lease_expires_at.has_value()
    ) {
        expect(
            false,
            "Attempt two has a lease"
        );

        return;
    }

    coordinator.mark_expired_job_leases(
        *record->lease_expires_at
    );

    expect(
        coordinator.requeue_retry_wait_jobs() == 0,
        "Exhausted job is not requeued"
    );

    record = job_repository.get(job_id);

    expect(
        record.has_value() &&
        record->job.state() ==
            JobState::failed,
        "Job becomes FAILED after final attempt"
    );

    expect(
        record.has_value() &&
        record->job.attempt_count() == 2,
        "FAILED job preserves final attempt count"
    );

    const auto third_dispatch =
        coordinator.dispatch_once_for_worker(
            worker_c
        );

    expect(
        !third_dispatch.has_value(),
        "Third worker cannot receive exhausted job"
    );
}

void test_sqlite_retry_round_trip() {
    using radahn::domain::JobId;

    using radahn::persistence::
        JobRecord;

    using radahn::persistence::
        SqliteDatabase;

    using radahn::persistence::
        SqliteJobRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteJobRepository repository{
        database
    };

    auto job =
        make_job(
            "sqlite-retry-job",
            5
        );

    job.record_attempt();
    job.record_attempt();

    repository.insert(
        JobRecord{
            std::move(job),
            std::nullopt,
            std::nullopt
        }
    );

    const auto restored =
        repository.get(
            JobId{
                "sqlite-retry-job"
            }
        );

    expect(
        restored.has_value() &&
        restored->job.attempt_count() == 2,
        "SQLite preserves attempt count"
    );

    expect(
        restored.has_value() &&
        restored->job.max_attempts() == 5,
        "SQLite preserves max attempts"
    );
}

void test_invalid_retry_limit() {
    bool rejected = false;

    try {
        static_cast<void>(
            make_job(
                "invalid-retry-job",
                0
            )
        );
    } catch (
        const std::invalid_argument&
    ) {
        rejected = true;
    }

    expect(
        rejected,
        "Zero max attempts is rejected"
    );
}

}  // namespace

int main() {
    try {
        test_retry_limit();
        test_sqlite_retry_round_trip();
        test_invalid_retry_limit();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected retry-limit exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " retry-limit assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll job retry-limit tests passed\n";

    return EXIT_SUCCESS;
}