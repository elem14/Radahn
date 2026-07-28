#include <cstdlib>
#include <cstdint>
#include <exception>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/in_memory_worker_repository.hpp"
#include "radahn/persistence/job_record.hpp"

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
    std::string name,
    int priority
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
        std::move(name),
        priority,
        std::move(requirements)
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

void test_job_repository_insert_and_get() {
    using radahn::domain::JobId;
    using radahn::persistence::
        InMemoryJobRepository;
    using radahn::persistence::JobRecord;

    InMemoryJobRepository repository;

    repository.insert(
        JobRecord{
            make_job(
                "job-1",
                "Repository test",
                50
            ),
            std::nullopt
        }
    );

    expect(
        repository.size() == 1,
        "Job repository stores inserted job"
    );

    expect(
        repository.contains(
            JobId{"job-1"}
        ),
        "Job repository reports existing job"
    );

    const auto record =
        repository.get(
            JobId{"job-1"}
        );

    expect(
        record.has_value(),
        "Job repository retrieves job by ID"
    );

    expect(
        record.has_value() &&
        record->job.name() ==
            "Repository test",
        "Retrieved job preserves its data"
    );

    expect(
        record.has_value() &&
        !record->assigned_worker_id
             .has_value(),
        "Queued job has no assigned worker"
    );
}

void test_job_repository_rejects_duplicates() {
    using radahn::persistence::
        InMemoryJobRepository;
    using radahn::persistence::JobRecord;

    InMemoryJobRepository repository;

    repository.insert(
        JobRecord{
            make_job(
                "duplicate-job",
                "First job",
                10
            ),
            std::nullopt
        }
    );

    bool duplicate_rejected = false;

    try {
        repository.insert(
            JobRecord{
                make_job(
                    "duplicate-job",
                    "Second job",
                    20
                ),
                std::nullopt
            }
        );
    } catch (
        const std::invalid_argument&
    ) {
        duplicate_rejected = true;
    }

    expect(
        duplicate_rejected,
        "Job repository rejects duplicate IDs"
    );
}

void test_job_repository_update() {
    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;
    using radahn::persistence::
        InMemoryJobRepository;
    using radahn::persistence::JobRecord;

    InMemoryJobRepository repository;

    repository.insert(
        JobRecord{
            make_job(
                "job-update",
                "Update test",
                50
            ),
            std::nullopt
        }
    );

    auto record =
        repository.get(
            JobId{"job-update"}
        );

    expect(
        record.has_value(),
        "Job exists before repository update"
    );

    if (!record.has_value()) {
        return;
    }

    record->job.transition_to(
        JobState::leased
    );

    record->assigned_worker_id =
        WorkerId{"worker-1"};

    repository.update(
        std::move(*record)
    );

    const auto updated =
        repository.get(
            JobId{"job-update"}
        );

    expect(
        updated.has_value() &&
        updated->job.state() ==
            JobState::leased,
        "Job repository saves updated state"
    );

    expect(
        updated.has_value() &&
        updated->assigned_worker_id
            .has_value() &&
        updated->assigned_worker_id
            ->value() == "worker-1",
        "Job repository saves worker assignment"
    );
}

void test_job_repository_list_and_erase() {
    using radahn::domain::JobId;
    using radahn::persistence::
        InMemoryJobRepository;
    using radahn::persistence::JobRecord;

    InMemoryJobRepository repository;

    repository.insert(
        JobRecord{
            make_job(
                "job-a",
                "Job A",
                10
            ),
            std::nullopt
        }
    );

    repository.insert(
        JobRecord{
            make_job(
                "job-b",
                "Job B",
                20
            ),
            std::nullopt
        }
    );

    expect(
        repository.list().size() == 2,
        "Job repository lists all jobs"
    );

    repository.erase(
        JobId{"job-a"}
    );

    expect(
        repository.size() == 1,
        "Job repository erases a job"
    );

    expect(
        !repository.contains(
            JobId{"job-a"}
        ),
        "Erased job no longer exists"
    );
}

void test_worker_repository() {
    using radahn::domain::WorkerId;
    using radahn::persistence::
        InMemoryWorkerRepository;

    InMemoryWorkerRepository repository;

    repository.insert(
        make_worker("worker-1")
    );

    expect(
        repository.size() == 1,
        "Worker repository stores worker"
    );

    expect(
        repository.contains(
            WorkerId{"worker-1"}
        ),
        "Worker repository reports existing worker"
    );

    const auto worker =
        repository.get(
            WorkerId{"worker-1"}
        );

    expect(
        worker.has_value(),
        "Worker repository retrieves worker"
    );

    expect(
        worker.has_value() &&
        worker->id().value() ==
            "worker-1",
        "Retrieved worker preserves its ID"
    );

    repository.update(
        make_worker("worker-1")
    );

    expect(
        repository.list().size() == 1,
        "Worker repository updates without duplicating"
    );

    repository.erase(
        WorkerId{"worker-1"}
    );

    expect(
        repository.size() == 0,
        "Worker repository erases worker"
    );
}

void test_worker_repository_rejects_duplicates() {
    using radahn::persistence::
        InMemoryWorkerRepository;

    InMemoryWorkerRepository repository;

    repository.insert(
        make_worker("duplicate-worker")
    );

    bool duplicate_rejected = false;

    try {
        repository.insert(
            make_worker("duplicate-worker")
        );
    } catch (
        const std::invalid_argument&
    ) {
        duplicate_rejected = true;
    }

    expect(
        duplicate_rejected,
        "Worker repository rejects duplicate IDs"
    );
}

}  // namespace

int main() {
    test_job_repository_insert_and_get();
    test_job_repository_rejects_duplicates();
    test_job_repository_update();
    test_job_repository_list_and_erase();

    test_worker_repository();
    test_worker_repository_rejects_duplicates();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " persistence assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll persistence tests passed\n";

    return EXIT_SUCCESS;
}