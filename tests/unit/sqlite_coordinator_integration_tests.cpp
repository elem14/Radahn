#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <iostream>
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

#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"
#include "radahn/persistence/sqlite_worker_repository.hpp"

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

class TemporaryDatabaseFile {
public:
    TemporaryDatabaseFile() {
        const auto unique_value =
            std::chrono::steady_clock::now()
                .time_since_epoch()
                .count();

        path_ =
            std::filesystem::temp_directory_path() /
            (
                "radahn-coordinator-sqlite-test-" +
                std::to_string(unique_value) +
                ".db"
            );

        std::filesystem::remove(
            path_
        );
    }

    ~TemporaryDatabaseFile() {
        std::error_code error;

        std::filesystem::remove(
            path_,
            error
        );

        std::filesystem::remove(
            path_.string() + "-wal",
            error
        );

        std::filesystem::remove(
            path_.string() + "-shm",
            error
        );
    }

    [[nodiscard]]
    const std::filesystem::path&
    path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

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
            "sqlite-coordinator-job"
        },
        "SQLite coordinator integration job",
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
            "sqlite-coordinator-worker"
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

void test_sqlite_backed_lifecycle_and_reopen() {
    using radahn::coordinator::
        InMemoryCoordinator;

    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;

    using radahn::persistence::
        SqliteDatabase;

    using radahn::persistence::
        SqliteJobRepository;

    using radahn::persistence::
        SqliteWorkerRepository;

    using radahn::scheduler::
        LeastLoadedPolicy;

    TemporaryDatabaseFile database_file;

    {
        SqliteDatabase database{
            database_file.path()
        };

        SqliteJobRepository
            job_repository{
                database
            };

        SqliteWorkerRepository
            worker_repository{
                database
            };

        LeastLoadedPolicy policy;

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

        const auto decision =
            coordinator.dispatch_once_for_worker(
                WorkerId{
                    "sqlite-coordinator-worker"
                }
            );

        expect(
            decision.has_value(),
            "SQLite coordinator dispatches job"
        );

        coordinator.mark_running(
            JobId{
                "sqlite-coordinator-job"
            }
        );

        coordinator.mark_succeeded(
            JobId{
                "sqlite-coordinator-job"
            }
        );

        expect(
            coordinator.job_state(
                JobId{
                    "sqlite-coordinator-job"
                }
            ) == JobState::succeeded,
            "SQLite coordinator reaches SUCCEEDED"
        );

        const auto worker =
            worker_repository.get(
                WorkerId{
                    "sqlite-coordinator-worker"
                }
            );

        expect(
            worker.has_value() &&
            worker->snapshot().running_jobs() == 0,
            "SQLite coordinator releases worker slot"
        );

        expect(
            worker.has_value() &&
            worker->snapshot()
                .resources()
                .available_cpu_cores() == 8.0,
            "SQLite coordinator restores worker CPU"
        );
    }

    /*
     * Reopen the same database and verify that the completed
     * job and worker remain stored
     */
    {
        SqliteDatabase database{
            database_file.path()
        };

        SqliteJobRepository
            job_repository{
                database
            };

        SqliteWorkerRepository
            worker_repository{
                database
            };

        const auto restored_job =
            job_repository.get(
                JobId{
                    "sqlite-coordinator-job"
                }
            );

        expect(
            restored_job.has_value(),
            "Completed job survives SQLite reopen"
        );

        expect(
            restored_job.has_value() &&
            restored_job->job.state() ==
                JobState::succeeded,
            "Reopened job preserves SUCCEEDED state"
        );

        expect(
            restored_job.has_value() &&
            restored_job->assigned_worker_id
                .has_value() &&
            restored_job->assigned_worker_id
                ->value() ==
                "sqlite-coordinator-worker",
            "Reopened job preserves worker assignment"
        );

        const auto restored_worker =
            worker_repository.get(
                WorkerId{
                    "sqlite-coordinator-worker"
                }
            );

        expect(
            restored_worker.has_value(),
            "Registered worker survives SQLite reopen"
        );

        expect(
            restored_worker.has_value() &&
            restored_worker->snapshot()
                .running_jobs() == 0,
            "Reopened worker preserves released state"
        );
    }
}

}  // namespace

int main() {
    try {
        test_sqlite_backed_lifecycle_and_reopen();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected SQLite coordinator exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " SQLite coordinator assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll SQLite coordinator integration tests passed\n";

    return EXIT_SUCCESS;
}