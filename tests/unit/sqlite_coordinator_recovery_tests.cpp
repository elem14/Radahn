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
                "radahn-recovery-test-" +
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

radahn::domain::Job make_job(
    std::string id,
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
        "Recovery test job",
        priority,
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
            "recovery-worker"
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

void test_coordinator_restart_recovery() {
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

    const WorkerId worker_id{
        "recovery-worker"
    };

    TemporaryDatabaseFile database_file;

    /*
     * Phase 1:
     *
     * Persist one RUNNING job, one QUEUED job, and one
     * SUCCEEDED job before simulating a coordinator crash.
     */
    {
        SqliteDatabase database{
            database_file.path()
        };

        SqliteJobRepository job_repository{
            database
        };

        SqliteWorkerRepository worker_repository{
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
            make_job(
                "running-before-restart",
                100
            )
        );

        coordinator.submit_job(
            make_job(
                "finished-before-restart",
                90
            )
        );

        coordinator.submit_job(
            make_job(
                "queued-before-restart",
                10
            )
        );

        const auto running_decision =
            coordinator.dispatch_once_for_worker(
                worker_id
            );

        expect(
            running_decision.has_value(),
            "First job is dispatched before restart"
        );

        if (running_decision.has_value()) {
            coordinator.mark_running(
                running_decision->job_id
            );
        }

        const auto finished_decision =
            coordinator.dispatch_once_for_worker(
                worker_id
            );

        expect(
            finished_decision.has_value(),
            "Second job is dispatched before restart"
        );

        if (finished_decision.has_value()) {
            coordinator.mark_running(
                finished_decision->job_id
            );

            coordinator.mark_succeeded(
                finished_decision->job_id
            );
        }

        expect(
            coordinator.active_job_count() == 1,
            "One job is active when restart occurs"
        );

        expect(
            coordinator.queued_job_count() == 1,
            "One job is queued when restart occurs"
        );

        expect(
            coordinator.finished_job_count() == 1,
            "One job is finished when restart occurs"
        );
    }

    /*
     * Phase 2:
     *
     * Reopen the database and construct a new coordinator.
     * Its constructor should rebuild and reconcile state.
     */
    {
        SqliteDatabase database{
            database_file.path()
        };

        SqliteJobRepository job_repository{
            database
        };

        SqliteWorkerRepository worker_repository{
            database
        };

        LeastLoadedPolicy policy;

        InMemoryCoordinator coordinator{
            policy,
            job_repository,
            worker_repository
        };

        expect(
            coordinator.queued_job_count() == 2,
            "Queued and interrupted jobs are restored"
        );

        expect(
            coordinator.active_job_count() == 0,
            "Interrupted job is no longer active"
        );

        expect(
            coordinator.finished_job_count() == 1,
            "Finished job remains finished"
        );

        const auto recovered_running_job =
            job_repository.get(
                JobId{
                    "running-before-restart"
                }
            );

        expect(
            recovered_running_job.has_value() &&
            recovered_running_job->job.state() ==
                JobState::queued,
            "RUNNING job is reset to QUEUED"
        );

        expect(
            recovered_running_job.has_value() &&
            !recovered_running_job
                 ->assigned_worker_id
                 .has_value(),
            "Recovered job assignment is cleared"
        );

        const auto finished_job =
            job_repository.get(
                JobId{
                    "finished-before-restart"
                }
            );

        expect(
            finished_job.has_value() &&
            finished_job->job.state() ==
                JobState::succeeded,
            "SUCCEEDED job is not requeued"
        );

        const auto recovered_worker =
            worker_repository.get(
                worker_id
            );

        expect(
            recovered_worker.has_value() &&
            recovered_worker->snapshot()
                .running_jobs() == 0,
            "Worker running-job count is reset"
        );

        expect(
            recovered_worker.has_value() &&
            recovered_worker->snapshot()
                .resources()
                .available_cpu_cores() == 8.0,
            "Worker CPU reservation is reset"
        );

        expect(
            recovered_worker.has_value() &&
            recovered_worker->snapshot()
                .resources()
                .available_memory_bytes() ==
                16ULL * gibibyte,
            "Worker memory reservation is reset"
        );

        const auto first_recovered_decision =
            coordinator.dispatch_once_for_worker(
                worker_id
            );

        const auto second_recovered_decision =
            coordinator.dispatch_once_for_worker(
                worker_id
            );

        const auto no_more_jobs =
            coordinator.dispatch_once_for_worker(
                worker_id
            );

        expect(
            first_recovered_decision.has_value(),
            "First recovered job can be dispatched"
        );

        expect(
            second_recovered_decision.has_value(),
            "Second recovered job can be dispatched"
        );

        expect(
            !no_more_jobs.has_value(),
            "Finished job was not restored to queue"
        );
    }
}

}  // namespace

int main() {
    try {
        test_coordinator_restart_recovery();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected coordinator recovery exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " recovery assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll coordinator recovery tests passed\n";

    return EXIT_SUCCESS;
}