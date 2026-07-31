#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/workload.hpp"

#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t mebibyte =
    1024ULL * 1024ULL;

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
                "radahn-sqlite-job-test-" +
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
    std::string id
) {
    using radahn::domain::Job;
    using radahn::domain::JobId;
    using radahn::domain::ResourceRequirements;
    using radahn::domain::WorkloadSpec;

    ResourceRequirements requirements{
        2.5,
        768ULL * mebibyte,
        2048ULL * mebibyte,
        true,
        {"arm64", "macos"}
    };

    const auto created_at =
        Job::TimePoint{
            std::chrono::duration_cast<
                Job::Clock::duration
            >(
                std::chrono::milliseconds{
                    1'700'000'000'123LL
                }
            )
        };

    return Job{
        JobId{
            std::move(id)
        },
        "SQLite repository job",
        75,
        std::move(requirements),
        WorkloadSpec::sleep(
            std::chrono::milliseconds{
                2'500
            }
        ),
        created_at
    };
}

void insert_test_worker(
    radahn::persistence::SqliteDatabase&
        database
) {
    database.execute(
        R"sql(
            INSERT INTO workers (
                worker_id,
                state,
                total_cpu_cores,
                available_cpu_cores,
                total_memory_bytes,
                available_memory_bytes,
                total_disk_bytes,
                available_disk_bytes,
                gpu_available,
                running_jobs,
                max_concurrent_jobs
            )
            VALUES (
                'worker-1',
                1,
                8.0,
                8.0,
                17179869184,
                17179869184,
                107374182400,
                107374182400,
                1,
                0,
                4
            );
        )sql"
    );
}

void test_insert_and_get() {
    using radahn::domain::JobId;

    using radahn::persistence::JobRecord;
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteJobRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteJobRepository repository{
        database
    };

    repository.insert(
        JobRecord{
            make_job("job-1"),
            std::nullopt
        }
    );

    expect(
        repository.size() == 1,
        "SQLite repository stores inserted job"
    );

    expect(
        repository.contains(
            JobId{"job-1"}
        ),
        "SQLite repository reports existing job"
    );

    const auto record =
        repository.get(
            JobId{"job-1"}
        );

    expect(
        record.has_value(),
        "SQLite repository retrieves job"
    );

    expect(
        record.has_value() &&
        record->job.name() ==
            "SQLite repository job",
        "SQLite repository preserves job name"
    );

    expect(
        record.has_value() &&
        record->job.priority() == 75,
        "SQLite repository preserves priority"
    );

    expect(
        record.has_value() &&
        record->job.requirements()
            .cpu_cores() == 2.5,
        "SQLite repository preserves CPU requirement"
    );

    expect(
        record.has_value() &&
        record->job.requirements()
            .memory_bytes() ==
            768ULL * mebibyte,
        "SQLite repository preserves memory requirement"
    );

    expect(
        record.has_value() &&
        record->job.requirements()
            .required_tags().size() == 2,
        "SQLite repository preserves required tags"
    );

    expect(
        record.has_value() &&
        record->job.workload()
            .sleep_duration() ==
            std::chrono::milliseconds{2'500},
        "SQLite repository preserves workload"
    );

    expect(
        record.has_value() &&
        record->job.state() ==
            radahn::domain::JobState::queued,
        "SQLite repository preserves queued state"
    );

    expect(
        record.has_value() &&
        !record->assigned_worker_id.has_value(),
        "SQLite repository preserves empty assignment"
    );
}

void test_update_and_assignment() {
    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;

    using radahn::persistence::JobRecord;
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteJobRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteJobRepository repository{
        database
    };

    insert_test_worker(
        database
    );

    repository.insert(
        JobRecord{
            make_job("job-update"),
            std::nullopt
        }
    );

    auto record =
        repository.get(
            JobId{"job-update"}
        );

    expect(
        record.has_value(),
        "Job exists before SQLite update"
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
        "SQLite repository saves leased state"
    );

    expect(
        updated.has_value() &&
        updated->assigned_worker_id
            .has_value() &&
        updated->assigned_worker_id
            ->value() == "worker-1",
        "SQLite repository saves assigned worker"
    );
}

void test_duplicate_and_unknown_operations() {
    using radahn::domain::JobId;

    using radahn::persistence::JobRecord;
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteJobRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteJobRepository repository{
        database
    };

    repository.insert(
        JobRecord{
            make_job("duplicate-job"),
            std::nullopt
        }
    );

    bool duplicate_rejected = false;

    try {
        repository.insert(
            JobRecord{
                make_job("duplicate-job"),
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
        "SQLite repository rejects duplicate IDs"
    );

    bool unknown_update_rejected = false;

    try {
        repository.update(
            JobRecord{
                make_job("unknown-job"),
                std::nullopt
            }
        );
    } catch (
        const std::invalid_argument&
    ) {
        unknown_update_rejected = true;
    }

    expect(
        unknown_update_rejected,
        "SQLite repository rejects unknown update"
    );

    bool unknown_erase_rejected = false;

    try {
        repository.erase(
            JobId{"unknown-job"}
        );
    } catch (
        const std::invalid_argument&
    ) {
        unknown_erase_rejected = true;
    }

    expect(
        unknown_erase_rejected,
        "SQLite repository rejects unknown erase"
    );
}

void test_list_and_erase() {
    using radahn::domain::JobId;

    using radahn::persistence::JobRecord;
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteJobRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteJobRepository repository{
        database
    };

    repository.insert(
        JobRecord{
            make_job("job-a"),
            std::nullopt
        }
    );

    repository.insert(
        JobRecord{
            make_job("job-b"),
            std::nullopt
        }
    );

    expect(
        repository.list().size() == 2,
        "SQLite repository lists all jobs"
    );

    repository.erase(
        JobId{"job-a"}
    );

    expect(
        repository.size() == 1,
        "SQLite repository erases a job"
    );

    expect(
        !repository.contains(
            JobId{"job-a"}
        ),
        "Erased SQLite job no longer exists"
    );
}

void test_data_survives_reopen() {
    using radahn::domain::JobId;

    using radahn::persistence::JobRecord;
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteJobRepository;

    TemporaryDatabaseFile database_file;

    {
        SqliteDatabase database{
            database_file.path()
        };

        SqliteJobRepository repository{
            database
        };

        repository.insert(
            JobRecord{
                make_job("persistent-job"),
                std::nullopt
            }
        );
    }

    {
        SqliteDatabase database{
            database_file.path()
        };

        SqliteJobRepository repository{
            database
        };

        const auto restored =
            repository.get(
                JobId{"persistent-job"}
            );

        expect(
            restored.has_value(),
            "SQLite job survives database reopen"
        );

        expect(
            restored.has_value() &&
            restored->job.workload()
                .sleep_duration() ==
                std::chrono::milliseconds{2'500},
            "Reopened job preserves workload"
        );

        expect(
            restored.has_value() &&
            restored->job.requirements()
                .required_tags().size() == 2,
            "Reopened job preserves tags"
        );
    }
}

}  // namespace

int main() {
    try {
        test_insert_and_get();
        test_update_and_assignment();
        test_duplicate_and_unknown_operations();
        test_list_and_erase();
        test_data_survives_reopen();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected SQLite job repository exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " SQLite job assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll SQLite job repository tests passed\n";

    return EXIT_SUCCESS;
}