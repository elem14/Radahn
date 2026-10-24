#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "radahn/domain/execution_result.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/workload.hpp"

#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"
#include "radahn/persistence/sqlite_schema.hpp"

namespace {

using namespace std::chrono_literals;

namespace domain = radahn::domain;
namespace persistence = radahn::persistence;

void require(
    bool condition,
    std::string_view description
) {
    if (!condition) {
        throw std::runtime_error{
            std::string{description}
        };
    }

    std::cout << "[PASS] " << description << '\n';
}

class TemporaryDatabase {
public:
    TemporaryDatabase()
        : path{
            std::filesystem::temp_directory_path() /
            (
                "radahn-command-test-" +
                std::to_string(
                    std::chrono::steady_clock::now()
                        .time_since_epoch()
                        .count()
                ) +
                ".db"
            )
        } {
    }

    ~TemporaryDatabase() {
        std::error_code ignored;

        for (const auto* suffix : {"", "-wal", "-shm"}) {
            std::filesystem::remove(
                path.string() + suffix,
                ignored
            );
        }
    }

    std::filesystem::path path;
};

persistence::JobRecord make_record(
    std::string id,
    domain::WorkloadSpec workload
) {
    return persistence::JobRecord{
        domain::Job{
            domain::JobId{std::move(id)},
            "Persistence test",
            1,
            domain::ResourceRequirements{
                1.0,
                1024,
                1024,
                false,
                {}
            },
            std::move(workload)
        },
        std::nullopt,
        std::nullopt
    };
}

void test_persistence() {
    TemporaryDatabase temporary;

    const std::vector<std::string> args{
        "simulation.py",
        "trial one",
        "",
        "'quoted'",
        "repeat",
        "repeat"
    };

    // First connection: create and save jobs.
    {
        persistence::SqliteDatabase database{
            temporary.path
        };

        persistence::SqliteJobRepository repository{
            database
        };

        repository.insert(
            make_record(
                "command-job",
                domain::WorkloadSpec::command({
                    "python3",
                    args,
                    30s
                })
            )
        );

        repository.insert(
            make_record(
                "sleep-job",
                domain::WorkloadSpec::sleep(250ms)
            )
        );

        const auto saved = repository.get(
            domain::JobId{"command-job"}
        );

        require(
            saved.has_value(),
            "Command can be retrieved after insertion"
        );

        require(
            saved->job.workload().command_spec().args == args,
            "Arguments retain order, spaces, empty values and duplicates"
        );

        require(
            repository.list().size() == 2,
            "Repository lists both command and sleep jobs"
        );
    }

    // Second connection: reconstruct from disk.
    {
        persistence::SqliteDatabase database{
            temporary.path
        };

        persistence::SqliteJobRepository repository{
            database
        };

        const auto restored = repository.get(
            domain::JobId{"command-job"}
        );

        require(
            restored.has_value(),
            "Command survives closing and reopening the database"
        );

        const auto& command =
            restored->job.workload().command_spec();

        require(
            command.executable == "python3" &&
            command.args == args &&
            command.timeout == 30s,
            "Complete command specification survives reopening"
        );

        repository.update(
            make_record(
                "command-job",
                domain::WorkloadSpec::command({
                    "hostname",
                    {},
                    std::nullopt
                })
            )
        );

        const auto updated = repository.get(
            domain::JobId{"command-job"}
        );

        require(
            updated.has_value(),
            "Updated command exists"
        );

        require(
            updated->job.workload().command_spec()
                .executable == "hostname" &&
            !updated->job.workload().command_spec()
                .timeout.has_value() &&
            updated->job.workload().command_spec()
                .args.empty(),
            "Update replaces executable, arguments and timeout"
        );

        require(
            database.query_int64(
                "SELECT COUNT(*) FROM job_command_args;"
            ) == 0,
            "Update removes old argument rows"
        );

        // Deliberately fail a command insert during update.
        database.execute(
            R"sql(
                CREATE TRIGGER reject_command
                BEFORE INSERT ON job_commands
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'Injected command write failure'
                    );
                END;
            )sql"
        );

        bool rejected = false;

        try {
            repository.update(
                make_record(
                    "command-job",
                    domain::WorkloadSpec::command({
                        "replacement",
                        {"new argument"},
                        1s
                    })
                )
            );
        } catch (const std::runtime_error&) {
            rejected = true;
        }

        require(
            rejected,
            "Injected database failure is reported"
        );

        const auto after_failure = repository.get(
            domain::JobId{"command-job"}
        );

        require(
            after_failure.has_value(),
            "Job still exists after failed update"
        );

        require(
            after_failure->job.workload().command_spec()
                .executable == "hostname",
            "Failed update restores the previous command"
        );

        database.execute(
            "DROP TRIGGER reject_command;"
        );

        repository.update(
            make_record(
                "command-job",
                domain::WorkloadSpec::sleep(100ms)
            )
        );

        require(
            database.query_int64(
                "SELECT COUNT(*) FROM job_commands;"
            ) == 0,
            "Changing workload to sleep removes command data"
        );
    }
}

void test_version_four_migration() {
    TemporaryDatabase temporary;

    {
        persistence::SqliteDatabase database{
            temporary.path
        };

        persistence::SqliteJobRepository repository{
            database
        };

        repository.insert(
            make_record(
                "legacy-sleep",
                domain::WorkloadSpec::sleep(500ms)
            )
        );

        // Version 5 only adds these tables. Removing them
        // reconstructs the version 4 schema for this fixture.
        database.execute(
            R"sql(
                DROP TABLE job_execution_results;
                DROP TABLE job_command_args;
                DROP TABLE job_commands;

                DELETE FROM schema_migrations
                WHERE version >= 5;

                PRAGMA user_version = 4;
            )sql"
        );
    }

    {
        persistence::SqliteDatabase database{
            temporary.path
        };

        persistence::initialize_sqlite_schema(database);
        persistence::initialize_sqlite_schema(database);

        require(
            database.query_int64(
                "PRAGMA user_version;"
            ) == 7,
            "Version 5 database upgrades to the current version"
        );

        require(
            database.query_int64(
                "SELECT COUNT(*) FROM schema_migrations "
                "WHERE version = 6;"
            ) == 1,
            "Execution-result migration is recorded once"
        );

        persistence::SqliteJobRepository repository{
            database
        };

        const auto legacy = repository.get(
            domain::JobId{"legacy-sleep"}
        );

        require(
            legacy.has_value(),
            "Migration preserves existing jobs"
        );

        require(
            legacy->job.workload().sleep_duration() == 500ms,
            "Migration preserves existing sleep workload"
        );

        repository.insert(
            make_record(
                "new-command",
                domain::WorkloadSpec::command({
                    "hostname",
                    {},
                    std::nullopt
                })
            )
        );

        require(
            repository.get(
                domain::JobId{"new-command"}
            ).has_value(),
            "Migrated database accepts command workloads"
        );
    }
}

void test_version_six_migration_expands_outcomes() {
    TemporaryDatabase temporary;

    {
        persistence::SqliteDatabase database{
            temporary.path
        };

        persistence::SqliteJobRepository repository{
            database
        };

        domain::Job finished{
            domain::JobId{"legacy-result"},
            "Legacy outcome",
            1,
            domain::ResourceRequirements{
                1.0, 1024, 1024, false, {}
            },
            domain::WorkloadSpec::sleep(10ms)
        };

        finished.transition_to(domain::JobState::leased);
        finished.transition_to(domain::JobState::running);
        finished.transition_to(domain::JobState::succeeded);

        finished.set_execution_result(
            domain::ExecutionResult{
                domain::ExecutionOutcome::exited,
                0,
                std::nullopt,
                ""
            }
        );

        repository.insert(
            persistence::JobRecord{
                std::move(finished),
                std::nullopt,
                std::nullopt
            }
        );

        /*
         * Reconstruct the version-6 job_execution_results shape
         * (outcome restricted to 1-4) under an otherwise current
         * database, so the next open must migrate it forward.
         */
        database.execute(
            R"sql(
                CREATE TABLE job_execution_results_v6 (
                    job_id TEXT PRIMARY KEY NOT NULL,

                    outcome INTEGER NOT NULL
                        CHECK(outcome IN (1, 2, 3, 4)),

                    exit_code INTEGER
                        CHECK(exit_code BETWEEN 0 AND 255),

                    terminating_signal INTEGER
                        CHECK(
                            terminating_signal > 0 AND
                            terminating_signal <= 2147483647
                        ),

                    diagnostic TEXT NOT NULL,

                    CHECK(
                        (
                            outcome = 1 AND
                            exit_code IS NOT NULL AND
                            terminating_signal IS NULL
                        )
                        OR
                        (
                            outcome = 2 AND
                            exit_code IS NULL AND
                            terminating_signal IS NOT NULL
                        )
                        OR
                        (
                            outcome IN (3, 4) AND
                            exit_code IS NULL AND
                            terminating_signal IS NULL
                        )
                    ),

                    FOREIGN KEY (job_id)
                        REFERENCES jobs(job_id)
                        ON DELETE CASCADE
                );

                INSERT INTO job_execution_results_v6
                SELECT * FROM job_execution_results;

                DROP TABLE job_execution_results;

                ALTER TABLE job_execution_results_v6
                    RENAME TO job_execution_results;

                DELETE FROM schema_migrations
                WHERE version >= 7;

                PRAGMA user_version = 6;
            )sql"
        );
    }

    {
        persistence::SqliteDatabase database{
            temporary.path
        };

        persistence::initialize_sqlite_schema(database);
        persistence::initialize_sqlite_schema(database);

        require(
            database.query_int64(
                "PRAGMA user_version;"
            ) == 7,
            "Version 6 database upgrades to version 7"
        );

        require(
            database.query_int64(
                "SELECT COUNT(*) FROM schema_migrations "
                "WHERE version = 7;"
            ) == 1,
            "Outcome-expansion migration is recorded once"
        );

        persistence::SqliteJobRepository repository{
            database
        };

        const auto legacy = repository.get(
            domain::JobId{"legacy-result"}
        );

        require(
            legacy.has_value() &&
            legacy->job.execution_result().has_value() &&
            legacy->job.execution_result()->outcome ==
                domain::ExecutionOutcome::exited &&
            legacy->job.execution_result()->exit_code == 0,
            "Migration preserves the existing execution result"
        );

        domain::Job timed_out{
            domain::JobId{"new-timeout"},
            "New outcome",
            1,
            domain::ResourceRequirements{
                1.0, 1024, 1024, false, {}
            },
            domain::WorkloadSpec::sleep(10ms)
        };

        timed_out.transition_to(domain::JobState::leased);
        timed_out.transition_to(domain::JobState::running);
        timed_out.transition_to(domain::JobState::failed);

        timed_out.set_execution_result(
            domain::ExecutionResult{
                domain::ExecutionOutcome::timed_out,
                std::nullopt,
                std::nullopt,
                "Execution deadline exceeded"
            }
        );

        repository.insert(
            persistence::JobRecord{
                std::move(timed_out),
                std::nullopt,
                std::nullopt
            }
        );

        const auto stored = repository.get(
            domain::JobId{"new-timeout"}
        );

        require(
            stored.has_value() &&
            stored->job.execution_result().has_value() &&
            stored->job.execution_result()->outcome ==
                domain::ExecutionOutcome::timed_out,
            "Migrated database accepts the new TIMED_OUT outcome"
        );
    }
}

}  // namespace

int main() {
    try {
        test_persistence();
        test_version_four_migration();
        test_version_six_migration_expands_outcomes();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr
            << "[FAIL] "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }
}