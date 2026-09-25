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

#include "radahn/domain/job.hpp"
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
                DROP TABLE job_command_args;
                DROP TABLE job_commands;

                DELETE FROM schema_migrations
                WHERE version = 5;

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
            ) == 5,
            "Version 4 database upgrades to version 5"
        );

        require(
            database.query_int64(
                "SELECT COUNT(*) FROM schema_migrations "
                "WHERE version = 5;"
            ) == 1,
            "Repeated initialization records migration once"
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

}  // namespace

int main() {
    try {
        test_persistence();
        test_version_four_migration();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr
            << "[FAIL] "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }
}