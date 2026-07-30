#include "radahn/persistence/sqlite_schema.hpp"

#include <stdexcept>

#include "radahn/persistence/sqlite_database.hpp"

namespace radahn::persistence {

namespace {

constexpr std::int64_t current_schema_version = 1;

}  // namespace

void initialize_sqlite_schema(
    SqliteDatabase& database
) {
    const std::int64_t existing_version =
        database.query_int64(
            "PRAGMA user_version;"
        );

    if (
        existing_version >
        current_schema_version
    ) {
        throw std::runtime_error{
            "SQLite database schema is newer "
            "than this Radahn build supports"
        };
    }

    database.execute(
        "BEGIN IMMEDIATE TRANSACTION;"
    );

    try {
        database.execute(
            R"sql(
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY NOT NULL,

                    state INTEGER NOT NULL,

                    total_cpu_cores REAL NOT NULL
                        CHECK(total_cpu_cores >= 0),

                    available_cpu_cores REAL NOT NULL
                        CHECK(available_cpu_cores >= 0),

                    total_memory_bytes INTEGER NOT NULL
                        CHECK(total_memory_bytes >= 0),

                    available_memory_bytes INTEGER NOT NULL
                        CHECK(available_memory_bytes >= 0),

                    total_disk_bytes INTEGER NOT NULL
                        CHECK(total_disk_bytes >= 0),

                    available_disk_bytes INTEGER NOT NULL
                        CHECK(available_disk_bytes >= 0),

                    gpu_available INTEGER NOT NULL
                        CHECK(gpu_available IN (0, 1)),

                    running_jobs INTEGER NOT NULL
                        CHECK(running_jobs >= 0),

                    max_concurrent_jobs INTEGER NOT NULL
                        CHECK(max_concurrent_jobs >= 0)
                );

                CREATE TABLE IF NOT EXISTS worker_tags (
                    worker_id TEXT NOT NULL,
                    tag TEXT NOT NULL,

                    PRIMARY KEY (
                        worker_id,
                        tag
                    ),

                    FOREIGN KEY (worker_id)
                        REFERENCES workers(worker_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    state INTEGER NOT NULL,

                    cpu_cores REAL NOT NULL
                        CHECK(cpu_cores >= 0),

                    memory_bytes INTEGER NOT NULL
                        CHECK(memory_bytes >= 0),

                    disk_bytes INTEGER NOT NULL
                        CHECK(disk_bytes >= 0),

                    requires_gpu INTEGER NOT NULL
                        CHECK(requires_gpu IN (0, 1)),

                    workload_kind INTEGER NOT NULL,

                    sleep_duration_ms INTEGER NOT NULL
                        CHECK(sleep_duration_ms >= 0),

                    created_at_unix_ms INTEGER NOT NULL,

                    assigned_worker_id TEXT,

                    FOREIGN KEY (assigned_worker_id)
                        REFERENCES workers(worker_id)
                        ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS job_required_tags (
                    job_id TEXT NOT NULL,
                    tag TEXT NOT NULL,

                    PRIMARY KEY (
                        job_id,
                        tag
                    ),

                    FOREIGN KEY (job_id)
                        REFERENCES jobs(job_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY NOT NULL,
                    applied_at_unix_ms INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS
                    index_jobs_state
                ON jobs(state);

                CREATE INDEX IF NOT EXISTS
                    index_jobs_assigned_worker
                ON jobs(assigned_worker_id);

                CREATE INDEX IF NOT EXISTS
                    index_worker_tags_tag
                ON worker_tags(tag);

                CREATE INDEX IF NOT EXISTS
                    index_job_required_tags_tag
                ON job_required_tags(tag);
            )sql"
        );

        database.execute(
            R"sql(
                INSERT OR IGNORE INTO schema_migrations (
                    version,
                    applied_at_unix_ms
                )
                VALUES (
                    1,
                    CAST(strftime('%s', 'now') AS INTEGER)
                        * 1000
                );
            )sql"
        );

        database.execute(
            "PRAGMA user_version = 1;"
        );

        database.execute(
            "COMMIT;"
        );
    } catch (...) {
        try {
            database.execute(
                "ROLLBACK;"
            );
        } catch (...) {
            // preserves original schema exception.
        }

        throw;
    }
}

}  // namespace radahn::persistence