#include "radahn/persistence/sqlite_worker_repository.hpp"

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include "radahn/domain/id.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/persistence/sqlite_schema.hpp"

namespace radahn::persistence {

namespace {

struct StatementDeleter {
    void operator()(
        sqlite3_stmt* statement
    ) const noexcept {
        sqlite3_finalize(statement);
    }
};

using StatementPointer =
    std::unique_ptr<
        sqlite3_stmt,
        StatementDeleter
    >;

[[nodiscard]]
std::runtime_error database_error(
    sqlite3* database,
    std::string_view operation
) {
    std::string message{
        operation
    };

    message += ": ";
    message += sqlite3_errmsg(database);

    return std::runtime_error{
        std::move(message)
    };
}

[[nodiscard]]
bool is_constraint_result(
    int result
) noexcept {
    return
        (result & 0xFF) ==
        SQLITE_CONSTRAINT;
}

[[nodiscard]]
StatementPointer prepare_statement(
    sqlite3* database,
    std::string_view sql
) {
    const std::string owned_sql{
        sql
    };

    sqlite3_stmt* raw_statement = nullptr;

    const int result =
        sqlite3_prepare_v2(
            database,
            owned_sql.c_str(),
            -1,
            &raw_statement,
            nullptr
        );

    if (result != SQLITE_OK) {
        throw database_error(
            database,
            "Could not prepare SQLite statement"
        );
    }

    return StatementPointer{
        raw_statement
    };
}

void require_bind_success(
    sqlite3* database,
    int result,
    std::string_view field_name
) {
    if (result == SQLITE_OK) {
        return;
    }

    throw database_error(
        database,
        std::string{"Could not bind "} +
            std::string{field_name}
    );
}

void bind_text(
    sqlite3* database,
    sqlite3_stmt* statement,
    int index,
    std::string_view value,
    std::string_view field_name
) {
    if (
        value.size() >
        static_cast<std::size_t>(
            std::numeric_limits<int>::max()
        )
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " is too large for SQLite"
        };
    }

    require_bind_success(
        database,
        sqlite3_bind_text(
            statement,
            index,
            value.data(),
            static_cast<int>(
                value.size()
            ),
            SQLITE_TRANSIENT
        ),
        field_name
    );
}

void bind_int64(
    sqlite3* database,
    sqlite3_stmt* statement,
    int index,
    std::int64_t value,
    std::string_view field_name
) {
    require_bind_success(
        database,
        sqlite3_bind_int64(
            statement,
            index,
            value
        ),
        field_name
    );
}

void bind_double(
    sqlite3* database,
    sqlite3_stmt* statement,
    int index,
    double value,
    std::string_view field_name
) {
    require_bind_success(
        database,
        sqlite3_bind_double(
            statement,
            index,
            value
        ),
        field_name
    );
}

[[nodiscard]]
std::int64_t checked_sql_integer(
    std::uint64_t value,
    std::string_view field_name
) {
    if (
        value >
        static_cast<std::uint64_t>(
            std::numeric_limits<
                std::int64_t
            >::max()
        )
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " exceeds SQLite's signed integer range"
        };
    }

    return static_cast<std::int64_t>(
        value
    );
}

[[nodiscard]]
std::int64_t checked_sql_integer(
    std::size_t value,
    std::string_view field_name
) {
    if (
        value >
        static_cast<std::size_t>(
            std::numeric_limits<
                std::int64_t
            >::max()
        )
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " exceeds SQLite's signed integer range"
        };
    }

    return static_cast<std::int64_t>(
        value
    );
}

[[nodiscard]]
std::uint64_t checked_unsigned_column(
    sqlite3_stmt* statement,
    int column,
    std::string_view field_name
) {
    const std::int64_t value =
        sqlite3_column_int64(
            statement,
            column
        );

    if (value < 0) {
        throw std::runtime_error{
            std::string{field_name} +
            " contains a negative database value"
        };
    }

    return static_cast<std::uint64_t>(
        value
    );
}

[[nodiscard]]
std::size_t checked_size_column(
    sqlite3_stmt* statement,
    int column,
    std::string_view field_name
) {
    const std::uint64_t value =
        checked_unsigned_column(
            statement,
            column,
            field_name
        );

    if (
        value >
        static_cast<std::uint64_t>(
            std::numeric_limits<
                std::size_t
            >::max()
        )
    ) {
        throw std::runtime_error{
            std::string{field_name} +
            " exceeds the platform size_t range"
        };
    }

    return static_cast<std::size_t>(
        value
    );
}

[[nodiscard]]
std::int64_t heartbeat_to_unix_ms(
    WorkerHeartbeatTimePoint heartbeat_time
) {
    return 
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(
            heartbeat_time.time_since_epoch()
        ).count();
}

[[nodiscard]]
WorkerHeartbeatTimePoint heartbeat_from_unix_ms(
    std::int64_t unix_ms
) {
    if (unix_ms < 0) {
        throw std::runtime_error{
            "Stored heartbeat timestamp is negative"
        };
    }

    return WorkerHeartbeatTimePoint{
        std::chrono::duration_cast<
            WorkerHeartbeatClock::duration
        >(
            std::chrono::milliseconds{
                unix_ms
            }
        )
    };
}

[[nodiscard]]
std::string text_column(
    sqlite3_stmt* statement,
    int column,
    std::string_view field_name
) {
    const auto* text =
        sqlite3_column_text(
            statement,
            column
        );

    if (text == nullptr) {
        throw std::runtime_error{
            std::string{field_name} +
            " unexpectedly contains NULL"
        };
    }

    return std::string{
        reinterpret_cast<
            const char*
        >(text)
    };
}

[[nodiscard]]
int worker_state_to_integer(
    domain::WorkerState state
) noexcept {
    return static_cast<int>(
        state
    );
}

[[nodiscard]]
domain::WorkerState worker_state_from_integer(
    int value
) noexcept {
    return static_cast<
        domain::WorkerState
    >(value);
}

[[nodiscard]]
std::vector<std::string> load_worker_tags(
    sqlite3* database,
    const domain::WorkerId& worker_id
) {
    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT tag
                FROM worker_tags
                WHERE worker_id = ?
                ORDER BY tag;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        worker_id.value(),
        "worker_id"
    );

    std::vector<std::string> tags;

    while (true) {
        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result == SQLITE_DONE) {
            break;
        }

        if (result != SQLITE_ROW) {
            throw database_error(
                database,
                "Could not read worker tags"
            );
        }

        tags.push_back(
            text_column(
                statement.get(),
                0,
                "tag"
            )
        );
    }

    return tags;
}

void insert_worker_tags(
    sqlite3* database,
    const domain::WorkerRecord& worker
) {
    auto statement =
        prepare_statement(
            database,
            R"sql(
                INSERT INTO worker_tags (
                    worker_id,
                    tag
                )
                VALUES (?, ?);
            )sql"
        );

    const auto snapshot =
        worker.snapshot();

    for (const auto& tag : snapshot.tags()) {
        sqlite3_reset(
            statement.get()
        );

        sqlite3_clear_bindings(
            statement.get()
        );

        bind_text(
            database,
            statement.get(),
            1,
            worker.id().value(),
            "worker_id"
        );

        bind_text(
            database,
            statement.get(),
            2,
            tag,
            "tag"
        );

        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result == SQLITE_DONE) {
            continue;
        }

        if (is_constraint_result(result)) {
            throw std::invalid_argument{
                "Worker contains duplicate or invalid tags"
            };
        }

        throw database_error(
            database,
            "Could not insert worker tag"
        );
    }
}

void delete_worker_tags(
    sqlite3* database,
    const domain::WorkerId& worker_id
) {
    auto statement =
        prepare_statement(
            database,
            R"sql(
                DELETE FROM worker_tags
                WHERE worker_id = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        worker_id.value(),
        "worker_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result != SQLITE_DONE) {
        throw database_error(
            database,
            "Could not delete existing worker tags"
        );
    }
}

[[nodiscard]]
domain::WorkerRecord worker_from_row(
    sqlite3* database,
    sqlite3_stmt* statement
) {
    const std::string worker_id =
        text_column(
            statement,
            0,
            "worker_id"
        );

    const domain::WorkerState state =
        worker_state_from_integer(
            sqlite3_column_int(
                statement,
                1
            )
        );

    const double total_cpu_cores =
        sqlite3_column_double(
            statement,
            2
        );

    const double available_cpu_cores =
        sqlite3_column_double(
            statement,
            3
        );

    const std::uint64_t total_memory_bytes =
        checked_unsigned_column(
            statement,
            4,
            "total_memory_bytes"
        );

    const std::uint64_t available_memory_bytes =
        checked_unsigned_column(
            statement,
            5,
            "available_memory_bytes"
        );

    const std::uint64_t total_disk_bytes =
        checked_unsigned_column(
            statement,
            6,
            "total_disk_bytes"
        );

    const std::uint64_t available_disk_bytes =
        checked_unsigned_column(
            statement,
            7,
            "available_disk_bytes"
        );

    const bool gpu_available =
        sqlite3_column_int(
            statement,
            8
        ) != 0;

    const std::size_t running_jobs =
        checked_size_column(
            statement,
            9,
            "running_jobs"
        );

    const std::size_t max_concurrent_jobs =
        checked_size_column(
            statement,
            10,
            "max_concurrent_jobs"
        );

    const domain::WorkerId domain_worker_id{
        worker_id
    };

    auto tags =
        load_worker_tags(
            database,
            domain_worker_id
        );

    domain::WorkerResources resources{
        total_cpu_cores,
        available_cpu_cores,
        total_memory_bytes,
        available_memory_bytes,
        total_disk_bytes,
        available_disk_bytes,
        gpu_available
    };

    domain::WorkerSnapshot snapshot{
        domain_worker_id,
        state,
        std::move(resources),
        running_jobs,
        max_concurrent_jobs,
        std::move(tags)
    };

    return domain::WorkerRecord{
        std::move(snapshot)
    };
}

void rollback_preserving_original_error(
    SqliteDatabase& database
) noexcept {
    try {
        database.execute(
            "ROLLBACK;"
        );
    } catch (...) {
        // Preserve the original exception.
    }
}

}  // namespace

SqliteWorkerRepository::SqliteWorkerRepository(
    SqliteDatabase& database
)
    : database_{database} {
    initialize_sqlite_schema(
        database_
    );
}

void SqliteWorkerRepository::insert(
    domain::WorkerRecord worker
) {
    sqlite3* database =
        database_.native_handle();

    database_.execute(
        "BEGIN IMMEDIATE TRANSACTION;"
    );

    try {
        const auto snapshot =
            worker.snapshot();

        const auto& resources =
            snapshot.resources();

        auto statement =
            prepare_statement(
                database,
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
                        max_concurrent_jobs,
                        last_heartbeat_unix_ms
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?
                    );
                )sql"
            );

        bind_text(
            database,
            statement.get(),
            1,
            worker.id().value(),
            "worker_id"
        );

        bind_int64(
            database,
            statement.get(),
            2,
            worker_state_to_integer(
                snapshot.state()
            ),
            "state"
        );

        bind_double(
            database,
            statement.get(),
            3,
            resources.total_cpu_cores(),
            "total_cpu_cores"
        );

        bind_double(
            database,
            statement.get(),
            4,
            resources.available_cpu_cores(),
            "available_cpu_cores"
        );

        bind_int64(
            database,
            statement.get(),
            5,
            checked_sql_integer(
                resources.total_memory_bytes(),
                "total_memory_bytes"
            ),
            "total_memory_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            6,
            checked_sql_integer(
                resources.available_memory_bytes(),
                "available_memory_bytes"
            ),
            "available_memory_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            7,
            checked_sql_integer(
                resources.total_disk_bytes(),
                "total_disk_bytes"
            ),
            "total_disk_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            8,
            checked_sql_integer(
                resources.available_disk_bytes(),
                "available_disk_bytes"
            ),
            "available_disk_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            9,
            resources.gpu_available()
                ? 1
                : 0,
            "gpu_available"
        );

        bind_int64(
            database,
            statement.get(),
            10,
            checked_sql_integer(
                snapshot.running_jobs(),
                "running_jobs"
            ),
            "running_jobs"
        );

        bind_int64(
            database,
            statement.get(),
            11,
            checked_sql_integer(
                snapshot.max_concurrent_jobs(),
                "max_concurrent_jobs"
            ),
            "max_concurrent_jobs"
        );

        bind_int64(
            database,
            statement.get(),
            12,
            heartbeat_to_unix_ms(
                WorkerHeartbeatClock::now()
            ),
            "last_heartbeat_unix_ms"
        );

        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result != SQLITE_DONE) {
            if (is_constraint_result(result)) {
                throw std::invalid_argument{
                    "Worker insert violated a repository constraint"
                };
            }

            throw database_error(
                database,
                "Could not insert worker"
            );
        }

        insert_worker_tags(
            database,
            worker
        );

        database_.execute(
            "COMMIT;"
        );
    } catch (...) {
        rollback_preserving_original_error(
            database_
        );

        throw;
    }
}

void SqliteWorkerRepository::update(
    domain::WorkerRecord worker
) {
    const domain::WorkerId worker_id{
        worker.id()
    };

    if (!contains(worker_id)) {
        throw std::invalid_argument{
            "Cannot update an unknown worker"
        };
    }

    sqlite3* database =
        database_.native_handle();

    database_.execute(
        "BEGIN IMMEDIATE TRANSACTION;"
    );

    try {
        const auto snapshot =
            worker.snapshot();

        const auto& resources =
            snapshot.resources();

        auto statement =
            prepare_statement(
                database,
                R"sql(
                    UPDATE workers
                    SET
                        state = ?,
                        total_cpu_cores = ?,
                        available_cpu_cores = ?,
                        total_memory_bytes = ?,
                        available_memory_bytes = ?,
                        total_disk_bytes = ?,
                        available_disk_bytes = ?,
                        gpu_available = ?,
                        running_jobs = ?,
                        max_concurrent_jobs = ?
                    WHERE worker_id = ?;
                )sql"
            );

        bind_int64(
            database,
            statement.get(),
            1,
            worker_state_to_integer(
                snapshot.state()
            ),
            "state"
        );

        bind_double(
            database,
            statement.get(),
            2,
            resources.total_cpu_cores(),
            "total_cpu_cores"
        );

        bind_double(
            database,
            statement.get(),
            3,
            resources.available_cpu_cores(),
            "available_cpu_cores"
        );

        bind_int64(
            database,
            statement.get(),
            4,
            checked_sql_integer(
                resources.total_memory_bytes(),
                "total_memory_bytes"
            ),
            "total_memory_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            5,
            checked_sql_integer(
                resources.available_memory_bytes(),
                "available_memory_bytes"
            ),
            "available_memory_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            6,
            checked_sql_integer(
                resources.total_disk_bytes(),
                "total_disk_bytes"
            ),
            "total_disk_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            7,
            checked_sql_integer(
                resources.available_disk_bytes(),
                "available_disk_bytes"
            ),
            "available_disk_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            8,
            resources.gpu_available()
                ? 1
                : 0,
            "gpu_available"
        );

        bind_int64(
            database,
            statement.get(),
            9,
            checked_sql_integer(
                snapshot.running_jobs(),
                "running_jobs"
            ),
            "running_jobs"
        );

        bind_int64(
            database,
            statement.get(),
            10,
            checked_sql_integer(
                snapshot.max_concurrent_jobs(),
                "max_concurrent_jobs"
            ),
            "max_concurrent_jobs"
        );

        bind_text(
            database,
            statement.get(),
            11,
            worker_id.value(),
            "worker_id"
        );

        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result != SQLITE_DONE) {
            if (is_constraint_result(result)) {
                throw std::invalid_argument{
                    "Worker update violated a repository constraint"
                };
            }

            throw database_error(
                database,
                "Could not update worker"
            );
        }

        delete_worker_tags(
            database,
            worker_id
        );

        insert_worker_tags(
            database,
            worker
        );

        database_.execute(
            "COMMIT;"
        );
    } catch (...) {
        rollback_preserving_original_error(
            database_
        );

        throw;
    }
}

std::optional<domain::WorkerRecord>
SqliteWorkerRepository::get(
    const domain::WorkerId& worker_id
) const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT
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
                FROM workers
                WHERE worker_id = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        worker_id.value(),
        "worker_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result == SQLITE_DONE) {
        return std::nullopt;
    }

    if (result != SQLITE_ROW) {
        throw database_error(
            database,
            "Could not retrieve worker"
        );
    }

    return worker_from_row(
        database,
        statement.get()
    );
}

std::vector<domain::WorkerRecord>
SqliteWorkerRepository::list() const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT
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
                FROM workers
                ORDER BY worker_id;
            )sql"
        );

    std::vector<domain::WorkerRecord>
        workers;

    while (true) {
        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result == SQLITE_DONE) {
            break;
        }

        if (result != SQLITE_ROW) {
            throw database_error(
                database,
                "Could not list workers"
            );
        }

        workers.push_back(
            worker_from_row(
                database,
                statement.get()
            )
        );
    }

    return workers;
}

bool SqliteWorkerRepository::contains(
    const domain::WorkerId& worker_id
) const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT 1
                FROM workers
                WHERE worker_id = ?
                LIMIT 1;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        worker_id.value(),
        "worker_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result == SQLITE_ROW) {
        return true;
    }

    if (result == SQLITE_DONE) {
        return false;
    }

    throw database_error(
        database,
        "Could not check whether worker exists"
    );
}

void SqliteWorkerRepository::record_heartbeat(
    const domain::WorkerId& worker_id,
    WorkerHeartbeatTimePoint heartbeat_time
) {
    if (!contains(worker_id)) {
        throw std::invalid_argument{
            "Cannot record a heartbeat for an unknown worker"
        };
    }

    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                UPDATE workers
                SET last_heartbeat_unix_ms = ?
                WHERE worker_id = ?;
            )sql"
        );

    bind_int64(
        database,
        statement.get(),
        1,
        heartbeat_to_unix_ms(
            heartbeat_time
        ),
        "last_heartbeat_unix_ms"
    );

    bind_text(
        database,
        statement.get(),
        2,
        worker_id.value(),
        "worker_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result != SQLITE_DONE) {
        throw database_error(
            database,
            "Could not record worker heartbeat"
        );
    }
}

std::optional<WorkerHeartbeatTimePoint>
SqliteWorkerRepository::last_heartbeat(
    const domain::WorkerId& worker_id
) const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT last_heartbeat_unix_ms
                FROM workers
                WHERE worker_id = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        worker_id.value(),
        "worker_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result == SQLITE_DONE) {
        return std::nullopt;
    }

    if (result != SQLITE_ROW) {
        throw database_error(
            database,
            "Could not retrieve worker heartbeat"
        );
    }

    return heartbeat_from_unix_ms(
        sqlite3_column_int64(
            statement.get(),
            0
        )
    );
}

void SqliteWorkerRepository::erase(
    const domain::WorkerId& worker_id
) {
    if (!contains(worker_id)) {
        throw std::invalid_argument{
            "Cannot erase an unknown worker"
        };
    }

    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                DELETE FROM workers
                WHERE worker_id = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        worker_id.value(),
        "worker_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result != SQLITE_DONE) {
        throw database_error(
            database,
            "Could not erase worker"
        );
    }
}

std::size_t
SqliteWorkerRepository::size() const {
    const std::int64_t count =
        database_.query_int64(
            "SELECT COUNT(*) FROM workers;"
        );

    if (count < 0) {
        throw std::runtime_error{
            "SQLite returned a negative worker count"
        };
    }

    if (
        static_cast<std::uint64_t>(count) >
        static_cast<std::uint64_t>(
            std::numeric_limits<
                std::size_t
            >::max()
        )
    ) {
        throw std::runtime_error{
            "Stored worker count exceeds size_t"
        };
    }

    return static_cast<std::size_t>(
        count
    );
}

}  // namespace radahn::persistence