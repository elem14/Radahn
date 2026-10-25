#include "radahn/persistence/sqlite_job_log_repository.hpp"

#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <sqlite3.h>

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
    std::string message{operation};

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
    const std::string owned_sql{sql};

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
        throw std::runtime_error(
            std::string("Failed SQL:\n") +
            owned_sql +
            "\n\nSQLite says:\n" +
            sqlite3_errmsg(database)
        );
    }

    return StatementPointer{raw_statement};
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
            static_cast<int>(value.size()),
            SQLITE_TRANSIENT
        ),
        field_name
    );
}

void bind_blob(
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
        sqlite3_bind_blob(
            statement,
            index,
            value.data(),
            static_cast<int>(value.size()),
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

[[nodiscard]]
std::int64_t checked_sql_integer(
    std::uint64_t value,
    std::string_view field_name
) {
    if (
        value >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max()
        )
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " exceeds SQLite's signed integer range"
        };
    }

    return static_cast<std::int64_t>(value);
}

[[nodiscard]]
std::uint64_t checked_unsigned_column(
    sqlite3_stmt* statement,
    int column,
    std::string_view field_name
) {
    const std::int64_t value =
        sqlite3_column_int64(statement, column);

    if (value < 0) {
        throw std::runtime_error{
            std::string{field_name} +
            " contains a negative database value"
        };
    }

    return static_cast<std::uint64_t>(value);
}

[[nodiscard]]
std::string text_column(
    sqlite3_stmt* statement,
    int column,
    std::string_view field_name
) {
    const auto* text =
        sqlite3_column_text(statement, column);

    if (text == nullptr) {
        throw std::runtime_error{
            std::string{field_name} +
            " unexpectedly contains NULL"
        };
    }

    return std::string{
        reinterpret_cast<const char*>(text)
    };
}

[[nodiscard]]
std::string blob_column(
    sqlite3_stmt* statement,
    int column
) {
    const auto* data =
        sqlite3_column_blob(statement, column);

    const int size =
        sqlite3_column_bytes(statement, column);

    if (data == nullptr || size == 0) {
        return {};
    }

    return std::string{
        reinterpret_cast<const char*>(data),
        static_cast<std::size_t>(size)
    };
}

[[nodiscard]]
int stream_to_integer(
    JobLogStream stream
) {
    switch (stream) {
        case JobLogStream::standard_output:
            return 1;

        case JobLogStream::standard_error:
            return 2;
    }

    throw std::invalid_argument{
        "Unsupported log stream"
    };
}

[[nodiscard]]
JobLogStream stream_from_integer(
    int value
) {
    switch (value) {
        case 1:
            return JobLogStream::standard_output;

        case 2:
            return JobLogStream::standard_error;

        default:
            throw std::runtime_error{
                "Database contains an unknown log stream"
            };
    }
}

[[nodiscard]]
JobLogRecord record_from_row(
    sqlite3_stmt* statement
) {
    const domain::JobId job_id{
        text_column(statement, 0, "job_id")
    };

    const std::uint64_t attempt_number =
        checked_unsigned_column(
            statement,
            1,
            "attempt_number"
        );

    const JobLogStream stream =
        stream_from_integer(
            sqlite3_column_int(statement, 2)
        );

    const domain::WorkerId worker_id{
        text_column(statement, 3, "worker_id")
    };

    std::string content =
        blob_column(statement, 4);

    const std::uint64_t original_byte_count =
        checked_unsigned_column(
            statement,
            5,
            "original_byte_count"
        );

    const bool truncated =
        sqlite3_column_int(statement, 6) != 0;

    return JobLogRecord{
        job_id,
        attempt_number,
        stream,
        worker_id,
        std::move(content),
        original_byte_count,
        truncated
    };
}

void rollback_preserving_original_error(
    SqliteDatabase& database
) noexcept {
    try {
        database.rollback_transaction();
    } catch (...) {
        // Preserve the original exception.
    }
}

constexpr std::string_view select_columns =
    R"sql(
        job_id,
        attempt_number,
        stream,
        worker_id,
        content,
        original_byte_count,
        truncated
    )sql";

}  // namespace

SqliteJobLogRepository::SqliteJobLogRepository(
    SqliteDatabase& database
)
    : database_{database} {
    initialize_sqlite_schema(database_);
}

void SqliteJobLogRepository::insert(
    JobLogRecord record
) {
    sqlite3* database =
        database_.native_handle();

    database_.begin_transaction();

    try {
        auto statement =
            prepare_statement(
                database,
                R"sql(
                    INSERT INTO job_logs (
                        job_id,
                        attempt_number,
                        stream,
                        worker_id,
                        content,
                        original_byte_count,
                        truncated
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                )sql"
            );

        bind_text(
            database,
            statement.get(),
            1,
            record.job_id.value(),
            "job_id"
        );

        bind_int64(
            database,
            statement.get(),
            2,
            checked_sql_integer(
                record.attempt_number,
                "attempt_number"
            ),
            "attempt_number"
        );

        bind_int64(
            database,
            statement.get(),
            3,
            stream_to_integer(record.stream),
            "stream"
        );

        bind_text(
            database,
            statement.get(),
            4,
            record.worker_id.value(),
            "worker_id"
        );

        bind_blob(
            database,
            statement.get(),
            5,
            record.content,
            "content"
        );

        bind_int64(
            database,
            statement.get(),
            6,
            checked_sql_integer(
                record.original_byte_count,
                "original_byte_count"
            ),
            "original_byte_count"
        );

        bind_int64(
            database,
            statement.get(),
            7,
            record.truncated ? 1 : 0,
            "truncated"
        );

        const int result =
            sqlite3_step(statement.get());

        if (result != SQLITE_DONE) {
            if (is_constraint_result(result)) {
                throw std::invalid_argument{
                    "A log already exists for this "
                    "(job_id, attempt_number, stream) key"
                };
            }

            throw database_error(
                database,
                "Could not insert job log"
            );
        }

        database_.commit_transaction();
    } catch (...) {
        rollback_preserving_original_error(database_);

        throw;
    }
}

std::optional<JobLogRecord>
SqliteJobLogRepository::get(
    const domain::JobId& job_id,
    std::uint64_t attempt_number,
    JobLogStream stream
) const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            std::string{"SELECT "} +
            std::string{select_columns} +
            R"sql(
                FROM job_logs
                WHERE job_id = ?
                    AND attempt_number = ?
                    AND stream = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        job_id.value(),
        "job_id"
    );

    bind_int64(
        database,
        statement.get(),
        2,
        checked_sql_integer(attempt_number, "attempt_number"),
        "attempt_number"
    );

    bind_int64(
        database,
        statement.get(),
        3,
        stream_to_integer(stream),
        "stream"
    );

    const int result =
        sqlite3_step(statement.get());

    if (result == SQLITE_DONE) {
        return std::nullopt;
    }

    if (result != SQLITE_ROW) {
        throw database_error(
            database,
            "Could not retrieve job log"
        );
    }

    return record_from_row(statement.get());
}

bool SqliteJobLogRepository::contains(
    const domain::JobId& job_id,
    std::uint64_t attempt_number,
    JobLogStream stream
) const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT 1
                FROM job_logs
                WHERE job_id = ?
                    AND attempt_number = ?
                    AND stream = ?
                LIMIT 1;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        job_id.value(),
        "job_id"
    );

    bind_int64(
        database,
        statement.get(),
        2,
        checked_sql_integer(attempt_number, "attempt_number"),
        "attempt_number"
    );

    bind_int64(
        database,
        statement.get(),
        3,
        stream_to_integer(stream),
        "stream"
    );

    const int result =
        sqlite3_step(statement.get());

    if (result == SQLITE_ROW) {
        return true;
    }

    if (result == SQLITE_DONE) {
        return false;
    }

    throw database_error(
        database,
        "Could not check whether job log exists"
    );
}

void SqliteJobLogRepository::erase(
    const domain::JobId& job_id,
    std::uint64_t attempt_number,
    JobLogStream stream
) {
    if (!contains(job_id, attempt_number, stream)) {
        throw std::invalid_argument{
            "Cannot erase an unknown job log"
        };
    }

    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                DELETE FROM job_logs
                WHERE job_id = ?
                    AND attempt_number = ?
                    AND stream = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        job_id.value(),
        "job_id"
    );

    bind_int64(
        database,
        statement.get(),
        2,
        checked_sql_integer(attempt_number, "attempt_number"),
        "attempt_number"
    );

    bind_int64(
        database,
        statement.get(),
        3,
        stream_to_integer(stream),
        "stream"
    );

    const int result =
        sqlite3_step(statement.get());

    if (result != SQLITE_DONE) {
        throw database_error(
            database,
            "Could not erase job log"
        );
    }
}

std::size_t
SqliteJobLogRepository::size() const {
    const std::int64_t count =
        database_.query_int64(
            "SELECT COUNT(*) FROM job_logs;"
        );

    if (count < 0) {
        throw std::runtime_error{
            "SQLite returned a negative job log count"
        };
    }

    return static_cast<std::size_t>(count);
}

}  // namespace radahn::persistence
