#include "radahn/persistence/sqlite_database.hpp"

#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <sqlite3.h>

namespace radahn::persistence {

namespace {

[[nodiscard]]
std::runtime_error sqlite_error(
    sqlite3* database,
    std::string_view operation
) {
    std::string message{
        operation
    };

    message += ": ";

    if (database != nullptr) {
        message += sqlite3_errmsg(database);
    } else {
        message += "unknown SQLite error";
    }

    return std::runtime_error{
        std::move(message)
    };
}

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

}  // namespace

SqliteDatabase::SqliteDatabase(
    const std::filesystem::path& database_path
) {
    const std::string path_string =
        database_path.string();

    sqlite3* opened_database = nullptr;

    const int open_result =
        sqlite3_open_v2(
            path_string.c_str(),
            &opened_database,
            SQLITE_OPEN_READWRITE |
                SQLITE_OPEN_CREATE |
                SQLITE_OPEN_FULLMUTEX,
            nullptr
        );

    if (open_result != SQLITE_OK) {
        std::string error_message{
            "Could not open SQLite database"
        };

        if (opened_database != nullptr) {
            error_message += ": ";
            error_message +=
                sqlite3_errmsg(
                    opened_database
                );

            sqlite3_close_v2(
                opened_database
            );
        }

        throw std::runtime_error{
            std::move(error_message)
        };
    }

    database_ = opened_database;

    sqlite3_extended_result_codes(
        database_,
        1
    );

    const int timeout_result =
        sqlite3_busy_timeout(
            database_,
            5'000
        );

    if (timeout_result != SQLITE_OK) {
        const auto error =
            sqlite_error(
                database_,
                "Could not configure SQLite busy timeout"
            );

        sqlite3_close_v2(database_);
        database_ = nullptr;

        throw error;
    }

    try {
        execute(
            "PRAGMA foreign_keys = ON;"
        );
    } catch (...) {
        sqlite3_close_v2(database_);
        database_ = nullptr;
        throw;
    }
}

SqliteDatabase::~SqliteDatabase() {
    if (database_ != nullptr) {
        sqlite3_close_v2(
            database_
        );
    }
}

void SqliteDatabase::execute(
    std::string_view sql
) {
    const std::string owned_sql{
        sql
    };

    char* sqlite_message = nullptr;

    const int result =
        sqlite3_exec(
            database_,
            owned_sql.c_str(),
            nullptr,
            nullptr,
            &sqlite_message
        );

    if (result == SQLITE_OK) {
        return;
    }

    std::string message{
        "SQLite execution failed"
    };

    if (sqlite_message != nullptr) {
        message += ": ";
        message += sqlite_message;

        sqlite3_free(
            sqlite_message
        );
    } else {
        message += ": ";
        message +=
            sqlite3_errmsg(
                database_
            );
    }

    throw std::runtime_error{
        std::move(message)
    };
}

std::int64_t SqliteDatabase::query_int64(
    std::string_view sql
) const {
    const std::string owned_sql{
        sql
    };

    sqlite3_stmt* raw_statement = nullptr;

    const int prepare_result =
        sqlite3_prepare_v2(
            database_,
            owned_sql.c_str(),
            -1,
            &raw_statement,
            nullptr
        );

    if (prepare_result != SQLITE_OK) {
        throw sqlite_error(
            database_,
            "Could not prepare integer query"
        );
    }

    StatementPointer statement{
        raw_statement
    };

    const int step_result =
        sqlite3_step(
            statement.get()
        );

    if (step_result != SQLITE_ROW) {
        throw sqlite_error(
            database_,
            "Integer query did not return a row"
        );
    }

    return sqlite3_column_int64(
        statement.get(),
        0
    );
}

void SqliteDatabase::begin_transaction() {
    if (transaction_depth_ == 0) {
        execute(
            "BEGIN IMMEDIATE TRANSACTION;"
        );
    } else {
        execute(
            "SAVEPOINT radahn_sp_" +
            std::to_string(transaction_depth_) +
            ";"
        );
    }

    ++transaction_depth_;
}

void SqliteDatabase::commit_transaction() {
    if (transaction_depth_ <= 0) {
        throw std::runtime_error{
            "commit_transaction called without a matching "
            "begin_transaction"
        };
    }

    --transaction_depth_;

    if (transaction_depth_ == 0) {
        execute(
            "COMMIT;"
        );
    } else {
        execute(
            "RELEASE SAVEPOINT radahn_sp_" +
            std::to_string(transaction_depth_) +
            ";"
        );
    }
}

void SqliteDatabase::rollback_transaction() {
    if (transaction_depth_ <= 0) {
        throw std::runtime_error{
            "rollback_transaction called without a matching "
            "begin_transaction"
        };
    }

    --transaction_depth_;

    if (transaction_depth_ == 0) {
        execute(
            "ROLLBACK;"
        );
    } else {
        const auto savepoint_name =
            "radahn_sp_" +
            std::to_string(transaction_depth_);

        execute(
            "ROLLBACK TO SAVEPOINT " +
            savepoint_name +
            ";"
        );

        execute(
            "RELEASE SAVEPOINT " +
            savepoint_name +
            ";"
        );
    }
}

sqlite3*
SqliteDatabase::native_handle() const noexcept {
    return database_;
}

}  // namespace radahn::persistence