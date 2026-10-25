#pragma once

#include <cstdint>
#include <filesystem>
#include <string_view>

// Forward declaration from SQLite.
struct sqlite3;

namespace radahn::persistence {

class SqliteDatabase {
public:
    /*
     * Opens an existing SQLite database or creates a new one.
     *
     * The special path ":memory:" creates a temporary,
     * process-local database useful for tests.
     */
    explicit SqliteDatabase(
        const std::filesystem::path& database_path
    );

    ~SqliteDatabase();

    SqliteDatabase(
        const SqliteDatabase&
    ) = delete;

    SqliteDatabase& operator=(
        const SqliteDatabase&
    ) = delete;

    SqliteDatabase(
        SqliteDatabase&&
    ) = delete;

    SqliteDatabase& operator=(
        SqliteDatabase&&
    ) = delete;

    /*
     * Execute SQL that does not return data.
     *
     * Intended for schema creation, transactions, pragmas, and similar
     */
    void execute(
        std::string_view sql
    );

    /*
     * Execute a query expected to return one integer in its first row and first column.
     */
    [[nodiscard]]
    std::int64_t query_int64(
        std::string_view sql
    ) const;

    /*
     * Nesting-aware transaction control.
     *
     * The outermost call opens a real SQLite transaction (BEGIN
     * IMMEDIATE); any call made while one is already open instead
     * opens a SAVEPOINT. This lets a caller (e.g. the coordinator)
     * wrap several repository writes - each of which opens its own
     * transaction when used standalone - in one outer transaction,
     * without ever issuing a nested BEGIN (which SQLite rejects).
     *
     * commit_transaction / rollback_transaction close exactly the
     * scope opened by the matching begin_transaction call: the
     * outermost pair maps to COMMIT / ROLLBACK, an inner pair to
     * RELEASE / ROLLBACK TO <savepoint> followed by RELEASE.
     */
    void begin_transaction();

    void commit_transaction();

    void rollback_transaction();

    //low level connection access for persistence classes
    [[nodiscard]]
    sqlite3* native_handle() const noexcept;

private:
    sqlite3* database_{nullptr};

    int transaction_depth_{0};
};

}  // namespace radahn::persistence