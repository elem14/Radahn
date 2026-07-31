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

    //low level connection access for persistence classes
    [[nodiscard]]
    sqlite3* native_handle() const noexcept;

private:
    sqlite3* database_{nullptr};
};

}  // namespace radahn::persistence