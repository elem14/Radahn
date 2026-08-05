#include <cstdlib>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string_view>

#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_schema.hpp"

namespace {

int failure_count = 0;

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

void test_database_configuration() {
    using radahn::persistence::
        SqliteDatabase;

    SqliteDatabase database{
        ":memory:"
    };

    expect(
        database.query_int64(
            "PRAGMA foreign_keys;"
        ) == 1,
        "SQLite foreign keys are enabled"
    );
}

void test_schema_creation() {
    using radahn::persistence::
        SqliteDatabase;

    using radahn::persistence::
        initialize_sqlite_schema;

    SqliteDatabase database{
        ":memory:"
    };

    initialize_sqlite_schema(
        database
    );

    expect(
        database.query_int64(
            "PRAGMA user_version;"
        ) == 3,
        "SQLite schema version is three"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM sqlite_master "
            "WHERE type = 'table' "
            "AND name = 'workers';"
        ) == 1,
        "Workers table exists"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM pragma_table_info('workers') "
            "WHERE name = 'last_heartbeat_unix_ms';"
        ) == 1,
        "Workers table contains heartbeat timestamp"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM sqlite_master "
            "WHERE type = 'table' "
            "AND name = 'worker_tags';"
        ) == 1,
        "Worker tags table exists"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM sqlite_master "
            "WHERE type = 'table' "
            "AND name = 'jobs';"
        ) == 1,
        "Jobs table exists"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM pragma_table_info('jobs') "
            "WHERE name = "
            "'lease_expires_at_unix_ms';"
        ) == 1,
        "Jobs table contains lease-expiration timestamp"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM sqlite_master "
            "WHERE type = 'table' "
            "AND name = 'job_required_tags';"
        ) == 1,
        "Job required tags table exists"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM sqlite_master "
            "WHERE type = 'table' "
            "AND name = 'schema_migrations';"
        ) == 1,
        "Schema migrations table exists"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM schema_migrations "
            "WHERE version = 1;"
        ) == 1,
        "Schema migration version is recorded"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM schema_migrations "
            "WHERE version = 2;"
        ) == 1,
        "Heartbeat schema migration is recorded"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM schema_migrations "
            "WHERE version = 3;"
        ) == 1,
        "Job lease schema migration is recorded"
    );

    /*
     * initializer must be safe to call again
     */
    initialize_sqlite_schema(
        database
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) "
            "FROM schema_migrations "
            "WHERE version = 3;"
        ) == 1,
        "Schema initialization is idempotent"
    );
}

void test_invalid_sql_is_rejected() {
    using radahn::persistence::
        SqliteDatabase;

    SqliteDatabase database{
        ":memory:"
    };

    bool error_reported = false;

    try {
        database.execute(
            "THIS IS NOT VALID SQL;"
        );
    } catch (
        const std::runtime_error&
    ) {
        error_reported = true;
    }

    expect(
        error_reported,
        "Invalid SQL produces an exception"
    );
}

}  // namespace

int main() {
    try {
        test_database_configuration();
        test_schema_creation();
        test_invalid_sql_is_rejected();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected SQLite test exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " SQLite assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll SQLite database tests passed\n";

    return EXIT_SUCCESS;
}