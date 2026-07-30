#pragma once

namespace radahn::persistence {

class SqliteDatabase;

/*
 * Creates or verifies current schema
 * safe to call repeatedly
 */
void initialize_sqlite_schema(
    SqliteDatabase& database
);

}  // namespace radahn::persistence