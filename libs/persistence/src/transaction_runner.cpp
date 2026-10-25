#include "radahn/persistence/transaction_runner.hpp"

namespace radahn::persistence {

void NullTransactionRunner::run(
    const std::function<void()>& body
) {
    body();
}

SqliteTransactionRunner::SqliteTransactionRunner(
    SqliteDatabase& database
)
    : database_{database} {
}

void SqliteTransactionRunner::run(
    const std::function<void()>& body
) {
    database_.begin_transaction();

    try {
        body();
    } catch (...) {
        database_.rollback_transaction();
        throw;
    }

    database_.commit_transaction();
}

}  // namespace radahn::persistence
