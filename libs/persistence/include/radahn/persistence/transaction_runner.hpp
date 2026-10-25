#pragma once

#include <functional>

#include "radahn/persistence/sqlite_database.hpp"

namespace radahn::persistence {

/*
 * Narrow seam letting a caller (e.g. the coordinator) run a body
 * that writes through more than one repository as a single atomic
 * unit, without the caller needing to know whether the underlying
 * storage is SQLite or in-memory.
 */
class ITransactionRunner {
public:
    virtual ~ITransactionRunner() = default;

    /*
     * Runs `body` inside one transaction. If `body` throws, every
     * write it performed is rolled back and the exception
     * propagates; otherwise the transaction commits.
     */
    virtual void run(
        const std::function<void()>& body
    ) = 0;
};

/*
 * In-memory repositories perform each write immediately and have no
 * separate commit step, so there is nothing for this runner to
 * coordinate. It exists so callers that are backend-agnostic (e.g.
 * InMemoryCoordinator, used both with in-memory repositories in
 * tests and with SQLite repositories in production) can always go
 * through an ITransactionRunner rather than branching on backend.
 */
class NullTransactionRunner final : public ITransactionRunner {
public:
    void run(
        const std::function<void()>& body
    ) override;
};

/*
 * Runs `body` inside a real SQLite transaction on the shared
 * database connection. Composes correctly with repository methods
 * that themselves open their own (nested) transaction against the
 * same SqliteDatabase, since SqliteDatabase::begin_transaction /
 * commit_transaction / rollback_transaction use SAVEPOINTs once a
 * transaction is already open.
 */
class SqliteTransactionRunner final : public ITransactionRunner {
public:
    explicit SqliteTransactionRunner(
        SqliteDatabase& database
    );

    void run(
        const std::function<void()>& body
    ) override;

private:
    SqliteDatabase& database_;
};

}  // namespace radahn::persistence
