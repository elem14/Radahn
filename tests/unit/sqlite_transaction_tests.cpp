#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "radahn/coordinator/in_memory_coordinator.hpp"

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"
#include "radahn/domain/workload.hpp"

#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/job_repository.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"
#include "radahn/persistence/sqlite_worker_repository.hpp"
#include "radahn/persistence/transaction_runner.hpp"

#include "radahn/scheduler/least_loaded_policy.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t mebibyte = 1024ULL * 1024ULL;
constexpr std::uint64_t gibibyte = 1024ULL * 1024ULL * 1024ULL;

void expect(bool condition, std::string_view description) {
    if (condition) {
        std::cout << "[PASS] " << description << '\n';
        return;
    }

    std::cerr << "[FAIL] " << description << '\n';
    ++failure_count;
}

radahn::domain::Job make_job(std::string id) {
    radahn::domain::ResourceRequirements requirements{
        1.0,
        512ULL * mebibyte,
        1024ULL * mebibyte,
        false,
        {}
    };

    return radahn::domain::Job{
        radahn::domain::JobId{std::move(id)},
        "Transaction test job",
        50,
        std::move(requirements),
        radahn::domain::WorkloadSpec::sleep(
            std::chrono::seconds{30}
        )
    };
}

radahn::domain::WorkerRecord make_worker(std::string id) {
    radahn::domain::WorkerResources resources{
        8.0, 8.0,
        16ULL * gibibyte, 16ULL * gibibyte,
        100ULL * gibibyte, 100ULL * gibibyte,
        false
    };

    radahn::domain::WorkerSnapshot snapshot{
        radahn::domain::WorkerId{std::move(id)},
        radahn::domain::WorkerState::online,
        std::move(resources),
        0,
        4,
        {}
    };

    return radahn::domain::WorkerRecord{std::move(snapshot)};
}

/*
 * Verifies that SqliteDatabase::begin_transaction / commit_transaction
 * / rollback_transaction compose correctly across nesting: an inner
 * scope opened while an outer one is already active uses a SAVEPOINT
 * rather than a nested BEGIN (which SQLite rejects outright), and
 * rolling back the inner scope undoes only the inner scope's writes.
 */
void test_nested_transactions_compose() {
    using radahn::persistence::SqliteDatabase;

    SqliteDatabase database{":memory:"};

    database.execute(
        "CREATE TABLE t (id INTEGER PRIMARY KEY, value TEXT);"
    );

    database.begin_transaction();
    database.execute("INSERT INTO t (id, value) VALUES (1, 'outer');");

    bool inner_threw = false;

    try {
        database.begin_transaction();
        database.execute(
            "INSERT INTO t (id, value) VALUES (2, 'inner');"
        );

        throw std::runtime_error{"simulated inner failure"};
    } catch (const std::runtime_error&) {
        inner_threw = true;
        database.rollback_transaction();
    }

    expect(
        inner_threw,
        "The simulated inner failure propagated as expected"
    );

    database.commit_transaction();

    expect(
        database.query_int64("SELECT COUNT(*) FROM t;") == 1,
        "Rolling back only the inner (nested) scope keeps the "
        "outer scope's write while discarding the inner one"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) FROM t WHERE id = 1;"
        ) == 1,
        "The outer write committed"
    );

    expect(
        database.query_int64(
            "SELECT COUNT(*) FROM t WHERE id = 2;"
        ) == 0,
        "The rolled-back inner write never committed"
    );
}

/*
 * A repository's own insert/update already opens its own transaction
 * when used standalone. Proves that calling it from inside an outer
 * SqliteTransactionRunner::run(...) does not hit SQLite's "cannot
 * start a transaction within a transaction" error, and that a
 * failure after a worker write but before the matching job write
 * rolls back the worker write too - atomically, with no compensating
 * application code involved at all.
 */
void test_transaction_runner_rolls_back_worker_write() {
    using radahn::domain::WorkerId;
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteTransactionRunner;
    using radahn::persistence::SqliteWorkerRepository;

    SqliteDatabase database{":memory:"};
    SqliteWorkerRepository workers{database};

    const WorkerId worker_id{"txn-worker"};

    workers.insert(make_worker(worker_id.value()));

    SqliteTransactionRunner runner{database};

    bool threw = false;

    try {
        runner.run([&] {
            auto worker = *workers.get(worker_id);

            worker.reserve(
                radahn::domain::ResourceRequirements{
                    1.0,
                    512ULL * mebibyte,
                    1024ULL * mebibyte,
                    false,
                    {}
                }
            );

            // This nested transaction must not fail with a SQLite
            // "cannot start a transaction within a transaction"
            // error now that it goes through begin_transaction's
            // nesting-aware SAVEPOINT logic.
            workers.update(worker);

            throw std::runtime_error{
                "simulated failure before the matching job write"
            };
        });
    } catch (const std::runtime_error&) {
        threw = true;
    }

    expect(
        threw,
        "The simulated failure propagated out of the transaction"
    );

    const auto reloaded = *workers.get(worker_id);

    expect(
        reloaded.snapshot().resources().available_cpu_cores() == 8.0,
        "The worker's reservation was rolled back atomically, with "
        "no compensating application code involved"
    );
}

/*
 * End-to-end: a coordinator wired with real SQLite repositories and
 * a SqliteTransactionRunner must not leave a durable, cross-table
 * inconsistency behind when the job write of a dispatch fails after
 * the worker write already happened. Re-reading both tables through
 * brand-new repository instances (simulating a process restart)
 * must show the original, pre-dispatch state, and the job must still
 * be schedulable.
 */
class FailOnUpdateJobRepository final
    : public radahn::persistence::IJobRepository {
public:
    /*
     * `fail_at_call` is 1-based: the update() call with that index
     * throws instead of forwarding. This lets a test let an
     * unrelated earlier update (e.g. the one InMemoryCoordinator's
     * constructor performs while recovering persisted state) succeed
     * normally, and fail only the specific write under test.
     */
    FailOnUpdateJobRepository(
        radahn::persistence::IJobRepository& inner,
        std::size_t fail_at_call
    )
        : inner_{inner},
          fail_at_call_{fail_at_call} {
    }

    void insert(radahn::persistence::JobRecord record) override {
        inner_.insert(std::move(record));
    }

    void update(radahn::persistence::JobRecord record) override {
        ++call_count_;

        if (call_count_ == fail_at_call_) {
            throw std::runtime_error{"simulated job write failure"};
        }

        inner_.update(std::move(record));
    }

    [[nodiscard]]
    std::optional<radahn::persistence::JobRecord> get(
        const radahn::domain::JobId& job_id
    ) const override {
        return inner_.get(job_id);
    }

    [[nodiscard]]
    std::vector<radahn::persistence::JobRecord>
    list() const override {
        return inner_.list();
    }

    [[nodiscard]]
    bool contains(
        const radahn::domain::JobId& job_id
    ) const override {
        return inner_.contains(job_id);
    }

    void erase(const radahn::domain::JobId& job_id) override {
        inner_.erase(job_id);
    }

    [[nodiscard]]
    std::size_t size() const override {
        return inner_.size();
    }

private:
    radahn::persistence::IJobRepository& inner_;
    std::size_t fail_at_call_;
    std::size_t call_count_ = 0;
};

void test_coordinator_dispatch_is_atomic_across_repositories() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobId;
    using radahn::domain::WorkerId;
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteJobRepository;
    using radahn::persistence::SqliteTransactionRunner;
    using radahn::persistence::SqliteWorkerRepository;
    using radahn::scheduler::LeastLoadedPolicy;

    SqliteDatabase database{":memory:"};

    SqliteJobRepository real_jobs{database};
    SqliteWorkerRepository workers{database};

    // Call #1 is InMemoryCoordinator's own constructor recovering
    // persisted state (it rewrites the one queued job unchanged);
    // call #2 is the job write inside dispatch_once_for_worker,
    // which is the write under test.
    FailOnUpdateJobRepository failing_jobs{real_jobs, 2};

    LeastLoadedPolicy policy;
    SqliteTransactionRunner runner{database};

    const JobId job_id{"atomic-dispatch-job"};
    const WorkerId worker_id{"atomic-dispatch-worker"};

    {
        InMemoryCoordinator setup_coordinator{
            policy,
            real_jobs,
            workers,
            std::chrono::seconds{15},
            &runner
        };

        setup_coordinator.register_worker(
            make_worker(worker_id.value())
        );

        setup_coordinator.submit_job(
            make_job(job_id.value())
        );
    }

    InMemoryCoordinator coordinator{
        policy,
        failing_jobs,
        workers,
        std::chrono::seconds{15},
        &runner
    };

    bool threw = false;

    try {
        static_cast<void>(
            coordinator.dispatch_once_for_worker(worker_id)
        );
    } catch (const std::exception&) {
        threw = true;
    }

    expect(
        threw,
        "The simulated job-write failure propagated out of dispatch"
    );

    // Re-read through brand-new repository instances against the
    // same database, simulating a process restart after the
    // interrupted transition.
    SqliteJobRepository reloaded_jobs{database};
    SqliteWorkerRepository reloaded_workers{database};

    const auto worker_record = reloaded_workers.get(worker_id);

    expect(
        worker_record.has_value() &&
        worker_record->snapshot().resources()
            .available_cpu_cores() == 8.0,
        "The worker's reservation was rolled back; it was never "
        "durably committed on its own"
    );

    const auto job_record = reloaded_jobs.get(job_id);

    expect(
        job_record.has_value() &&
        job_record->job.state() ==
            radahn::domain::JobState::queued &&
        job_record->job.attempt_count() == 0,
        "The job's persisted state still shows it queued and "
        "un-attempted - no job disappeared from persisted state"
    );

    expect(
        !job_record->assigned_worker_id.has_value(),
        "The job was never durably assigned to the worker"
    );
}

}  // namespace

int main() {
    try {
        test_nested_transactions_compose();
        test_transaction_runner_rolls_back_worker_write();
        test_coordinator_dispatch_is_atomic_across_repositories();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected transaction-consistency exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " transaction-consistency assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout << "\nAll SQLite transaction-consistency tests passed\n";
    return EXIT_SUCCESS;
}
