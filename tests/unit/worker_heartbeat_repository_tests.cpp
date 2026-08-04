#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "radahn/domain/id.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/persistence/in_memory_worker_repository.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_worker_repository.hpp"
#include "radahn/persistence/worker_repository.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t gibibyte =
    1024ULL * 1024ULL * 1024ULL;

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

radahn::domain::WorkerRecord make_worker(
    std::string id
) {
    radahn::domain::WorkerResources resources{
        8.0,
        8.0,
        16ULL * gibibyte,
        16ULL * gibibyte,
        100ULL * gibibyte,
        100ULL * gibibyte,
        false
    };

    radahn::domain::WorkerSnapshot snapshot{
        radahn::domain::WorkerId{
            std::move(id)
        },
        radahn::domain::WorkerState::online,
        std::move(resources),
        0,
        4,
        {"macos", "arm64"}
    };

    return radahn::domain::WorkerRecord{
        std::move(snapshot)
    };
}

void run_heartbeat_contract(
    radahn::persistence::IWorkerRepository&
        repository,
    std::string_view repository_name
) {
    using radahn::domain::WorkerId;

    using radahn::persistence::
        WorkerHeartbeatClock;

    using radahn::persistence::
        WorkerHeartbeatTimePoint;

    const WorkerId worker_id{
        "heartbeat-worker"
    };

    repository.insert(
        make_worker(
            worker_id.value()
        )
    );

    const auto initial_heartbeat =
        repository.last_heartbeat(
            worker_id
        );

    expect(
        initial_heartbeat.has_value(),
        std::string{repository_name} +
            " records an initial heartbeat"
    );

    const WorkerHeartbeatTimePoint
        fixed_heartbeat{
            std::chrono::duration_cast<
                WorkerHeartbeatClock::duration
            >(
                std::chrono::milliseconds{
                    1'700'000'000'123LL
                }
            )
        };

    repository.record_heartbeat(
        worker_id,
        fixed_heartbeat
    );

    const auto stored_heartbeat =
        repository.last_heartbeat(
            worker_id
        );

    expect(
        stored_heartbeat.has_value() &&
        *stored_heartbeat ==
            fixed_heartbeat,
        std::string{repository_name} +
            " stores the supplied heartbeat time"
    );

    /*
     * A normal worker update must preserve the heartbeat.
     */
    repository.update(
        make_worker(
            worker_id.value()
        )
    );

    const auto heartbeat_after_update =
        repository.last_heartbeat(
            worker_id
        );

    expect(
        heartbeat_after_update.has_value() &&
        *heartbeat_after_update ==
            fixed_heartbeat,
        std::string{repository_name} +
            " preserves heartbeat during worker update"
    );

    expect(
        !repository.last_heartbeat(
            WorkerId{"missing-worker"}
        ).has_value(),
        std::string{repository_name} +
            " returns no heartbeat for unknown worker"
    );

    bool unknown_heartbeat_rejected = false;

    try {
        repository.record_heartbeat(
            WorkerId{"missing-worker"},
            fixed_heartbeat
        );
    } catch (
        const std::invalid_argument&
    ) {
        unknown_heartbeat_rejected = true;
    }

    expect(
        unknown_heartbeat_rejected,
        std::string{repository_name} +
            " rejects heartbeat for unknown worker"
    );
}

void test_in_memory_repository() {
    radahn::persistence::
        InMemoryWorkerRepository repository;

    run_heartbeat_contract(
        repository,
        "In-memory repository"
    );
}

void test_sqlite_repository() {
    radahn::persistence::SqliteDatabase
        database{
            ":memory:"
        };

    radahn::persistence::
        SqliteWorkerRepository repository{
            database
        };

    run_heartbeat_contract(
        repository,
        "SQLite repository"
    );
}

}  // namespace

int main() {
    try {
        test_in_memory_repository();
        test_sqlite_repository();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected heartbeat repository exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " heartbeat assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll worker heartbeat repository tests passed\n";

    return EXIT_SUCCESS;
}