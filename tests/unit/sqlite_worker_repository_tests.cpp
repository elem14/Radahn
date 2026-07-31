#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "radahn/domain/id.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_worker_repository.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t mebibyte =
    1024ULL * 1024ULL;

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

class TemporaryDatabaseFile {
public:
    TemporaryDatabaseFile() {
        const auto unique_value =
            std::chrono::steady_clock::now()
                .time_since_epoch()
                .count();

        path_ =
            std::filesystem::temp_directory_path() /
            (
                "radahn-sqlite-worker-test-" +
                std::to_string(unique_value) +
                ".db"
            );

        std::filesystem::remove(
            path_
        );
    }

    ~TemporaryDatabaseFile() {
        std::error_code error;

        std::filesystem::remove(
            path_,
            error
        );

        std::filesystem::remove(
            path_.string() + "-wal",
            error
        );

        std::filesystem::remove(
            path_.string() + "-shm",
            error
        );
    }

    [[nodiscard]]
    const std::filesystem::path&
    path() const noexcept {
        return path_;
    }

private:
    std::filesystem::path path_;
};

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
        true
    };

    radahn::domain::WorkerSnapshot snapshot{
        radahn::domain::WorkerId{
            std::move(id)
        },
        radahn::domain::WorkerState::online,
        std::move(resources),
        0,
        4,
        {"arm64", "macos"}
    };

    return radahn::domain::WorkerRecord{
        std::move(snapshot)
    };
}

void test_insert_and_get() {
    using radahn::domain::WorkerId;

    using radahn::persistence::
        SqliteDatabase;

    using radahn::persistence::
        SqliteWorkerRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteWorkerRepository repository{
        database
    };

    repository.insert(
        make_worker("worker-1")
    );

    expect(
        repository.size() == 1,
        "SQLite repository stores inserted worker"
    );

    expect(
        repository.contains(
            WorkerId{"worker-1"}
        ),
        "SQLite repository reports existing worker"
    );

    const auto worker =
        repository.get(
            WorkerId{"worker-1"}
        );

    expect(
        worker.has_value(),
        "SQLite repository retrieves worker"
    );

    if (!worker.has_value()) {
        return;
    }

    const auto snapshot =
        worker->snapshot();

    expect(
        worker->id().value() ==
            "worker-1",
        "SQLite repository preserves worker ID"
    );

    expect(
        snapshot.state() ==
            radahn::domain::WorkerState::online,
        "SQLite repository preserves worker state"
    );

    expect(
        snapshot.resources()
            .total_cpu_cores() == 8.0,
        "SQLite repository preserves total CPU"
    );

    expect(
        snapshot.resources()
            .available_cpu_cores() == 8.0,
        "SQLite repository preserves available CPU"
    );

    expect(
        snapshot.resources()
            .total_memory_bytes() ==
            16ULL * gibibyte,
        "SQLite repository preserves total memory"
    );

    expect(
        snapshot.resources()
            .available_disk_bytes() ==
            100ULL * gibibyte,
        "SQLite repository preserves available disk"
    );

    expect(
        snapshot.resources()
            .gpu_available(),
        "SQLite repository preserves GPU availability"
    );

    expect(
        snapshot.running_jobs() == 0,
        "SQLite repository preserves running-job count"
    );

    expect(
        snapshot.max_concurrent_jobs() == 4,
        "SQLite repository preserves concurrency limit"
    );

    expect(
        snapshot.tags().size() == 2,
        "SQLite repository preserves worker tags"
    );
}

void test_update_after_reservation() {
    using radahn::domain::
        ResourceRequirements;

    using radahn::domain::WorkerId;

    using radahn::persistence::
        SqliteDatabase;

    using radahn::persistence::
        SqliteWorkerRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteWorkerRepository repository{
        database
    };

    repository.insert(
        make_worker("worker-update")
    );

    auto worker =
        repository.get(
            WorkerId{"worker-update"}
        );

    expect(
        worker.has_value(),
        "Worker exists before SQLite update"
    );

    if (!worker.has_value()) {
        return;
    }

    ResourceRequirements requirements{
        2.0,
        512ULL * mebibyte,
        1024ULL * mebibyte,
        false,
        {"macos"}
    };

    worker->reserve(
        requirements
    );

    repository.update(
        std::move(*worker)
    );

    const auto updated =
        repository.get(
            WorkerId{"worker-update"}
        );

    expect(
        updated.has_value(),
        "SQLite repository retrieves updated worker"
    );

    if (!updated.has_value()) {
        return;
    }

    const auto snapshot =
        updated->snapshot();

    expect(
        snapshot.resources()
            .available_cpu_cores() == 6.0,
        "SQLite repository saves reserved CPU"
    );

    expect(
        snapshot.resources()
            .available_memory_bytes() ==
            (16ULL * gibibyte) -
                (512ULL * mebibyte),
        "SQLite repository saves reserved memory"
    );

    expect(
        snapshot.running_jobs() == 1,
        "SQLite repository saves running-job count"
    );
}

void test_duplicate_and_unknown_operations() {
    using radahn::domain::WorkerId;

    using radahn::persistence::
        SqliteDatabase;

    using radahn::persistence::
        SqliteWorkerRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteWorkerRepository repository{
        database
    };

    repository.insert(
        make_worker("duplicate-worker")
    );

    bool duplicate_rejected = false;

    try {
        repository.insert(
            make_worker("duplicate-worker")
        );
    } catch (
        const std::invalid_argument&
    ) {
        duplicate_rejected = true;
    }

    expect(
        duplicate_rejected,
        "SQLite repository rejects duplicate worker IDs"
    );

    bool unknown_update_rejected = false;

    try {
        repository.update(
            make_worker("unknown-worker")
        );
    } catch (
        const std::invalid_argument&
    ) {
        unknown_update_rejected = true;
    }

    expect(
        unknown_update_rejected,
        "SQLite repository rejects unknown worker update"
    );

    bool unknown_erase_rejected = false;

    try {
        repository.erase(
            WorkerId{"unknown-worker"}
        );
    } catch (
        const std::invalid_argument&
    ) {
        unknown_erase_rejected = true;
    }

    expect(
        unknown_erase_rejected,
        "SQLite repository rejects unknown worker erase"
    );
}

void test_list_and_erase() {
    using radahn::domain::WorkerId;

    using radahn::persistence::
        SqliteDatabase;

    using radahn::persistence::
        SqliteWorkerRepository;

    SqliteDatabase database{
        ":memory:"
    };

    SqliteWorkerRepository repository{
        database
    };

    repository.insert(
        make_worker("worker-a")
    );

    repository.insert(
        make_worker("worker-b")
    );

    expect(
        repository.list().size() == 2,
        "SQLite repository lists every worker"
    );

    repository.erase(
        WorkerId{"worker-a"}
    );

    expect(
        repository.size() == 1,
        "SQLite repository erases worker"
    );

    expect(
        !repository.contains(
            WorkerId{"worker-a"}
        ),
        "Erased SQLite worker no longer exists"
    );
}

void test_data_survives_reopen() {
    using radahn::domain::WorkerId;

    using radahn::persistence::
        SqliteDatabase;

    using radahn::persistence::
        SqliteWorkerRepository;

    TemporaryDatabaseFile database_file;

    {
        SqliteDatabase database{
            database_file.path()
        };

        SqliteWorkerRepository repository{
            database
        };

        repository.insert(
            make_worker("persistent-worker")
        );
    }

    {
        SqliteDatabase database{
            database_file.path()
        };

        SqliteWorkerRepository repository{
            database
        };

        const auto restored =
            repository.get(
                WorkerId{"persistent-worker"}
            );

        expect(
            restored.has_value(),
            "SQLite worker survives database reopen"
        );

        if (!restored.has_value()) {
            return;
        }

        const auto snapshot =
            restored->snapshot();

        expect(
            snapshot.resources()
                .total_memory_bytes() ==
                16ULL * gibibyte,
            "Reopened worker preserves resources"
        );

        expect(
            snapshot.tags().size() == 2,
            "Reopened worker preserves tags"
        );

        expect(
            snapshot.max_concurrent_jobs() == 4,
            "Reopened worker preserves concurrency limit"
        );
    }
}

}  // namespace

int main() {
    try {
        test_insert_and_get();
        test_update_after_reservation();
        test_duplicate_and_unknown_operations();
        test_list_and_erase();
        test_data_survives_reopen();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected SQLite worker repository exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " SQLite worker assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll SQLite worker repository tests passed\n";

    return EXIT_SUCCESS;
}