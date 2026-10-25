#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/workload.hpp"

#include "radahn/persistence/in_memory_job_log_repository.hpp"
#include "radahn/persistence/job_log_repository.hpp"
#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_log_repository.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"

namespace {

using radahn::persistence::IJobLogRepository;
using radahn::persistence::JobLogRecord;
using radahn::persistence::JobLogStream;

int failure_count = 0;

void expect(bool condition, std::string_view description) {
    if (condition) {
        std::cout << "[PASS] " << description << '\n';
        return;
    }

    std::cerr << "[FAIL] " << description << '\n';
    ++failure_count;
}

[[nodiscard]]
radahn::domain::Job make_job(std::string id) {
    using radahn::domain::Job;
    using radahn::domain::JobId;
    using radahn::domain::ResourceRequirements;
    using radahn::domain::WorkloadSpec;

    ResourceRequirements requirements{
        1.0,
        128ULL * 1024ULL * 1024ULL,
        128ULL * 1024ULL * 1024ULL,
        false,
        {}
    };

    return Job{
        JobId{std::move(id)},
        "job log repository test job",
        10,
        std::move(requirements),
        WorkloadSpec::sleep(std::chrono::milliseconds{1})
    };
}

/*
 * Exercises the same scenarios against any IJobLogRepository
 * implementation, so the in-memory and SQLite backends are held to
 * the same contract.
 */
void run_shared_scenarios(
    IJobLogRepository& repository,
    const std::string& job_id
) {
    const radahn::domain::JobId job{job_id};
    const radahn::domain::WorkerId worker{"worker-1"};

    // Binary content, including an embedded NUL byte, must round-trip
    // exactly.
    std::string binary_content{"before"};
    binary_content.push_back('\0');
    binary_content += "after";

    expect(
        !repository.contains(
            job, 1, JobLogStream::standard_output
        ),
        "No stdout log exists before anything is uploaded"
    );

    repository.insert(
        JobLogRecord{
            job,
            1,
            JobLogStream::standard_output,
            worker,
            binary_content,
            binary_content.size(),
            false
        }
    );

    const auto stored =
        repository.get(job, 1, JobLogStream::standard_output);

    expect(
        stored.has_value() &&
        stored->content == binary_content &&
        stored->content.size() == binary_content.size(),
        "Binary content with an embedded NUL byte round-trips exactly"
    );

    expect(
        stored.has_value() &&
        stored->worker_id == worker &&
        !stored->truncated &&
        stored->original_byte_count == binary_content.size(),
        "Stored metadata (worker, truncated, original size) is preserved"
    );

    // An empty log is distinguishable from a missing one.
    repository.insert(
        JobLogRecord{
            job,
            1,
            JobLogStream::standard_error,
            worker,
            "",
            0,
            false
        }
    );

    const auto empty_log =
        repository.get(job, 1, JobLogStream::standard_error);

    expect(
        empty_log.has_value() && empty_log->content.empty(),
        "An empty, uploaded log is present (not nullopt) with empty content"
    );

    expect(
        !repository.get(job, 2, JobLogStream::standard_error)
            .has_value(),
        "A log that was never uploaded for a given attempt is "
        "indistinguishable from absent, not an empty log"
    );

    // Two attempts of the same job never collide.
    repository.insert(
        JobLogRecord{
            job,
            2,
            JobLogStream::standard_output,
            worker,
            "second attempt output",
            22,
            false
        }
    );

    const auto first_attempt_log =
        repository.get(job, 1, JobLogStream::standard_output);

    const auto second_attempt_log =
        repository.get(job, 2, JobLogStream::standard_output);

    expect(
        first_attempt_log.has_value() &&
        second_attempt_log.has_value() &&
        first_attempt_log->content != second_attempt_log->content,
        "Two attempts of the same job keep independent log content"
    );

    // Duplicate insert at an existing key is rejected by the
    // repository itself (the caller is responsible for the
    // accept-identical-repeat / reject-conflicting policy).
    bool threw = false;

    try {
        repository.insert(
            JobLogRecord{
                job,
                1,
                JobLogStream::standard_output,
                worker,
                "different content",
                18,
                false
            }
        );
    } catch (const std::exception&) {
        threw = true;
    }

    expect(
        threw,
        "Inserting a second log at an existing "
        "(job_id, attempt_number, stream) key is rejected"
    );

    // A truncated upload is stored with its flag and original size.
    repository.insert(
        JobLogRecord{
            job,
            3,
            JobLogStream::standard_output,
            worker,
            std::string(10, 'x'),
            1'000'000,
            true
        }
    );

    const auto truncated_log =
        repository.get(job, 3, JobLogStream::standard_output);

    expect(
        truncated_log.has_value() &&
        truncated_log->truncated &&
        truncated_log->original_byte_count == 1'000'000 &&
        truncated_log->content.size() == 10,
        "Oversized output is stored truncated, with its true "
        "original byte count preserved"
    );

    repository.erase(job, 3, JobLogStream::standard_output);

    expect(
        !repository.contains(job, 3, JobLogStream::standard_output),
        "Erasing a stored log removes it"
    );
}

void test_in_memory_repository() {
    radahn::persistence::InMemoryJobLogRepository repository;

    run_shared_scenarios(repository, "job-in-memory");

    expect(
        repository.size() == 3,
        "In-memory repository retains exactly the non-erased records"
    );
}

void test_sqlite_repository() {
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteJobLogRepository;
    using radahn::persistence::SqliteJobRepository;

    SqliteDatabase database{":memory:"};

    SqliteJobRepository job_repository{database};

    job_repository.insert(
        radahn::persistence::JobRecord{
            make_job("job-sqlite"),
            std::nullopt,
            std::nullopt
        }
    );

    SqliteJobLogRepository repository{database};

    run_shared_scenarios(repository, "job-sqlite");

    expect(
        repository.size() == 3,
        "SQLite repository retains exactly the non-erased records"
    );
}

/*
 * 7. "Logs survive coordinator restart" at the repository level:
 * a log inserted through one SqliteJobLogRepository is visible
 * through a brand-new repository object opened against the same
 * durable database file afterward.
 */
void test_sqlite_repository_survives_reopen() {
    using radahn::persistence::SqliteDatabase;
    using radahn::persistence::SqliteJobLogRepository;
    using radahn::persistence::SqliteJobRepository;

    const auto unique_value =
        std::chrono::steady_clock::now()
            .time_since_epoch()
            .count();

    const auto database_path =
        std::filesystem::temp_directory_path() /
        (
            "radahn-job-log-repository-test-" +
            std::to_string(unique_value) +
            ".db"
        );

    std::filesystem::remove(database_path);

    const radahn::domain::JobId job{"job-restart"};
    const radahn::domain::WorkerId worker{"worker-1"};

    {
        SqliteDatabase database{database_path};

        SqliteJobRepository job_repository{database};

        job_repository.insert(
            radahn::persistence::JobRecord{
                make_job("job-restart"),
                std::nullopt,
                std::nullopt
            }
        );

        SqliteJobLogRepository repository{database};

        repository.insert(
            JobLogRecord{
                job,
                1,
                JobLogStream::standard_output,
                worker,
                "survives restart",
                17,
                false
            }
        );
    }

    {
        SqliteDatabase database{database_path};
        SqliteJobLogRepository repository{database};

        const auto stored =
            repository.get(job, 1, JobLogStream::standard_output);

        expect(
            stored.has_value() &&
            stored->content == "survives restart",
            "A log stored before a simulated coordinator restart is "
            "still readable through a freshly opened repository"
        );
    }

    std::error_code error;
    std::filesystem::remove(database_path, error);
}

}  // namespace

int main() {
    try {
        test_in_memory_repository();
        test_sqlite_repository();
        test_sqlite_repository_survives_reopen();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " job log repository assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout << "\nAll job log repository tests passed\n";

    return EXIT_SUCCESS;
}
