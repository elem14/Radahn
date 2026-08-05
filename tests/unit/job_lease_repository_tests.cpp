#include <chrono>
#include <cstdlib>
#include <cstdint>
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

#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/job_repository.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t mebibyte =
    1024ULL * 1024ULL;

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

radahn::domain::Job make_job(
    std::string id
) {
    radahn::domain::ResourceRequirements
        requirements{
            1.0,
            512ULL * mebibyte,
            1024ULL * mebibyte,
            false,
            {"macos", "arm64"}
        };

    return radahn::domain::Job{
        radahn::domain::JobId{
            std::move(id)
        },
        "Lease repository test job",
        50,
        std::move(requirements),
        radahn::domain::WorkloadSpec::sleep(
            std::chrono::milliseconds{
                100
            }
        )
    };
}

void run_lease_contract(
    radahn::persistence::IJobRepository&
        repository,
    std::string_view repository_name
) {
    using radahn::domain::JobId;

    using radahn::persistence::
        JobLeaseClock;

    using radahn::persistence::
        JobLeaseTimePoint;

    using radahn::persistence::
        JobRecord;

    const JobId job_id{
        "lease-contract-job"
    };

    repository.insert(
        JobRecord{
            make_job(
                job_id.value()
            ),
            std::nullopt,
            std::nullopt
        }
    );

    const auto initially_stored =
        repository.get(
            job_id
        );

    expect(
        initially_stored.has_value() &&
        !initially_stored
             ->lease_expires_at
             .has_value(),
        std::string{repository_name} +
            " stores a job without a lease"
    );

    const JobLeaseTimePoint
        first_expiration{
            std::chrono::duration_cast<
                JobLeaseClock::duration
            >(
                std::chrono::milliseconds{
                    1'700'000'030'000LL
                }
            )
        };

    auto leased_record =
        repository.get(
            job_id
        );

    if (!leased_record.has_value()) {
        expect(
            false,
            std::string{repository_name} +
                " retrieves job before lease update"
        );

        return;
    }

    leased_record->lease_expires_at =
        first_expiration;

    repository.update(
        std::move(*leased_record)
    );

    const auto stored_lease =
        repository.get(
            job_id
        );

    expect(
        stored_lease.has_value() &&
        stored_lease->lease_expires_at
            .has_value() &&
        *stored_lease->lease_expires_at ==
            first_expiration,
        std::string{repository_name} +
            " persists the lease expiration"
    );

    const JobLeaseTimePoint
        renewed_expiration{
            std::chrono::duration_cast<
                JobLeaseClock::duration
            >(
                std::chrono::milliseconds{
                    1'700'000'060'000LL
                }
            )
        };

    auto renewed_record =
        repository.get(
            job_id
        );

    if (!renewed_record.has_value()) {
        expect(
            false,
            std::string{repository_name} +
                " retrieves job before lease renewal"
        );

        return;
    }

    renewed_record->lease_expires_at =
        renewed_expiration;

    repository.update(
        std::move(*renewed_record)
    );

    const auto renewed_lease =
        repository.get(
            job_id
        );

    expect(
        renewed_lease.has_value() &&
        renewed_lease->lease_expires_at
            .has_value() &&
        *renewed_lease->lease_expires_at ==
            renewed_expiration,
        std::string{repository_name} +
            " persists a renewed lease"
    );

    auto cleared_record =
        repository.get(
            job_id
        );

    if (!cleared_record.has_value()) {
        expect(
            false,
            std::string{repository_name} +
                " retrieves job before clearing lease"
        );

        return;
    }

    cleared_record->lease_expires_at.reset();

    repository.update(
        std::move(*cleared_record)
    );

    const auto cleared_lease =
        repository.get(
            job_id
        );

    expect(
        cleared_lease.has_value() &&
        !cleared_lease->lease_expires_at
             .has_value(),
        std::string{repository_name} +
            " clears the lease expiration"
    );
}

void test_in_memory_repository() {
    radahn::persistence::
        InMemoryJobRepository repository;

    run_lease_contract(
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
        SqliteJobRepository repository{
            database
        };

    run_lease_contract(
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
            << "Unexpected job lease repository exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " job lease assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll job lease repository tests passed\n";

    return EXIT_SUCCESS;
}