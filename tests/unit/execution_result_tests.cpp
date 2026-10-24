#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include "radahn/domain/job.hpp"
#include "radahn/domain/execution_result.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"
#include "workload_codec.hpp"

namespace {

namespace d = radahn::domain;
namespace p = radahn::persistence;

void require(bool condition, const char* description) {
    if (!condition) {
        throw std::runtime_error{description};
    }

    std::cout << "[PASS] " << description << '\n';
}

struct TemporaryDatabase {
    std::filesystem::path path =
        std::filesystem::temp_directory_path() /
        (
            "radahn-results-" +
            std::to_string(
                std::chrono::steady_clock::now()
                    .time_since_epoch().count()
            ) +
            ".db"
        );

    ~TemporaryDatabase() {
        std::error_code ignored;

        for (const auto* suffix : {"", "-wal", "-shm"}) {
            std::filesystem::remove(
                path.string() + suffix,
                ignored
            );
        }
    }
};

d::Job finished_job(
    const std::string& id,
    const d::ExecutionResult& result
) {
    d::Job job{
        d::JobId{id},
        "Result test",
        1,
        d::ResourceRequirements{1, 1024, 1024, false, {}},
        d::WorkloadSpec::command({
            "test-program",
            {},
            std::nullopt
        })
    };

    job.transition_to(d::JobState::leased);
    job.transition_to(d::JobState::running);

    job.transition_to(
        result.succeeded()
            ? d::JobState::succeeded
            : d::JobState::failed
    );

    job.set_execution_result(result);
    return job;
}

void run_tests() {
    const std::vector<d::ExecutionResult> results{
        {d::ExecutionOutcome::exited, 0, std::nullopt, ""},
        {d::ExecutionOutcome::exited, 7, std::nullopt, ""},
        {d::ExecutionOutcome::signaled, std::nullopt, 15, ""},
        {
            d::ExecutionOutcome::spawn_failed,
            std::nullopt,
            std::nullopt,
            "Executable not found"
        },
        {
            d::ExecutionOutcome::worker_error,
            std::nullopt,
            std::nullopt,
            "Could not create output directory"
        },
        {
            d::ExecutionOutcome::timed_out,
            std::nullopt,
            std::nullopt,
            "Execution deadline exceeded"
        },
        {
            d::ExecutionOutcome::worker_stopped,
            std::nullopt,
            std::nullopt,
            "Worker shutdown stopped execution"
        }
    };

    TemporaryDatabase temporary;

    {
        p::SqliteDatabase database{temporary.path};
        p::SqliteJobRepository repository{database};

        for (std::size_t index = 0; index < results.size(); ++index) {
            const auto& result = results[index];

            radahn::rpc::v1::ExecutionResult wire;
            radahn::rpc_codec::encode_execution_result(result, wire);

            radahn::rpc::v1::ExecutionResult parsed;

            require(
                parsed.ParseFromString(wire.SerializeAsString()),
                "Result protobuf parses"
            );

            const auto decoded =
                radahn::rpc_codec::decode_execution_result(parsed);

            require(
                decoded.outcome == result.outcome &&
                decoded.exit_code == result.exit_code &&
                decoded.terminating_signal == result.terminating_signal &&
                decoded.diagnostic == result.diagnostic,
                "Result survives protobuf serialization"
            );

            repository.insert(p::JobRecord{
                finished_job(std::to_string(index), result),
                std::nullopt,
                std::nullopt
            });
        }
    }

    {
        p::SqliteDatabase database{temporary.path};
        p::SqliteJobRepository repository{database};

        for (std::size_t index = 0; index < results.size(); ++index) {
            const auto record = repository.get(
                d::JobId{std::to_string(index)}
            );

            require(record.has_value(), "Finished job survives reopening");

            const auto& stored = record->job.execution_result();

            require(stored.has_value(), "Execution result survives reopening");

            require(
                stored->outcome == results[index].outcome &&
                stored->exit_code == results[index].exit_code &&
                stored->terminating_signal == results[index].terminating_signal &&
                stored->diagnostic == results[index].diagnostic,
                "All persisted result fields are preserved"
            );
        }
    }

    bool invalid_rejected = false;

    try {
        d::ExecutionResult{
            d::ExecutionOutcome::exited,
            std::nullopt,
            std::nullopt,
            ""
        }.validate();
    } catch (const std::invalid_argument&) {
        invalid_rejected = true;
    }

    require(invalid_rejected, "Exited result requires an exit code");

    auto succeeded = finished_job("success", results[0]);
    bool mismatch_rejected = false;

    try {
        succeeded.set_execution_result(results[1]);
    } catch (const std::invalid_argument&) {
        mismatch_rejected = true;
    }

    require(mismatch_rejected, "Success cannot store a failure result");
}

}  // namespace

int main() {
    try {
        run_tests();
        return EXIT_SUCCESS;
    } catch (const std::exception& error) {
        std::cerr << "[FAIL] " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}