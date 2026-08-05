#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "radahn/coordinator/in_memory_coordinator.hpp"

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"

#include "radahn/persistence/in_memory_job_repository.hpp"
#include "radahn/persistence/in_memory_worker_repository.hpp"
#include "radahn/persistence/worker_repository.hpp"

#include "radahn/scheduler/least_loaded_policy.hpp"

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

radahn::domain::WorkerRecord make_worker() {
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
            "liveness-worker"
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

radahn::domain::Job make_job() {
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
            "liveness-job"
        },
        "Worker liveness test job",
        50,
        std::move(requirements)
    };
}

void test_worker_liveness_lifecycle() {
    using radahn::coordinator::
        InMemoryCoordinator;

    using radahn::domain::WorkerId;
    using radahn::domain::WorkerState;

    using radahn::persistence::
        InMemoryJobRepository;

    using radahn::persistence::
        InMemoryWorkerRepository;

    using radahn::persistence::
        WorkerHeartbeatClock;

    using radahn::persistence::
        WorkerHeartbeatTimePoint;

    using radahn::scheduler::
        LeastLoadedPolicy;

    InMemoryJobRepository
        job_repository;

    InMemoryWorkerRepository
        worker_repository;

    LeastLoadedPolicy policy;

    InMemoryCoordinator coordinator{
        policy,
        job_repository,
        worker_repository
    };

    const WorkerId worker_id{
        "liveness-worker"
    };

    coordinator.register_worker(
        make_worker()
    );

    const WorkerHeartbeatTimePoint
        initial_time{
            std::chrono::duration_cast<
                WorkerHeartbeatClock::duration
            >(
                std::chrono::milliseconds{
                    1'700'000'000'000LL
                }
            )
        };

    coordinator.record_worker_heartbeat(
        worker_id,
        initial_time
    );

    const auto early_scan_count =
        coordinator.mark_stale_workers_offline(
            initial_time +
                std::chrono::seconds{4},
            std::chrono::seconds{5}
        );

    expect(
        early_scan_count == 0,
        "Fresh worker is not marked offline"
    );

    const auto fresh_snapshot =
        coordinator.worker_snapshot(
            worker_id
        );

    expect(
        fresh_snapshot.has_value() &&
        fresh_snapshot->state() ==
            WorkerState::online,
        "Fresh worker remains online"
    );

    const auto stale_scan_count =
        coordinator.mark_stale_workers_offline(
            initial_time +
                std::chrono::seconds{5},
            std::chrono::seconds{5}
        );

    expect(
        stale_scan_count == 1,
        "Expired heartbeat marks worker offline"
    );

    const auto offline_snapshot =
        coordinator.worker_snapshot(
            worker_id
        );

    expect(
        offline_snapshot.has_value() &&
        offline_snapshot->state() ==
            WorkerState::offline,
        "Stale worker state is OFFLINE"
    );

    const auto repeated_scan_count =
        coordinator.mark_stale_workers_offline(
            initial_time +
                std::chrono::seconds{10},
            std::chrono::seconds{5}
        );

    expect(
        repeated_scan_count == 0,
        "Already offline worker is not counted again"
    );

    coordinator.submit_job(
        make_job()
    );

    const auto offline_dispatch =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        !offline_dispatch.has_value(),
        "Offline worker cannot receive a new job"
    );

    const auto recovery_time =
        initial_time +
        std::chrono::seconds{11};

    coordinator.record_worker_heartbeat(
        worker_id,
        recovery_time
    );

    const auto recovered_snapshot =
        coordinator.worker_snapshot(
            worker_id
        );

    expect(
        recovered_snapshot.has_value() &&
        recovered_snapshot->state() ==
            WorkerState::online,
        "Heartbeat returns offline worker to ONLINE"
    );

    const auto restored_heartbeat =
        worker_repository.last_heartbeat(
            worker_id
        );

    expect(
        restored_heartbeat.has_value() &&
        *restored_heartbeat ==
            recovery_time,
        "Recovery heartbeat timestamp is stored"
    );

    const auto online_dispatch =
        coordinator.dispatch_once_for_worker(
            worker_id
        );

    expect(
        online_dispatch.has_value(),
        "Recovered worker can receive a job"
    );

    bool invalid_timeout_rejected = false;

    try {
        static_cast<void>(
            coordinator
                .mark_stale_workers_offline(
                    recovery_time,
                    std::chrono::milliseconds{
                        0
                    }
                )
        );
    } catch (
        const std::invalid_argument&
    ) {
        invalid_timeout_rejected = true;
    }

    expect(
        invalid_timeout_rejected,
        "Non-positive heartbeat timeout is rejected"
    );
}

}  // namespace

int main() {
    try {
        test_worker_liveness_lifecycle();
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected worker liveness exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " worker liveness assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll worker liveness tests passed\n";

    return EXIT_SUCCESS;
}