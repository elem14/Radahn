#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "radahn/coordinator/in_memory_coordinator.hpp"
#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"

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

radahn::domain::ResourceRequirements
make_cpu_requirements() {
    return radahn::domain::ResourceRequirements{
        2.0,
        2ULL * gibibyte,
        5ULL * gibibyte,
        false,
        {"linux"}
    };
}

radahn::domain::ResourceRequirements
make_gpu_requirements() {
    return radahn::domain::ResourceRequirements{
        2.0,
        2ULL * gibibyte,
        5ULL * gibibyte,
        true,
        {"linux", "gpu"}
    };
}

radahn::domain::Job make_job(
    std::string id,
    int priority,
    radahn::domain::ResourceRequirements requirements
) {
    return radahn::domain::Job{
        radahn::domain::JobId{std::move(id)},
        "Coordinator test job",
        priority,
        std::move(requirements)
    };
}

radahn::domain::WorkerRecord make_worker(
    std::string id,
    double available_cpu,
    std::size_t running_jobs,
    std::size_t max_concurrent_jobs
) {
    using radahn::domain::WorkerId;
    using radahn::domain::WorkerRecord;
    using radahn::domain::WorkerResources;
    using radahn::domain::WorkerSnapshot;
    using radahn::domain::WorkerState;

    return WorkerRecord{
        WorkerSnapshot{
            WorkerId{std::move(id)},
            WorkerState::online,
            WorkerResources{
                8.0,
                available_cpu,
                16ULL * gibibyte,
                12ULL * gibibyte,
                500ULL * gibibyte,
                300ULL * gibibyte,
                false
            },
            running_jobs,
            max_concurrent_jobs,
            {"linux", "x86_64"}
        }
    };
}

void test_complete_successful_job_flow() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;
    using radahn::scheduler::LeastLoadedPolicy;

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    coordinator.register_worker(
        make_worker(
            "busy-worker",
            6.0,
            2,
            4
        )
    );

    coordinator.register_worker(
        make_worker(
            "idle-worker",
            8.0,
            0,
            4
        )
    );

    coordinator.submit_job(
        make_job(
            "gpu-job",
            100,
            make_gpu_requirements()
        )
    );

    coordinator.submit_job(
        make_job(
            "cpu-job",
            50,
            make_cpu_requirements()
        )
    );

    const auto decision =
        coordinator.dispatch_once();

    expect(
        decision.has_value(),
        "Coordinator produces a dispatch decision"
    );

    expect(
        decision.has_value() &&
        decision->job_id.value() == "cpu-job",
        "Coordinator bypasses unschedulable job"
    );

    expect(
        decision.has_value() &&
        decision->worker_id.value() == "idle-worker",
        "Coordinator selects least-loaded worker"
    );

    expect(
        coordinator.queued_job_count() == 1,
        "Dispatched job is removed from queue"
    );

    expect(
        coordinator.active_job_count() == 1,
        "Dispatched job becomes active"
    );

    expect(
        coordinator.job_state(
            JobId{"cpu-job"}
        ) == JobState::leased,
        "Dispatched job enters leased state"
    );

    const auto reserved_worker =
        coordinator.worker_snapshot(
            WorkerId{"idle-worker"}
        );

    expect(
        reserved_worker.has_value() &&
        reserved_worker->resources()
                .available_cpu_cores() == 6.0,
        "Dispatch reserves worker CPU"
    );

    expect(
        reserved_worker.has_value() &&
        reserved_worker->running_jobs() == 1,
        "Dispatch increments running job count"
    );

    coordinator.mark_running(
        JobId{"cpu-job"}
    );

    expect(
        coordinator.job_state(
            JobId{"cpu-job"}
        ) == JobState::running,
        "Leased job enters running state"
    );

    coordinator.mark_succeeded(
        JobId{"cpu-job"}
    );

    expect(
        coordinator.job_state(
            JobId{"cpu-job"}
        ) == JobState::succeeded,
        "Running job enters succeeded state"
    );

    expect(
        coordinator.active_job_count() == 0,
        "Completed job leaves active set"
    );

    expect(
        coordinator.finished_job_count() == 1,
        "Completed job enters finished set"
    );

    const auto released_worker =
        coordinator.worker_snapshot(
            WorkerId{"idle-worker"}
        );

    expect(
        released_worker.has_value() &&
        released_worker->resources()
                .available_cpu_cores() == 8.0,
        "Completion restores worker CPU"
    );

    expect(
        released_worker.has_value() &&
        released_worker->running_jobs() == 0,
        "Completion restores worker execution slot"
    );

    expect(
        coordinator.job_state(
            JobId{"gpu-job"}
        ) == JobState::queued,
        "Unschedulable job remains queued"
    );
}

void test_failed_job_releases_resources() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::domain::JobId;
    using radahn::domain::JobState;
    using radahn::domain::WorkerId;
    using radahn::scheduler::LeastLoadedPolicy;

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    coordinator.register_worker(
        make_worker(
            "worker-1",
            8.0,
            0,
            4
        )
    );

    coordinator.submit_job(
        make_job(
            "job-1",
            50,
            make_cpu_requirements()
        )
    );

    const auto decision =
        coordinator.dispatch_once();

    expect(
        decision.has_value(),
        "Job is dispatched before failure"
    );

    coordinator.mark_running(
        JobId{"job-1"}
    );

    coordinator.mark_failed(
        JobId{"job-1"}
    );

    expect(
        coordinator.job_state(
            JobId{"job-1"}
        ) == JobState::failed,
        "Failed job enters failed state"
    );

    const auto worker =
        coordinator.worker_snapshot(
            WorkerId{"worker-1"}
        );

    expect(
        worker.has_value() &&
        worker->resources()
            .available_cpu_cores() == 8.0,
        "Failed job releases worker CPU"
    );

    expect(
        worker.has_value() &&
        worker->running_jobs() == 0,
        "Failed job releases execution slot"
    );
}

void test_duplicate_worker_rejected() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::scheduler::LeastLoadedPolicy;

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    coordinator.register_worker(
        make_worker(
            "worker-1",
            8.0,
            0,
            4
        )
    );

    bool rejected = false;

    try {
        coordinator.register_worker(
            make_worker(
                "worker-1",
                8.0,
                0,
                4
            )
        );
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    expect(
        rejected,
        "Duplicate worker ID is rejected"
    );
}

void test_duplicate_job_rejected_while_active() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::scheduler::LeastLoadedPolicy;

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    coordinator.register_worker(
        make_worker(
            "worker-1",
            8.0,
            0,
            4
        )
    );

    coordinator.submit_job(
        make_job(
            "job-1",
            50,
            make_cpu_requirements()
        )
    );

    static_cast<void>(
        coordinator.dispatch_once()
    );

    bool rejected = false;

    try {
        coordinator.submit_job(
            make_job(
                "job-1",
                100,
                make_cpu_requirements()
            )
        );
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    expect(
        rejected,
        "Duplicate active job ID is rejected"
    );
}

void test_no_dispatch_without_work() {
    using radahn::coordinator::InMemoryCoordinator;
    using radahn::scheduler::LeastLoadedPolicy;

    LeastLoadedPolicy policy;
    InMemoryCoordinator coordinator{policy};

    coordinator.register_worker(
        make_worker(
            "worker-1",
            8.0,
            0,
            4
        )
    );

    expect(
        !coordinator.dispatch_once().has_value(),
        "Coordinator returns no dispatch without jobs"
    );
}

}  // namespace

int main() {
    test_complete_successful_job_flow();
    test_failed_job_releases_resources();
    test_duplicate_worker_rejected();
    test_duplicate_job_rejected_while_active();
    test_no_dispatch_without_work();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " test assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll in-memory coordinator tests passed\n";

    return EXIT_SUCCESS;
}

