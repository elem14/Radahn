#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"

#include "radahn/scheduler/best_fit_policy.hpp"
#include "radahn/scheduler/dispatch_planner.hpp"
#include "radahn/scheduler/job_queue.hpp"

#include "radahn/domain/job.hpp"

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
make_requirements(
    std::vector<std::string> tags = {}
) {
    return radahn::domain::ResourceRequirements{
        2.0,
        1ULL * gibibyte,
        1ULL * gibibyte,
        false,
        std::move(tags)
    };
}

radahn::domain::WorkerSnapshot make_worker(
    std::string id,
    double total_cpu,
    double available_cpu,
    std::uint64_t total_memory_gib,
    std::uint64_t available_memory_gib,
    std::uint64_t total_disk_gib,
    std::uint64_t available_disk_gib,
    std::vector<std::string> tags = {"linux"}
) {
    using radahn::domain::WorkerId;
    using radahn::domain::WorkerResources;
    using radahn::domain::WorkerSnapshot;
    using radahn::domain::WorkerState;

    return WorkerSnapshot{
        WorkerId{std::move(id)},
        WorkerState::online,
        WorkerResources{
            total_cpu,
            available_cpu,
            total_memory_gib * gibibyte,
            available_memory_gib * gibibyte,
            total_disk_gib * gibibyte,
            available_disk_gib * gibibyte,
            false
        },
        0,
        10,
        std::move(tags)
    };
}

void test_best_fit_prefers_tighter_remaining_capacity() {
    using radahn::scheduler::BestFitPolicy;

    const std::vector workers{
        make_worker(
            "roomy-worker",
            16.0, 16.0,
            16, 16,
            100, 100
        ),
        make_worker(
            "snug-worker",
            4.0, 4.0,
            2, 2,
            2, 2
        )
    };

    BestFitPolicy policy;

    const auto decision = policy.select_worker(
        make_requirements(),
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        decision.has_value() &&
        decision->worker_id.value() ==
            "snug-worker",
        "Best-fit packs the job onto the worker left "
        "with the least remaining capacity"
    );

    expect(
        decision.has_value() &&
        decision->policy_name == "best-fit",
        "Decision records the best-fit policy name"
    );

    expect(
        decision.has_value() &&
        decision->eligible_worker_count == 2,
        "Both workers are eligible candidates"
    );
}

void test_best_fit_still_excludes_ineligible_workers() {
    using radahn::scheduler::BestFitPolicy;

    const std::vector workers{
        make_worker(
            "tight-but-wrong-tag",
            4.0, 4.0,
            2, 2,
            2, 2,
            {"windows"}
        ),
        make_worker(
            "loose-but-right-tag",
            16.0, 16.0,
            16, 16,
            100, 100,
            {"linux"}
        )
    };

    BestFitPolicy policy;

    const auto decision = policy.select_worker(
        make_requirements({"linux"}),
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        decision.has_value() &&
        decision->worker_id.value() ==
            "loose-but-right-tag",
        "The tighter-fitting worker is skipped when it "
        "fails eligibility"
    );

    expect(
        decision.has_value() &&
        decision->eligible_worker_count == 1,
        "Only the tag-matching worker is eligible"
    );
}

void test_best_fit_breaks_ties_by_worker_id() {
    using radahn::scheduler::BestFitPolicy;

    const std::vector workers{
        make_worker(
            "worker-z",
            8.0, 8.0,
            8, 8,
            8, 8
        ),
        make_worker(
            "worker-a",
            8.0, 8.0,
            8, 8,
            8, 8
        )
    };

    BestFitPolicy policy;

    const auto decision = policy.select_worker(
        make_requirements(),
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        decision.has_value() &&
        decision->worker_id.value() == "worker-a",
        "Identical scores break the tie toward the "
        "lexicographically smaller worker ID"
    );
}

void test_best_fit_returns_no_decision_when_nothing_fits() {
    using radahn::scheduler::BestFitPolicy;

    const std::vector workers{
        make_worker(
            "too-small",
            1.0, 1.0,
            1, 1,
            1, 1
        )
    };

    BestFitPolicy policy;

    const auto decision = policy.select_worker(
        make_requirements(),
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        !decision.has_value(),
        "No decision is produced when no worker fits"
    );
}

void test_dispatch_planner_works_with_best_fit_policy() {
    using radahn::domain::Job;
    using radahn::domain::JobId;
    using radahn::scheduler::BestFitPolicy;
    using radahn::scheduler::DispatchPlanner;
    using radahn::scheduler::InMemoryJobQueue;

    InMemoryJobQueue queue;

    queue.enqueue(
        Job{
            JobId{"packed-job"},
            "Best-fit integration job",
            50,
            make_requirements()
        }
    );

    const std::vector workers{
        make_worker(
            "roomy-worker",
            16.0, 16.0,
            16, 16,
            100, 100
        ),
        make_worker(
            "snug-worker",
            4.0, 4.0,
            2, 2,
            2, 2
        )
    };

    BestFitPolicy policy;
    DispatchPlanner planner{policy};

    const auto decision = planner.plan(
        queue,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        decision.has_value() &&
        decision->worker_id.value() ==
            "snug-worker",
        "DispatchPlanner honors best-fit placement end to end"
    );
}

}  // namespace

int main() {
    test_best_fit_prefers_tighter_remaining_capacity();
    test_best_fit_still_excludes_ineligible_workers();
    test_best_fit_breaks_ties_by_worker_id();
    test_best_fit_returns_no_decision_when_nothing_fits();
    test_dispatch_planner_works_with_best_fit_policy();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " best-fit policy assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll best-fit policy tests passed\n";

    return EXIT_SUCCESS;
}
