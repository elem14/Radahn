#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"
#include "radahn/scheduler/round_robin_policy.hpp"
#include "radahn/scheduler/scheduling_decision.hpp"

namespace {

int failure_count = 0;

constexpr std::uint64_t gibibyte =
    1024ULL * 1024ULL * 1024ULL;

void expect(
    bool condition,
    std::string_view description
) {
    if (condition) {
        std::cout << "[PASS] "
                  << description
                  << '\n';
        return;
    }

    std::cerr << "[FAIL] "
              << description
              << '\n';

    ++failure_count;
}

void expect_selected_worker(
    const std::optional<
        radahn::scheduler::SchedulingDecision
    >& decision,
    std::string_view expected_worker,
    std::string_view description
) {
    const bool matches =
        decision.has_value() &&
        decision->worker_id.value() ==
            expected_worker;

    expect(matches, description);
}

radahn::domain::ResourceRequirements
make_requirements() {
    return radahn::domain::ResourceRequirements{
        1.0,
        1ULL * gibibyte,
        1ULL * gibibyte,
        false,
        {"linux"}
    };
}

radahn::domain::WorkerSnapshot make_worker(
    std::string id,
    radahn::domain::WorkerState state,
    double available_cpu,
    std::size_t running_jobs,
    std::size_t max_concurrent_jobs,
    std::vector<std::string> tags = {"linux"}
) {
    using radahn::domain::WorkerId;
    using radahn::domain::WorkerResources;
    using radahn::domain::WorkerSnapshot;

    return WorkerSnapshot{
        WorkerId{std::move(id)},
        state,
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
        std::move(tags)
    };
}

void test_round_robin_cycles_workers() {
    using radahn::domain::WorkerState;
    using radahn::scheduler::RoundRobinPolicy;

    const auto requirements = make_requirements();

    const std::vector workers{
        make_worker(
            "worker-c",
            WorkerState::online,
            6.0,
            0,
            4
        ),
        make_worker(
            "worker-a",
            WorkerState::online,
            6.0,
            0,
            4
        ),
        make_worker(
            "worker-b",
            WorkerState::online,
            6.0,
            0,
            4
        )
    };

    RoundRobinPolicy policy;

    const auto first = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    const auto second = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    const auto third = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    const auto fourth = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect_selected_worker(
        first,
        "worker-a",
        "Round robin begins with first sorted worker"
    );

    expect_selected_worker(
        second,
        "worker-b",
        "Round robin advances to second worker"
    );

    expect_selected_worker(
        third,
        "worker-c",
        "Round robin advances to third worker"
    );

    expect_selected_worker(
        fourth,
        "worker-a",
        "Round robin wraps to first worker"
    );

    expect(
        first.has_value() &&
        first->eligible_worker_count == 3,
        "Round robin reports eligible worker count"
    );
}

void test_ineligible_workers_are_filtered() {
    using radahn::domain::WorkerState;
    using radahn::scheduler::RoundRobinPolicy;

    const auto requirements = make_requirements();

    const std::vector workers{
        make_worker(
            "worker-offline",
            WorkerState::offline,
            6.0,
            0,
            4
        ),
        make_worker(
            "worker-full",
            WorkerState::online,
            6.0,
            4,
            4
        ),
        make_worker(
            "worker-valid",
            WorkerState::online,
            6.0,
            0,
            4
        )
    };

    RoundRobinPolicy policy;

    const auto decision = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect_selected_worker(
        decision,
        "worker-valid",
        "Scheduler ignores ineligible workers"
    );

    expect(
        decision.has_value() &&
        decision->eligible_worker_count == 1,
        "Only eligible workers are counted"
    );
}

void test_no_eligible_worker() {
    using radahn::domain::WorkerState;
    using radahn::scheduler::RoundRobinPolicy;

    const auto requirements = make_requirements();

    const std::vector workers{
        make_worker(
            "worker-offline",
            WorkerState::offline,
            6.0,
            0,
            4
        ),
        make_worker(
            "worker-disabled",
            WorkerState::disabled,
            6.0,
            0,
            4
        )
    };

    RoundRobinPolicy policy;

    const auto decision = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect(
        !decision.has_value(),
        "Scheduler returns no decision when no worker is eligible"
    );
}

void test_least_loaded_worker_selected() {
    using radahn::domain::WorkerState;
    using radahn::scheduler::LeastLoadedPolicy;

    const auto requirements = make_requirements();

    const std::vector workers{
        make_worker(
            "worker-a",
            WorkerState::online,
            6.0,
            2,
            4
        ),
        make_worker(
            "worker-b",
            WorkerState::online,
            6.0,
            1,
            8
        ),
        make_worker(
            "worker-c",
            WorkerState::online,
            6.0,
            1,
            2
        )
    };

    LeastLoadedPolicy policy;

    const auto decision = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect_selected_worker(
        decision,
        "worker-b",
        "Least-loaded policy uses normalized load"
    );

    expect(
        decision.has_value() &&
        decision->policy_name == "least-loaded",
        "Decision records scheduler policy"
    );

    expect(
        decision.has_value() &&
        decision->score.has_value(),
        "Least-loaded decision contains a score"
    );
}

void test_least_loaded_cpu_tiebreaker() {
    using radahn::domain::WorkerState;
    using radahn::scheduler::LeastLoadedPolicy;

    const auto requirements = make_requirements();

    const std::vector workers{
        make_worker(
            "worker-a",
            WorkerState::online,
            2.0,
            1,
            4
        ),
        make_worker(
            "worker-b",
            WorkerState::online,
            6.0,
            2,
            8
        )
    };

    LeastLoadedPolicy policy;

    const auto decision = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect_selected_worker(
        decision,
        "worker-b",
        "More available CPU breaks equal-load tie"
    );
}

void test_deterministic_id_tiebreaker() {
    using radahn::domain::WorkerState;
    using radahn::scheduler::LeastLoadedPolicy;

    const auto requirements = make_requirements();

    const std::vector workers{
        make_worker(
            "worker-z",
            WorkerState::online,
            6.0,
            1,
            4
        ),
        make_worker(
            "worker-a",
            WorkerState::online,
            6.0,
            1,
            4
        )
    };

    LeastLoadedPolicy policy;

    const auto decision = policy.select_worker(
        requirements,
        std::span<const radahn::domain::WorkerSnapshot>{
            workers
        }
    );

    expect_selected_worker(
        decision,
        "worker-a",
        "Worker ID provides deterministic final tiebreaker"
    );
}

}  // namespace

int main() {
    test_round_robin_cycles_workers();
    test_ineligible_workers_are_filtered();
    test_no_eligible_worker();
    test_least_loaded_worker_selected();
    test_least_loaded_cpu_tiebreaker();
    test_deterministic_id_tiebreaker();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " test assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll scheduler tests passed\n";

    return EXIT_SUCCESS;
}