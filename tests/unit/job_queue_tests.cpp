#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/scheduler/job_queue.hpp"

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
make_requirements() {
    return radahn::domain::ResourceRequirements{
        1.0,
        1ULL * gibibyte,
        1ULL * gibibyte,
        false,
        {"linux"}
    };
}

radahn::domain::Job make_job(
    std::string id,
    int priority,
    radahn::domain::Job::TimePoint created_at
) {
    return radahn::domain::Job{
        radahn::domain::JobId{std::move(id)},
        "Test job",
        priority,
        make_requirements(),
        created_at
    };
}

void test_job_initial_state() {
    using radahn::domain::Job;
    using radahn::domain::JobId;
    using radahn::domain::JobState;

    const Job job{
        JobId{"job-1"},
        "Pendulum simulation",
        50,
        make_requirements()
    };

    expect(
        job.id().value() == "job-1",
        "Job preserves its ID"
    );

    expect(
        job.name() == "Pendulum simulation",
        "Job preserves its name"
    );

    expect(
        job.priority() == 50,
        "Job preserves its priority"
    );

    expect(
        job.state() == JobState::queued,
        "New job begins in queued state"
    );
}

void test_job_state_transition() {
    using radahn::domain::Job;
    using radahn::domain::JobId;
    using radahn::domain::JobState;

    Job job{
        JobId{"job-1"},
        "Pendulum simulation",
        50,
        make_requirements()
    };

    job.transition_to(JobState::leased);

    expect(
        job.state() == JobState::leased,
        "Job performs a valid state transition"
    );

    bool invalid_transition_rejected = false;

    try {
        job.transition_to(JobState::succeeded);
    } catch (const std::logic_error&) {
        invalid_transition_rejected = true;
    }

    expect(
        invalid_transition_rejected,
        "Job rejects an invalid state transition"
    );
}

void test_empty_job_name_rejected() {
    using radahn::domain::Job;
    using radahn::domain::JobId;

    bool rejected = false;

    try {
        const Job invalid{
            JobId{"job-invalid"},
            "",
            10,
            make_requirements()
        };

        static_cast<void>(invalid);
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    expect(
        rejected,
        "Empty job name is rejected"
    );
}

void test_priority_ordering() {
    using radahn::domain::Job;
    using radahn::scheduler::InMemoryJobQueue;

    const Job::TimePoint base_time{};

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "low-priority",
            10,
            base_time
        )
    );

    queue.enqueue(
        make_job(
            "high-priority",
            100,
            base_time + std::chrono::seconds{1}
        )
    );

    const auto selected = queue.pop_next();

    expect(
        selected.has_value() &&
        selected->id().value() == "high-priority",
        "Higher-priority job is selected first"
    );
}

void test_fifo_for_equal_priority() {
    using radahn::domain::Job;
    using radahn::scheduler::InMemoryJobQueue;

    const Job::TimePoint base_time{};

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "newer-job",
            50,
            base_time + std::chrono::seconds{10}
        )
    );

    queue.enqueue(
        make_job(
            "older-job",
            50,
            base_time
        )
    );

    const auto selected = queue.pop_next();

    expect(
        selected.has_value() &&
        selected->id().value() == "older-job",
        "Older job wins equal-priority tie"
    );
}

void test_deterministic_id_tiebreaker() {
    using radahn::domain::Job;
    using radahn::scheduler::InMemoryJobQueue;

    const Job::TimePoint same_time{};

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "job-z",
            50,
            same_time
        )
    );

    queue.enqueue(
        make_job(
            "job-a",
            50,
            same_time
        )
    );

    const auto selected = queue.pop_next();

    expect(
        selected.has_value() &&
        selected->id().value() == "job-a",
        "Job ID provides deterministic final tiebreaker"
    );
}

void test_pop_removes_job() {
    using radahn::domain::Job;
    using radahn::scheduler::InMemoryJobQueue;

    const Job::TimePoint base_time{};

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "job-1",
            10,
            base_time
        )
    );

    queue.enqueue(
        make_job(
            "job-2",
            20,
            base_time
        )
    );

    expect(
        queue.size() == 2,
        "Queue reports jobs before selection"
    );

    const auto selected = queue.pop_next();

    expect(
        selected.has_value(),
        "Queue returns a selected job"
    );

    expect(
        queue.size() == 1,
        "Selected job is removed from queue"
    );

    expect(
        !queue.contains(
            radahn::domain::JobId{"job-2"}
        ),
        "Queue no longer contains selected job"
    );
}

void test_duplicate_job_rejected() {
    using radahn::domain::Job;
    using radahn::scheduler::InMemoryJobQueue;

    const Job::TimePoint base_time{};

    InMemoryJobQueue queue;

    queue.enqueue(
        make_job(
            "duplicate-job",
            10,
            base_time
        )
    );

    bool duplicate_rejected = false;

    try {
        queue.enqueue(
            make_job(
                "duplicate-job",
                20,
                base_time
            )
        );
    } catch (const std::invalid_argument&) {
        duplicate_rejected = true;
    }

    expect(
        duplicate_rejected,
        "Duplicate job ID is rejected"
    );
}

void test_nonqueued_job_rejected() {
    using radahn::domain::JobState;
    using radahn::scheduler::InMemoryJobQueue;

    const radahn::domain::Job::TimePoint base_time{};

    auto job = make_job(
        "leased-job",
        10,
        base_time
    );

    job.transition_to(JobState::leased);

    InMemoryJobQueue queue;

    bool rejected = false;

    try {
        queue.enqueue(std::move(job));
    } catch (const std::invalid_argument&) {
        rejected = true;
    }

    expect(
        rejected,
        "Nonqueued job cannot enter queue"
    );
}

void test_empty_queue_returns_nothing() {
    radahn::scheduler::InMemoryJobQueue queue;

    const auto selected = queue.pop_next();

    expect(
        !selected.has_value(),
        "Empty queue returns no job"
    );

    expect(
        queue.empty(),
        "New queue reports empty"
    );
}

}  // namespace

int main() {
    test_job_initial_state();
    test_job_state_transition();
    test_empty_job_name_rejected();
    test_priority_ordering();
    test_fifo_for_equal_priority();
    test_deterministic_id_tiebreaker();
    test_pop_removes_job();
    test_duplicate_job_rejected();
    test_nonqueued_job_rejected();
    test_empty_queue_returns_nothing();

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " test assertion(s) failed\n";

        return EXIT_FAILURE;
    }

    std::cout
        << "\nAll job queue tests passed\n";

    return EXIT_SUCCESS;
}