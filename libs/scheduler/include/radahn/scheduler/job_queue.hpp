#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"

namespace radahn::scheduler {

using OrderedJobs =
    std::vector<const domain::Job*>;

class InMemoryJobQueue {
public:
    void enqueue(domain::Job job);

    [[nodiscard]] std::optional<domain::Job>
    pop_next();

    [[nodiscard]] OrderedJobs
    ordered_jobs() const;

    [[nodiscard]] bool contains(
        const domain::JobId& job_id
    ) const;

    [[nodiscard]] std::size_t size() const noexcept;

    [[nodiscard]] bool empty() const noexcept;

private:
    std::vector<domain::Job> jobs_;
};

}  // namespace radahn::scheduler