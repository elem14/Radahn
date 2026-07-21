#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/resource.hpp"

namespace radahn::domain {

enum class WorkerState {
    online,
    draining,
    offline,
    disabled
};

class WorkerSnapshot {
public:
    WorkerSnapshot(
        WorkerId id,
        WorkerState state,
        WorkerResources resources,
        std::size_t running_jobs,
        std::size_t max_concurrent_jobs,
        std::vector<std::string> tags
    );

    [[nodiscard]] const WorkerId& id() const noexcept;
    [[nodiscard]] WorkerState state() const noexcept;

    [[nodiscard]] const WorkerResources&
    resources() const noexcept;

    [[nodiscard]] std::size_t running_jobs() const noexcept;
    [[nodiscard]] std::size_t max_concurrent_jobs() const noexcept;

    [[nodiscard]] const std::vector<std::string>&
    tags() const noexcept;

    [[nodiscard]] bool has_execution_slot() const noexcept;
    [[nodiscard]] bool has_tag(const std::string& tag) const;

private:
    WorkerId id_;
    WorkerState state_;
    WorkerResources resources_;

    std::size_t running_jobs_;
    std::size_t max_concurrent_jobs_;

    std::vector<std::string> tags_;
};

}  // namespace radahn::domain