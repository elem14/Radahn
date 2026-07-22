#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/worker.hpp"

namespace radahn::domain {

class WorkerRecord {
public:
    explicit WorkerRecord(WorkerSnapshot snapshot);

    [[nodiscard]] const WorkerId& id() const noexcept;

    [[nodiscard]] WorkerSnapshot snapshot() const;

    void reserve(
        const ResourceRequirements& requirements
    );

    void release(
        const ResourceRequirements& requirements
    );

private:
    WorkerId id_;
    WorkerState state_;

    double total_cpu_cores_;
    double available_cpu_cores_;

    std::uint64_t total_memory_bytes_;
    std::uint64_t available_memory_bytes_;

    std::uint64_t total_disk_bytes_;
    std::uint64_t available_disk_bytes_;

    bool gpu_available_;

    std::size_t running_jobs_;
    std::size_t max_concurrent_jobs_;

    std::vector<std::string> tags_;
};

}  // namespace radahn::domain