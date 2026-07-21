#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace radahn::domain {

class ResourceRequirements {
public:
    ResourceRequirements(
        double cpu_cores,
        std::uint64_t memory_bytes,
        std::uint64_t disk_bytes,
        bool requires_gpu,
        std::vector<std::string> required_tags = {}
    );

    [[nodiscard]] double cpu_cores() const noexcept;
    [[nodiscard]] std::uint64_t memory_bytes() const noexcept;
    [[nodiscard]] std::uint64_t disk_bytes() const noexcept;
    [[nodiscard]] bool requires_gpu() const noexcept;

    [[nodiscard]] const std::vector<std::string>&
    required_tags() const noexcept;

private:
    double cpu_cores_;
    std::uint64_t memory_bytes_;
    std::uint64_t disk_bytes_;
    bool requires_gpu_;
    std::vector<std::string> required_tags_;
};

class WorkerResources {
public:
    WorkerResources(
        double total_cpu_cores,
        double available_cpu_cores,
        std::uint64_t total_memory_bytes,
        std::uint64_t available_memory_bytes,
        std::uint64_t total_disk_bytes,
        std::uint64_t available_disk_bytes,
        bool gpu_available
    );

    [[nodiscard]] double total_cpu_cores() const noexcept;
    [[nodiscard]] double available_cpu_cores() const noexcept;

    [[nodiscard]] std::uint64_t total_memory_bytes() const noexcept;
    [[nodiscard]] std::uint64_t available_memory_bytes() const noexcept;

    [[nodiscard]] std::uint64_t total_disk_bytes() const noexcept;
    [[nodiscard]] std::uint64_t available_disk_bytes() const noexcept;

    [[nodiscard]] bool gpu_available() const noexcept;

private:
    double total_cpu_cores_;
    double available_cpu_cores_;

    std::uint64_t total_memory_bytes_;
    std::uint64_t available_memory_bytes_;

    std::uint64_t total_disk_bytes_;
    std::uint64_t available_disk_bytes_;

    bool gpu_available_;
};

}  // namespace radahn::domain