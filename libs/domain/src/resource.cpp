#include "radahn/domain/resource.hpp"

#include <cmath>
#include <stdexcept>
#include <utility>

namespace radahn::domain {

ResourceRequirements::ResourceRequirements(
    double cpu_cores,
    std::uint64_t memory_bytes,
    std::uint64_t disk_bytes,
    bool requires_gpu,
    std::vector<std::string> required_tags
)
    : cpu_cores_{cpu_cores},
      memory_bytes_{memory_bytes},
      disk_bytes_{disk_bytes},
      requires_gpu_{requires_gpu},
      required_tags_{std::move(required_tags)} {
    if (!std::isfinite(cpu_cores_) || cpu_cores_ <= 0.0) {
        throw std::invalid_argument{
            "Required CPU cores must be finite and greater than zero"
        };
    }

    for (const auto& tag : required_tags_) {
        if (tag.empty()) {
            throw std::invalid_argument{
                "Required worker tags cannot be empty"
            };
        }
    }
}

double ResourceRequirements::cpu_cores() const noexcept {
    return cpu_cores_;
}

std::uint64_t ResourceRequirements::memory_bytes() const noexcept {
    return memory_bytes_;
}

std::uint64_t ResourceRequirements::disk_bytes() const noexcept {
    return disk_bytes_;
}

bool ResourceRequirements::requires_gpu() const noexcept {
    return requires_gpu_;
}

const std::vector<std::string>&
ResourceRequirements::required_tags() const noexcept {
    return required_tags_;
}

WorkerResources::WorkerResources(
    double total_cpu_cores,
    double available_cpu_cores,
    std::uint64_t total_memory_bytes,
    std::uint64_t available_memory_bytes,
    std::uint64_t total_disk_bytes,
    std::uint64_t available_disk_bytes,
    bool gpu_available
)
    : total_cpu_cores_{total_cpu_cores},
      available_cpu_cores_{available_cpu_cores},
      total_memory_bytes_{total_memory_bytes},
      available_memory_bytes_{available_memory_bytes},
      total_disk_bytes_{total_disk_bytes},
      available_disk_bytes_{available_disk_bytes},
      gpu_available_{gpu_available} {
    if (!std::isfinite(total_cpu_cores_) ||
        total_cpu_cores_ <= 0.0) {
        throw std::invalid_argument{
            "Total CPU cores must be finite and greater than zero"
        };
    }

    if (!std::isfinite(available_cpu_cores_) ||
        available_cpu_cores_ < 0.0 ||
        available_cpu_cores_ > total_cpu_cores_) {
        throw std::invalid_argument{
            "Available CPU cores must be between zero and total CPU cores"
        };
    }

    if (available_memory_bytes_ > total_memory_bytes_) {
        throw std::invalid_argument{
            "Available memory cannot exceed total memory"
        };
    }

    if (available_disk_bytes_ > total_disk_bytes_) {
        throw std::invalid_argument{
            "Available disk cannot exceed total disk"
        };
    }
}

double WorkerResources::total_cpu_cores() const noexcept {
    return total_cpu_cores_;
}

double WorkerResources::available_cpu_cores() const noexcept {
    return available_cpu_cores_;
}

std::uint64_t WorkerResources::total_memory_bytes() const noexcept {
    return total_memory_bytes_;
}

std::uint64_t WorkerResources::available_memory_bytes() const noexcept {
    return available_memory_bytes_;
}

std::uint64_t WorkerResources::total_disk_bytes() const noexcept {
    return total_disk_bytes_;
}

std::uint64_t WorkerResources::available_disk_bytes() const noexcept {
    return available_disk_bytes_;
}

bool WorkerResources::gpu_available() const noexcept {
    return gpu_available_;
}

}  // namespace radahn::domain