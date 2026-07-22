#include "radahn/domain/worker_record.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "radahn/domain/worker_eligibility.hpp"

namespace radahn::domain {

WorkerRecord::WorkerRecord(
    WorkerSnapshot snapshot
)
    : id_{snapshot.id()},
      state_{snapshot.state()},
      total_cpu_cores_{
          snapshot.resources().total_cpu_cores()
      },
      available_cpu_cores_{
          snapshot.resources().available_cpu_cores()
      },
      total_memory_bytes_{
          snapshot.resources().total_memory_bytes()
      },
      available_memory_bytes_{
          snapshot.resources().available_memory_bytes()
      },
      total_disk_bytes_{
          snapshot.resources().total_disk_bytes()
      },
      available_disk_bytes_{
          snapshot.resources().available_disk_bytes()
      },
      gpu_available_{
          snapshot.resources().gpu_available()
      },
      running_jobs_{snapshot.running_jobs()},
      max_concurrent_jobs_{
          snapshot.max_concurrent_jobs()
      },
      tags_{snapshot.tags()} {
}

const WorkerId& WorkerRecord::id() const noexcept {
    return id_;
}

WorkerSnapshot WorkerRecord::snapshot() const {
    return WorkerSnapshot{
        id_,
        state_,
        WorkerResources{
            total_cpu_cores_,
            available_cpu_cores_,
            total_memory_bytes_,
            available_memory_bytes_,
            total_disk_bytes_,
            available_disk_bytes_,
            gpu_available_
        },
        running_jobs_,
        max_concurrent_jobs_,
        tags_
    };
}

void WorkerRecord::reserve(
    const ResourceRequirements& requirements
) {
    const auto eligibility = evaluate_worker(
        requirements,
        snapshot()
    );

    if (!eligibility.eligible()) {
        throw std::invalid_argument{
            "Worker cannot reserve the requested resources"
        };
    }

    available_cpu_cores_ -= requirements.cpu_cores();

    available_memory_bytes_ -=
        requirements.memory_bytes();

    available_disk_bytes_ -=
        requirements.disk_bytes();

    ++running_jobs_;
}

void WorkerRecord::release(
    const ResourceRequirements& requirements
) {
    if (running_jobs_ == 0) {
        throw std::logic_error{
            "Worker has no running job to release"
        };
    }

    const double reserved_cpu =
        total_cpu_cores_ - available_cpu_cores_;

    const std::uint64_t reserved_memory =
        total_memory_bytes_ - available_memory_bytes_;

    const std::uint64_t reserved_disk =
        total_disk_bytes_ - available_disk_bytes_;

    constexpr double cpu_tolerance = 0.000001;

    if (requirements.cpu_cores() >
        reserved_cpu + cpu_tolerance) {
        throw std::invalid_argument{
            "Cannot release more CPU than is reserved"
        };
    }

    if (requirements.memory_bytes() >
        reserved_memory) {
        throw std::invalid_argument{
            "Cannot release more memory than is reserved"
        };
    }

    if (requirements.disk_bytes() >
        reserved_disk) {
        throw std::invalid_argument{
            "Cannot release more disk than is reserved"
        };
    }

    available_cpu_cores_ = std::min(
        total_cpu_cores_,
        available_cpu_cores_ +
            requirements.cpu_cores()
    );

    available_memory_bytes_ +=
        requirements.memory_bytes();

    available_disk_bytes_ +=
        requirements.disk_bytes();

    --running_jobs_;
}

}  // namespace radahn::domain