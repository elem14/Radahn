#include "radahn/domain/workload.hpp"

#include <stdexcept>

namespace radahn::domain {

WorkloadSpec::WorkloadSpec(
    WorkloadKind kind,
    std::chrono::milliseconds sleep_duration
)
    : kind_{kind},
      sleep_duration_{sleep_duration} {
    if (sleep_duration_.count() <= 0) {
        throw std::invalid_argument{
            "Sleep duration must be positive"
        };
    }
}

WorkloadSpec WorkloadSpec::sleep(
    std::chrono::milliseconds duration
) {
    return WorkloadSpec{
        WorkloadKind::sleep,
        duration
    };
}

WorkloadKind WorkloadSpec::kind() const noexcept {
    return kind_;
}

std::chrono::milliseconds
WorkloadSpec::sleep_duration() const noexcept {
    return sleep_duration_;
}

}  // namespace radahn::domain