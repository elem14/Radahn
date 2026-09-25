#include "radahn/domain/workload.hpp"

#include <stdexcept>
#include <utility>

namespace radahn::domain {

WorkloadSpec::WorkloadSpec(
    std::chrono::milliseconds duration
)
    : kind_{WorkloadKind::sleep},
      sleep_duration_{duration} {
    if (duration.count() <= 0) {
        throw std::invalid_argument{
            "Sleep duration must be positive"
        };
    }
}

WorkloadSpec::WorkloadSpec(
    CommandWorkload command
)
    : kind_{WorkloadKind::command},
      command_{std::move(command)} {
    const auto& spec = *command_;

    if (
        spec.executable.empty() ||
        spec.executable.find('\0') != std::string::npos
    ) {
        throw std::invalid_argument{
            "Executable must be nonempty and contain no NUL bytes"
        };
    }

    for (const auto& arg : spec.args) {
        if (arg.find('\0') != std::string::npos) {
            throw std::invalid_argument{
                "Command arguments cannot contain NUL bytes"
            };
        }
    }

    if (
        spec.timeout &&
        spec.timeout->count() <= 0
    ) {
        throw std::invalid_argument{
            "Command timeout must be positive when supplied"
        };
    }
}

WorkloadSpec WorkloadSpec::sleep(
    std::chrono::milliseconds duration
) {
    return WorkloadSpec{duration};
}

WorkloadSpec WorkloadSpec::command(
    CommandWorkload command
) {
    return WorkloadSpec{
        std::move(command)
    };
}

WorkloadKind WorkloadSpec::kind() const noexcept {
    return kind_;
}

std::chrono::milliseconds
WorkloadSpec::sleep_duration() const noexcept {
    return sleep_duration_;
}

const CommandWorkload&
WorkloadSpec::command_spec() const {
    if (!command_) {
        throw std::logic_error{
            "Sleep workload has no command specification"
        };
    }

    return *command_;
}

}  // namespace radahn::domain