#include "workload_codec.hpp"

#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>

namespace radahn::rpc_codec {

namespace {

[[nodiscard]]
std::chrono::milliseconds checked_duration(
    std::uint64_t value
) {
    using Rep = std::chrono::milliseconds::rep;

    const auto maximum =
        static_cast<std::uint64_t>(
            std::numeric_limits<Rep>::max()
        );

    if (value == 0 || value > maximum) {
        throw std::invalid_argument{
            "Duration must be positive and fit "
            "the supported milliseconds range"
        };
    }

    return std::chrono::milliseconds{
        static_cast<Rep>(value)
    };
}

}  // namespace

void encode_workload(
    const domain::WorkloadSpec& workload,
    rpc::v1::WorkloadSpec& output
) {
    output.Clear();

    switch (workload.kind()) {
        case domain::WorkloadKind::sleep:
            output.set_kind(
                rpc::v1::WORKLOAD_KIND_SLEEP
            );

            output.set_sleep_duration_ms(
                static_cast<std::uint64_t>(
                    workload.sleep_duration().count()
                )
            );

            return;

        case domain::WorkloadKind::command: {
            output.set_kind(
                rpc::v1::WORKLOAD_KIND_COMMAND
            );

            const auto& source =
                workload.command_spec();

            auto* destination =
                output.mutable_command();

            destination->set_executable(
                source.executable
            );

            for (const auto& arg : source.args) {
                destination->add_args(arg);
            }

            if (source.timeout) {
                destination->set_timeout_ms(
                    static_cast<std::uint64_t>(
                        source.timeout->count()
                    )
                );
            }

            return;
        }
    }

    throw std::invalid_argument{
        "Unsupported domain workload kind"
    };
}

domain::WorkloadSpec decode_workload(
    const rpc::v1::WorkloadSpec& workload
) {
    switch (workload.kind()) {
        case rpc::v1::WORKLOAD_KIND_SLEEP: {
            if (workload.has_command()) {
                throw std::invalid_argument{
                    "Sleep workload cannot contain "
                    "command fields"
                };
            }

            return domain::WorkloadSpec::sleep(
                checked_duration(
                    workload.sleep_duration_ms()
                )
            );
        }

        case rpc::v1::WORKLOAD_KIND_COMMAND: {
            if (!workload.has_command()) {
                throw std::invalid_argument{
                    "Command workload is missing "
                    "its command specification"
                };
            }

            if (workload.sleep_duration_ms() != 0) {
                throw std::invalid_argument{
                    "Command workload cannot contain "
                    "a sleep duration"
                };
            }

            const auto& source =
                workload.command();

            domain::CommandWorkload command{
                source.executable(),
                {},
                std::nullopt
            };

            for (const auto& arg : source.args()) {
                command.args.push_back(arg);
            }

            if (source.has_timeout_ms()) {
                command.timeout =
                    checked_duration(
                        source.timeout_ms()
                    );
            }

            return domain::WorkloadSpec::command(
                std::move(command)
            );
        }

        default:
            throw std::invalid_argument{
                "Unsupported or missing workload kind"
            };
    }
}


void encode_execution_result(
    const domain::ExecutionResult& result,
    rpc::v1::ExecutionResult& output
) {
    result.validate();
    output.Clear();

    // Values intentionally match the stable domain enum.
    output.set_outcome(
        static_cast<rpc::v1::ExecutionOutcome>(
            static_cast<int>(result.outcome)
        )
    );

    if (result.exit_code.has_value()) {
        output.set_exit_code(*result.exit_code);
    }

    if (result.terminating_signal.has_value()) {
        output.set_terminating_signal(
            *result.terminating_signal
        );
    }

    output.set_diagnostic(result.diagnostic);
}

domain::ExecutionResult decode_execution_result(
    const rpc::v1::ExecutionResult& input
) {
    domain::ExecutionResult result{
        static_cast<domain::ExecutionOutcome>(
            static_cast<int>(input.outcome())
        ),
        std::nullopt,
        std::nullopt,
        input.diagnostic()
    };

    if (input.has_exit_code()) {
        result.exit_code = input.exit_code();
    }

    if (input.has_terminating_signal()) {
        result.terminating_signal =
            input.terminating_signal();
    }

    result.validate();
    return result;
}

}  // namespace radahn::rpc_codec