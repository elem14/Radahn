#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "client_service.grpc.pb.h"
#include "radahn/domain/version.hpp"

namespace {

namespace rpc = radahn::rpc::v1;

constexpr std::uint64_t mebibyte =
    1024ULL * 1024ULL;

constexpr std::uint64_t default_sleep_duration_ms =
    10'000ULL;

void print_usage() {
    std::cout
        << "Radahn distributed computing platform\n\n"
        << "Usage:\n"
        << "  radahn [--coordinator <host:port>] version\n"
        << "  radahn [--coordinator <host:port>] ping [message]\n"
        << "  radahn [--coordinator <host:port>] job submit <id> <name>"
        << " <priority> <cpu> <memory-mib> <disk-mib>"
        << " [--gpu] [--tag <tag>]... [--max-attempts <n>]"
        << " [--timeout-ms <n>]"
        << " [-- <executable> [args...]]\n"
        << "  radahn [--coordinator <host:port>] job get <id>\n"
        << "  radahn [--coordinator <host:port>] job list\n"
        << "  radahn [--coordinator <host:port>] job logs <id>"
        << " [--stderr] [--attempt <n>]\n"
        << "  radahn [--coordinator <host:port>] worker list\n"
        << "  radahn [--coordinator <host:port>] worker get <worker-id>\n"
        << "  radahn help\n"
        << "\n"
        << "  --coordinator <host:port>   Coordinator address to"
        << " connect to for this invocation. Must come before the"
        << " command. Overrides RADAHN_COORDINATOR_ADDRESS and the"
        << " default (localhost:50051).\n"
        << "\n"
        << "Operating contract: there is no authentication and no"
        << " transport encryption; connect only over a trusted"
        << " network or VPN.\n";
}

[[nodiscard]] int parse_int(
    std::string_view text,
    std::string_view field_name
) {
    try {
        std::size_t consumed = 0;

        const long value = std::stol(
            std::string{text},
            &consumed
        );

        if (consumed != text.size()) {
            throw std::invalid_argument{
                "Trailing characters"
            };
        }

        if (
            value <
                std::numeric_limits<int>::min() ||
            value >
                std::numeric_limits<int>::max()
        ) {
            throw std::out_of_range{
                "Integer is outside the supported range"
            };
        }

        return static_cast<int>(value);
    } catch (const std::exception&) {
        throw std::invalid_argument{
            std::string{field_name} +
            " must be a valid integer"
        };
    }
}

[[nodiscard]] double parse_double(
    std::string_view text,
    std::string_view field_name
) {
    try {
        std::size_t consumed = 0;

        const double value = std::stod(
            std::string{text},
            &consumed
        );

        if (consumed != text.size()) {
            throw std::invalid_argument{
                "Trailing characters"
            };
        }

        return value;
    } catch (const std::exception&) {
        throw std::invalid_argument{
            std::string{field_name} +
            " must be a valid number"
        };
    }
}

[[nodiscard]] std::uint64_t parse_mib(
    std::string_view text,
    std::string_view field_name
) {
    if (
        text.empty() ||
        text.front() == '-'
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " must be a nonnegative integer"
        };
    }

    try {
        std::size_t consumed = 0;

        const unsigned long long value =
            std::stoull(
                std::string{text},
                &consumed
            );

        if (consumed != text.size()) {
            throw std::invalid_argument{
                "Trailing characters"
            };
        }

        if (
            value >
            std::numeric_limits<
                std::uint64_t
            >::max() / mebibyte
        ) {
            throw std::out_of_range{
                "MiB value is too large"
            };
        }

        return
            static_cast<std::uint64_t>(value) *
            mebibyte;
    } catch (const std::exception&) {
        throw std::invalid_argument{
            std::string{field_name} +
            " must be a valid nonnegative integer"
        };
    }
}

[[nodiscard]] std::uint64_t parse_positive_uint64(
    std::string_view text,
    std::string_view field_name
) {
    if (
        text.empty() ||
        text.front() == '-'
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " must be a positive integer"
        };
    }

    try {
        std::size_t consumed = 0;

        const unsigned long long value =
            std::stoull(
                std::string{text},
                &consumed
            );

        if (
            consumed != text.size() ||
            value == 0
        ) {
            throw std::invalid_argument{
                "Out of range"
            };
        }

        if (
            value >
            std::numeric_limits<std::uint64_t>::max()
        ) {
            throw std::out_of_range{
                "Value is too large"
            };
        }

        return static_cast<std::uint64_t>(value);
    } catch (const std::exception&) {
        throw std::invalid_argument{
            std::string{field_name} +
            " must be a positive integer"
        };
    }
}

[[nodiscard]] std::string_view state_name(
    rpc::JobState state
) noexcept {
    switch (state) {
        case rpc::JOB_STATE_QUEUED:
            return "QUEUED";

        case rpc::JOB_STATE_LEASED:
            return "LEASED";

        case rpc::JOB_STATE_RUNNING:
            return "RUNNING";

        case rpc::JOB_STATE_RETRY_WAIT:
            return "RETRY_WAIT";

        case rpc::JOB_STATE_CANCELLATION_REQUESTED:
            return "CANCELLATION_REQUESTED";

        case rpc::JOB_STATE_SUCCEEDED:
            return "SUCCEEDED";

        case rpc::JOB_STATE_FAILED:
            return "FAILED";

        case rpc::JOB_STATE_CANCELLED:
            return "CANCELLED";

        case rpc::JOB_STATE_UNSPECIFIED:
        default:
            return "UNSPECIFIED";
    }
}

[[nodiscard]] std::string_view workload_name(
    rpc::WorkloadKind kind
) noexcept {
    switch (kind) {
        case rpc::WORKLOAD_KIND_SLEEP:
            return "SLEEP";

        case rpc::WORKLOAD_KIND_UNSPECIFIED:
        default:
            return "UNSPECIFIED";
        
        case rpc::WORKLOAD_KIND_COMMAND:
            return "COMMAND";
    }
}

[[nodiscard]]
std::string_view execution_outcome_name(
    rpc::ExecutionOutcome outcome
) noexcept {
    switch (outcome) {
        case rpc::EXECUTION_OUTCOME_EXITED:
            return "EXITED";

        case rpc::EXECUTION_OUTCOME_SIGNALED:
            return "SIGNALED";

        case rpc::EXECUTION_OUTCOME_SPAWN_FAILED:
            return "SPAWN_FAILED";

        case rpc::EXECUTION_OUTCOME_WORKER_ERROR:
            return "WORKER_ERROR";

        case rpc::EXECUTION_OUTCOME_TIMED_OUT:
            return "TIMED_OUT";

        case rpc::EXECUTION_OUTCOME_WORKER_STOPPED:
            return "WORKER_STOPPED";

        default:
            return "UNSPECIFIED";
    }
}

void print_job(
    const rpc::JobInfo& job
) {
    std::cout
        << "Job: "
        << job.id()
        << '\n'
        << "  Name: "
        << job.name()
        << '\n'
        << "  State: "
        << state_name(job.state())
        << '\n'
        << "  Priority: "
        << job.priority()
        << '\n'
        << "  CPU cores: "
        << job.requirements().cpu_cores()
        << '\n'
        << "  Memory: "
        << (
            job.requirements().memory_bytes() /
            mebibyte
        )
        << " MiB\n"
        << "  Disk: "
        << (
            job.requirements().disk_bytes() /
            mebibyte
        )
        << " MiB\n"
        << "  GPU required: "
        << (
            job.requirements().requires_gpu()
                ? "yes"
                : "no"
        )
        << '\n'
        << "  Tags:";

    if (
        job.requirements()
            .required_tags()
            .empty()
    ) {
        std::cout << " none\n";
    } else {
        for (
            const auto& tag :
            job.requirements().required_tags()
        ) {
            std::cout
                << ' '
                << tag;
        }

        std::cout << '\n';
    }

    if (job.has_workload()) {
        std::cout
            << "  Workload: "
            << workload_name(
                job.workload().kind()
            );

        if (
            job.workload().kind() ==
            rpc::WORKLOAD_KIND_SLEEP
        ) {
            std::cout
                << " for "
                << job.workload()
                       .sleep_duration_ms()
                << " ms";
        }

        if (
            job.workload().kind() ==
            rpc::WORKLOAD_KIND_COMMAND
        ) {
            std::cout
                << " executable="
                << job.workload().command().executable()
                << " arguments="
                << job.workload().command().args_size();
        }

        std::cout << '\n';
    } else {
        std::cout
            << "  Workload: none\n";
    }

    std::cout
        << "  Attempts: "
        << job.attempt_count()
        << " / "
        << job.max_attempts()
        << '\n';

    if (job.has_execution_result()) {
        const auto& result = job.execution_result();

        std::cout
            << "  Execution outcome: "
            << execution_outcome_name(result.outcome())
            << '\n';

        if (result.has_exit_code()) {
            std::cout
                << "  Exit code: "
                << result.exit_code()
                << '\n';
        }

        if (result.has_terminating_signal()) {
            std::cout
                << "  Terminating signal: "
                << result.terminating_signal()
                << '\n';
        }

        if (!result.diagnostic().empty()) {
            std::cout
                << "  Diagnostic: "
                << result.diagnostic()
                << '\n';
        }
    } else {
        std::cout
            << "  Execution result: not recorded\n";
    }

    if (job.has_assigned_worker_id()) {
        std::cout
            << "  Assigned worker: "
            << job.assigned_worker_id()
            << '\n';
    } else {
        std::cout
            << "  Assigned worker: none\n";
    }

    std::cout
        << "  stdout log uploaded: "
        << (job.stdout_log_available() ? "yes" : "no")
        << '\n'
        << "  stderr log uploaded: "
        << (job.stderr_log_available() ? "yes" : "no")
        << '\n';
}

[[nodiscard]] std::string_view worker_online_label(
    bool online
) noexcept {
    return online ? "ONLINE" : "OFFLINE";
}

void print_worker(
    const rpc::WorkerInfo& worker
) {
    std::cout
        << "Worker: "
        << worker.worker_id()
        << '\n'
        << "  State: "
        << worker_online_label(worker.online())
        << '\n'
        << "  Last heartbeat (unix ms): "
        << worker.last_heartbeat_unix_ms()
        << '\n'
        << "  CPU cores: "
        << worker.allocated_cpu_cores()
        << " / "
        << worker.total_cpu_cores()
        << " allocated\n"
        << "  Memory: "
        << (worker.allocated_memory_bytes() / mebibyte)
        << " / "
        << (worker.total_memory_bytes() / mebibyte)
        << " MiB allocated\n"
        << "  Disk: "
        << (worker.allocated_disk_bytes() / mebibyte)
        << " / "
        << (worker.total_disk_bytes() / mebibyte)
        << " MiB allocated\n"
        << "  Running jobs: "
        << worker.running_jobs()
        << " / "
        << worker.max_concurrent_jobs()
        << '\n'
        << "  GPU available: "
        << (worker.gpu_available() ? "yes" : "no")
        << '\n'
        << "  Tags:";

    if (worker.tags().empty()) {
        std::cout << " none\n";
    } else {
        for (const auto& tag : worker.tags()) {
            std::cout << ' ' << tag;
        }

        std::cout << '\n';
    }
}

constexpr std::string_view
    default_coordinator_address = "localhost:50051";

/*
 * Resolves which coordinator this invocation talks to, and strips
 * any leading "--coordinator <address>" / "--coordinator=<address>"
 * global option off the front of argv so the rest of main() can go
 * on indexing argv exactly as before. Only recognized immediately
 * after the program name (before the command verb) - this keeps it
 * out of the way of "job submit ... -- <executable> [args...]",
 * whose own "--" separator and following tokens must never be
 * treated as global options.
 */
[[nodiscard]] std::string resolve_coordinator_address(
    int& argc,
    char**& argv,
    std::vector<char*>& storage
) {
    std::string address{
        default_coordinator_address
    };

    if (
        const char* environment_value =
            std::getenv("RADAHN_COORDINATOR_ADDRESS")
    ) {
        if (*environment_value != '\0') {
            address = environment_value;
        }
    }

    storage.push_back(argv[0]);

    int index = 1;

    while (index < argc) {
        const std::string_view argument{
            argv[index]
        };

        if (argument == "--coordinator") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--coordinator requires a value"
                };
            }

            address = argv[index + 1];
            index += 2;

            continue;
        }

        constexpr std::string_view inline_prefix =
            "--coordinator=";

        if (argument.substr(0, inline_prefix.size()) ==
            inline_prefix) {
            address =
                std::string{
                    argument.substr(inline_prefix.size())
                };

            ++index;

            continue;
        }

        break;
    }

    for (; index < argc; ++index) {
        storage.push_back(argv[index]);
    }

    argc = static_cast<int>(storage.size());
    argv = storage.data();

    return address;
}

[[nodiscard]]
std::unique_ptr<
    rpc::ClientService::Stub
>
make_stub(const std::string& address) {
    std::cerr
        << "Connecting to coordinator at "
        << address
        << '\n';

    const auto channel =
        grpc::CreateChannel(
            address,
            grpc::InsecureChannelCredentials()
        );

    return rpc::ClientService::NewStub(
        channel
    );
}

int run_ping(
    rpc::ClientService::Stub& stub,
    std::string message
) {
    rpc::PingRequest request;

    request.set_message(
        std::move(message)
    );

    rpc::PingResponse response;
    grpc::ClientContext context;

    const grpc::Status status =
        stub.Ping(
            &context,
            request,
            &response
        );

    if (!status.ok()) {
        std::cerr
            << "Ping failed"
            << " (gRPC code "
            << static_cast<int>(
                status.error_code()
            )
            << "): "
            << status.error_message()
            << '\n';

        return 1;
    }

    std::cout
        << response.message()
        << '\n'
        << "Coordinator version: "
        << response.coordinator_version()
        << '\n';

    return 0;
}

int run_submit_job(
    rpc::ClientService::Stub& stub,
    int argc,
    char* argv[]
) {
    if (argc < 9) {
        std::cerr
            << "Not enough arguments for job submission\n";

        print_usage();

        return 1;
    }

    try {
        const std::string id{
            argv[3]
        };

        const std::string name{
            argv[4]
        };

        const int priority =
            parse_int(
                argv[5],
                "priority"
            );

        const double cpu_cores =
            parse_double(
                argv[6],
                "cpu"
            );

        const std::uint64_t memory_bytes =
            parse_mib(
                argv[7],
                "memory-mib"
            );

        const std::uint64_t disk_bytes =
            parse_mib(
                argv[8],
                "disk-mib"
            );

        bool requires_gpu = false;

        std::uint64_t max_attempts = 3;

        std::optional<std::uint64_t> timeout_ms;

        std::vector<std::string> tags;

        int command_start = -1;

        for (
            int index = 9;
            index < argc;
            ++index
        ) {
            const std::string_view argument{
                argv[index]
            };

            if (argument == "--") {
                if (index + 1 >= argc) {
                    throw std::invalid_argument{
                        "-- requires an executable"
                    };
                }

                command_start = index + 1;
                break;
            }

            if (argument == "--gpu") {
                requires_gpu = true;
                continue;
            }

            if (argument == "--tag") {
                if (index + 1 >= argc) {
                    throw std::invalid_argument{
                        "--tag requires a value"
                    };
                }

                tags.emplace_back(
                    argv[++index]
                );

                continue;
            }

            if (argument == "--max-attempts") {
                if (index + 1 >= argc) {
                    throw std::invalid_argument{
                        "--max-attempts requires a value"
                    };
                }

                const int parsed_max_attempts =
                    parse_int(
                        argv[++index],
                        "max-attempts"
                    );

                if (parsed_max_attempts <= 0) {
                    throw std::invalid_argument{
                        "max-attempts must be at least 1"
                    };
                }

                max_attempts =
                    static_cast<std::uint64_t>(
                        parsed_max_attempts
                    );

                continue;
            }

            if (argument == "--timeout-ms") {
                if (index + 1 >= argc) {
                    throw std::invalid_argument{
                        "--timeout-ms requires a value"
                    };
                }

                timeout_ms =
                    parse_positive_uint64(
                        argv[++index],
                        "timeout-ms"
                    );

                continue;
            }

            throw std::invalid_argument{
                "Unknown job option: " +
                std::string{argument}
            };
        }

        rpc::SubmitJobRequest request;

        request.set_id(id);
        request.set_name(name);

        request.set_priority(
            static_cast<std::int32_t>(
                priority
            )
        );

        request.set_max_attempts(
            max_attempts
        );

        auto* requirements =
            request.mutable_requirements();

        requirements->set_cpu_cores(
            cpu_cores
        );

        requirements->set_memory_bytes(
            memory_bytes
        );

        requirements->set_disk_bytes(
            disk_bytes
        );

        requirements->set_requires_gpu(
            requires_gpu
        );

        for (const auto& tag : tags) {
            requirements->add_required_tags(
                tag
            );
        }

        /*
         * Every job submitted by this CLI currently sleeps
         * for 10 seconds after a worker starts it.
         */
        auto* workload =
            request.mutable_workload();

        if (command_start >= 0) {
            workload->set_kind(
                rpc::WORKLOAD_KIND_COMMAND
            );

            auto* command =
                workload->mutable_command();

            command->set_executable(
                argv[command_start]
            );

            for (
                int index = command_start + 1;
                index < argc;
                ++index
            ) {
                command->add_args(argv[index]);
            }

            if (timeout_ms.has_value()) {
                command->set_timeout_ms(*timeout_ms);
            }
        } else {
            if (timeout_ms.has_value()) {
                throw std::invalid_argument{
                    "--timeout-ms requires a command "
                    "(pass -- <executable> [args...])"
                };
            }

            workload->set_kind(
                rpc::WORKLOAD_KIND_SLEEP
            );

            workload->set_sleep_duration_ms(
                default_sleep_duration_ms
            );
        }

        rpc::SubmitJobResponse response;
        grpc::ClientContext context;

        const grpc::Status status =
            stub.SubmitJob(
                &context,
                request,
                &response
            );

        if (!status.ok()) {
            std::cerr
                << "Job submission failed"
                << " (gRPC code "
                << static_cast<int>(
                    status.error_code()
                )
                << "): "
                << status.error_message()
                << '\n';

            return 1;
        }

        std::cout
            << "Job submitted successfully\n";

        print_job(
            response.job()
        );

        return 0;
    } catch (const std::exception& error) {
        std::cerr
            << "Invalid submission: "
            << error.what()
            << '\n';

        return 1;
    }
}

int run_get_job(
    rpc::ClientService::Stub& stub,
    std::string id
) {
    rpc::GetJobRequest request;

    request.set_id(
        std::move(id)
    );

    rpc::GetJobResponse response;
    grpc::ClientContext context;

    const grpc::Status status =
        stub.GetJob(
            &context,
            request,
            &response
        );

    if (!status.ok()) {
        std::cerr
            << "Get job failed"
            << " (gRPC code "
            << static_cast<int>(
                status.error_code()
            )
            << "): "
            << status.error_message()
            << '\n';

        return 1;
    }

    print_job(
        response.job()
    );

    return 0;
}

int run_list_jobs(
    rpc::ClientService::Stub& stub
) {
    rpc::ListJobsRequest request;
    rpc::ListJobsResponse response;
    grpc::ClientContext context;

    const grpc::Status status =
        stub.ListJobs(
            &context,
            request,
            &response
        );

    if (!status.ok()) {
        std::cerr
            << "List jobs failed"
            << " (gRPC code "
            << static_cast<int>(
                status.error_code()
            )
            << "): "
            << status.error_message()
            << '\n';

        return 1;
    }

    if (response.jobs().empty()) {
        std::cout
            << "No jobs found\n";

        return 0;
    }

    for (
        const auto& job :
        response.jobs()
    ) {
        std::cout
            << job.id()
            << " | "
            << state_name(
                job.state()
            )
            << " | priority "
            << job.priority()
            << " | "
            << job.name()
            << " | attempts "
            << job.attempt_count()
            << " / "
            << job.max_attempts();

        if (job.has_workload()) {
            std::cout
                << " | "
                << workload_name(
                    job.workload().kind()
                );

            if (
                job.workload().kind() ==
                rpc::WORKLOAD_KIND_SLEEP
            ) {
                std::cout
                    << ' '
                    << job.workload()
                           .sleep_duration_ms()
                    << " ms";
            }
        }

        std::cout << '\n';
    }

    return 0;
}

int run_list_workers(
    rpc::ClientService::Stub& stub
) {
    rpc::ListWorkersRequest request;
    rpc::ListWorkersResponse response;
    grpc::ClientContext context;

    const grpc::Status status =
        stub.ListWorkers(
            &context,
            request,
            &response
        );

    if (!status.ok()) {
        std::cerr
            << "List workers failed"
            << " (gRPC code "
            << static_cast<int>(
                status.error_code()
            )
            << "): "
            << status.error_message()
            << '\n';

        return 1;
    }

    if (response.workers().empty()) {
        std::cout
            << "No workers found\n";

        return 0;
    }

    for (const auto& worker : response.workers()) {
        std::cout
            << worker.worker_id()
            << " | "
            << worker_online_label(worker.online())
            << " | jobs "
            << worker.running_jobs()
            << " / "
            << worker.max_concurrent_jobs()
            << " | CPU "
            << worker.allocated_cpu_cores()
            << " / "
            << worker.total_cpu_cores()
            << '\n';
    }

    return 0;
}

int run_get_worker(
    rpc::ClientService::Stub& stub,
    const std::string& worker_id
) {
    rpc::GetWorkerRequest request;

    request.set_worker_id(worker_id);

    rpc::GetWorkerResponse response;
    grpc::ClientContext context;

    const grpc::Status status =
        stub.GetWorker(
            &context,
            request,
            &response
        );

    if (!status.ok()) {
        std::cerr
            << "Get worker failed"
            << " (gRPC code "
            << static_cast<int>(
                status.error_code()
            )
            << "): "
            << status.error_message()
            << '\n';

        return 1;
    }

    print_worker(
        response.worker()
    );

    return 0;
}

int run_get_job_logs(
    rpc::ClientService::Stub& stub,
    const std::string& id,
    bool use_stderr,
    std::optional<std::uint64_t> attempt_number
) {
    rpc::GetJobLogRequest request;

    request.set_id(id);

    request.set_stream(
        use_stderr
            ? rpc::LOG_STREAM_STDERR
            : rpc::LOG_STREAM_STDOUT
    );

    if (attempt_number.has_value()) {
        request.set_attempt_number(*attempt_number);
    }

    rpc::GetJobLogResponse response;
    grpc::ClientContext context;

    const grpc::Status status =
        stub.GetJobLog(
            &context,
            request,
            &response
        );

    if (!status.ok()) {
        if (
            status.error_code() ==
            grpc::StatusCode::NOT_FOUND
        ) {
            std::cerr
                << "Job does not exist: "
                << id
                << '\n';
        } else if (
            status.error_code() ==
            grpc::StatusCode::UNAVAILABLE
        ) {
            std::cerr
                << "Logs have not been uploaded for job "
                << id
                << '\n';
        } else {
            std::cerr
                << "Get job log failed"
                << " (gRPC code "
                << static_cast<int>(
                    status.error_code()
                )
                << "): "
                << status.error_message()
                << '\n';
        }

        return 1;
    }

    std::cout.write(
        response.content().data(),
        static_cast<std::streamsize>(
            response.content().size()
        )
    );

    std::cout.flush();

    if (response.truncated()) {
        std::cerr
            << "Notice: log output was truncated; stored "
            << response.content().size()
            << " of "
            << response.original_byte_count()
            << " original byte(s) (attempt "
            << response.attempt_number()
            << ")\n";
    }

    return 0;
}

}  // namespace

int main(
    int argc,
    char* argv[]
) {
    std::vector<char*> argument_storage;
    std::string coordinator_address;

    try {
        coordinator_address =
            resolve_coordinator_address(
                argc,
                argv,
                argument_storage
            );
    } catch (const std::exception& error) {
        std::cerr
            << error.what()
            << '\n';

        print_usage();

        return 1;
    }

    if (argc < 2) {
        print_usage();
        return 0;
    }

    const std::string_view command{
        argv[1]
    };

    if (command == "version") {
        std::cout
            << "Radahn "
            << radahn::domain::version()
            << '\n';

        return 0;
    }

    if (command == "help") {
        print_usage();
        return 0;
    }

    auto stub = make_stub(coordinator_address);

    if (command == "ping") {
        std::string message{
            "hello"
        };

        if (argc >= 3) {
            message = argv[2];
        }

        return run_ping(
            *stub,
            std::move(message)
        );
    }

    if (command == "job") {
        if (argc < 3) {
            print_usage();
            return 1;
        }

        const std::string_view job_command{
            argv[2]
        };

        if (job_command == "submit") {
            return run_submit_job(
                *stub,
                argc,
                argv
            );
        }

        if (job_command == "get") {
            if (argc != 4) {
                std::cerr
                    << "Usage: radahn job get <id>\n";

                return 1;
            }

            return run_get_job(
                *stub,
                argv[3]
            );
        }

        if (job_command == "list") {
            if (argc != 3) {
                std::cerr
                    << "Usage: radahn job list\n";

                return 1;
            }

            return run_list_jobs(
                *stub
            );
        }

        if (job_command == "logs") {
            if (argc < 4) {
                std::cerr
                    << "Usage: radahn job logs <id> "
                    << "[--stderr] [--attempt <n>]\n";

                return 1;
            }

            const std::string id{argv[3]};

            bool use_stderr = false;
            std::optional<std::uint64_t> attempt_number;

            try {
                for (int index = 4; index < argc; ++index) {
                    const std::string_view argument{
                        argv[index]
                    };

                    if (argument == "--stderr") {
                        use_stderr = true;
                        continue;
                    }

                    if (argument == "--attempt") {
                        if (index + 1 >= argc) {
                            throw std::invalid_argument{
                                "--attempt requires a value"
                            };
                        }

                        attempt_number =
                            parse_positive_uint64(
                                argv[++index],
                                "attempt"
                            );

                        continue;
                    }

                    throw std::invalid_argument{
                        "Unknown job logs option: " +
                        std::string{argument}
                    };
                }
            } catch (const std::exception& error) {
                std::cerr
                    << "Invalid arguments: "
                    << error.what()
                    << '\n';

                return 1;
            }

            return run_get_job_logs(
                *stub,
                id,
                use_stderr,
                attempt_number
            );
        }

        std::cerr
            << "Unknown job command: "
            << job_command
            << '\n';

        print_usage();

        return 1;
    }

    if (command == "worker") {
        if (argc < 3) {
            print_usage();
            return 1;
        }

        const std::string_view worker_command{
            argv[2]
        };

        if (worker_command == "list") {
            if (argc != 3) {
                std::cerr
                    << "Usage: radahn worker list\n";

                return 1;
            }

            return run_list_workers(
                *stub
            );
        }

        if (worker_command == "get") {
            if (argc != 4) {
                std::cerr
                    << "Usage: radahn worker get <worker-id>\n";

                return 1;
            }

            return run_get_worker(
                *stub,
                argv[3]
            );
        }

        std::cerr
            << "Unknown worker command: "
            << worker_command
            << '\n';

        print_usage();

        return 1;
    }

    std::cerr
        << "Unknown command: "
        << command
        << '\n';

    print_usage();

    return 1;
}
