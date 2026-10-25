#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>

#include <unistd.h>

#include "radahn/execution/command_executor.hpp"
#include "workload_codec.hpp"

#include <grpcpp/grpcpp.h>

#include "worker_service.grpc.pb.h"
#include "active_job_registry.hpp"
#include "completion_reporter.hpp"
#include "execution_log_registry.hpp"
#include "heartbeat_loop.hpp"
#include "radahn/domain/version.hpp"

namespace {

namespace rpc = radahn::rpc::v1;

std::mutex g_log_mutex;

void write_log(
    std::ostream& destination,
    const std::string& message
) {
    const std::lock_guard<std::mutex> lock{
        g_log_mutex
    };

    destination << message << '\n';
    destination.flush();
}

constexpr std::uint64_t gibibyte =
    1024ULL * 1024ULL * 1024ULL;

/*
 * Set only from a signal handler (SIGINT/SIGTERM); every other
 * access is a lock-free read from the main loop. sig_atomic_t via
 * std::atomic is the standard safe pattern for a handler that must
 * not do anything beyond flipping a flag.
 */
std::atomic<bool> g_shutdown_requested{false};

extern "C" void handle_shutdown_signal(int) {
    g_shutdown_requested.store(true);
}

/*
 * Set only once the bounded shutdown drain's final reporting grace
 * interval has elapsed (see main()'s drain loop). Completion
 * reporting keeps retrying through the drain itself - only this
 * flag, not g_shutdown_requested, cuts it off, so a short-running
 * job's completion still gets a real chance to reach the
 * coordinator during an ordinary shutdown.
 */
std::atomic<bool> g_abort_reporting{false};

[[nodiscard]] unsigned int detected_cpu_count() {
    return std::max(
        1U,
        std::thread::hardware_concurrency()
    );
}

/*
 * Derived from cpu_cores (which may itself be an overridden,
 * simulated value — see --cpu) rather than always reading real
 * hardware, so a locally-simulated heterogeneous cluster gets a
 * concurrency cap that scales with the resources it claims to have.
 */
[[nodiscard]] std::uint64_t
default_max_concurrent_jobs(double cpu_cores) {
    return std::max<std::uint64_t>(
        1ULL,
        std::min<std::uint64_t>(
            static_cast<std::uint64_t>(cpu_cores),
            4ULL
        )
    );
}

/*
 * Sleep in short increments so a requested shutdown is noticed
 * promptly instead of waiting out a full backoff/poll interval.
 */
void interruptible_sleep(
    std::chrono::milliseconds duration
) {
    constexpr std::chrono::milliseconds
        poll_granularity{100};

    auto remaining = duration;

    while (
        remaining > std::chrono::milliseconds::zero() &&
        !g_shutdown_requested.load()
    ) {
        const auto this_sleep =
            std::min(
                remaining,
                poll_granularity
            );

        std::this_thread::sleep_for(
            this_sleep
        );

        remaining -= this_sleep;
    }
}

[[nodiscard]] std::vector<std::string>
default_worker_tags() {
    std::vector<std::string> tags;

#if defined(__APPLE__)
    tags.emplace_back("macos");
#elif defined(__linux__)
    tags.emplace_back("linux");
#elif defined(_WIN32)
    tags.emplace_back("windows");
#endif

#if defined(__aarch64__) || defined(__arm64__)
    tags.emplace_back("arm64");
#elif defined(__x86_64__) || defined(_M_X64)
    tags.emplace_back("x86_64");
#endif

    return tags;
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
    const double value =
        parse_double(text, field_name);

    if (value < 0.0) {
        throw std::invalid_argument{
            std::string{field_name} +
            " cannot be negative"
        };
    }

    return static_cast<std::uint64_t>(
        value * 1024.0 * 1024.0
    );
}

[[nodiscard]] std::uint64_t parse_positive_count(
    std::string_view text,
    std::string_view field_name
) {
    try {
        std::size_t consumed = 0;

        const long long value = std::stoll(
            std::string{text},
            &consumed
        );

        if (
            consumed != text.size() ||
            value <= 0
        ) {
            throw std::invalid_argument{
                "Out of range"
            };
        }

        return static_cast<std::uint64_t>(
            value
        );
    } catch (const std::exception&) {
        throw std::invalid_argument{
            std::string{field_name} +
            " must be a positive integer"
        };
    }
}

/*
 * Everything needed to register with the coordinator and run the
 * job execution loop. Defaults match real detected hardware, but
 * every field can be overridden from the command line so a single
 * physical machine can stand in for several differently-shaped
 * workers (e.g. a simulated GPU worker, or a simulated large Linux
 * box) when exercising heterogeneous-cluster scheduling locally.
 */
struct WorkerConfig {
    std::string worker_id{"local-worker"};
    std::string coordinator_address{
        "localhost:50051"
    };
    double cpu_cores =
        static_cast<double>(
            detected_cpu_count()
        );
    std::uint64_t memory_bytes =
        8ULL * gibibyte;
    std::uint64_t disk_bytes =
        50ULL * gibibyte;
    bool gpu_available = false;
    std::vector<std::string> tags =
        default_worker_tags();
    std::uint64_t max_concurrent_jobs =
        default_max_concurrent_jobs(
            cpu_cores
        );

    /*
     * Bounded graceful-shutdown policy (see main()'s drain loop):
     * how long a short-running job is given to finish normally
     * after SIGINT/SIGTERM before remaining executions are
     * forcibly terminated.
     */
    std::chrono::milliseconds shutdown_grace_ms{10000};

    /*
     * Root directory under which every execution's captured
     * stdout/stderr is written. Always resolved to an absolute path
     * (see parse_worker_config) so it is unambiguous regardless of
     * the worker's current working directory at execution time.
     */
    std::filesystem::path log_dir{"radahn-logs"};
};

[[nodiscard]] WorkerConfig parse_worker_config(
    int argc,
    char* argv[]
) {
    WorkerConfig config;

    bool tags_overridden = false;
    bool cpu_overridden = false;
    bool max_concurrent_jobs_overridden = false;

    int positional_index = 0;

    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{
            argv[index]
        };

        if (argument == "--cpu") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--cpu requires a value"
                };
            }

            config.cpu_cores = parse_double(
                argv[++index],
                "cpu"
            );

            cpu_overridden = true;

            continue;
        }

        if (argument == "--memory-mib") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--memory-mib requires a value"
                };
            }

            config.memory_bytes = parse_mib(
                argv[++index],
                "memory-mib"
            );

            continue;
        }

        if (argument == "--disk-mib") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--disk-mib requires a value"
                };
            }

            config.disk_bytes = parse_mib(
                argv[++index],
                "disk-mib"
            );

            continue;
        }

        if (argument == "--gpu") {
            config.gpu_available = true;
            continue;
        }

        if (argument == "--tag") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--tag requires a value"
                };
            }

            if (!tags_overridden) {
                config.tags.clear();
                tags_overridden = true;
            }

            config.tags.emplace_back(
                argv[++index]
            );

            continue;
        }

        if (argument == "--shutdown-grace-ms") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--shutdown-grace-ms requires a value"
                };
            }

            config.shutdown_grace_ms =
                std::chrono::milliseconds{
                    static_cast<std::int64_t>(
                        parse_positive_count(
                            argv[++index],
                            "shutdown-grace-ms"
                        )
                    )
                };

            continue;
        }

        if (argument == "--log-dir") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--log-dir requires a value"
                };
            }

            config.log_dir = argv[++index];

            continue;
        }

        if (argument == "--max-concurrent-jobs") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--max-concurrent-jobs requires a value"
                };
            }

            config.max_concurrent_jobs =
                parse_positive_count(
                    argv[++index],
                    "max-concurrent-jobs"
                );

            max_concurrent_jobs_overridden = true;

            continue;
        }

        if (
            !argument.empty() &&
            argument.front() == '-'
        ) {
            throw std::invalid_argument{
                "Unknown flag: " +
                std::string{argument}
            };
        }

        switch (positional_index) {
            case 0:
                config.worker_id = argument;
                break;

            case 1:
                config.coordinator_address =
                    argument;
                break;

            default:
                throw std::invalid_argument{
                    "Too many positional arguments"
                };
        }

        ++positional_index;
    }

    if (
        config.cpu_cores <= 0.0
    ) {
        throw std::invalid_argument{
            "cpu must be positive"
        };
    }

    /*
     * max_concurrent_jobs has a default derived from cpu_cores at
     * construction time, using the real detected hardware value.
     * If --cpu was overridden but --max-concurrent-jobs was not,
     * recompute the default from the overridden CPU count instead.
     */
    if (cpu_overridden && !max_concurrent_jobs_overridden) {
        config.max_concurrent_jobs =
            default_max_concurrent_jobs(
                config.cpu_cores
            );
    }

    config.log_dir =
        std::filesystem::absolute(
            config.log_dir
        );

    return config;
}

void print_job(const rpc::JobInfo& job) {
    std::cout
        << "Acquired job\n"
        << "  ID: " << job.id() << '\n'
        << "  Name: " << job.name() << '\n'
        << "  Priority: " << job.priority() << '\n'
        << "  CPU: "
        << job.requirements().cpu_cores()
        << " cores\n"
        << "  Memory: "
        << job.requirements().memory_bytes() /
               (1024ULL * 1024ULL)
        << " MiB\n"
        << "  Disk: "
        << job.requirements().disk_bytes() /
               (1024ULL * 1024ULL)
        << " MiB\n";
}

[[nodiscard]]
std::unique_ptr<rpc::WorkerService::Stub>
make_stub(std::string address) {
    const auto channel = grpc::CreateChannel(
        std::move(address),
        grpc::InsecureChannelCredentials()
    );

    return rpc::WorkerService::NewStub(channel);
}

grpc::Status register_worker(
    rpc::WorkerService::Stub& stub,
    const WorkerConfig& config
) {
    rpc::RegisterWorkerRequest request;

    request.set_worker_id(config.worker_id);
    request.set_running_jobs(0);
    request.set_max_concurrent_jobs(
        config.max_concurrent_jobs
    );

    auto* resources =
        request.mutable_resources();

    resources->set_total_cpu_cores(
        config.cpu_cores
    );

    resources->set_available_cpu_cores(
        config.cpu_cores
    );

    resources->set_total_memory_bytes(
        config.memory_bytes
    );

    resources->set_available_memory_bytes(
        config.memory_bytes
    );

    resources->set_total_disk_bytes(
        config.disk_bytes
    );

    resources->set_available_disk_bytes(
        config.disk_bytes
    );

    resources->set_gpu_available(
        config.gpu_available
    );

    for (const auto& tag : config.tags) {
        request.add_tags(tag);
    }

    rpc::RegisterWorkerResponse response;
    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds{5}
    );

    const grpc::Status status =
        stub.RegisterWorker(
            &context,
            request,
            &response
        );

    if (status.ok()) {
        std::cout
            << (
                response.already_registered()
                    ? "Worker already registered: "
                    : "Worker registered: "
            )
            << response.worker_id()
            << '\n';
    }

    return status;
}

grpc::Status acquire_job(
    rpc::WorkerService::Stub& stub,
    const std::string& worker_id,
    rpc::AcquireJobResponse* response
) {
    rpc::AcquireJobRequest request;
    request.set_worker_id(worker_id);

    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds{5}
    );

    return stub.AcquireJob(
        &context,
        request,
        response
    );
}

grpc::Status start_job(
    rpc::WorkerService::Stub& stub,
    const std::string& worker_id,
    const std::string& job_id,
    std::uint64_t attempt_number,
    rpc::StartJobResponse* response
) {
    rpc::StartJobRequest request;

    request.set_worker_id(worker_id);
    request.set_job_id(job_id);
    request.set_attempt_number(attempt_number);

    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds{5}
    );

    return stub.StartJob(
        &context,
        request,
        response
    );
}

/*
 * Must match radahn::persistence::max_job_log_bytes - the
 * coordinator rejects any upload larger than this regardless, but
 * capping here avoids ever reading more of a local log file than
 * could possibly be accepted.
 */
constexpr std::uint64_t max_job_log_bytes =
    1024ULL * 1024ULL;

struct BoundedLogContent {
    std::string content;
    std::uint64_t original_byte_count;
    bool truncated;
};

/*
 * Reads up to max_job_log_bytes from the start of `path`, preserving
 * the true original size separately so truncation is explicit rather
 * than silently lossy. Returns std::nullopt if the file cannot be
 * read at all (e.g. the workload never produced it, such as a failed
 * spawn) - that is not an upload failure, just nothing to upload.
 */
[[nodiscard]]
std::optional<BoundedLogContent> read_bounded_log(
    const std::filesystem::path& path
) {
    std::error_code size_error;

    const auto file_size =
        std::filesystem::file_size(
            path,
            size_error
        );

    if (size_error) {
        return std::nullopt;
    }

    std::ifstream stream{
        path,
        std::ios::binary
    };

    if (!stream) {
        return std::nullopt;
    }

    const auto to_read =
        std::min<std::uint64_t>(
            file_size,
            max_job_log_bytes
        );

    std::string content(
        static_cast<std::size_t>(to_read),
        '\0'
    );

    if (to_read > 0) {
        stream.read(
            content.data(),
            static_cast<std::streamsize>(to_read)
        );
    }

    return BoundedLogContent{
        std::move(content),
        file_size,
        file_size > max_job_log_bytes
    };
}

[[nodiscard]]
grpc::Status upload_job_log_once(
    rpc::WorkerService::Stub& stub,
    const std::string& worker_id,
    const std::string& job_id,
    std::uint64_t attempt_number,
    rpc::LogStream stream,
    const BoundedLogContent& content
) {
    rpc::UploadJobLogRequest request;

    request.set_worker_id(worker_id);
    request.set_job_id(job_id);
    request.set_attempt_number(attempt_number);
    request.set_stream(stream);
    request.set_content(content.content);
    request.set_original_byte_count(content.original_byte_count);
    request.set_truncated(content.truncated);

    rpc::UploadJobLogResponse response;
    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds{5}
    );

    return stub.UploadJobLog(
        &context,
        request,
        &response
    );
}

/*
 * Retries a transient upload failure with bounded exponential
 * backoff over a bounded total interval. A rejection the coordinator
 * considers definitive (the attempt is no longer the accepted one,
 * the content conflicts with what is already stored, or the request
 * itself is malformed) is not retried - none of those become true
 * merely by trying again. Local files are never deleted here, win or
 * lose: an upload failure must not change an already-accepted job
 * result, and the operator can always recover the output from disk.
 */
void upload_job_log_with_retry(
    rpc::WorkerService::Stub& stub,
    const std::string& worker_id,
    const std::string& job_id,
    std::uint64_t attempt_number,
    rpc::LogStream stream,
    const BoundedLogContent& content
) {
    constexpr std::chrono::milliseconds
        upload_deadline_budget{30000};

    const auto deadline =
        std::chrono::steady_clock::now() +
        upload_deadline_budget;

    std::chrono::milliseconds backoff{250};

    constexpr std::chrono::milliseconds
        max_backoff{2000};

    while (true) {
        const auto status =
            upload_job_log_once(
                stub,
                worker_id,
                job_id,
                attempt_number,
                stream,
                content
            );

        if (status.ok()) {
            return;
        }

        const bool definitive =
            status.error_code() ==
                grpc::StatusCode::ALREADY_EXISTS ||
            status.error_code() ==
                grpc::StatusCode::FAILED_PRECONDITION ||
            status.error_code() ==
                grpc::StatusCode::INVALID_ARGUMENT ||
            status.error_code() ==
                grpc::StatusCode::NOT_FOUND;

        if (
            definitive ||
            std::chrono::steady_clock::now() >= deadline
        ) {
            write_log(
                std::cerr,
                "event=log_upload_failed job_id=" + job_id +
                " attempt=" + std::to_string(attempt_number) +
                " worker_id=" + worker_id +
                " reason=" +
                (
                    definitive
                        ? std::string{"rejected"}
                        : std::string{"deadline_exceeded"}
                )
            );

            return;
        }

        std::this_thread::sleep_for(backoff);

        backoff = std::min(
            backoff * 2,
            max_backoff
        );
    }
}

void upload_execution_logs(
    rpc::WorkerService::Stub& stub,
    const radahn::worker_app::ExecutionLogRecord& record
) {
    if (record.stdout_path.has_value()) {
        const auto content =
            read_bounded_log(*record.stdout_path);

        if (content.has_value()) {
            upload_job_log_with_retry(
                stub,
                record.worker_id,
                record.job_id,
                record.attempt_number,
                rpc::LOG_STREAM_STDOUT,
                *content
            );
        }
    }

    if (record.stderr_path.has_value()) {
        const auto content =
            read_bounded_log(*record.stderr_path);

        if (content.has_value()) {
            upload_job_log_with_retry(
                stub,
                record.worker_id,
                record.job_id,
                record.attempt_number,
                rpc::LOG_STREAM_STDERR,
                *content
            );
        }
    }
}

/*
 * The outcome of running one job's workload, together with the local
 * paths (if any) its captured output was written to - absent for a
 * workload that has no subprocess output at all (the internal sleep
 * workload).
 */
struct JobExecutionOutcome {
    radahn::domain::ExecutionResult result;
    std::optional<std::filesystem::path> output_directory;
    std::optional<std::filesystem::path> stdout_path;
    std::optional<std::filesystem::path> stderr_path;
};

[[nodiscard]]
JobExecutionOutcome execute_command_job(
    const rpc::JobInfo& job,
    const radahn::worker_app::AttemptControlPtr& control,
    const std::filesystem::path& log_dir
) {
    const auto workload =
        radahn::rpc_codec::decode_workload(
            job.workload()
        );

    const auto output_directory =
        radahn::worker_app::
            next_execution_output_directory(
                log_dir
            );

    write_log(
        std::cout,
        "Command job " + job.id() +
        " logs: " + output_directory.string()
    );

    const auto result =
        radahn::execution::execute_command(
            workload,
            output_directory,
            radahn::execution::ExecutionControl{
                radahn::execution::StopToken{
                    &control->stop_requested
                }
            }
        );

    std::ostringstream output;

    output
        << "Command job " << job.id();

    if (result.exit_code.has_value()) {
        output
            << " exited with code "
            << *result.exit_code;
    }

    if (result.terminating_signal.has_value()) {
        output
            << " terminated by signal "
            << *result.terminating_signal;
    }

    if (!result.diagnostic.empty()) {
        output
            << ": "
            << result.diagnostic;
    }

    output
        << "; result="
        << (result.succeeded() ? "SUCCEEDED" : "FAILED");

    write_log(
        std::cout,
        output.str()
    );

    if (
        result.outcome ==
        radahn::domain::ExecutionOutcome::timed_out
    ) {
        write_log(
            std::cout,
            "event=timeout job_id=" + job.id() +
            " attempt=" +
            std::to_string(job.attempt_count())
        );
    }

    return JobExecutionOutcome{
        result,
        output_directory,
        output_directory / "stdout.log",
        output_directory / "stderr.log"
    };
}


/*
 * Sleep is an internal workload, not a subprocess, so it has no
 * child process for a stop token to terminate - but it must still
 * follow the same lifecycle as a command job: a lost lease or an
 * explicit coordinator rejection has to interrupt it promptly rather
 * than letting it run to completion regardless.
 */
[[nodiscard]]
JobExecutionOutcome execute_sleep_job(
    std::chrono::milliseconds duration,
    const radahn::worker_app::AttemptControlPtr& control
) {
    constexpr std::chrono::milliseconds
        poll_granularity{50};

    auto remaining = duration;

    while (
        remaining >
        std::chrono::milliseconds::zero()
    ) {
        if (control->stop_was_requested()) {
            return JobExecutionOutcome{
                radahn::domain::ExecutionResult{
                    radahn::domain::ExecutionOutcome::
                        worker_stopped,
                    std::nullopt,
                    std::nullopt,
                    "Worker stopped execution"
                },
                std::nullopt,
                std::nullopt,
                std::nullopt
            };
        }

        const auto this_sleep =
            std::min(
                remaining,
                poll_granularity
            );

        std::this_thread::sleep_for(
            this_sleep
        );

        remaining -= this_sleep;
    }

    // Successful completion is represented as exit code 0.
    return JobExecutionOutcome{
        radahn::domain::ExecutionResult{
            radahn::domain::ExecutionOutcome::exited,
            0,
            std::nullopt,
            {}
        },
        std::nullopt,
        std::nullopt,
        std::nullopt
    };
}

[[nodiscard]]
JobExecutionOutcome execute_job(
    const rpc::JobInfo& job,
    const radahn::worker_app::AttemptControlPtr& control,
    const std::filesystem::path& log_dir
) {
    if (!job.has_workload()) {
        throw std::invalid_argument{
            "Job does not contain a workload"
        };
    }

    if (
        job.workload().kind() ==
        rpc::WORKLOAD_KIND_COMMAND
    ) {
        return execute_command_job(job, control, log_dir);
    }

    const auto workload =
        radahn::rpc_codec::decode_workload(
            job.workload()
        );

    if (
        workload.kind() !=
        radahn::domain::WorkloadKind::sleep
    ) {
        throw std::invalid_argument{
            "Unsupported workload type"
        };
    }

    return execute_sleep_job(
        workload.sleep_duration(),
        control
    );
}

}  // namespace

int main(int argc, char* argv[]) {
    /*
     * stdout is fully buffered (not line-buffered) whenever it's
     * redirected to a file or pipe rather than a TTY, which is
     * exactly the deployed case — logs would sit unflushed for an
     * unbounded time, invisible to anything tailing the worker's
     * log. Force every write to flush immediately instead.
     */
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    WorkerConfig config;

    try {
        config = parse_worker_config(
            argc,
            argv
        );
    } catch (const std::invalid_argument& error) {
        std::cerr
            << error.what()
            << '\n'
            << "Usage: radahn-worker"
            << " [worker-id]"
            << " [coordinator-address]"
            << " [--cpu <cores>]"
            << " [--memory-mib <n>]"
            << " [--disk-mib <n>]"
            << " [--gpu]"
            << " [--tag <tag>]..."
            << " [--max-concurrent-jobs <n>]"
            << " [--shutdown-grace-ms <n>]"
            << " [--log-dir <directory>]\n";

        return 1;
    }

    std::cout
        << "Radahn Worker "
        << radahn::domain::version()
        << '\n'
        << "Worker ID: "
        << config.worker_id
        << '\n'
        << "Coordinator: "
        << config.coordinator_address
        << '\n'
        << "CPU: "
        << config.cpu_cores
        << " cores, Memory: "
        << config.memory_bytes / (1024ULL * 1024ULL)
        << " MiB, Disk: "
        << config.disk_bytes / (1024ULL * 1024ULL)
        << " MiB, GPU: "
        << (config.gpu_available ? "yes" : "no")
        << '\n';

    auto stub = make_stub(
        config.coordinator_address
    );

    const grpc::Status registration_status =
        register_worker(
            *stub,
            config
        );

    if (!registration_status.ok()) {
        std::cerr
            << "Worker registration failed"
            << " (gRPC code "
            << static_cast<int>(
                registration_status.error_code()
            )
            << "): "
            << registration_status.error_message()
            << '\n';

        return 1;
    }

    std::signal(SIGINT, handle_shutdown_signal);
    std::signal(SIGTERM, handle_shutdown_signal);

    radahn::worker_app::ActiveJobRegistry
        active_jobs;

    radahn::worker_app::ExecutionLogRegistry
        execution_logs;

    radahn::worker_app::HeartbeatLoop
        heartbeat_loop{
            config.coordinator_address,
            config.worker_id,
            active_jobs,
            std::chrono::seconds{2}
        };

    const std::uint64_t max_concurrent_jobs =
        config.max_concurrent_jobs;

    std::cout
        << "Worker ready; up to "
        << max_concurrent_jobs
        << " concurrent job(s)\n";

    std::vector<std::future<void>>
        running_jobs;

    while (!g_shutdown_requested.load()) {
        running_jobs.erase(
            std::remove_if(
                running_jobs.begin(),
                running_jobs.end(),
                [](std::future<void>& job_future) {
                    if (
                        job_future.wait_for(
                            std::chrono::seconds::zero()
                        ) !=
                        std::future_status::ready
                    ) {
                        return false;
                    }

                    try {
                        job_future.get();
                    } catch (
                        const std::exception& error
                    ) {
                        std::cerr
                            << "Job execution thread raised: "
                            << error.what()
                            << '\n';
                    }

                    return true;
                }
            ),
            running_jobs.end()
        );

        if (
            running_jobs.size() >=
            max_concurrent_jobs
        ) {
            interruptible_sleep(
                std::chrono::milliseconds{200}
            );

            continue;
        }

        if (!heartbeat_loop.is_connectivity_healthy()) {
            /*
             * Coordinator connectivity has been down long enough
             * that a lease acquired now could already be
             * unrenewable by the time it matters. Pause acquiring
             * new work until a heartbeat succeeds again; in-flight
             * attempts are unaffected - each one's own local
             * deadline independently requests its termination if
             * connectivity never recovers.
             */
            interruptible_sleep(
                std::chrono::seconds{2}
            );

            continue;
        }

        rpc::AcquireJobResponse response;

        const grpc::Status status =
            acquire_job(
                *stub,
                config.worker_id,
                &response
            );

        if (!status.ok()) {
            std::cerr
                << "Job acquisition failed"
                << " (gRPC code "
                << static_cast<int>(
                    status.error_code()
                )
                << "): "
                << status.error_message()
                << '\n';

            interruptible_sleep(
                std::chrono::seconds{2}
            );

            continue;
        }

        if (!response.has_job()) {
            interruptible_sleep(
                std::chrono::seconds{2}
            );

            continue;
        }

        print_job(response.job());

        const std::uint64_t attempt_number =
            response.job().attempt_count();

        if (attempt_number == 0) {
            std::cerr
                << "Coordinator assigned job "
                << response.job().id()
                << " with an invalid attempt number 0;"
                << " will retry acquiring it\n";

            interruptible_sleep(
                std::chrono::seconds{2}
            );

            continue;
        }

        rpc::StartJobResponse start_response;

        /*
         * Captured before the RPC, not after the response arrives -
         * the lease clock on the coordinator started ticking no
         * later than when it received this request, so anchoring
         * the local deadline to the response's arrival time would
         * overestimate remaining authority by the round-trip delay.
         */
        const auto start_send_time =
            std::chrono::steady_clock::now();

        const grpc::Status start_status =
            start_job(
                *stub,
                config.worker_id,
                response.job().id(),
                attempt_number,
                &start_response
            );

        if (!start_status.ok()) {
            std::cerr
                << "Could not start job"
                << " (gRPC code "
                << static_cast<int>(
                    start_status.error_code()
                )
                << "): "
                << start_status.error_message()
                << "; will retry acquiring it\n";

            continue;
        }

        if (
            start_response.job().id() !=
                response.job().id() ||
            start_response.job().attempt_count() !=
                attempt_number
        ) {
            std::cerr
                << "StartJob response did not confirm the "
                << "expected job and attempt; will retry "
                << "acquiring it\n";

            continue;
        }

        std::cout
            << "Job entered RUNNING state\n";

        const std::string job_id =
            response.job().id();

        const rpc::JobInfo job_info =
            start_response.job();

        const auto control =
            active_jobs.add(
                job_id,
                attempt_number
            );

        control->lease_deadline.store(
            start_send_time +
            std::chrono::milliseconds{
                start_response.lease_duration_ms()
            } -
            radahn::worker_app::lease_safety_margin
        );

        try {
            running_jobs.push_back(
                std::async(
                    std::launch::async,
                    [
                        &stub_ref = *stub,
                        &active_jobs,
                        &execution_logs,
                        worker_id = config.worker_id,
                        job_id,
                        attempt_number,
                        job_info,
                        control,
                        log_dir = config.log_dir
                    ]() {

                        JobExecutionOutcome outcome{
                            radahn::domain::ExecutionResult{
                                radahn::domain::ExecutionOutcome::worker_error,
                                std::nullopt,
                                std::nullopt,
                                "Execution did not complete"
                            },
                            std::nullopt,
                            std::nullopt,
                            std::nullopt
                        };

                        try {
                            outcome = execute_job(job_info, control, log_dir);
                        } catch (const std::exception& error) {
                            outcome = JobExecutionOutcome{
                                radahn::domain::ExecutionResult{
                                    radahn::domain::ExecutionOutcome::worker_error,
                                    std::nullopt,
                                    std::nullopt,
                                    error.what()
                                },
                                std::nullopt,
                                std::nullopt,
                                std::nullopt
                            };

                            write_log(
                                std::cerr,
                                "Job " + job_id +
                                " execution error: " + error.what()
                            );
                        }

                        /*
                         * Execution is finished; this attempt
                         * enters its reporting phase. It stays
                         * registered for heartbeat renewal while
                         * its completion is pending, but there is
                         * no child left to stop, and must not be
                         * executed again.
                         */
                        control->phase.store(
                            radahn::worker_app::
                                AttemptPhase::reporting
                        );

                        execution_logs.record(
                            radahn::worker_app::ExecutionLogRecord{
                                job_id,
                                attempt_number,
                                worker_id,
                                outcome.output_directory.value_or(
                                    std::filesystem::path{}
                                ),
                                outcome.stdout_path,
                                outcome.stderr_path,
                                outcome.result
                            }
                        );

                        radahn::worker_app::PendingCompletion
                            pending{
                                radahn::worker_app::CompletionRecord{
                                    worker_id,
                                    job_id,
                                    attempt_number,
                                    outcome.result
                                },
                                radahn::worker_app::
                                    CompletionPhase::reporting
                            };

                        radahn::worker_app::
                            GrpcCompletionTransport
                                transport{stub_ref};

                        const radahn::worker_app::
                            CompletionReporter
                                reporter{transport};

                        const auto report_result =
                            reporter.report(
                                pending.record,
                                [] {
                                    /*
                                     * Deliberately not
                                     * g_shutdown_requested: a
                                     * shutdown signal alone must
                                     * not cut reporting short -
                                     * the bounded drain loop below
                                     * only sets g_abort_reporting
                                     * once its own final reporting
                                     * grace interval elapses.
                                     */
                                    return
                                        !g_abort_reporting
                                            .load();
                                }
                            );

                        switch (report_result) {
                            case radahn::worker_app::
                                CompletionReportResult::
                                    accepted:
                                std::cout
                                    << "Job "
                                    << job_id
                                    << " completed with state "
                                    << (
                                        outcome.result.succeeded()
                                            ? "SUCCEEDED"
                                            : "FAILED"
                                    )
                                    << '\n';

                                write_log(
                                    std::cout,
                                    "event=completion_accepted job_id=" +
                                    job_id + " attempt=" +
                                    std::to_string(attempt_number) +
                                    " worker_id=" + worker_id
                                );

                                {
                                    const auto log_record =
                                        execution_logs.find(
                                            job_id,
                                            attempt_number
                                        );

                                    if (log_record.has_value()) {
                                        upload_execution_logs(
                                            stub_ref,
                                            *log_record
                                        );
                                    }
                                }

                                break;

                            case radahn::worker_app::
                                CompletionReportResult::
                                    rejected:
                                std::cerr
                                    << "Completion for job "
                                    << job_id
                                    << " attempt "
                                    << attempt_number
                                    << " was rejected; this "
                                    << "worker no longer holds "
                                    << "lease authority over it\n";
                                break;

                            case radahn::worker_app::
                                CompletionReportResult::
                                    invalid:
                                std::cerr
                                    << "Completion for job "
                                    << job_id
                                    << " was reported as "
                                    << "malformed; giving up\n";
                                break;

                            case radahn::worker_app::
                                CompletionReportResult::
                                    deadline_exceeded:
                                std::cerr
                                    << "Giving up reporting "
                                    << "completion for job "
                                    << job_id
                                    << " after the reporting "
                                    << "deadline elapsed\n";
                                break;
                        }

                        active_jobs.remove(control);
                    }
                )
            );
        } catch (const std::exception& error) {
            /*
             * std::async failed to launch the execution thread.
             * Remove the registration and let the assignment's
             * lease expire naturally rather than renewing a
             * phantom task forever.
             */
            active_jobs.remove(control);

            std::cerr
                << "Could not launch execution for job "
                << job_id
                << ": "
                << error.what()
                << '\n';
        }
    }

    std::cout
        << "Shutdown requested; draining "
        << running_jobs.size()
        << " in-flight job(s) (grace "
        << config.shutdown_grace_ms.count()
        << "ms)...\n";

    write_log(
        std::cout,
        "event=shutdown_started worker_id=" +
        config.worker_id +
        " in_flight_jobs=" +
        std::to_string(running_jobs.size())
    );

    /*
     * Bounded graceful-shutdown policy:
     *
     * 1. New acquisition already stopped (the loop above exited).
     * 2. Heartbeats and completion reporting continue unaffected
     *    during the grace interval below - a short-running job's
     *    completion still has a real chance to reach the
     *    coordinator.
     * 3. A short-running job simply finishes normally and is
     *    reaped by the poll below before the grace deadline.
     * 4. At the grace deadline, remaining executions are asked to
     *    stop (SIGTERM, escalating to SIGKILL per
     *    ExecutionControl::termination_grace / the sleep poll).
     * 5. Completion reporting then gets one more bounded interval
     *    (final_reporting_grace) before g_abort_reporting cuts it
     *    off.
     * 6. The loop below only returns once every execution task has
     *    actually finished (joined) - std::future's destructor
     *    would otherwise block on it anyway.
     * 7. The heartbeat thread stops in its own destructor as this
     *    function returns.
     */
    constexpr std::chrono::milliseconds
        final_reporting_grace{5000};

    constexpr std::chrono::milliseconds
        drain_poll_interval{100};

    const auto drain_deadline =
        std::chrono::steady_clock::now() +
        config.shutdown_grace_ms;

    bool escalated = false;

    std::chrono::steady_clock::time_point
        reporting_abort_deadline{};

    while (!running_jobs.empty()) {
        running_jobs.erase(
            std::remove_if(
                running_jobs.begin(),
                running_jobs.end(),
                [](std::future<void>& job_future) {
                    if (
                        job_future.wait_for(
                            std::chrono::seconds::zero()
                        ) !=
                        std::future_status::ready
                    ) {
                        return false;
                    }

                    try {
                        job_future.get();
                    } catch (
                        const std::exception& error
                    ) {
                        std::cerr
                            << "Job execution thread raised"
                            << " during drain: "
                            << error.what()
                            << '\n';
                    }

                    return true;
                }
            ),
            running_jobs.end()
        );

        if (running_jobs.empty()) {
            break;
        }

        const auto now =
            std::chrono::steady_clock::now();

        if (!escalated && now >= drain_deadline) {
            std::cout
                << "Shutdown grace period elapsed;"
                << " terminating "
                << running_jobs.size()
                << " remaining execution(s)\n";

            for (
                const auto& attempt :
                active_jobs.snapshot()
            ) {
                if (
                    attempt->phase.load() ==
                    radahn::worker_app::
                        AttemptPhase::executing
                ) {
                    attempt->request_stop();
                }
            }

            escalated = true;

            reporting_abort_deadline =
                now + final_reporting_grace;
        }

        if (
            escalated &&
            now >= reporting_abort_deadline
        ) {
            g_abort_reporting.store(true);
        }

        std::this_thread::sleep_for(
            drain_poll_interval
        );
    }

    std::cout
        << "All in-flight jobs finished;"
        << " shutting down\n";

    write_log(
        std::cout,
        "event=shutdown_completed worker_id=" +
        config.worker_id
    );

    return 0;
}