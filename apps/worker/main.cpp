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

#include <grpcpp/grpcpp.h>

#include "worker_service.grpc.pb.h"
#include "active_job_registry.hpp"
#include "heartbeat_loop.hpp"
#include "radahn/domain/version.hpp"

namespace {

namespace rpc = radahn::rpc::v1;

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
    rpc::StartJobResponse* response
) {
    rpc::StartJobRequest request;

    request.set_worker_id(worker_id);
    request.set_job_id(job_id);

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

grpc::Status finish_job(
    rpc::WorkerService::Stub& stub,
    const std::string& worker_id,
    const std::string& job_id,
    rpc::JobOutcome outcome,
    rpc::FinishJobResponse* response
) {
    rpc::FinishJobRequest request;

    request.set_worker_id(worker_id);
    request.set_job_id(job_id);
    request.set_outcome(outcome);

    grpc::ClientContext context;

    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds{5}
    );

    return stub.FinishJob(
        &context,
        request,
        response
    );
}

[[nodiscard]]
bool execute_job(
    const rpc::JobInfo& job
) {
    if (!job.has_workload()) {
        std::cerr
            << "Job does not contain a workload\n";

        return false;
    }

    switch (job.workload().kind()) {
        case rpc::WORKLOAD_KIND_SLEEP: {
            const std::uint64_t duration_ms =
                job.workload().sleep_duration_ms();

            if (duration_ms == 0) {
                std::cerr
                    << "Sleep duration must be positive\n";

                return false;
            }

            using MillisecondsRep =
                std::chrono::milliseconds::rep;

            const auto maximum_duration =
                static_cast<std::uint64_t>(
                    std::numeric_limits<
                        MillisecondsRep
                    >::max()
                );

            if (duration_ms > maximum_duration) {
                std::cerr
                    << "Sleep duration is too large\n";

                return false;
            }

            std::cout
                << "Executing sleep workload for "
                << duration_ms
                << " ms\n";

            std::this_thread::sleep_for(
                std::chrono::milliseconds{
                    static_cast<MillisecondsRep>(
                        duration_ms
                    )
                }
            );

            return true;
        }

        case rpc::WORKLOAD_KIND_UNSPECIFIED:
        default:
            std::cerr
                << "Unsupported workload type\n";

            return false;
    }
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
            << " [--max-concurrent-jobs <n>]\n";

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

        rpc::StartJobResponse start_response;

        const grpc::Status start_status =
            start_job(
                *stub,
                config.worker_id,
                response.job().id(),
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

        std::cout
            << "Job entered RUNNING state\n";

        const std::string job_id =
            response.job().id();

        const rpc::JobInfo job_info =
            response.job();

        active_jobs.add(job_id);

        running_jobs.push_back(
            std::async(
                std::launch::async,
                [
                    &stub_ref = *stub,
                    &active_jobs,
                    worker_id = config.worker_id,
                    job_id,
                    job_info
                ]() {
                    const bool succeeded =
                        execute_job(job_info);

                    rpc::FinishJobResponse
                        finish_response;

                    const grpc::Status
                        finish_status =
                            finish_job(
                                stub_ref,
                                worker_id,
                                job_id,
                                succeeded
                                    ? rpc::JOB_OUTCOME_SUCCEEDED
                                    : rpc::JOB_OUTCOME_FAILED,
                                &finish_response
                            );

                    if (!finish_status.ok()) {
                        std::cerr
                            << "Could not finish job "
                            << job_id
                            << " (gRPC code "
                            << static_cast<int>(
                                finish_status
                                    .error_code()
                            )
                            << "): "
                            << finish_status
                                   .error_message()
                            << '\n';
                    } else {
                        std::cout
                            << "Job "
                            << job_id
                            << " completed with state "
                            << (
                                succeeded
                                    ? "SUCCEEDED"
                                    : "FAILED"
                            )
                            << '\n';
                    }

                    active_jobs.remove(job_id);
                }
            )
        );
    }

    std::cout
        << "Shutdown requested; draining "
        << running_jobs.size()
        << " in-flight job(s)...\n";

    for (auto& job_future : running_jobs) {
        try {
            job_future.get();
        } catch (const std::exception& error) {
            std::cerr
                << "Job execution thread raised"
                << " during drain: "
                << error.what()
                << '\n';
        }
    }

    std::cout
        << "All in-flight jobs finished;"
        << " shutting down\n";

    return 0;
}