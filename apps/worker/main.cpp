#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "worker_service.grpc.pb.h"
#include "radahn/domain/version.hpp"

namespace {

namespace rpc = radahn::rpc::v1;

constexpr std::uint64_t gibibyte =
    1024ULL * 1024ULL * 1024ULL;

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
    const std::string& worker_id
) {
    const unsigned int detected_cpu_count =
        std::max(
            1U,
            std::thread::hardware_concurrency()
        );

    const double cpu_cores =
        static_cast<double>(
            detected_cpu_count
        );

    const std::uint64_t configured_memory =
        8ULL * gibibyte;

    const std::uint64_t configured_disk =
        50ULL * gibibyte;

    const std::uint64_t max_concurrent_jobs =
        std::min<std::uint64_t>(
            detected_cpu_count,
            4ULL
        );

    rpc::RegisterWorkerRequest request;

    request.set_worker_id(worker_id);
    request.set_running_jobs(0);
    request.set_max_concurrent_jobs(
        max_concurrent_jobs
    );

    auto* resources =
        request.mutable_resources();

    resources->set_total_cpu_cores(cpu_cores);
    resources->set_available_cpu_cores(cpu_cores);

    resources->set_total_memory_bytes(
        configured_memory
    );

    resources->set_available_memory_bytes(
        configured_memory
    );

    resources->set_total_disk_bytes(
        configured_disk
    );

    resources->set_available_disk_bytes(
        configured_disk
    );

    resources->set_gpu_available(false);

    for (const auto& tag : default_worker_tags()) {
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

}  // namespace

int main(int argc, char* argv[]) {
    std::string worker_id{"local-worker"};
    std::string coordinator_address{
        "localhost:50051"
    };

    if (argc >= 2) {
        worker_id = argv[1];
    }

    if (argc >= 3) {
        coordinator_address = argv[2];
    }

    if (argc > 3) {
        std::cerr
            << "Usage: radahn-worker"
            << " [worker-id]"
            << " [coordinator-address]\n";

        return 1;
    }

    std::cout
        << "Radahn Worker "
        << radahn::domain::version()
        << '\n'
        << "Worker ID: "
        << worker_id
        << '\n'
        << "Coordinator: "
        << coordinator_address
        << '\n';

    auto stub = make_stub(
        coordinator_address
    );

    const grpc::Status registration_status =
        register_worker(
            *stub,
            worker_id
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

    while (true) {
        rpc::AcquireJobResponse response;

        const grpc::Status status =
            acquire_job(
                *stub,
                worker_id,
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

            return 1;
        }

        if (response.has_job()) {
            print_job(response.job());
            break;
        }

        std::cout
            << "No eligible job available; retrying...\n";

        std::this_thread::sleep_for(
            std::chrono::seconds{2}
        );
    }

    return 0;
}