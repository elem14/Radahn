#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
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

void print_usage() {
    std::cout
        << "Radahn distributed computing platform\n\n"
        << "Usage:\n"
        << "  radahn version\n"
        << "  radahn ping [message]\n"
        << "  radahn job submit <id> <name> <priority>"
        << " <cpu> <memory-mib> <disk-mib>"
        << " [--gpu] [--tag <tag>]...\n"
        << "  radahn job get <id>\n"
        << "  radahn job list\n"
        << "  radahn help\n";
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

        if (value <
                std::numeric_limits<int>::min() ||
            value >
                std::numeric_limits<int>::max()) {
            throw std::out_of_range{
                "Integer is outside supported range"
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
    if (text.empty() || text.front() == '-') {
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

        if (value >
            std::numeric_limits<std::uint64_t>::max() /
                mebibyte) {
            throw std::out_of_range{
                "MiB value is too large"
            };
        }

        return static_cast<std::uint64_t>(value) *
               mebibyte;
    } catch (const std::exception&) {
        throw std::invalid_argument{
            std::string{field_name} +
            " must be a valid nonnegative integer"
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

void print_job(const rpc::JobInfo& job) {
    std::cout
        << "Job: " << job.id() << '\n'
        << "  Name: " << job.name() << '\n'
        << "  State: " << state_name(job.state()) << '\n'
        << "  Priority: " << job.priority() << '\n'
        << "  CPU cores: "
        << job.requirements().cpu_cores()
        << '\n'
        << "  Memory: "
        << job.requirements().memory_bytes() /
               mebibyte
        << " MiB\n"
        << "  Disk: "
        << job.requirements().disk_bytes() /
               mebibyte
        << " MiB\n"
        << "  GPU required: "
        << (
            job.requirements().requires_gpu()
                ? "yes"
                : "no"
        )
        << '\n'
        << "  Tags:";

    if (job.requirements().required_tags().empty()) {
        std::cout << " none\n";
        return;
    }

    for (const auto& tag :
         job.requirements().required_tags()) {
        std::cout << ' ' << tag;
    }

    std::cout << '\n';
}

[[nodiscard]]
std::unique_ptr<rpc::ClientService::Stub>
make_stub() {
    const auto channel = grpc::CreateChannel(
        "localhost:50051",
        grpc::InsecureChannelCredentials()
    );

    return rpc::ClientService::NewStub(channel);
}

int run_ping(
    rpc::ClientService::Stub& stub,
    std::string message
) {
    rpc::PingRequest request;
    request.set_message(std::move(message));

    rpc::PingResponse response;
    grpc::ClientContext context;

    const grpc::Status status = stub.Ping(
        &context,
        request,
        &response
    );

    if (!status.ok()) {
        std::cerr
            << "Ping failed: "
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
        const std::string id{argv[3]};
        const std::string name{argv[4]};

        const int priority = parse_int(
            argv[5],
            "priority"
        );

        const double cpu_cores = parse_double(
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
        std::vector<std::string> tags;

        for (int index = 9;
             index < argc;
             ++index) {
            const std::string_view argument{
                argv[index]
            };

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

                tags.emplace_back(argv[++index]);
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
            static_cast<std::int32_t>(priority)
        );

        auto* requirements =
            request.mutable_requirements();

        requirements->set_cpu_cores(cpu_cores);
        requirements->set_memory_bytes(memory_bytes);
        requirements->set_disk_bytes(disk_bytes);
        requirements->set_requires_gpu(requires_gpu);

        for (const auto& tag : tags) {
            requirements->add_required_tags(tag);
        }

        rpc::SubmitJobResponse response;
        grpc::ClientContext context;

        const grpc::Status status = stub.SubmitJob(
            &context,
            request,
            &response
        );

        if (!status.ok()) {
            std::cerr
                << "Job submission failed: "
                << " (gRPC code "
                << static_cast<int>(status.error_code())
                << "): "
                << status.error_message()
                << '\n';

            return 1;
        }

        std::cout << "Job submitted successfully\n";
        print_job(response.job());

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
    request.set_id(std::move(id));

    rpc::GetJobResponse response;
    grpc::ClientContext context;

    const grpc::Status status = stub.GetJob(
        &context,
        request,
        &response
    );

    if (!status.ok()) {
        std::cerr
            << "Get job failed: "
            << status.error_message()
            << '\n';

        return 1;
    }

    print_job(response.job());
    return 0;
}

int run_list_jobs(
    rpc::ClientService::Stub& stub
) {
    rpc::ListJobsRequest request;
    rpc::ListJobsResponse response;
    grpc::ClientContext context;

    const grpc::Status status = stub.ListJobs(
        &context,
        request,
        &response
    );

    if (!status.ok()) {
        std::cerr
            << "List jobs failed: "
            << status.error_message()
            << '\n';

        return 1;
    }

    if (response.jobs().empty()) {
        std::cout << "No jobs found\n";
        return 0;
    }

    for (const auto& job : response.jobs()) {
        std::cout
            << job.id()
            << " | "
            << state_name(job.state())
            << " | priority "
            << job.priority()
            << " | "
            << job.name()
            << '\n';
    }

    return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 0;
    }

    const std::string_view command{argv[1]};

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

    auto stub = make_stub();

    if (command == "ping") {
        std::string message{"hello"};

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

            return run_list_jobs(*stub);
        }
    }

    std::cerr
        << "Unknown command\n";

    print_usage();
    return 1;
}

