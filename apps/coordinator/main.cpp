#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "client_service.grpc.pb.h"
#include "radahn/coordinator/in_memory_coordinator.hpp"
#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/version.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"

namespace {

namespace domain = radahn::domain;
namespace rpc = radahn::rpc::v1;

[[nodiscard]] rpc::JobState to_rpc_job_state(
    domain::JobState state
) noexcept {
    switch (state) {
        case domain::JobState::queued:
            return rpc::JOB_STATE_QUEUED;

        case domain::JobState::leased:
            return rpc::JOB_STATE_LEASED;

        case domain::JobState::running:
            return rpc::JOB_STATE_RUNNING;

        case domain::JobState::retry_wait:
            return rpc::JOB_STATE_RETRY_WAIT;

        case domain::JobState::cancellation_requested:
            return rpc::JOB_STATE_CANCELLATION_REQUESTED;

        case domain::JobState::succeeded:
            return rpc::JOB_STATE_SUCCEEDED;

        case domain::JobState::failed:
            return rpc::JOB_STATE_FAILED;

        case domain::JobState::cancelled:
            return rpc::JOB_STATE_CANCELLED;
    }

    return rpc::JOB_STATE_UNSPECIFIED;
}

void fill_job_info(
    const domain::Job& job,
    rpc::JobInfo* output
) {
    output->set_id(job.id().value());
    output->set_name(job.name());

    output->set_priority(
        static_cast<std::int32_t>(job.priority())
    );

    output->set_state(
        to_rpc_job_state(job.state())
    );

    auto* requirements = output->mutable_requirements();

    requirements->set_cpu_cores(
        job.requirements().cpu_cores()
    );

    requirements->set_memory_bytes(
        job.requirements().memory_bytes()
    );

    requirements->set_disk_bytes(
        job.requirements().disk_bytes()
    );

    requirements->set_requires_gpu(
        job.requirements().requires_gpu()
    );

    for (const auto& tag :
         job.requirements().required_tags()) {
        requirements->add_required_tags(tag);
    }

    const auto unix_milliseconds =
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(
            job.created_at().time_since_epoch()
        ).count();

    output->set_created_at_unix_ms(
        static_cast<std::int64_t>(
            unix_milliseconds
        )
    );
}

[[nodiscard]]
std::vector<std::string> copy_required_tags(
    const rpc::ResourceRequirements& requirements
) {
    std::vector<std::string> tags;

    tags.reserve(
        static_cast<std::size_t>(
            requirements.required_tags_size()
        )
    );

    for (const auto& tag :
         requirements.required_tags()) {
        tags.push_back(tag);
    }

    return tags;
}

class ClientServiceImpl final
    : public rpc::ClientService::Service {
public:
    ClientServiceImpl()
        : coordinator_{policy_} {
    }

    grpc::Status Ping(
        grpc::ServerContext* context,
        const rpc::PingRequest* request,
        rpc::PingResponse* response
    ) override {
        static_cast<void>(context);

        response->set_message(
            "pong: " + request->message()
        );

        response->set_coordinator_version(
            std::string{domain::version()}
        );

        return grpc::Status::OK;
    }

    grpc::Status SubmitJob(
        grpc::ServerContext* context,
        const rpc::SubmitJobRequest* request,
        rpc::SubmitJobResponse* response
    ) override {
        static_cast<void>(context);

        try {
            if (!request->has_requirements()) {
                return grpc::Status{
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "Job requirements are required"
                };
            }

            const domain::JobId job_id{
                request->id()
            };

            domain::ResourceRequirements requirements{
                request->requirements().cpu_cores(),
                request->requirements().memory_bytes(),
                request->requirements().disk_bytes(),
                request->requirements().requires_gpu(),
                copy_required_tags(
                    request->requirements()
                )
            };

            domain::Job job{
                job_id,
                request->name(),
                static_cast<int>(request->priority()),
                std::move(requirements)
            };

            {
                const std::lock_guard lock{mutex_};

                if (coordinator_.get_job(job_id).has_value()) {
                    return grpc::Status{
                        grpc::StatusCode::ALREADY_EXISTS,
                        "A job with this ID already exists"
                    };
                }

                coordinator_.submit_job(job);
            }

            fill_job_info(
                job,
                response->mutable_job()
            );

            return grpc::Status::OK;
        } catch (const std::invalid_argument& error) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                error.what()
            };
        } catch (const std::exception& error) {
            return grpc::Status{
                grpc::StatusCode::INTERNAL,
                error.what()
            };
        }
    }

    grpc::Status GetJob(
        grpc::ServerContext* context,
        const rpc::GetJobRequest* request,
        rpc::GetJobResponse* response
    ) override {
        static_cast<void>(context);

        try {
            const domain::JobId job_id{
                request->id()
            };

            std::optional<domain::Job> job;

            {
                const std::lock_guard lock{mutex_};

                job = coordinator_.get_job(job_id);
            }

            if (!job.has_value()) {
                return grpc::Status{
                    grpc::StatusCode::NOT_FOUND,
                    "Job was not found"
                };
            }

            fill_job_info(
                *job,
                response->mutable_job()
            );

            return grpc::Status::OK;
        } catch (const std::invalid_argument& error) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                error.what()
            };
        } catch (const std::exception& error) {
            return grpc::Status{
                grpc::StatusCode::INTERNAL,
                error.what()
            };
        }
    }

    grpc::Status ListJobs(
        grpc::ServerContext* context,
        const rpc::ListJobsRequest* request,
        rpc::ListJobsResponse* response
    ) override {
        static_cast<void>(context);
        static_cast<void>(request);

        try {
            std::vector<domain::Job> jobs;

            {
                const std::lock_guard lock{mutex_};

                jobs = coordinator_.list_jobs();
            }

            for (const auto& job : jobs) {
                fill_job_info(
                    job,
                    response->add_jobs()
                );
            }

            return grpc::Status::OK;
        } catch (const std::exception& error) {
            return grpc::Status{
                grpc::StatusCode::INTERNAL,
                error.what()
            };
        }
    }

private:
    // policy_ must be declared before coordinator_ because
    // coordinator_ stores a reference to it.
    radahn::scheduler::LeastLoadedPolicy policy_;

    radahn::coordinator::InMemoryCoordinator
        coordinator_;

    std::mutex mutex_;
};

}  // namespace

int main() {
    const std::string server_address{
        "0.0.0.0:50051"
    };

    ClientServiceImpl service;

    grpc::ServerBuilder builder;

    builder.AddListeningPort(
        server_address,
        grpc::InsecureServerCredentials()
    );

    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server{
        builder.BuildAndStart()
    };

    if (!server) {
        std::cerr
            << "Failed to start Radahn Coordinator\n";

        return 1;
    }

    std::cout
        << "Radahn Coordinator "
        << domain::version()
        << " listening on "
        << server_address
        << '\n';

    server->Wait();

    return 0;
}