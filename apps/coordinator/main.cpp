#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <filesystem>

#include <grpcpp/grpcpp.h>

#include "client_service.grpc.pb.h"
#include "worker_service.grpc.pb.h"

#include "radahn/coordinator/in_memory_coordinator.hpp"

#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"
#include "radahn/persistence/sqlite_worker_repository.hpp"

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/version.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"
#include "radahn/domain/workload.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"

namespace {

namespace domain = radahn::domain;
namespace rpc = radahn::rpc::v1;

/*
 * Convert the internal Radahn job state into the
 * corresponding Protocol Buffer job state
 */
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

/*
 * Convert an internal workload into its RPC representation
 */
void fill_workload_info(
    const domain::WorkloadSpec& workload,
    rpc::WorkloadSpec* output
) {
    switch (workload.kind()) {
        case domain::WorkloadKind::sleep:
            output->set_kind(
                rpc::WORKLOAD_KIND_SLEEP
            );

            output->set_sleep_duration_ms(
                static_cast<std::uint64_t>(
                    workload.sleep_duration().count()
                )
            );

            return;
    }

    output->set_kind(
        rpc::WORKLOAD_KIND_UNSPECIFIED
    );
}

/*
 * Fill a protobuf JobInfo message from a domain Job
 *
 * This helper is reused by SubmitJob, GetJob, ListJobs,
 * AcquireJob, StartJob, and FinishJob.
 */
void fill_job_info(
    const domain::Job& job,
    rpc::JobInfo* output
) {
    output->set_id(
        job.id().value()
    );

    output->set_name(
        job.name()
    );

    output->set_priority(
        static_cast<std::int32_t>(
            job.priority()
        )
    );

    output->set_state(
        to_rpc_job_state(
            job.state()
        )
    );

    auto* requirements =
        output->mutable_requirements();

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

    requirements->clear_required_tags();

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

    fill_workload_info(
        job.workload(),
        output->mutable_workload()
    );
}

/*
 * Copy required resource tags from an RPC request
 * into a standard vector
 */
[[nodiscard]] std::vector<std::string>
copy_required_tags(
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

/*
 * Convert an RPC workload into a validated domain workload
 */
[[nodiscard]] domain::WorkloadSpec copy_workload(
    const rpc::WorkloadSpec& workload
) {
    switch (workload.kind()) {
        case rpc::WORKLOAD_KIND_SLEEP: {
            const std::uint64_t duration_ms =
                workload.sleep_duration_ms();

            if (duration_ms == 0) {
                throw std::invalid_argument{
                    "Sleep duration must be positive"
                };
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
                throw std::invalid_argument{
                    "Sleep duration is outside the supported range"
                };
            }

            return domain::WorkloadSpec::sleep(
                std::chrono::milliseconds{
                    static_cast<MillisecondsRep>(
                        duration_ms
                    )
                }
            );
        }

        case rpc::WORKLOAD_KIND_UNSPECIFIED:
        default:
            throw std::invalid_argument{
                "Unsupported or missing workload"
            };
    }
}

/*
 * Copy worker tags from a registration request
 */
[[nodiscard]] std::vector<std::string>
copy_worker_tags(
    const rpc::RegisterWorkerRequest& request
) {
    std::vector<std::string> tags;

    tags.reserve(
        static_cast<std::size_t>(
            request.tags_size()
        )
    );

    for (const auto& tag : request.tags()) {
        tags.push_back(tag);
    }

    return tags;
}

/*
 * Safely convert a protobuf uint64 into size_t
 */
[[nodiscard]] std::size_t checked_size_t(
    std::uint64_t value,
    std::string_view field_name
) {
    if (
        value >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::size_t>::max()
        )
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " is outside the supported range"
        };
    }

    return static_cast<std::size_t>(value);
}

/*
 * One object implements both network-facing services:
 *
 * ClientService:
 *   - Ping
 *   - SubmitJob
 *   - GetJob
 *   - ListJobs
 *
 * WorkerService:
 *   - RegisterWorker
 *   - AcquireJob
 *   - StartJob
 *   - FinishJob
 */
class CoordinatorServiceImpl final
    : public rpc::ClientService::Service,
      public rpc::WorkerService::Service {
public:
    explicit CoordinatorServiceImpl(
        const std::filesystem::path& database_path
    )
        : database_{database_path},
          job_repository_{database_},
          worker_repository_{database_},
          coordinator_{
              policy_,
              job_repository_,
              worker_repository_
        } {
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

            if (!request->has_workload()) {
                return grpc::Status{
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "Job workload is required"
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

            domain::WorkloadSpec workload =
                copy_workload(
                    request->workload()
                );

            domain::Job job{
                job_id,
                request->name(),
                static_cast<int>(
                    request->priority()
                ),
                std::move(requirements),
                std::move(workload)
            };

            {
                const std::lock_guard lock{mutex_};

                if (
                    coordinator_.get_job(
                        job_id
                    ).has_value()
                ) {
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

                job = coordinator_.get_job(
                    job_id
                );
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

    grpc::Status RegisterWorker(
        grpc::ServerContext* context,
        const rpc::RegisterWorkerRequest* request,
        rpc::RegisterWorkerResponse* response
    ) override {
        static_cast<void>(context);

        try {
            if (!request->has_resources()) {
                return grpc::Status{
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "Worker resources are required"
                };
            }

            const domain::WorkerId worker_id{
                request->worker_id()
            };

            const std::size_t running_jobs =
                checked_size_t(
                    request->running_jobs(),
                    "running_jobs"
                );

            const std::size_t max_concurrent_jobs =
                checked_size_t(
                    request->max_concurrent_jobs(),
                    "max_concurrent_jobs"
                );

            const auto& rpc_resources =
                request->resources();

            domain::WorkerResources resources{
                rpc_resources.total_cpu_cores(),
                rpc_resources.available_cpu_cores(),
                rpc_resources.total_memory_bytes(),
                rpc_resources.available_memory_bytes(),
                rpc_resources.total_disk_bytes(),
                rpc_resources.available_disk_bytes(),
                rpc_resources.gpu_available()
            };

            domain::WorkerSnapshot snapshot{
                worker_id,
                domain::WorkerState::online,
                std::move(resources),
                running_jobs,
                max_concurrent_jobs,
                copy_worker_tags(*request)
            };
            
            domain::WorkerRecord worker_record{
                std::move(snapshot)
            };

            bool already_registered = false;

            {
                const std::lock_guard lock{mutex_};

                already_registered =
                    worker_repository_.contains(
                        worker_id
                    );

                if (already_registered) {
                    worker_repository_.update(
                        std::move(worker_record)
                    );
                } else {
                    coordinator_.register_worker(
                        std::move(worker_record)
                    );
                }
            }

            response->set_worker_id(
                worker_id.value()
            );

            response->set_already_registered(
                already_registered
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

    grpc::Status AcquireJob(
        grpc::ServerContext* context,
        const rpc::AcquireJobRequest* request,
        rpc::AcquireJobResponse* response
    ) override {
        static_cast<void>(context);

        try {
            const domain::WorkerId worker_id{
                request->worker_id()
            };

            std::optional<domain::Job> assigned_job;

            {
                const std::lock_guard lock{mutex_};

                if (
                    !coordinator_.worker_snapshot(
                        worker_id
                    ).has_value()
                ) {
                    return grpc::Status{
                        grpc::StatusCode::NOT_FOUND,
                        "Worker is not registered"
                    };
                }

                assigned_job =
                    coordinator_.leased_job_for_worker(
                        worker_id
                    );

                if (!assigned_job.has_value()) {
                    static_cast<void>(
                        coordinator_
                            .dispatch_once_for_worker(
                                worker_id
                            )
                    );

                    assigned_job =
                        coordinator_
                            .leased_job_for_worker(
                                worker_id
                            );
                }
            }

            /*
             * AcquireJobResponse contains only the JobInfo
             * message, empty response = no job
             *
             * Protobuf automatically generates has_job()
             * for the nested message field.
             */
            if (!assigned_job.has_value()) {
                return grpc::Status::OK;
            }

            fill_job_info(
                *assigned_job,
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

    grpc::Status StartJob(
        grpc::ServerContext* context,
        const rpc::StartJobRequest* request,
        rpc::StartJobResponse* response
    ) override {
        static_cast<void>(context);

        try {
            const domain::WorkerId worker_id{
                request->worker_id()
            };

            const domain::JobId job_id{
                request->job_id()
            };

            std::optional<domain::Job> job;

            {
                const std::lock_guard lock{mutex_};

                if (
                    !coordinator_
                        .is_job_assigned_to_worker(
                            job_id,
                            worker_id
                        )
                ) {
                    return grpc::Status{
                        grpc::StatusCode::FAILED_PRECONDITION,
                        "Job is not assigned to this worker"
                    };
                }

                const auto state =
                    coordinator_.job_state(
                        job_id
                    );

                if (
                    state.has_value() &&
                    *state ==
                        domain::JobState::leased
                ) {
                    coordinator_.mark_running(
                        job_id
                    );
                } else if (
                    !state.has_value() ||
                    *state !=
                        domain::JobState::running
                ) {
                    return grpc::Status{
                        grpc::StatusCode::FAILED_PRECONDITION,
                        "Job is not in a startable state"
                    };
                }

                job = coordinator_.get_job(
                    job_id
                );
            }

            if (!job.has_value()) {
                return grpc::Status{
                    grpc::StatusCode::INTERNAL,
                    "Job disappeared after being started"
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

    grpc::Status FinishJob(
        grpc::ServerContext* context,
        const rpc::FinishJobRequest* request,
        rpc::FinishJobResponse* response
    ) override {
        static_cast<void>(context);

        try {
            const domain::WorkerId worker_id{
                request->worker_id()
            };

            const domain::JobId job_id{
                request->job_id()
            };

            std::optional<domain::Job> job;

            {
                const std::lock_guard lock{mutex_};

                if (
                    !coordinator_
                        .is_job_assigned_to_worker(
                            job_id,
                            worker_id
                        )
                ) {
                    return grpc::Status{
                        grpc::StatusCode::FAILED_PRECONDITION,
                        "Job is not assigned to this worker"
                    };
                }

                const auto state =
                    coordinator_.job_state(
                        job_id
                    );

                if (
                    !state.has_value() ||
                    *state !=
                        domain::JobState::running
                ) {
                    return grpc::Status{
                        grpc::StatusCode::FAILED_PRECONDITION,
                        "Job is not running"
                    };
                }

                switch (request->outcome()) {
                    case rpc::JOB_OUTCOME_SUCCEEDED:
                        coordinator_.mark_succeeded(
                            job_id
                        );
                        break;

                    case rpc::JOB_OUTCOME_FAILED:
                        coordinator_.mark_failed(
                            job_id
                        );
                        break;

                    case rpc::JOB_OUTCOME_UNSPECIFIED:
                    default:
                        return grpc::Status{
                            grpc::StatusCode::INVALID_ARGUMENT,
                            "A valid job outcome is required"
                        };
                }

                job = coordinator_.get_job(
                    job_id
                );
            }

            if (!job.has_value()) {
                return grpc::Status{
                    grpc::StatusCode::INTERNAL,
                    "Job disappeared after being finished"
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

private:
    /*
     * Member order matters
     *
     * 1. policy_
     * 2. repositories
     * 3. coordinator_
     *
     * The coordinator stores references to all three
     */
    radahn::scheduler::LeastLoadedPolicy
        policy_;

    radahn::persistence::SqliteDatabase
        database_;

    radahn::persistence::SqliteJobRepository
        job_repository_;

    radahn::persistence::SqliteWorkerRepository
        worker_repository_;

    radahn::coordinator::InMemoryCoordinator
        coordinator_;

    std::mutex mutex_;

};

}  // namespace

int main(
    int argc,
    char* argv[]
) {
    if (argc > 2) {
        std::cerr
            << "Usage: radahn-coordinator"
            << " [database-path]\n";

        return 1;
    }

    /*
     * Running without an argument creates or opens ./radahn.db
     * Tests can pass a temporary database path instead
     */
    std::filesystem::path database_path{
        "radahn.db"
    };

    if (argc == 2) {
        database_path =
            std::filesystem::path{
                argv[1]
            };
    }

    const std::string server_address{
        "0.0.0.0:50051"
    };

    try {
        CoordinatorServiceImpl service{
            database_path
        };

        grpc::ServerBuilder builder;

        builder.AddListeningPort(
            server_address,
            grpc::InsecureServerCredentials()
        );

        builder.RegisterService(
            static_cast<
                rpc::ClientService::Service*
            >(
                &service
            )
        );

        builder.RegisterService(
            static_cast<
                rpc::WorkerService::Service*
            >(
                &service
            )
        );

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
            << '\n'
            << "Database: "
            << database_path.string()
            << '\n';

        server->Wait();

        return 0;
    } catch (const std::exception& error) {
        std::cerr
            << "Coordinator startup failed: "
            << error.what()
            << '\n';

        return 1;
    }
}