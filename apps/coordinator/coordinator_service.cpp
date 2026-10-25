#include "coordinator_service.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "workload_codec.hpp"

#include "radahn/coordinator/event_log.hpp"
#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/version.hpp"
#include "radahn/domain/worker.hpp"
#include "radahn/domain/worker_record.hpp"
#include "radahn/domain/workload.hpp"

namespace radahn::coordinator_app {

namespace domain = radahn::domain;

namespace {

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
    radahn::rpc_codec::encode_workload(
        workload,
        *output
    );
}

/*
 * JobRecord-level information fill_job_info cannot read off a
 * domain::Job alone - gathered under the coordinator mutex by
 * CoordinatorServiceImpl::job_info_extras_locked before formatting.
 */
struct JobInfoExtras {
    std::optional<domain::WorkerId> assigned_worker_id;
    bool stdout_log_available = false;
    bool stderr_log_available = false;
};

/*
 * Fill a protobuf JobInfo message from a domain Job
 *
 * This helper is reused by SubmitJob, GetJob, ListJobs,
 * AcquireJob, StartJob, and FinishJob.
 */
void fill_job_info(
    const domain::Job& job,
    const JobInfoExtras& extras,
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

    output->set_attempt_count(
        static_cast<std::uint64_t>(
            job.attempt_count()
        )
    );

    output->set_max_attempts(
        static_cast<std::uint64_t>(
            job.max_attempts()
        )
    );

    output->clear_execution_result();

    if (job.execution_result().has_value()) {
        radahn::rpc_codec::encode_execution_result(
            *job.execution_result(),
            *output->mutable_execution_result()
        );
    }

    if (extras.assigned_worker_id.has_value()) {
        output->set_assigned_worker_id(
            extras.assigned_worker_id->value()
        );
    } else {
        output->clear_assigned_worker_id();
    }

    output->set_stdout_log_available(
        extras.stdout_log_available
    );

    output->set_stderr_log_available(
        extras.stderr_log_available
    );
}

/*
 * Fill a protobuf WorkerInfo message from a worker snapshot and its
 * last known heartbeat.
 */
void fill_worker_info(
    const domain::WorkerSnapshot& snapshot,
    const std::optional<
        radahn::persistence::WorkerHeartbeatTimePoint
    >& last_heartbeat,
    rpc::WorkerInfo* output
) {
    output->set_worker_id(
        snapshot.id().value()
    );

    output->set_online(
        snapshot.state() ==
        domain::WorkerState::online
    );

    output->set_last_heartbeat_unix_ms(
        last_heartbeat.has_value()
            ? std::chrono::duration_cast<
                  std::chrono::milliseconds
              >(
                  last_heartbeat->time_since_epoch()
              ).count()
            : 0
    );

    const auto& resources = snapshot.resources();

    output->set_total_cpu_cores(
        resources.total_cpu_cores()
    );

    output->set_allocated_cpu_cores(
        resources.total_cpu_cores() -
        resources.available_cpu_cores()
    );

    output->set_total_memory_bytes(
        resources.total_memory_bytes()
    );

    output->set_allocated_memory_bytes(
        resources.total_memory_bytes() -
        resources.available_memory_bytes()
    );

    output->set_total_disk_bytes(
        resources.total_disk_bytes()
    );

    output->set_allocated_disk_bytes(
        resources.total_disk_bytes() -
        resources.available_disk_bytes()
    );

    output->set_running_jobs(
        static_cast<std::uint64_t>(
            snapshot.running_jobs()
        )
    );

    output->set_max_concurrent_jobs(
        static_cast<std::uint64_t>(
            snapshot.max_concurrent_jobs()
        )
    );

    output->set_gpu_available(
        resources.gpu_available()
    );

    output->clear_tags();

    for (const auto& tag : snapshot.tags()) {
        output->add_tags(tag);
    }
}

/*
 * Gathers JobRecord-level extras (assigned worker, log availability)
 * for `job`. Callers must already hold the coordinator mutex -
 * formatting the gathered snapshot into a response happens outside
 * the lock, at each call site.
 */
[[nodiscard]] JobInfoExtras job_info_extras(
    const radahn::coordinator::InMemoryCoordinator& coordinator,
    const radahn::persistence::IJobLogRepository& job_log_repository,
    const domain::Job& job
) {
    JobInfoExtras extras;

    extras.assigned_worker_id =
        coordinator.assigned_worker(
            job.id()
        );

    extras.stdout_log_available =
        job_log_repository.contains(
            job.id(),
            job.attempt_count(),
            radahn::persistence::JobLogStream::standard_output
        );

    extras.stderr_log_available =
        job_log_repository.contains(
            job.id(),
            job.attempt_count(),
            radahn::persistence::JobLogStream::standard_error
        );

    return extras;
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
    return radahn::rpc_codec::decode_workload(
        workload
    );
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

[[nodiscard]] bool is_terminal_job_state(
    domain::JobState state
) noexcept {
    return
        state == domain::JobState::succeeded ||
        state == domain::JobState::failed ||
        state == domain::JobState::cancelled;
}

[[nodiscard]] radahn::persistence::JobLogStream
to_persistence_log_stream(
    rpc::LogStream stream
) {
    switch (stream) {
        case rpc::LOG_STREAM_STDOUT:
            return radahn::persistence::
                JobLogStream::standard_output;

        case rpc::LOG_STREAM_STDERR:
            return radahn::persistence::
                JobLogStream::standard_error;

        case rpc::LOG_STREAM_UNSPECIFIED:
        default:
            throw std::invalid_argument{
                "A valid log stream is required"
            };
    }
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

}  // namespace

CoordinatorServiceImpl::CoordinatorServiceImpl(
    const std::filesystem::path& database_path
)
    : database_{database_path},
    job_repository_{database_},
    worker_repository_{database_},
    job_log_repository_{database_},
    transaction_runner_{database_},
    coordinator_{
        policy_,
        job_repository_,
        worker_repository_,
        std::chrono::seconds{15},
        &transaction_runner_
    },
    liveness_thread_{
        [this] {
            run_liveness_monitor();
        }
    } {
}

CoordinatorServiceImpl::~CoordinatorServiceImpl() {
    radahn::coordinator::log_event("shutdown_started");

    stop_liveness_monitor_.store(
        true
    );

    liveness_wait_condition_.notify_all();

    if (liveness_thread_.joinable()) {
        liveness_thread_.join();
    }

    radahn::coordinator::log_event("shutdown_completed");
}

grpc::Status CoordinatorServiceImpl::Ping(
    grpc::ServerContext* context,
    const rpc::PingRequest* request,
    rpc::PingResponse* response
) {
    static_cast<void>(context);

    response->set_message(
        "pong: " + request->message()
    );

    response->set_coordinator_version(
        std::string{domain::version()}
    );

    return grpc::Status::OK;
}

grpc::Status CoordinatorServiceImpl::SubmitJob(
    grpc::ServerContext* context,
    const rpc::SubmitJobRequest* request,
    rpc::SubmitJobResponse* response
) {
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

        std::size_t max_attempts =
            domain::Job::default_max_attempts;

        if (request->max_attempts() != 0) {
            max_attempts =
                checked_size_t(
                    request->max_attempts(),
                    "max_attempts"
                );

            if (max_attempts == 0) {
                return grpc::Status{
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "max_attempts must be positive"
                };
            }
        }

        domain::Job job{
            job_id,
            request->name(),
            static_cast<int>(
                request->priority()
            ),
            std::move(requirements),
            std::move(workload),
            max_attempts
        };

        JobInfoExtras extras;

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

            extras = job_info_extras(
                coordinator_,
                job_log_repository_,
                job
            );
        }

        radahn::coordinator::log_event(
            "job_submitted",
            {
                {"job_id", job_id.value()}
            }
        );

        fill_job_info(
            job,
            extras,
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

grpc::Status CoordinatorServiceImpl::GetJob(
    grpc::ServerContext* context,
    const rpc::GetJobRequest* request,
    rpc::GetJobResponse* response
) {
    static_cast<void>(context);

    try {
        const domain::JobId job_id{
            request->id()
        };

        std::optional<domain::Job> job;
        JobInfoExtras extras;

        {
            const std::lock_guard lock{mutex_};

            job = coordinator_.get_job(
                job_id
            );

            if (job.has_value()) {
                extras = job_info_extras(
                    coordinator_,
                    job_log_repository_,
                    *job
                );
            }
        }

        if (!job.has_value()) {
            return grpc::Status{
                grpc::StatusCode::NOT_FOUND,
                "Job was not found"
            };
        }

        fill_job_info(
            *job,
            extras,
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

grpc::Status CoordinatorServiceImpl::ListJobs(
    grpc::ServerContext* context,
    const rpc::ListJobsRequest* request,
    rpc::ListJobsResponse* response
) {
    static_cast<void>(context);
    static_cast<void>(request);

    try {
        std::vector<domain::Job> jobs;
        std::vector<JobInfoExtras> extras;

        {
            const std::lock_guard lock{mutex_};

            jobs = coordinator_.list_jobs();

            extras.reserve(jobs.size());

            for (const auto& job : jobs) {
                extras.push_back(
                    job_info_extras(
                        coordinator_,
                        job_log_repository_,
                        job
                    )
                );
            }
        }

        for (std::size_t index = 0; index < jobs.size(); ++index) {
            fill_job_info(
                jobs[index],
                extras[index],
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

grpc::Status CoordinatorServiceImpl::GetJobLog(
    grpc::ServerContext* context,
    const rpc::GetJobLogRequest* request,
    rpc::GetJobLogResponse* response
) {
    static_cast<void>(context);

    try {
        const domain::JobId job_id{
            request->id()
        };

        const auto stream =
            to_persistence_log_stream(
                request->stream() ==
                    rpc::LOG_STREAM_UNSPECIFIED
                    ? rpc::LOG_STREAM_STDOUT
                    : request->stream()
            );

        std::optional<domain::Job> job;
        std::optional<radahn::persistence::JobLogRecord> log;

        {
            const std::lock_guard lock{mutex_};

            job = coordinator_.get_job(job_id);

            if (!job.has_value()) {
                return grpc::Status{
                    grpc::StatusCode::NOT_FOUND,
                    "Job was not found"
                };
            }

            const std::uint64_t attempt_number =
                request->attempt_number() == 0
                    ? static_cast<std::uint64_t>(
                          job->attempt_count()
                      )
                    : request->attempt_number();

            if (attempt_number == 0) {
                return grpc::Status{
                    grpc::StatusCode::UNAVAILABLE,
                    "Logs have not been uploaded for this job "
                    "(it has not been attempted yet)"
                };
            }

            log = job_log_repository_.get(
                job_id,
                attempt_number,
                stream
            );

            if (!log.has_value()) {
                return grpc::Status{
                    grpc::StatusCode::UNAVAILABLE,
                    "Logs have not been uploaded for this "
                    "attempt and stream"
                };
            }
        }

        response->set_content(log->content);

        response->set_attempt_number(
            log->attempt_number
        );

        response->set_original_byte_count(
            log->original_byte_count
        );

        response->set_truncated(
            log->truncated
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

grpc::Status CoordinatorServiceImpl::RegisterWorker(
    grpc::ServerContext* context,
    const rpc::RegisterWorkerRequest* request,
    rpc::RegisterWorkerResponse* response
) {
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

            coordinator_.record_worker_heartbeat(
                worker_id,
                radahn::persistence::WorkerHeartbeatClock::now()
            );
        }

        radahn::coordinator::log_event(
            "worker_registered",
            {
                {"worker_id", worker_id.value()},
                {
                    "already_registered",
                    already_registered
                        ? std::string{"true"}
                        : std::string{"false"}
                }
            }
        );

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

grpc::Status CoordinatorServiceImpl::Heartbeat(
    grpc::ServerContext*,
    const rpc::HeartbeatRequest* request,
    rpc::HeartbeatResponse* response
) {
    if (request->worker_id().empty()) {
        return grpc::Status{
            grpc::StatusCode::INVALID_ARGUMENT,
            "Worker ID cannot be empty"
        };
    }

    try {
        const domain::WorkerId worker_id{
            request->worker_id()
        };

        const auto heartbeat_time =
            radahn::persistence::
                WorkerHeartbeatClock::now();

        std::vector<radahn::coordinator::JobAttempt>
            requested_attempts;

        requested_attempts.reserve(
            static_cast<std::size_t>(
                request->active_attempts_size()
            )
        );

        for (
            const auto& attempt :
            request->active_attempts()
        ) {
            if (
                attempt.job_id().empty() ||
                attempt.attempt_number() == 0
            ) {
                return grpc::Status{
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "Heartbeat attempt entries require a job "
                    "ID and a positive attempt number"
                };
            }

            const radahn::coordinator::JobAttempt
                candidate{
                    domain::JobId{attempt.job_id()},
                    attempt.attempt_number()
                };

            bool already_present = false;

            for (
                const auto& existing :
                requested_attempts
            ) {
                if (
                    existing.job_id ==
                        candidate.job_id &&
                    existing.attempt_number ==
                        candidate.attempt_number
                ) {
                    already_present = true;
                    break;
                }
            }

            if (!already_present) {
                requested_attempts.push_back(
                    candidate
                );
            }
        }

        std::vector<radahn::coordinator::JobAttempt>
            renewed_attempts;

        {
            const std::lock_guard lock{
                mutex_
            };

            if (
                !worker_repository_.contains(
                    worker_id
                )
            ) {
                return grpc::Status{
                    grpc::StatusCode::NOT_FOUND,
                    "Worker is not registered"
                };
            }

            coordinator_.record_worker_heartbeat(
                worker_id,
                heartbeat_time
            );

            renewed_attempts =
                coordinator_
                    .renew_job_attempt_leases_detailed(
                        worker_id,
                        requested_attempts,
                        heartbeat_time
                    );
        }

        const auto coordinator_time_unix_ms =
            std::chrono::duration_cast<
                std::chrono::milliseconds
            >(
                heartbeat_time.time_since_epoch()
            ).count();

        response->set_coordinator_time_unix_ms(
            coordinator_time_unix_ms
        );

        for (const auto& attempt : renewed_attempts) {
            auto* renewed =
                response->add_renewed_attempts();

            renewed->set_job_id(
                attempt.job_id.value()
            );

            renewed->set_attempt_number(
                attempt.attempt_number
            );
        }

        response->set_lease_duration_ms(
            static_cast<std::uint64_t>(
                coordinator_.lease_duration().count()
            )
        );

        return grpc::Status::OK;
    } catch (
        const std::invalid_argument& error
    ) {
        return grpc::Status{
            grpc::StatusCode::INVALID_ARGUMENT,
            error.what()
        };
    } catch (
        const std::exception& error
    ) {
        return grpc::Status{
            grpc::StatusCode::INTERNAL,
            error.what()
        };
    }
}

grpc::Status CoordinatorServiceImpl::AcquireJob(
    grpc::ServerContext* context,
    const rpc::AcquireJobRequest* request,
    rpc::AcquireJobResponse* response
) {
    static_cast<void>(context);

    try {
        const domain::WorkerId worker_id{
            request->worker_id()
        };

        std::optional<domain::Job> assigned_job;
        JobInfoExtras extras;

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

            if (assigned_job.has_value()) {
                extras = job_info_extras(
                    coordinator_,
                    job_log_repository_,
                    *assigned_job
                );
            }
        }

        if (!assigned_job.has_value()) {
            return grpc::Status::OK;
        }

        fill_job_info(
            *assigned_job,
            extras,
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

grpc::Status CoordinatorServiceImpl::StartJob(
    grpc::ServerContext* context,
    const rpc::StartJobRequest* request,
    rpc::StartJobResponse* response
) {
    static_cast<void>(context);

    try {
        const domain::WorkerId worker_id{
            request->worker_id()
        };

        const domain::JobId job_id{
            request->job_id()
        };

        if (request->attempt_number() == 0) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "attempt_number must be positive"
            };
        }

        const radahn::coordinator::JobAttempt attempt{
            job_id,
            request->attempt_number()
        };

        std::optional<domain::Job> job;
        JobInfoExtras extras;

        {
            const std::lock_guard lock{mutex_};

            if (
                !coordinator_.start_job_attempt(
                    worker_id,
                    attempt,
                    radahn::persistence::
                        JobLeaseClock::now()
                )
            ) {
                return grpc::Status{
                    grpc::StatusCode::FAILED_PRECONDITION,
                    "Job attempt could not be started"
                };
            }

            job = coordinator_.get_job(
                job_id
            );

            if (job.has_value()) {
                extras = job_info_extras(
                    coordinator_,
                    job_log_repository_,
                    *job
                );
            }
        }

        if (!job.has_value()) {
            return grpc::Status{
                grpc::StatusCode::INTERNAL,
                "Job disappeared after being started"
            };
        }

        fill_job_info(
            *job,
            extras,
            response->mutable_job()
        );

        response->set_lease_duration_ms(
            static_cast<std::uint64_t>(
                coordinator_.lease_duration().count()
            )
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

grpc::Status CoordinatorServiceImpl::FinishJob(
    grpc::ServerContext* context,
    const rpc::FinishJobRequest* request,
    rpc::FinishJobResponse* response
) {
    static_cast<void>(context);

    try {
        const domain::WorkerId worker_id{
            request->worker_id()
        };

        const domain::JobId job_id{
            request->job_id()
        };

        if (request->attempt_number() == 0) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "attempt_number must be positive"
            };
        }

        if (!request->has_execution_result()) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "An execution result is required"
            };
        }

        if (
            request->outcome() ==
            rpc::JOB_OUTCOME_UNSPECIFIED
        ) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "A valid job outcome is required"
            };
        }

        const domain::ExecutionResult result =
            radahn::rpc_codec::decode_execution_result(
                request->execution_result()
            );

        const bool legacy_outcome_agrees =
            (
                request->outcome() ==
                    rpc::JOB_OUTCOME_SUCCEEDED
            ) == result.succeeded();

        if (!legacy_outcome_agrees) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "outcome does not match the execution result"
            };
        }

        const radahn::coordinator::JobAttempt attempt{
            job_id,
            request->attempt_number()
        };

        std::optional<domain::Job> job;
        JobInfoExtras extras;

        {
            const std::lock_guard lock{mutex_};

            if (
                !coordinator_.finish_job_attempt(
                    worker_id,
                    attempt,
                    result,
                    radahn::persistence::
                        JobLeaseClock::now()
                )
            ) {
                return grpc::Status{
                    grpc::StatusCode::FAILED_PRECONDITION,
                    "Job attempt could not be finished"
                };
            }

            job = coordinator_.get_job(
                job_id
            );

            if (job.has_value()) {
                extras = job_info_extras(
                    coordinator_,
                    job_log_repository_,
                    *job
                );
            }
        }

        if (!job.has_value()) {
            return grpc::Status{
                grpc::StatusCode::INTERNAL,
                "Job disappeared after being finished"
            };
        }

        fill_job_info(
            *job,
            extras,
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

grpc::Status CoordinatorServiceImpl::UploadJobLog(
    grpc::ServerContext* context,
    const rpc::UploadJobLogRequest* request,
    rpc::UploadJobLogResponse* response
) {
    static_cast<void>(context);
    static_cast<void>(response);

    try {
        if (
            request->worker_id().empty() ||
            request->job_id().empty()
        ) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "worker_id and job_id are required"
            };
        }

        if (request->attempt_number() == 0) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "attempt_number must be positive"
            };
        }

        if (
            request->content().size() >
            radahn::persistence::max_job_log_bytes
        ) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "Log content exceeds the maximum uploaded size"
            };
        }

        if (request->truncated()) {
            if (
                request->original_byte_count() <=
                request->content().size()
            ) {
                return grpc::Status{
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "A truncated log must report an original "
                    "byte count larger than its uploaded content"
                };
            }
        } else if (
            request->original_byte_count() !=
            request->content().size()
        ) {
            return grpc::Status{
                grpc::StatusCode::INVALID_ARGUMENT,
                "original_byte_count must match the uploaded "
                "content size when not truncated"
            };
        }

        const radahn::persistence::JobLogStream stream =
            to_persistence_log_stream(
                request->stream()
            );

        const domain::JobId job_id{
            request->job_id()
        };

        const domain::WorkerId worker_id{
            request->worker_id()
        };

        radahn::persistence::JobLogRecord record{
            job_id,
            request->attempt_number(),
            stream,
            worker_id,
            request->content(),
            request->original_byte_count(),
            request->truncated()
        };

        const std::lock_guard lock{mutex_};

        const auto job = coordinator_.get_job(job_id);

        if (!job.has_value()) {
            return grpc::Status{
                grpc::StatusCode::NOT_FOUND,
                "Job was not found"
            };
        }

        if (!is_terminal_job_state(job->state())) {
            return grpc::Status{
                grpc::StatusCode::FAILED_PRECONDITION,
                "Job has not reached a terminal state"
            };
        }

        if (
            job->attempt_count() !=
            request->attempt_number()
        ) {
            return grpc::Status{
                grpc::StatusCode::FAILED_PRECONDITION,
                "attempt_number does not match the accepted "
                "attempt"
            };
        }

        if (
            !coordinator_.is_job_assigned_to_worker(
                job_id,
                worker_id
            )
        ) {
            return grpc::Status{
                grpc::StatusCode::FAILED_PRECONDITION,
                "worker_id does not match the worker whose "
                "completion was accepted for this attempt"
            };
        }

        const auto existing =
            job_log_repository_.get(
                job_id,
                request->attempt_number(),
                stream
            );

        if (existing.has_value()) {
            const bool identical =
                existing->worker_id == worker_id &&
                existing->content == record.content &&
                existing->original_byte_count ==
                    record.original_byte_count &&
                existing->truncated == record.truncated;

            if (identical) {
                return grpc::Status::OK;
            }

            radahn::coordinator::log_event(
                "log_upload_failed",
                {
                    {"job_id", job_id.value()},
                    {
                        "attempt",
                        std::to_string(request->attempt_number())
                    },
                    {"worker_id", worker_id.value()},
                    {"reason", "conflicting_content"}
                }
            );

            return grpc::Status{
                grpc::StatusCode::ALREADY_EXISTS,
                "Conflicting log content already stored for "
                "this attempt and stream"
            };
        }

        job_log_repository_.insert(
            std::move(record)
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

grpc::Status CoordinatorServiceImpl::ListWorkers(
    grpc::ServerContext* context,
    const rpc::ListWorkersRequest* request,
    rpc::ListWorkersResponse* response
) {
    static_cast<void>(context);
    static_cast<void>(request);

    try {
        std::vector<domain::WorkerSnapshot> snapshots;

        std::vector<
            std::optional<
                radahn::persistence::WorkerHeartbeatTimePoint
            >
        > heartbeats;

        {
            const std::lock_guard lock{mutex_};

            snapshots = coordinator_.list_workers();

            heartbeats.reserve(snapshots.size());

            for (const auto& snapshot : snapshots) {
                heartbeats.push_back(
                    coordinator_.worker_last_heartbeat(
                        snapshot.id()
                    )
                );
            }
        }

        for (
            std::size_t index = 0;
            index < snapshots.size();
            ++index
        ) {
            fill_worker_info(
                snapshots[index],
                heartbeats[index],
                response->add_workers()
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

grpc::Status CoordinatorServiceImpl::GetWorker(
    grpc::ServerContext* context,
    const rpc::GetWorkerRequest* request,
    rpc::GetWorkerResponse* response
) {
    static_cast<void>(context);

    try {
        const domain::WorkerId worker_id{
            request->worker_id()
        };

        std::optional<domain::WorkerSnapshot> snapshot;

        std::optional<
            radahn::persistence::WorkerHeartbeatTimePoint
        > heartbeat;

        {
            const std::lock_guard lock{mutex_};

            snapshot = coordinator_.worker_snapshot(
                worker_id
            );

            if (snapshot.has_value()) {
                heartbeat =
                    coordinator_.worker_last_heartbeat(
                        worker_id
                    );
            }
        }

        if (!snapshot.has_value()) {
            return grpc::Status{
                grpc::StatusCode::NOT_FOUND,
                "Worker was not found"
            };
        }

        fill_worker_info(
            *snapshot,
            heartbeat,
            response->mutable_worker()
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

void CoordinatorServiceImpl::run_liveness_sweep_once() {
    const auto now =
        radahn::persistence::
            WorkerHeartbeatClock::now();

    constexpr auto heartbeat_timeout =
        std::chrono::seconds{6};

    std::size_t marked_offline = 0;
    std::size_t expired_leases = 0;
    std::size_t requeued_jobs = 0;

    {
        const std::lock_guard lock{mutex_};

        marked_offline =
            coordinator_.mark_stale_workers_offline(
                now,
                heartbeat_timeout
            );

        expired_leases =
            coordinator_.mark_expired_job_leases(
                now
            );

        requeued_jobs =
            coordinator_.requeue_retry_wait_jobs();
    }

    if (marked_offline != 0) {
        std::cout
            << "Marked "
            << marked_offline
            << " stale worker(s) offline"
            << '\n';
    }

    if (expired_leases != 0) {
        std::cout
            << "Marked "
            << expired_leases
            << " expired job lease(s) abandoned"
            << '\n';
    }

    if (requeued_jobs != 0) {
        std::cout
            << "Requeued "
            << requeued_jobs
            << " abandoned job(s)"
            << '\n';
    }
}

void CoordinatorServiceImpl::run_liveness_monitor() {
    constexpr auto scan_interval =
        std::chrono::seconds{1};

    while (
        !stop_liveness_monitor_.load()
    ) {
        try {
            run_liveness_sweep_once();
        } catch (const std::exception& error) {
            std::cerr
                << "Coordinator recovery scan failed: "
                << error.what()
                << '\n';
        }

        std::unique_lock wait_lock{
            liveness_wait_mutex_
        };

        liveness_wait_condition_.wait_for(
            wait_lock,
            scan_interval,
            [this] {
                return
                    stop_liveness_monitor_
                        .load();
            }
        );
    }
}

}  // namespace radahn::coordinator_app
