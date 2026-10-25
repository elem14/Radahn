/*
 * Integration tests that exercise the real coordinator RPC handlers
 * (CoordinatorServiceImpl, extracted into
 * apps/coordinator/coordinator_service.{hpp,cpp} for exactly this
 * purpose) over a real local gRPC server on an ephemeral port, with
 * a real gRPC client stub - rather than only the coordinator-layer
 * logic they call into (already covered deterministically by
 * job_attempt_fencing_tests.cpp, job_lease_lifecycle_tests.cpp,
 * job_lease_expiration_tests.cpp, etc).
 *
 * Deliberately does not exercise, at this RPC/process level:
 *   - A command timeout being enforced (already covered by
 *     radahn.command_executor against real subprocesses).
 *   - A job outliving a single lease purely via repeated Heartbeat
 *     renewal, or an un-swept lease rejecting an RPC purely from
 *     elapsed wall-clock time (both already covered deterministically,
 *     with a controllable clock, by job_lease_lifecycle_tests.cpp and
 *     job_lease_expiration_tests.cpp). Reproducing either through a
 *     real RPC path would mean actually waiting out a multi-second
 *     lease against the real wall clock, which is not worth the
 *     added runtime here.
 * "Kill a worker and recover the assignment" is instead simulated
 * deterministically by backdating the worker's persisted heartbeat
 * directly in the database (rather than sleeping past the real
 * heartbeat timeout) and then forcing one liveness sweep via
 * CoordinatorServiceImpl::run_liveness_sweep_once(), which runs
 * exactly what the background liveness thread runs on its own
 * schedule.
 */

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <grpcpp/grpcpp.h>
#include <unistd.h>

#include "client_service.grpc.pb.h"
#include "worker_service.grpc.pb.h"

#include "coordinator_service.hpp"

#include "radahn/domain/id.hpp"
#include "radahn/persistence/job_log_repository.hpp"
#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_log_repository.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"
#include "radahn/persistence/sqlite_worker_repository.hpp"
#include "radahn/persistence/worker_repository.hpp"

namespace {

namespace rpc = radahn::rpc::v1;

int failure_count = 0;

void require(bool condition, std::string_view description) {
    if (condition) {
        std::cout << "[PASS] " << description << '\n';
        return;
    }

    std::cerr << "[FAIL] " << description << '\n';
    ++failure_count;
}

/*
 * Owns one coordinator service bound to a real gRPC server on an
 * ephemeral local port, and the two client stubs a worker/CLI would
 * normally use against it.
 */
class RunningCoordinator {
public:
    explicit RunningCoordinator(
        const std::filesystem::path& database_path
    )
        : service_{
              std::make_unique<
                  radahn::coordinator_app::
                      CoordinatorServiceImpl
              >(database_path)
          } {
        grpc::ServerBuilder builder;

        int selected_port = 0;

        builder.AddListeningPort(
            "127.0.0.1:0",
            grpc::InsecureServerCredentials(),
            &selected_port
        );

        builder.RegisterService(
            static_cast<rpc::ClientService::Service*>(
                service_.get()
            )
        );

        builder.RegisterService(
            static_cast<rpc::WorkerService::Service*>(
                service_.get()
            )
        );

        server_ = builder.BuildAndStart();

        if (!server_ || selected_port == 0) {
            throw std::runtime_error{
                "Could not start an ephemeral test coordinator"
            };
        }

        address_ =
            "127.0.0.1:" +
            std::to_string(selected_port);

        const auto channel = grpc::CreateChannel(
            address_,
            grpc::InsecureChannelCredentials()
        );

        client_stub_ =
            rpc::ClientService::NewStub(channel);

        worker_stub_ =
            rpc::WorkerService::NewStub(channel);
    }

    ~RunningCoordinator() {
        if (server_) {
            server_->Shutdown();
        }
    }

    RunningCoordinator(const RunningCoordinator&) = delete;
    RunningCoordinator& operator=(
        const RunningCoordinator&
    ) = delete;

    [[nodiscard]]
    rpc::ClientService::Stub& client() {
        return *client_stub_;
    }

    [[nodiscard]]
    rpc::WorkerService::Stub& worker() {
        return *worker_stub_;
    }

    [[nodiscard]]
    radahn::coordinator_app::CoordinatorServiceImpl&
    service() {
        return *service_;
    }

private:
    std::unique_ptr<
        radahn::coordinator_app::CoordinatorServiceImpl
    > service_;

    std::unique_ptr<grpc::Server> server_;
    std::string address_;

    std::unique_ptr<rpc::ClientService::Stub> client_stub_;
    std::unique_ptr<rpc::WorkerService::Stub> worker_stub_;
};

void apply_deadline(grpc::ClientContext& context) {
    context.set_deadline(
        std::chrono::system_clock::now() +
        std::chrono::seconds{5}
    );
}

[[nodiscard]]
rpc::SubmitJobResponse submit_sleep_job(
    rpc::ClientService::Stub& client,
    const std::string& job_id
) {
    rpc::SubmitJobRequest request;

    request.set_id(job_id);
    request.set_name("integration test job");
    request.set_priority(10);

    auto* requirements = request.mutable_requirements();
    requirements->set_cpu_cores(1.0);
    requirements->set_memory_bytes(128ULL * 1024 * 1024);
    requirements->set_disk_bytes(128ULL * 1024 * 1024);

    auto* workload = request.mutable_workload();
    workload->set_kind(rpc::WORKLOAD_KIND_SLEEP);
    workload->set_sleep_duration_ms(1);

    rpc::SubmitJobResponse response;
    grpc::ClientContext context;
    apply_deadline(context);

    const auto status =
        client.SubmitJob(&context, request, &response);

    if (!status.ok()) {
        throw std::runtime_error{
            "SubmitJob failed: " + status.error_message()
        };
    }

    return response;
}

void register_worker(
    rpc::WorkerService::Stub& worker,
    const std::string& worker_id
) {
    rpc::RegisterWorkerRequest request;

    request.set_worker_id(worker_id);
    request.set_running_jobs(0);
    request.set_max_concurrent_jobs(4);

    auto* resources = request.mutable_resources();
    resources->set_total_cpu_cores(4.0);
    resources->set_available_cpu_cores(4.0);
    resources->set_total_memory_bytes(4096ULL * 1024 * 1024);
    resources->set_available_memory_bytes(4096ULL * 1024 * 1024);
    resources->set_total_disk_bytes(4096ULL * 1024 * 1024);
    resources->set_available_disk_bytes(4096ULL * 1024 * 1024);

    rpc::RegisterWorkerResponse response;
    grpc::ClientContext context;
    apply_deadline(context);

    const auto status =
        worker.RegisterWorker(&context, request, &response);

    if (!status.ok()) {
        throw std::runtime_error{
            "RegisterWorker failed: " + status.error_message()
        };
    }
}

/*
 * Acquires and starts an attempt for `job_id` on `worker_id` in one
 * step, returning the attempt number StartJob confirmed. Throws if
 * no job was acquired at all - tests poll this with a short retry
 * loop rather than assuming dispatch happens on the very first try.
 */
[[nodiscard]]
std::uint64_t acquire_and_start(
    rpc::WorkerService::Stub& worker,
    const std::string& worker_id,
    const std::string& expected_job_id
) {
    rpc::AcquireJobRequest acquire_request;
    acquire_request.set_worker_id(worker_id);

    rpc::AcquireJobResponse acquire_response;
    grpc::ClientContext acquire_context;
    apply_deadline(acquire_context);

    const auto acquire_status =
        worker.AcquireJob(
            &acquire_context,
            acquire_request,
            &acquire_response
        );

    if (!acquire_status.ok()) {
        throw std::runtime_error{
            "AcquireJob failed: " +
            acquire_status.error_message()
        };
    }

    if (
        !acquire_response.has_job() ||
        acquire_response.job().id() != expected_job_id
    ) {
        throw std::runtime_error{
            "AcquireJob did not return the expected job"
        };
    }

    const auto attempt_number =
        acquire_response.job().attempt_count();

    rpc::StartJobRequest start_request;
    start_request.set_worker_id(worker_id);
    start_request.set_job_id(expected_job_id);
    start_request.set_attempt_number(attempt_number);

    rpc::StartJobResponse start_response;
    grpc::ClientContext start_context;
    apply_deadline(start_context);

    const auto start_status =
        worker.StartJob(
            &start_context,
            start_request,
            &start_response
        );

    if (!start_status.ok()) {
        throw std::runtime_error{
            "StartJob failed: " + start_status.error_message()
        };
    }

    return attempt_number;
}

[[nodiscard]]
grpc::Status finish_job(
    rpc::WorkerService::Stub& worker,
    const std::string& worker_id,
    const std::string& job_id,
    std::uint64_t attempt_number,
    bool succeeded
) {
    rpc::FinishJobRequest request;

    request.set_worker_id(worker_id);
    request.set_job_id(job_id);
    request.set_attempt_number(attempt_number);

    request.set_outcome(
        succeeded
            ? rpc::JOB_OUTCOME_SUCCEEDED
            : rpc::JOB_OUTCOME_FAILED
    );

    auto* result = request.mutable_execution_result();

    if (succeeded) {
        result->set_outcome(rpc::EXECUTION_OUTCOME_EXITED);
        result->set_exit_code(0);
    } else {
        result->set_outcome(rpc::EXECUTION_OUTCOME_EXITED);
        result->set_exit_code(1);
    }

    rpc::FinishJobResponse response;
    grpc::ClientContext context;
    apply_deadline(context);

    return worker.FinishJob(&context, request, &response);
}

/*
 * Directly backdates a worker's persisted last-heartbeat timestamp,
 * through a second connection to the same SQLite database file the
 * running coordinator uses, far enough in the past that it reads as
 * stale without needing to wait out the real heartbeat timeout.
 */
void backdate_worker_heartbeat(
    const std::filesystem::path& database_path,
    const std::string& worker_id
) {
    radahn::persistence::SqliteDatabase side_channel{
        database_path
    };

    radahn::persistence::SqliteWorkerRepository
        side_repository{side_channel};

    side_repository.record_heartbeat(
        radahn::domain::WorkerId{worker_id},
        radahn::persistence::WorkerHeartbeatClock::now() -
            std::chrono::minutes{10}
    );
}

/*
 * Directly backdates a job's persisted lease deadline, through a
 * second connection to the same SQLite database file, so the next
 * liveness sweep treats it as abandoned without needing to wait out
 * the real lease duration.
 */
void backdate_job_lease(
    const std::filesystem::path& database_path,
    const std::string& job_id
) {
    radahn::persistence::SqliteDatabase side_channel{
        database_path
    };

    radahn::persistence::SqliteJobRepository
        side_repository{side_channel};

    auto record =
        side_repository.get(
            radahn::domain::JobId{job_id}
        );

    if (!record.has_value()) {
        throw std::runtime_error{
            "backdate_job_lease: job not found"
        };
    }

    record->lease_expires_at =
        radahn::persistence::JobLeaseClock::now() -
        std::chrono::minutes{10};

    side_repository.update(*record);
}

/*
 * Reads a stored log directly through a second connection to the
 * same database file, bypassing the coordinator's RPC layer -
 * exactly like backdate_worker_heartbeat/backdate_job_lease above -
 * so a test can verify what UploadJobLog actually persisted.
 */
[[nodiscard]]
std::optional<radahn::persistence::JobLogRecord>
read_stored_log(
    const std::filesystem::path& database_path,
    const std::string& job_id,
    std::uint64_t attempt_number,
    radahn::persistence::JobLogStream stream
) {
    radahn::persistence::SqliteDatabase side_channel{
        database_path
    };

    radahn::persistence::SqliteJobLogRepository
        side_repository{side_channel};

    return side_repository.get(
        radahn::domain::JobId{job_id},
        attempt_number,
        stream
    );
}

[[nodiscard]]
grpc::Status upload_job_log(
    rpc::WorkerService::Stub& worker,
    const std::string& worker_id,
    const std::string& job_id,
    std::uint64_t attempt_number,
    rpc::LogStream stream,
    const std::string& content,
    std::uint64_t original_byte_count,
    bool truncated
) {
    rpc::UploadJobLogRequest request;

    request.set_worker_id(worker_id);
    request.set_job_id(job_id);
    request.set_attempt_number(attempt_number);
    request.set_stream(stream);
    request.set_content(content);
    request.set_original_byte_count(original_byte_count);
    request.set_truncated(truncated);

    rpc::UploadJobLogResponse response;
    grpc::ClientContext context;
    apply_deadline(context);

    return worker.UploadJobLog(&context, request, &response);
}

/*
 * 1-6. UploadJobLog: valid upload, duplicate accepted, conflicting
 * content rejected, oversized payload rejected, not-yet-terminal
 * rejected, and an obsolete (superseded) worker's upload rejected.
 */
void test_upload_job_log_lifecycle(
    const std::filesystem::path& database_path
) {
    RunningCoordinator coordinator{database_path};

    static_cast<void>(
        submit_sleep_job(coordinator.client(), "job-log")
    );
    register_worker(coordinator.worker(), "worker-log");
    register_worker(coordinator.worker(), "worker-log-obsolete");

    const auto attempt =
        acquire_and_start(
            coordinator.worker(),
            "worker-log",
            "job-log"
        );

    // 5. Require the job to be terminal before any log is accepted.
    const auto too_early =
        upload_job_log(
            coordinator.worker(),
            "worker-log",
            "job-log",
            attempt,
            rpc::LOG_STREAM_STDOUT,
            "not yet finished",
            16,
            false
        );

    require(
        too_early.error_code() ==
            grpc::StatusCode::FAILED_PRECONDITION,
        "UploadJobLog is rejected before the job reaches a "
        "terminal state"
    );

    // 1. Reject oversized payloads, even before the job finishes.
    const std::string oversized_content(
        static_cast<std::size_t>(
            radahn::persistence::max_job_log_bytes
        ) + 1,
        'x'
    );

    const auto oversized =
        upload_job_log(
            coordinator.worker(),
            "worker-log",
            "job-log",
            attempt,
            rpc::LOG_STREAM_STDOUT,
            oversized_content,
            oversized_content.size(),
            false
        );

    require(
        oversized.error_code() ==
            grpc::StatusCode::INVALID_ARGUMENT,
        "UploadJobLog rejects a payload larger than the maximum "
        "uploaded log size"
    );

    require(
        finish_job(
            coordinator.worker(),
            "worker-log",
            "job-log",
            attempt,
            true
        ).ok(),
        "The attempt completes so its logs can now be uploaded"
    );

    // Binary content, including a NUL byte, must round-trip.
    std::string stdout_content{"line one\n"};
    stdout_content.push_back('\0');
    stdout_content += "line two\n";

    const auto stdout_upload =
        upload_job_log(
            coordinator.worker(),
            "worker-log",
            "job-log",
            attempt,
            rpc::LOG_STREAM_STDOUT,
            stdout_content,
            stdout_content.size(),
            false
        );

    require(
        stdout_upload.ok(),
        "A valid stdout upload for the accepted, terminal attempt "
        "is accepted"
    );

    const auto stored_stdout =
        read_stored_log(
            database_path,
            "job-log",
            attempt,
            radahn::persistence::JobLogStream::standard_output
        );

    require(
        stored_stdout.has_value() &&
        stored_stdout->content == stdout_content,
        "Binary content, including an embedded NUL byte, round-trips "
        "exactly through the stored log"
    );

    // 2. Empty logs are distinguishable from missing logs.
    const auto empty_stderr_upload =
        upload_job_log(
            coordinator.worker(),
            "worker-log",
            "job-log",
            attempt,
            rpc::LOG_STREAM_STDERR,
            "",
            0,
            false
        );

    require(
        empty_stderr_upload.ok(),
        "An empty stderr upload is accepted"
    );

    const auto stored_stderr =
        read_stored_log(
            database_path,
            "job-log",
            attempt,
            radahn::persistence::JobLogStream::standard_error
        );

    require(
        stored_stderr.has_value() && stored_stderr->content.empty(),
        "The empty stderr log is stored (present), not absent"
    );

    // 3. Duplicate uploads are safe.
    const auto duplicate_upload =
        upload_job_log(
            coordinator.worker(),
            "worker-log",
            "job-log",
            attempt,
            rpc::LOG_STREAM_STDOUT,
            stdout_content,
            stdout_content.size(),
            false
        );

    require(
        duplicate_upload.ok(),
        "An identical repeated upload is accepted"
    );

    // Reject conflicting content for an existing key.
    const auto conflicting_upload =
        upload_job_log(
            coordinator.worker(),
            "worker-log",
            "job-log",
            attempt,
            rpc::LOG_STREAM_STDOUT,
            "different content entirely",
            26,
            false
        );

    require(
        conflicting_upload.error_code() ==
            grpc::StatusCode::ALREADY_EXISTS,
        "Conflicting content for an existing key is rejected"
    );

    const auto stored_after_conflict =
        read_stored_log(
            database_path,
            "job-log",
            attempt,
            radahn::persistence::JobLogStream::standard_output
        );

    require(
        stored_after_conflict.has_value() &&
        stored_after_conflict->content == stdout_content,
        "The rejected conflicting upload did not replace the "
        "originally accepted content"
    );

    // 6. An obsolete worker cannot replace the accepted attempt's
    // logs - it never held this attempt in the first place.
    const auto obsolete_upload =
        upload_job_log(
            coordinator.worker(),
            "worker-log-obsolete",
            "job-log",
            attempt,
            rpc::LOG_STREAM_STDOUT,
            stdout_content,
            stdout_content.size(),
            false
        );

    require(
        obsolete_upload.error_code() ==
            grpc::StatusCode::FAILED_PRECONDITION,
        "A worker that never held this attempt cannot upload logs "
        "for it"
    );
}

/*
 * 4. Oversized output is marked truncated, and such a record is
 * still correctly stored and retrievable.
 */
void test_upload_job_log_truncation(
    const std::filesystem::path& database_path
) {
    RunningCoordinator coordinator{database_path};

    static_cast<void>(
        submit_sleep_job(coordinator.client(), "job-log-truncated")
    );
    register_worker(coordinator.worker(), "worker-log-truncated");

    const auto attempt =
        acquire_and_start(
            coordinator.worker(),
            "worker-log-truncated",
            "job-log-truncated"
        );

    require(
        finish_job(
            coordinator.worker(),
            "worker-log-truncated",
            "job-log-truncated",
            attempt,
            true
        ).ok(),
        "The attempt completes so its logs can now be uploaded"
    );

    const std::string captured_prefix(
        static_cast<std::size_t>(
            radahn::persistence::max_job_log_bytes
        ),
        'y'
    );

    const auto truncated_upload =
        upload_job_log(
            coordinator.worker(),
            "worker-log-truncated",
            "job-log-truncated",
            attempt,
            rpc::LOG_STREAM_STDOUT,
            captured_prefix,
            static_cast<std::uint64_t>(
                radahn::persistence::max_job_log_bytes
            ) + 4096,
            true
        );

    require(
        truncated_upload.ok(),
        "A truncated upload exactly at the maximum size is accepted"
    );

    const auto stored =
        read_stored_log(
            database_path,
            "job-log-truncated",
            attempt,
            radahn::persistence::JobLogStream::standard_output
        );

    require(
        stored.has_value() &&
        stored->truncated &&
        stored->original_byte_count ==
            radahn::persistence::max_job_log_bytes + 4096 &&
        stored->content.size() ==
            radahn::persistence::max_job_log_bytes,
        "The truncated flag and true original byte count are "
        "preserved alongside the capped content"
    );
}

[[nodiscard]]
rpc::GetJobResponse get_job(
    rpc::ClientService::Stub& client,
    const std::string& job_id
) {
    rpc::GetJobRequest request;
    request.set_id(job_id);

    rpc::GetJobResponse response;
    grpc::ClientContext context;
    apply_deadline(context);

    const auto status =
        client.GetJob(&context, request, &response);

    if (!status.ok()) {
        throw std::runtime_error{
            "GetJob failed: " + status.error_message()
        };
    }

    return response;
}

[[nodiscard]]
rpc::WorkerInfo get_worker(
    rpc::ClientService::Stub& client,
    const std::string& worker_id
) {
    rpc::GetWorkerRequest request;
    request.set_worker_id(worker_id);

    rpc::GetWorkerResponse response;
    grpc::ClientContext context;
    apply_deadline(context);

    const auto status =
        client.GetWorker(&context, request, &response);

    if (!status.ok()) {
        throw std::runtime_error{
            "GetWorker failed: " + status.error_message()
        };
    }

    return response.worker();
}

[[nodiscard]]
rpc::ListWorkersResponse list_workers(
    rpc::ClientService::Stub& client
) {
    rpc::ListWorkersRequest request;
    rpc::ListWorkersResponse response;
    grpc::ClientContext context;
    apply_deadline(context);

    const auto status =
        client.ListWorkers(&context, request, &response);

    if (!status.ok()) {
        throw std::runtime_error{
            "ListWorkers failed: " + status.error_message()
        };
    }

    return response;
}

/*
 * Worker visibility (ListWorkers/GetWorker) and job diagnostics:
 * active jobs show reservations, completed jobs release them, and a
 * lost worker's status changes to offline.
 */
void test_worker_visibility_and_job_diagnostics(
    const std::filesystem::path& database_path
) {
    RunningCoordinator coordinator{database_path};

    rpc::SubmitJobRequest submit_request;
    submit_request.set_id("job-visibility");
    submit_request.set_name("visibility test job");
    submit_request.set_priority(5);

    auto* requirements = submit_request.mutable_requirements();
    requirements->set_cpu_cores(2.0);
    requirements->set_memory_bytes(256ULL * 1024 * 1024);
    requirements->set_disk_bytes(256ULL * 1024 * 1024);

    auto* workload = submit_request.mutable_workload();
    workload->set_kind(rpc::WORKLOAD_KIND_SLEEP);
    workload->set_sleep_duration_ms(1);

    {
        rpc::SubmitJobResponse submit_response;
        grpc::ClientContext submit_context;
        apply_deadline(submit_context);

        require(
            coordinator.client().SubmitJob(
                &submit_context,
                submit_request,
                &submit_response
            ).ok(),
            "Job with explicit resource requirements is submitted"
        );
    }

    register_worker(coordinator.worker(), "worker-visibility");

    const auto before_dispatch =
        get_worker(coordinator.client(), "worker-visibility");

    require(
        before_dispatch.online() &&
        before_dispatch.allocated_cpu_cores() == 0.0 &&
        before_dispatch.running_jobs() == 0,
        "A freshly registered worker starts online with nothing "
        "reserved"
    );

    const auto attempt =
        acquire_and_start(
            coordinator.worker(),
            "worker-visibility",
            "job-visibility"
        );

    const auto running_job =
        get_job(coordinator.client(), "job-visibility");

    require(
        running_job.job().has_assigned_worker_id() &&
        running_job.job().assigned_worker_id() ==
            "worker-visibility",
        "GetJob reports the worker currently assigned to an "
        "active job"
    );

    require(
        !running_job.job().stdout_log_available() &&
        !running_job.job().stderr_log_available(),
        "A still-running job has no uploaded logs available"
    );

    const auto during_run =
        get_worker(coordinator.client(), "worker-visibility");

    require(
        during_run.allocated_cpu_cores() == 2.0 &&
        during_run.running_jobs() == 1,
        "GetWorker shows the active job's resource reservation "
        "while it is running"
    );

    const auto listed = list_workers(coordinator.client());

    const bool found_in_list =
        std::any_of(
            listed.workers().begin(),
            listed.workers().end(),
            [](const rpc::WorkerInfo& worker) {
                return
                    worker.worker_id() == "worker-visibility";
            }
        );

    require(
        found_in_list,
        "ListWorkers includes the registered worker"
    );

    require(
        finish_job(
            coordinator.worker(),
            "worker-visibility",
            "job-visibility",
            attempt,
            true
        ).ok(),
        "The active job completes successfully"
    );

    const auto after_finish =
        get_worker(coordinator.client(), "worker-visibility");

    require(
        after_finish.allocated_cpu_cores() == 0.0 &&
        after_finish.running_jobs() == 0,
        "GetWorker shows the reservation released after the job "
        "completes"
    );

    const auto finished_job =
        get_job(coordinator.client(), "job-visibility");

    require(
        finished_job.job().has_assigned_worker_id() &&
        finished_job.job().assigned_worker_id() ==
            "worker-visibility",
        "GetJob still reports which worker completed a terminal "
        "job"
    );

    // Worker loss changes its reported status.
    static_cast<void>(
        submit_sleep_job(coordinator.client(), "job-visibility-lost")
    );
    register_worker(coordinator.worker(), "worker-to-lose");

    static_cast<void>(
        acquire_and_start(
            coordinator.worker(),
            "worker-to-lose",
            "job-visibility-lost"
        )
    );

    backdate_worker_heartbeat(database_path, "worker-to-lose");
    backdate_job_lease(database_path, "job-visibility-lost");

    coordinator.service().run_liveness_sweep_once();

    const auto lost_worker =
        get_worker(coordinator.client(), "worker-to-lose");

    require(
        !lost_worker.online(),
        "A worker that stops heartbeating is reported offline"
    );
}

[[nodiscard]]
grpc::Status get_job_log(
    rpc::ClientService::Stub& client,
    const std::string& job_id,
    rpc::LogStream stream,
    rpc::GetJobLogResponse* response
) {
    rpc::GetJobLogRequest request;

    request.set_id(job_id);
    request.set_stream(stream);

    grpc::ClientContext context;
    apply_deadline(context);

    return client.GetJobLog(
        &context,
        request,
        response
    );
}

/*
 * GetJobLog: retrieve distinct stdout/stderr content, distinguish
 * "job does not exist" from "logs have not been uploaded", and
 * confirm logs remain readable after a simulated coordinator
 * restart.
 */
void test_get_job_log_retrieval_and_restart(
    const std::filesystem::path& database_path
) {
    {
        RunningCoordinator coordinator{database_path};

        static_cast<void>(
            submit_sleep_job(coordinator.client(), "job-getlog")
        );
        register_worker(coordinator.worker(), "worker-getlog");

        const auto attempt =
            acquire_and_start(
                coordinator.worker(),
                "worker-getlog",
                "job-getlog"
            );

        require(
            finish_job(
                coordinator.worker(),
                "worker-getlog",
                "job-getlog",
                attempt,
                true
            ).ok(),
            "The attempt completes so its logs can be uploaded"
        );

        const std::string stdout_content{
            "distinct stdout content"
        };

        const std::string stderr_content{
            "distinct stderr content"
        };

        require(
            upload_job_log(
                coordinator.worker(),
                "worker-getlog",
                "job-getlog",
                attempt,
                rpc::LOG_STREAM_STDOUT,
                stdout_content,
                stdout_content.size(),
                false
            ).ok(),
            "stdout is uploaded for the finished attempt"
        );

        require(
            upload_job_log(
                coordinator.worker(),
                "worker-getlog",
                "job-getlog",
                attempt,
                rpc::LOG_STREAM_STDERR,
                stderr_content,
                stderr_content.size(),
                false
            ).ok(),
            "stderr is uploaded for the finished attempt"
        );

        rpc::GetJobLogResponse stdout_response;

        const auto stdout_status =
            get_job_log(
                coordinator.client(),
                "job-getlog",
                rpc::LOG_STREAM_STDOUT,
                &stdout_response
            );

        require(
            stdout_status.ok() &&
            stdout_response.content() ==
                "distinct stdout content" &&
            stdout_response.attempt_number() == attempt,
            "GetJobLog retrieves the uploaded stdout content for "
            "the job's current attempt"
        );

        rpc::GetJobLogResponse stderr_response;

        const auto stderr_status =
            get_job_log(
                coordinator.client(),
                "job-getlog",
                rpc::LOG_STREAM_STDERR,
                &stderr_response
            );

        require(
            stderr_status.ok() &&
            stderr_response.content() ==
                "distinct stderr content",
            "GetJobLog retrieves the distinct uploaded stderr "
            "content, not the stdout content"
        );

        rpc::GetJobLogResponse missing_job_response;

        const auto missing_job_status =
            get_job_log(
                coordinator.client(),
                "job-getlog-does-not-exist",
                rpc::LOG_STREAM_STDOUT,
                &missing_job_response
            );

        require(
            missing_job_status.error_code() ==
                grpc::StatusCode::NOT_FOUND,
            "GetJobLog reports NOT_FOUND for a job that does not "
            "exist at all"
        );

        static_cast<void>(
            submit_sleep_job(
                coordinator.client(),
                "job-getlog-no-upload"
            )
        );

        register_worker(
            coordinator.worker(),
            "worker-getlog-no-upload"
        );

        const auto no_upload_attempt =
            acquire_and_start(
                coordinator.worker(),
                "worker-getlog-no-upload",
                "job-getlog-no-upload"
            );

        require(
            finish_job(
                coordinator.worker(),
                "worker-getlog-no-upload",
                "job-getlog-no-upload",
                no_upload_attempt,
                true
            ).ok(),
            // Finished (not left QUEUED) so it does not linger in
            // the shared database and compete for dispatch with
            // later tests - deliberately without ever uploading its
            // logs, to exercise the "successful job, no logs yet"
            // case below.
            "A job can succeed without ever uploading its logs"
        );

        rpc::GetJobLogResponse not_uploaded_response;

        const auto not_uploaded_status =
            get_job_log(
                coordinator.client(),
                "job-getlog-no-upload",
                rpc::LOG_STREAM_STDOUT,
                &not_uploaded_response
            );

        require(
            not_uploaded_status.error_code() ==
                grpc::StatusCode::UNAVAILABLE,
            "GetJobLog reports UNAVAILABLE (distinct from NOT_FOUND) "
            "for a job that exists but has no uploaded logs yet - "
            "a successful job is not assumed to have logs"
        );

        // RunningCoordinator's destructor shuts the server down
        // here, simulating a coordinator process restart.
    }

    RunningCoordinator restarted_coordinator{database_path};

    rpc::GetJobLogResponse response_after_restart;

    const auto status_after_restart =
        get_job_log(
            restarted_coordinator.client(),
            "job-getlog",
            rpc::LOG_STREAM_STDOUT,
            &response_after_restart
        );

    require(
        status_after_restart.ok() &&
        response_after_restart.content() ==
            "distinct stdout content",
        "A log uploaded before a coordinator restart remains "
        "readable afterward, with no dependency on the "
        "originating worker"
    );
}

/*
 * 1. Start and finish a valid attempt.
 * 5. Accept an identical repeated completion.
 * 6. Reject a conflicting repeated completion.
 */
void test_start_finish_and_repeated_completion(
    const std::filesystem::path& database_path
) {
    RunningCoordinator coordinator{database_path};

    static_cast<void>(

        submit_sleep_job(coordinator.client(), "job-one")

    );
    register_worker(coordinator.worker(), "worker-one");

    const auto attempt =
        acquire_and_start(
            coordinator.worker(),
            "worker-one",
            "job-one"
        );

    require(
        attempt == 1,
        "A freshly dispatched job starts at attempt 1"
    );

    const auto first_finish =
        finish_job(
            coordinator.worker(),
            "worker-one",
            "job-one",
            attempt,
            true
        );

    require(
        first_finish.ok(),
        "FinishJob accepts a valid, current attempt"
    );

    const auto repeated_finish =
        finish_job(
            coordinator.worker(),
            "worker-one",
            "job-one",
            attempt,
            true
        );

    require(
        repeated_finish.ok(),
        "An identical repeated FinishJob is accepted "
        "(idempotent retry of a lost response)"
    );

    const auto conflicting_finish =
        finish_job(
            coordinator.worker(),
            "worker-one",
            "job-one",
            attempt,
            false
        );

    require(
        conflicting_finish.error_code() ==
            grpc::StatusCode::FAILED_PRECONDITION,
        "A conflicting repeated FinishJob (different result) "
        "is rejected"
    );

    rpc::GetJobRequest get_request;
    get_request.set_id("job-one");

    rpc::GetJobResponse get_response;
    grpc::ClientContext get_context;
    apply_deadline(get_context);

    const auto get_status =
        coordinator.client().GetJob(
            &get_context,
            get_request,
            &get_response
        );

    require(
        get_status.ok() &&
        get_response.job().state() ==
            rpc::JOB_STATE_SUCCEEDED,
        "The job's durable state reflects the original, "
        "accepted completion - not the rejected conflict"
    );
}

/*
 * 2. Reject an old attempt after redispatch to the same worker.
 * 3. Reject old-attempt heartbeat renewal.
 * 10. Kill a worker and recover the assignment.
 */
void test_stale_worker_redispatch_fences_old_attempt(
    const std::filesystem::path& database_path
) {
    RunningCoordinator coordinator{database_path};

    static_cast<void>(

        submit_sleep_job(coordinator.client(), "job-two")

    );
    register_worker(coordinator.worker(), "worker-stale");
    register_worker(coordinator.worker(), "worker-replacement");

    const auto first_attempt =
        acquire_and_start(
            coordinator.worker(),
            "worker-stale",
            "job-two"
        );

    require(
        first_attempt == 1,
        "The original worker starts attempt 1"
    );

    // Simulate the worker disappearing without waiting out the
    // real heartbeat timeout.
    backdate_worker_heartbeat(database_path, "worker-stale");
    backdate_job_lease(database_path, "job-two");

    coordinator.service().run_liveness_sweep_once();

    const auto second_attempt =
        acquire_and_start(
            coordinator.worker(),
            "worker-replacement",
            "job-two"
        );

    require(
        second_attempt == 2,
        "The replacement worker is redispatched a new attempt "
        "number, not a repeat of the stale one"
    );

    const auto stale_start =
        finish_job(
            coordinator.worker(),
            "worker-stale",
            "job-two",
            first_attempt,
            true
        );

    require(
        stale_start.error_code() ==
            grpc::StatusCode::FAILED_PRECONDITION,
        "The stale worker's old attempt is rejected after "
        "redispatch, even though it still names the right job"
    );

    rpc::HeartbeatRequest heartbeat_request;
    heartbeat_request.set_worker_id("worker-stale");

    auto* active_attempt =
        heartbeat_request.add_active_attempts();

    active_attempt->set_job_id("job-two");
    active_attempt->set_attempt_number(first_attempt);

    rpc::HeartbeatResponse heartbeat_response;
    grpc::ClientContext heartbeat_context;
    apply_deadline(heartbeat_context);

    const auto heartbeat_status =
        coordinator.worker().Heartbeat(
            &heartbeat_context,
            heartbeat_request,
            &heartbeat_response
        );

    require(
        heartbeat_status.ok(),
        "The Heartbeat RPC itself still succeeds for the "
        "stale worker"
    );

    require(
        heartbeat_response.renewed_attempts_size() == 0,
        "But the old, superseded attempt is not among the "
        "renewed attempts - an explicit rejection, not a "
        "transient failure"
    );

    const auto replacement_finish =
        finish_job(
            coordinator.worker(),
            "worker-replacement",
            "job-two",
            second_attempt,
            true
        );

    require(
        replacement_finish.ok(),
        "The replacement worker's current attempt completes "
        "the job normally"
    );
}

/*
 * 7. Recover after coordinator restart.
 */
void test_recovery_after_coordinator_restart(
    const std::filesystem::path& database_path
) {
    std::uint64_t attempt_before_restart = 0;

    {
        RunningCoordinator coordinator{database_path};

        static_cast<void>(

            submit_sleep_job(coordinator.client(), "job-three")

        );
        register_worker(coordinator.worker(), "worker-before-restart");

        attempt_before_restart =
            acquire_and_start(
                coordinator.worker(),
                "worker-before-restart",
                "job-three"
            );

        require(
            attempt_before_restart == 1,
            "job-three is RUNNING under attempt 1 right before "
            "the simulated coordinator restart"
        );

        // RunningCoordinator's destructor shuts the server and
        // the coordinator service down here, simulating a
        // coordinator process restart; database_path durably
        // survives it.
    }

    RunningCoordinator restarted_coordinator{database_path};

    rpc::GetJobRequest get_request;
    get_request.set_id("job-three");

    rpc::GetJobResponse get_response;
    grpc::ClientContext get_context;
    apply_deadline(get_context);

    const auto get_status =
        restarted_coordinator.client().GetJob(
            &get_context,
            get_request,
            &get_response
        );

    require(
        get_status.ok() &&
        get_response.job().state() == rpc::JOB_STATE_QUEUED,
        "The abandoned RUNNING attempt is recovered back to "
        "QUEUED by the restarted coordinator, not left stuck "
        "or silently lost"
    );

    register_worker(
        restarted_coordinator.worker(),
        "worker-after-restart"
    );

    const auto attempt_after_restart =
        acquire_and_start(
            restarted_coordinator.worker(),
            "worker-after-restart",
            "job-three"
        );

    require(
        attempt_after_restart == attempt_before_restart + 1,
        "The recovered job is redispatched under a fresh, "
        "incremented attempt number after restart"
    );

    const auto finish_status =
        finish_job(
            restarted_coordinator.worker(),
            "worker-after-restart",
            "job-three",
            attempt_after_restart,
            true
        );

    require(
        finish_status.ok(),
        "The job recovered across a coordinator restart can "
        "still be completed normally"
    );
}

}  // namespace

int main() {
    const auto database_path =
        std::filesystem::temp_directory_path() /
        (
            "radahn-rpc-integration-" +
            std::to_string(::getpid()) +
            ".db"
        );

    std::filesystem::remove(database_path);

    int exit_code = EXIT_SUCCESS;

    try {
        test_start_finish_and_repeated_completion(
            database_path
        );

        test_stale_worker_redispatch_fences_old_attempt(
            database_path
        );

        test_recovery_after_coordinator_restart(
            database_path
        );

        test_upload_job_log_lifecycle(
            database_path
        );

        test_upload_job_log_truncation(
            database_path
        );

        test_get_job_log_retrieval_and_restart(
            database_path
        );

        test_worker_visibility_and_job_diagnostics(
            database_path
        );
    } catch (const std::exception& error) {
        std::cerr
            << "Unexpected integration-test exception: "
            << error.what()
            << '\n';

        exit_code = EXIT_FAILURE;
    }

    std::filesystem::remove(database_path);

    if (failure_count != 0) {
        std::cerr
            << '\n'
            << failure_count
            << " coordinator RPC integration assertion(s) failed\n";

        exit_code = EXIT_FAILURE;
    }

    if (exit_code == EXIT_SUCCESS) {
        std::cout
            << "\nAll coordinator RPC integration tests passed\n";
    }

    return exit_code;
}
