#pragma once

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "client_service.grpc.pb.h"
#include "worker_service.grpc.pb.h"

#include "radahn/coordinator/in_memory_coordinator.hpp"
#include "radahn/persistence/sqlite_database.hpp"
#include "radahn/persistence/sqlite_job_repository.hpp"
#include "radahn/persistence/sqlite_worker_repository.hpp"
#include "radahn/persistence/transaction_runner.hpp"
#include "radahn/scheduler/least_loaded_policy.hpp"

namespace radahn::coordinator_app {

namespace rpc = radahn::rpc::v1;

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
 *   - Heartbeat
 *   - AcquireJob
 *   - StartJob
 *   - FinishJob
 *
 * Extracted out of apps/coordinator/main.cpp (rather than kept in an
 * anonymous namespace there) so an integration test can construct
 * one directly, start it on an ephemeral local gRPC port, and drive
 * it with a real stub - exercising the actual RPC handlers rather
 * than only the coordinator-layer logic they call into.
 */
class CoordinatorServiceImpl final
    : public rpc::ClientService::Service,
      public rpc::WorkerService::Service {
public:
    explicit CoordinatorServiceImpl(
        const std::filesystem::path& database_path
    );

    ~CoordinatorServiceImpl() override;

    CoordinatorServiceImpl(
        const CoordinatorServiceImpl&
    ) = delete;

    CoordinatorServiceImpl& operator=(
        const CoordinatorServiceImpl&
    ) = delete;

    grpc::Status Ping(
        grpc::ServerContext* context,
        const rpc::PingRequest* request,
        rpc::PingResponse* response
    ) override;

    grpc::Status SubmitJob(
        grpc::ServerContext* context,
        const rpc::SubmitJobRequest* request,
        rpc::SubmitJobResponse* response
    ) override;

    grpc::Status GetJob(
        grpc::ServerContext* context,
        const rpc::GetJobRequest* request,
        rpc::GetJobResponse* response
    ) override;

    grpc::Status ListJobs(
        grpc::ServerContext* context,
        const rpc::ListJobsRequest* request,
        rpc::ListJobsResponse* response
    ) override;

    grpc::Status RegisterWorker(
        grpc::ServerContext* context,
        const rpc::RegisterWorkerRequest* request,
        rpc::RegisterWorkerResponse* response
    ) override;

    grpc::Status Heartbeat(
        grpc::ServerContext* context,
        const rpc::HeartbeatRequest* request,
        rpc::HeartbeatResponse* response
    ) override;

    grpc::Status AcquireJob(
        grpc::ServerContext* context,
        const rpc::AcquireJobRequest* request,
        rpc::AcquireJobResponse* response
    ) override;

    grpc::Status StartJob(
        grpc::ServerContext* context,
        const rpc::StartJobRequest* request,
        rpc::StartJobResponse* response
    ) override;

    grpc::Status FinishJob(
        grpc::ServerContext* context,
        const rpc::FinishJobRequest* request,
        rpc::FinishJobResponse* response
    ) override;

    /*
     * Runs exactly what the background liveness thread normally
     * runs on its own schedule (mark stale workers offline, expire
     * abandoned leases, requeue retry-wait jobs). Lets a test force
     * an immediate sweep instead of waiting out the real interval.
     */
    void run_liveness_sweep_once();

private:
    void run_liveness_monitor();

    /*
     * Member order matters - the coordinator stores references to
     * the policy, the repositories, and the transaction runner.
     */
    radahn::scheduler::LeastLoadedPolicy
        policy_;

    radahn::persistence::SqliteDatabase
        database_;

    radahn::persistence::SqliteJobRepository
        job_repository_;

    radahn::persistence::SqliteWorkerRepository
        worker_repository_;

    radahn::persistence::SqliteTransactionRunner
        transaction_runner_;

    radahn::coordinator::InMemoryCoordinator
        coordinator_;

    std::mutex mutex_;

    std::atomic<bool>
        stop_liveness_monitor_{
            false
        };

    std::mutex
        liveness_wait_mutex_;

    std::condition_variable
        liveness_wait_condition_;

    /*
     * Keep the thread last so every object it accesses has already
     * been constructed before the thread starts.
     */
    std::thread
        liveness_thread_;
};

}  // namespace radahn::coordinator_app
