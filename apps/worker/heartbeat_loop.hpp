#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "worker_service.grpc.pb.h"

#include "active_job_registry.hpp"

namespace radahn::worker_app {

class HeartbeatLoop final {
public:
    HeartbeatLoop(
        std::string coordinator_address,
        std::string worker_id,
        const ActiveJobRegistry& active_jobs,
        std::chrono::milliseconds interval =
            std::chrono::seconds{2},
        /*
         * How long coordinator connectivity may go without a single
         * acknowledged heartbeat before new job acquisition should
         * pause (see main.cpp). A short network interruption that
         * clears well within this window never affects acquisition;
         * only a prolonged one does.
         */
        std::chrono::milliseconds unhealthy_after =
            std::chrono::seconds{10}
    );

    ~HeartbeatLoop();

    HeartbeatLoop(
        const HeartbeatLoop&
    ) = delete;

    HeartbeatLoop& operator=(
        const HeartbeatLoop&
    ) = delete;

    HeartbeatLoop(
        HeartbeatLoop&&
    ) = delete;

    HeartbeatLoop& operator=(
        HeartbeatLoop&&
    ) = delete;

    /*
     * False once coordinator connectivity has gone unhealthy_after
     * without a single acknowledged heartbeat. Does not itself stop
     * or extend any attempt - main.cpp uses it only to decide
     * whether to pause acquiring new work.
     */
    [[nodiscard]]
    bool is_connectivity_healthy() const noexcept;

private:
    void run();

    std::unique_ptr<
        radahn::rpc::v1::WorkerService::Stub
    > stub_;

    std::string worker_id_;

    const ActiveJobRegistry& active_jobs_;

    std::chrono::milliseconds interval_;

    std::chrono::milliseconds unhealthy_after_;

    AtomicSteadyDeadline last_success_;

    std::atomic<bool> stop_requested_{
        false
    };

    std::mutex wait_mutex_;

    std::condition_variable
        wait_condition_;

    std::thread thread_;
};

}  // namespace radahn::worker_app
