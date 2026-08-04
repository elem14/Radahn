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

namespace radahn::worker_app {

class HeartbeatLoop final {
public:
    HeartbeatLoop(
        std::string coordinator_address,
        std::string worker_id,
        std::chrono::milliseconds interval =
            std::chrono::seconds{2}
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

private:
    void run();

    std::unique_ptr<
        radahn::rpc::v1::WorkerService::Stub
    > stub_;

    std::string worker_id_;

    std::chrono::milliseconds interval_;

    std::atomic<bool> stop_requested_{
        false
    };

    std::mutex wait_mutex_;

    std::condition_variable
        wait_condition_;

    std::thread thread_;
};

}  // namespace radahn::worker_app