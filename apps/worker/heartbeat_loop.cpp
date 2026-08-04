#include "heartbeat_loop.hpp"

#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

#include <grpcpp/grpcpp.h>

#include "worker_service.grpc.pb.h"

namespace radahn::worker_app {

namespace {

namespace rpc = radahn::rpc::v1;

[[nodiscard]]
std::string validate_worker_id(
    std::string worker_id
) {
    if (worker_id.empty()) {
        throw std::invalid_argument{
            "Heartbeat worker ID cannot be empty"
        };
    }

    return worker_id;
}

[[nodiscard]]
std::chrono::milliseconds validate_interval(
    std::chrono::milliseconds interval
) {
    if (interval.count() <= 0) {
        throw std::invalid_argument{
            "Heartbeat interval must be positive"
        };
    }

    return interval;
}

}  // namespace

HeartbeatLoop::HeartbeatLoop(
    std::string coordinator_address,
    std::string worker_id,
    std::chrono::milliseconds interval
)
    : stub_{
          rpc::WorkerService::NewStub(
              grpc::CreateChannel(
                  std::move(
                      coordinator_address
                  ),
                  grpc::
                      InsecureChannelCredentials()
              )
          )
      },
      worker_id_{
          validate_worker_id(
              std::move(worker_id)
          )
      },
      interval_{
          validate_interval(interval)
      },
      thread_{
          [this] {
              run();
          }
      } {
}

HeartbeatLoop::~HeartbeatLoop() {
    
    stop_requested_.store(
        true
    );

    wait_condition_.notify_all();

    if (thread_.joinable()) {
        thread_.join();
    }
}

void HeartbeatLoop::run() {
    bool acknowledged_once = false;

    while (
        !stop_requested_.load()
    ) {
        grpc::ClientContext context;

        context.set_deadline(
            std::chrono::system_clock::now() +
            std::chrono::seconds{3}
        );

        rpc::HeartbeatRequest request;

        request.set_worker_id(
            worker_id_
        );

        rpc::HeartbeatResponse response;

        const grpc::Status status =
            stub_->Heartbeat(
                &context,
                request,
                &response
            );

        if (status.ok()) {
            if (!acknowledged_once) {
                std::cout
                    << "Heartbeat acknowledged"
                    << '\n';

                acknowledged_once = true;
            }
        } else if (
            !stop_requested_.load()
        ) {
            std::cerr
                << "Heartbeat RPC failed: "
                << status.error_message()
                << '\n';
        }

        std::unique_lock lock{
            wait_mutex_
        };

        wait_condition_.wait_for(
            lock,
            interval_,
            [this] {
                return
                    stop_requested_.load();
            }
        );
    }
}

}  // namespace radahn::worker_app