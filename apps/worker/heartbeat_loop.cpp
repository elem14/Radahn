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
    const ActiveJobRegistry& active_jobs,
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
      active_jobs_{active_jobs},
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

        const auto active_snapshot =
            active_jobs_.snapshot();

        for (
            const auto& attempt :
            active_snapshot
        ) {
            auto* active_attempt =
                request.add_active_attempts();

            active_attempt->set_job_id(
                attempt.job_id
            );

            active_attempt->set_attempt_number(
                attempt.attempt_number
            );
        }

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

            /*
             * Renewal is never inferred merely because the RPC
             * returned OK - only the attempts the coordinator
             * explicitly names here were actually renewed.
             */
            if (
                static_cast<std::size_t>(
                    response.renewed_attempts_size()
                ) != active_snapshot.size()
            ) {
                std::cerr
                    << "Heartbeat renewed "
                    << response.renewed_attempts_size()
                    << " of "
                    << active_snapshot.size()
                    << " reported attempt(s)\n";
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