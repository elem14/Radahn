#include "heartbeat_loop.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

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

[[nodiscard]]
bool attempt_was_renewed(
    const AttemptControlPtr& control,
    const rpc::HeartbeatResponse& response
) {
    for (const auto& renewed : response.renewed_attempts()) {
        if (
            renewed.job_id() == control->job_id &&
            renewed.attempt_number() ==
                control->attempt_number
        ) {
            return true;
        }
    }

    return false;
}

}  // namespace

HeartbeatLoop::HeartbeatLoop(
    std::string coordinator_address,
    std::string worker_id,
    const ActiveJobRegistry& active_jobs,
    std::chrono::milliseconds interval,
    std::chrono::milliseconds unhealthy_after
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
      unhealthy_after_{unhealthy_after},
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

bool HeartbeatLoop::is_connectivity_healthy() const noexcept {
    return
        std::chrono::steady_clock::now() -
        last_success_.load() <
        unhealthy_after_;
}

void HeartbeatLoop::run() {
    bool acknowledged_once = false;

    while (
        !stop_requested_.load()
    ) {
        const auto active_snapshot =
            active_jobs_.snapshot();

        grpc::ClientContext context;

        // steady_clock for local deadline math (immune to wall-clock
        // adjustments); system_clock because that is what
        // grpc::ClientContext::set_deadline requires.
        const auto send_time =
            std::chrono::steady_clock::now();

        context.set_deadline(
            std::chrono::system_clock::now() +
            std::chrono::seconds{3}
        );

        rpc::HeartbeatRequest request;

        request.set_worker_id(
            worker_id_
        );

        for (
            const auto& attempt :
            active_snapshot
        ) {
            auto* active_attempt =
                request.add_active_attempts();

            active_attempt->set_job_id(
                attempt->job_id
            );

            active_attempt->set_attempt_number(
                attempt->attempt_number
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

            last_success_.store(
                std::chrono::steady_clock::now()
            );

            const auto acknowledged_deadline =
                send_time +
                std::chrono::milliseconds{
                    response.lease_duration_ms()
                } -
                lease_safety_margin;

            std::size_t renewed_count = 0;

            for (
                const auto& attempt :
                active_snapshot
            ) {
                if (
                    attempt_was_renewed(
                        attempt,
                        response
                    )
                ) {
                    ++renewed_count;

                    attempt->lease_deadline.store(
                        acknowledged_deadline
                    );

                    continue;
                }

                /*
                 * This attempt was reported as active but the
                 * coordinator's response does not list it among the
                 * renewed ones, despite the RPC itself succeeding -
                 * that is an explicit rejection (the coordinator has
                 * looked at this exact attempt and declined it, e.g.
                 * because a newer attempt has superseded it), not a
                 * transient failure. Ask it to stop immediately
                 * rather than waiting for its local deadline.
                 */
                attempt->request_stop();
            }

            if (renewed_count != active_snapshot.size()) {
                std::cerr
                    << "Heartbeat renewed "
                    << renewed_count
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

        /*
         * Whether or not this tick's RPC succeeded, a job still
         * running locally is never proof its lease remains valid -
         * only an acknowledged deadline is. Enforce every attempt's
         * own locally tracked deadline here so repeated heartbeat
         * failure eventually requests termination instead of
         * silently running forever.
         */
        const auto now = std::chrono::steady_clock::now();

        for (
            const auto& attempt :
            active_snapshot
        ) {
            if (
                attempt->phase.load() ==
                    AttemptPhase::executing &&
                now >= attempt->lease_deadline.load()
            ) {
                attempt->request_stop();
            }
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
