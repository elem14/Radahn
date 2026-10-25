#pragma once

#include <initializer_list>
#include <string>
#include <string_view>
#include <utility>

namespace radahn::coordinator {

/*
 * Writes one structured, single-line event to stdout:
 *
 *   event=completion_rejected job_id=example attempt=1 worker_id=w1 reason=stale_attempt
 *
 * Synchronized across every caller (the gRPC handler threads and the
 * background liveness thread all call into InMemoryCoordinator, and
 * CoordinatorServiceImpl itself also logs through this same
 * function) so one event's fields never interleave with another's on
 * the same line. Never logs full output content or the inherited
 * environment - only IDs, attempt numbers, and short reason codes.
 */
void log_event(
    std::string_view event,
    std::initializer_list<
        std::pair<std::string_view, std::string>
    > fields = {}
);

}  // namespace radahn::coordinator
