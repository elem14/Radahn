#pragma once

#include <cstddef>
#include <optional>
#include <string>

#include "radahn/domain/id.hpp"

namespace radahn::scheduler {
    struct SchedulingDecision {
        domain::WorkerId worker_id;
        std::string policy_name;
        std::size_t eligible_worker_count;
        std::optional<double> score;
    };

} //namespace radahn::scheduler