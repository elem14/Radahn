#pragma once

#include <optional>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"

namespace radahn::persistence {

/*
 * The domain::Job contains the job's actual domain data:
 *
 * - ID
 * - name
 * - priority
 * - requirements
 * - workload
 * - state
 * - creation time
 *
 * The repository record additionally stores which worker,
 * if any, currently owns the job.
 */
struct JobRecord {
    domain::Job job;

    std::optional<domain::WorkerId>
        assigned_worker_id;
};

}  // namespace radahn::persistence