#pragma once

#include <chrono>
#include <optional>

#include "radahn/domain/id.hpp"
#include "radahn/domain/job.hpp"

namespace radahn::persistence {

/*
 * Lease timestamps use system_clock because they must survive
 * process restarts and be serialized as Unix timestamps
 */
using JobLeaseClock =
    std::chrono::system_clock;

using JobLeaseTimePoint =
    JobLeaseClock::time_point;

/*
 * The domain::Job stores the job's domain data
 *
 * JobRecord additionally stores coordinator-owned persistence
 * metadata
 *
 * - The worker currently assigned to the job
 * - The time at which that assignment lease expires
 */
struct JobRecord {
    domain::Job job;

    std::optional<domain::WorkerId>
        assigned_worker_id;

    /*
     * QUEUED and terminal jobs normally have no lease.
     *
     * LEASED and RUNNING jobs will receive a deadline during
     */
    std::optional<JobLeaseTimePoint>
        lease_expires_at;
};

}  // namespace radahn::persistence