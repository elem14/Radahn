#!/usr/bin/env bash

set -euo pipefail

# Stands up a simulated heterogeneous 3-worker cluster (one
# physical machine, three radahn-worker processes each reporting
# different resources/tags via the --cpu/--memory-mib/--disk-mib/
# --gpu/--tag/--max-concurrent-jobs flags added for this purpose),
# drives mixed job types through it, and verifies:
#
#   1. Jobs route only to workers whose tags/GPU/CPU actually
#      satisfy their requirements (heterogeneous placement).
#   2. Multiple jobs run concurrently on the same worker, bounded
#      by that worker's own max_concurrent_jobs (resource sharing).
#   3. Every submitted job reaches SUCCEEDED — if the coordinator
#      ever let a worker's reservations exceed its real capacity,
#      WorkerRecord::reserve() throws and a job would never
#      complete, so "all jobs SUCCEEDED" is itself the capacity
#      safety proof for this run.
#   4. A worker crashing mid-job (SIGKILL, not a graceful
#      shutdown) does not lose the job: its lease expires, the
#      coordinator's liveness sweep reclaims and requeues it, and
#      a surviving worker picks it up and finishes it.
#
# Restart correctness (coordinator process restart preserving job/
# worker state) is not re-verified live here — it already has
# dedicated, faster unit coverage in
# tests/unit/job_reservation_lifecycle_tests.cpp and the sqlite
# coordinator recovery tests.

SCRIPT_DIRECTORY="$(
    cd "$(dirname "${BASH_SOURCE[0]}")" &&
    pwd
)"

PROJECT_ROOT="$(
    cd "${SCRIPT_DIRECTORY}/.." &&
    pwd
)"

BUILD_DIRECTORY="${PROJECT_ROOT}/build/debug"

COORDINATOR_BINARY="${BUILD_DIRECTORY}/apps/coordinator/radahn-coordinator"
CLI_BINARY="${BUILD_DIRECTORY}/apps/cli/radahn"
WORKER_BINARY="${BUILD_DIRECTORY}/apps/worker/radahn-worker"

TEMP_DIRECTORY="$(mktemp -d)"

DATABASE_PATH="${TEMP_DIRECTORY}/radahn-4g.db"

COORDINATOR_LOG="${TEMP_DIRECTORY}/coordinator.log"

WORKER_MACOS_ID="worker-macos-arm"
WORKER_GPU_ID="worker-linux-gpu"
WORKER_CPU_ID="worker-linux-cpu"

WORKER_MACOS_LOG="${TEMP_DIRECTORY}/${WORKER_MACOS_ID}.log"
WORKER_GPU_LOG="${TEMP_DIRECTORY}/${WORKER_GPU_ID}.log"
WORKER_CPU_LOG="${TEMP_DIRECTORY}/${WORKER_CPU_ID}.log"

COORDINATOR_PID=""
WORKER_MACOS_PID=""
WORKER_GPU_PID=""
WORKER_CPU_PID=""

cleanup() {
    local exit_status=$?

    trap - EXIT INT TERM

    for pid in \
        "${WORKER_MACOS_PID}" \
        "${WORKER_GPU_PID}" \
        "${WORKER_CPU_PID}" \
        "${COORDINATOR_PID}"
    do
        if [[ -n "${pid}" ]]; then
            kill "${pid}" 2>/dev/null || true
            wait "${pid}" 2>/dev/null || true
        fi
    done

    if [[ ${exit_status} -ne 0 ]]; then
        echo
        echo "Milestone 4G cluster demo failed."
        echo

        for log_file in \
            "${COORDINATOR_LOG}" \
            "${WORKER_MACOS_LOG}" \
            "${WORKER_GPU_LOG}" \
            "${WORKER_CPU_LOG}"
        do
            if [[ -f "${log_file}" ]]; then
                echo "===== ${log_file} ====="
                cat "${log_file}"
                echo
            fi
        done
    fi

    rm -rf "${TEMP_DIRECTORY}"

    exit "${exit_status}"
}

trap cleanup EXIT INT TERM

cd "${PROJECT_ROOT}"

echo "========================================"
echo "Radahn Milestone 4G cluster demo"
echo "========================================"
echo

echo "[1/9] Checking coordinator port"

if lsof -nP -iTCP:50051 -sTCP:LISTEN -t \
    >/dev/null 2>&1
then
    echo "Port 50051 is already in use."
    echo "Stop the currently running coordinator and retry."
    exit 1
fi

echo "[2/9] Configuring and building Radahn"

cmake --preset debug
cmake --build --preset debug

for binary in \
    "${COORDINATOR_BINARY}" \
    "${CLI_BINARY}" \
    "${WORKER_BINARY}"
do
    if [[ ! -x "${binary}" ]]; then
        echo "Expected executable was not found:"
        echo "${binary}"
        exit 1
    fi
done

echo
echo "[3/9] Starting coordinator"

"${COORDINATOR_BINARY}" \
    "${DATABASE_PATH}" \
    >"${COORDINATOR_LOG}" 2>&1 &

COORDINATOR_PID=$!

coordinator_ready=false

for attempt in {1..20}
do
    if "${CLI_BINARY}" ping smoke-test \
        >/dev/null 2>&1
    then
        coordinator_ready=true
        break
    fi

    sleep 0.25
done

if [[ "${coordinator_ready}" != true ]]; then
    echo "Coordinator did not become ready."
    exit 1
fi

echo "Coordinator is ready."

echo
echo "[4/9] Starting three simulated heterogeneous workers"

# A small macOS arm64 worker, with no GPU.
"${WORKER_BINARY}" \
    "${WORKER_MACOS_ID}" \
    localhost:50051 \
    --cpu 4 \
    --memory-mib 8192 \
    --disk-mib 51200 \
    --tag macos \
    --tag arm64 \
    --max-concurrent-jobs 4 \
    >"${WORKER_MACOS_LOG}" 2>&1 &

WORKER_MACOS_PID=$!

# A large Linux x86_64 worker with a GPU.
"${WORKER_BINARY}" \
    "${WORKER_GPU_ID}" \
    localhost:50051 \
    --cpu 16 \
    --memory-mib 65536 \
    --disk-mib 512000 \
    --gpu \
    --tag linux \
    --tag x86_64 \
    --tag gpu \
    --max-concurrent-jobs 8 \
    >"${WORKER_GPU_LOG}" 2>&1 &

WORKER_GPU_PID=$!

# A mid-size Linux x86_64 worker, no GPU.
"${WORKER_BINARY}" \
    "${WORKER_CPU_ID}" \
    localhost:50051 \
    --cpu 8 \
    --memory-mib 32768 \
    --disk-mib 256000 \
    --tag linux \
    --tag x86_64 \
    --max-concurrent-jobs 4 \
    >"${WORKER_CPU_LOG}" 2>&1 &

WORKER_CPU_PID=$!

for worker_log in \
    "${WORKER_MACOS_LOG}" \
    "${WORKER_GPU_LOG}" \
    "${WORKER_CPU_LOG}"
do
    worker_ready=false

    for attempt in {1..40}
    do
        if grep -q "Worker ready" "${worker_log}" 2>/dev/null
        then
            worker_ready=true
            break
        fi

        sleep 0.25
    done

    if [[ "${worker_ready}" != true ]]; then
        echo "Worker did not become ready: ${worker_log}"
        exit 1
    fi
done

echo "All three workers registered and ready."

echo
echo "[5/9] Submitting mixed heterogeneous job types"

# Three small jobs tagged "gpu" — only worker-linux-gpu carries
# that tag, so all three are forced onto it, proving multiple jobs
# genuinely run concurrently on one worker.
for index in 1 2 3
do
    "${CLI_BINARY}" job submit \
        "small-packed-${index}" \
        "Small packed job ${index}" \
        50 \
        0.5 \
        256 \
        128 \
        --tag gpu \
        >/dev/null
done

# Requires an actual GPU — only worker-linux-gpu qualifies.
"${CLI_BINARY}" job submit \
    "gpu-required-job" \
    "GPU-required job" \
    50 \
    1 \
    512 \
    128 \
    --gpu \
    --tag linux \
    >/dev/null

# Needs 6 CPU cores and a Linux tag — fits either Linux worker,
# but never the 4-core macOS worker.
"${CLI_BINARY}" job submit \
    "large-linux-job" \
    "Large Linux CPU job" \
    50 \
    6 \
    4096 \
    1024 \
    --tag linux \
    >/dev/null

# Requires arm64 — only worker-macos-arm qualifies.
"${CLI_BINARY}" job submit \
    "arm-only-job" \
    "arm64-only job" \
    50 \
    1 \
    512 \
    128 \
    --tag arm64 \
    >/dev/null

echo "Submitted 6 jobs across GPU, CPU-size, and architecture requirements."

echo
echo "[6/9] Waiting for all 6 jobs to reach SUCCEEDED"

all_job_ids=(
    small-packed-1
    small-packed-2
    small-packed-3
    gpu-required-job
    large-linux-job
    arm-only-job
)

for attempt in {1..80}
do
    remaining=0

    for job_id in "${all_job_ids[@]}"
    do
        if ! "${CLI_BINARY}" job get "${job_id}" 2>/dev/null \
            | grep -q "State: SUCCEEDED"
        then
            remaining=$((remaining + 1))
        fi
    done

    if [[ ${remaining} -eq 0 ]]; then
        break
    fi

    sleep 0.5
done

for job_id in "${all_job_ids[@]}"
do
    if ! "${CLI_BINARY}" job get "${job_id}" 2>/dev/null \
        | grep -q "State: SUCCEEDED"
    then
        echo "Job did not reach SUCCEEDED: ${job_id}"
        exit 1
    fi
done

echo "All 6 jobs reached SUCCEEDED (implies no worker was ever"
echo "over-reserved — a real over-reservation throws and a job"
echo "would never complete)."

echo
echo "[7/9] Verifying heterogeneous placement was actually enforced"

# The GPU worker must be the exclusive home for every
# GPU/"gpu"-tag-restricted job.
for job_id in \
    small-packed-1 small-packed-2 small-packed-3 gpu-required-job
do
    if ! grep -q "ID: ${job_id}$" "${WORKER_GPU_LOG}"; then
        echo "Expected ${job_id} on ${WORKER_GPU_ID}, but it wasn't there."
        exit 1
    fi

    for other_log in "${WORKER_MACOS_LOG}" "${WORKER_CPU_LOG}"; do
        if grep -q "ID: ${job_id}$" "${other_log}"; then
            echo "${job_id} leaked onto a worker without the required tag/GPU: ${other_log}"
            exit 1
        fi
    done
done

# arm-only-job must be exclusive to the macOS arm64 worker.
if ! grep -q "ID: arm-only-job$" "${WORKER_MACOS_LOG}"; then
    echo "Expected arm-only-job on ${WORKER_MACOS_ID}, but it wasn't there."
    exit 1
fi

for other_log in "${WORKER_GPU_LOG}" "${WORKER_CPU_LOG}"; do
    if grep -q "ID: arm-only-job$" "${other_log}"; then
        echo "arm-only-job leaked onto a worker without the arm64 tag: ${other_log}"
        exit 1
    fi
done

# large-linux-job must have landed on one of the two Linux
# workers, and never on the macOS worker.
if grep -q "ID: large-linux-job$" "${WORKER_MACOS_LOG}"; then
    echo "large-linux-job leaked onto the macOS worker (wrong tag)."
    exit 1
fi

if ! grep -q "ID: large-linux-job$" "${WORKER_GPU_LOG}" &&
   ! grep -q "ID: large-linux-job$" "${WORKER_CPU_LOG}"
then
    echo "large-linux-job did not land on either Linux worker."
    exit 1
fi

echo "Every job landed only on workers actually eligible for it."

echo
echo "[8/9] Verifying multiple jobs ran concurrently on one worker"

first_completion_line=$(
    grep -n "completed with state SUCCEEDED" "${WORKER_GPU_LOG}" \
        | head -1 \
        | cut -d: -f1
)

if [[ -z "${first_completion_line}" ]]; then
    echo "No job ever completed on ${WORKER_GPU_ID}."
    exit 1
fi

concurrent_starts=$(
    head -n "${first_completion_line}" "${WORKER_GPU_LOG}" \
        | grep -c "Job entered RUNNING state"
)

if [[ "${concurrent_starts}" -lt 2 ]]; then
    echo "Expected at least 2 jobs RUNNING on ${WORKER_GPU_ID} before"
    echo "its first completion; saw ${concurrent_starts}."
    exit 1
fi

echo "${concurrent_starts} jobs were RUNNING concurrently on ${WORKER_GPU_ID}"
echo "before any of them completed."

echo
echo "[9/9] Simulating a worker crash mid-job and verifying reclaim"

"${CLI_BINARY}" job submit \
    "crash-target-job" \
    "Crash target job" \
    50 \
    1 \
    256 \
    128 \
    >/dev/null

crash_worker_name=""
crash_worker_pid=""
crash_worker_log=""

for attempt in {1..40}
do
    if grep -q "ID: crash-target-job$" "${WORKER_MACOS_LOG}"; then
        crash_worker_name="${WORKER_MACOS_ID}"
        crash_worker_pid="${WORKER_MACOS_PID}"
        crash_worker_log="${WORKER_MACOS_LOG}"
        break
    fi

    if grep -q "ID: crash-target-job$" "${WORKER_GPU_LOG}"; then
        crash_worker_name="${WORKER_GPU_ID}"
        crash_worker_pid="${WORKER_GPU_PID}"
        crash_worker_log="${WORKER_GPU_LOG}"
        break
    fi

    if grep -q "ID: crash-target-job$" "${WORKER_CPU_LOG}"; then
        crash_worker_name="${WORKER_CPU_ID}"
        crash_worker_pid="${WORKER_CPU_PID}"
        crash_worker_log="${WORKER_CPU_LOG}"
        break
    fi

    sleep 0.25
done

if [[ -z "${crash_worker_name}" ]]; then
    echo "crash-target-job was never acquired by any worker."
    exit 1
fi

echo "crash-target-job landed on ${crash_worker_name}; simulating a hard crash (SIGKILL)."

# A hard kill, not the graceful SIGTERM path exercised by the
# other scripts — this is the real "worker just disappears"
# failure mode the coordinator's lease expiry exists for.
kill -KILL "${crash_worker_pid}"
wait "${crash_worker_pid}" 2>/dev/null || true

if [[ "${crash_worker_name}" == "${WORKER_MACOS_ID}" ]]; then
    WORKER_MACOS_PID=""
elif [[ "${crash_worker_name}" == "${WORKER_GPU_ID}" ]]; then
    WORKER_GPU_PID=""
else
    WORKER_CPU_PID=""
fi

echo "Waiting for lease expiry, requeue, and pickup by a surviving worker..."

crash_job_succeeded=false

for attempt in {1..90}
do
    if "${CLI_BINARY}" job get crash-target-job 2>/dev/null \
        | grep -q "State: SUCCEEDED"
    then
        crash_job_succeeded=true
        break
    fi

    sleep 0.5
done

if [[ "${crash_job_succeeded}" != true ]]; then
    echo "crash-target-job never recovered after its worker crashed."
    exit 1
fi

final_crash_job_state="$(
    "${CLI_BINARY}" job get crash-target-job 2>/dev/null
)"

if ! grep -q "Attempts: 2 /" <<<"${final_crash_job_state}"; then
    echo "Expected crash-target-job to have needed a second attempt."
    echo "${final_crash_job_state}"
    exit 1
fi

echo "crash-target-job survived its worker crashing: the coordinator"
echo "reclaimed its lease, requeued it, and a surviving worker"
echo "completed it on a second attempt."

echo
echo "========================================"
echo "Milestone 4G cluster demo PASSED"
echo "Milestone 4 (resource-aware scheduling) is done."
echo "========================================"
