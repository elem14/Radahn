#!/usr/bin/env bash

set -euo pipefail

# A single, repeatable demonstration of the whole MVP end to end
# (steps 20-21 of the project plan): one coordinator, two workers
# with distinct tags/capacities, mixed job outcomes (success, a
# nonzero exit code, a timeout), concurrency, tag-constrained
# routing, stdout/stderr retrieval, a worker crash with recovery on
# the surviving worker, and a coordinator restart against the same
# database that proves results and logs are durable.
#
# This also exercises the explicit configuration added in step 21:
# the coordinator is given a non-default --listen-address, and both
# the CLI and the workers are pointed at it with an explicit address
# rather than relying on any hardcoded default - the same shape of
# configuration two separate machines would use to talk to each
# other over a real network.

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

# A non-default port both proves --listen-address works and avoids
# colliding with any coordinator a developer already has running on
# the default 50051.
COORDINATOR_ADDRESS="127.0.0.1:50071"
COORDINATOR_PORT="${COORDINATOR_ADDRESS##*:}"

TEMP_DIRECTORY="$(mktemp -d)"

DATABASE_PATH="${TEMP_DIRECTORY}/radahn-delta.db"

COORDINATOR_LOG="${TEMP_DIRECTORY}/coordinator.log"
COORDINATOR_RESTART_LOG="${TEMP_DIRECTORY}/coordinator-restart.log"

WORKER_ALPHA_ID="delta-worker-alpha"
WORKER_BETA_ID="delta-worker-beta"

WORKER_ALPHA_LOG="${TEMP_DIRECTORY}/${WORKER_ALPHA_ID}.log"
WORKER_BETA_LOG="${TEMP_DIRECTORY}/${WORKER_BETA_ID}.log"

POLL_OUTPUT="${TEMP_DIRECTORY}/poll.out"
LOG_RETRIEVAL_OUTPUT="${TEMP_DIRECTORY}/retrieved.out"

RUN_ID="$(date +%s)-$$"

JOB_SUCCESS_ID="delta-success-${RUN_ID}"
JOB_EXIT7_ID="delta-exit7-${RUN_ID}"
JOB_TIMEOUT_ID="delta-timeout-${RUN_ID}"
JOB_ALPHA_ONLY_ID="delta-alpha-only-${RUN_ID}"
JOB_CRASH_TARGET_ID="delta-crash-target-${RUN_ID}"

CONCURRENCY_JOB_IDS=(
    "delta-concurrent-1-${RUN_ID}"
    "delta-concurrent-2-${RUN_ID}"
    "delta-concurrent-3-${RUN_ID}"
    "delta-concurrent-4-${RUN_ID}"
)

COORDINATOR_PID=""
WORKER_ALPHA_PID=""
WORKER_BETA_PID=""

cleanup() {
    local exit_status=$?

    trap - EXIT INT TERM

    for pid in \
        "${WORKER_ALPHA_PID}" \
        "${WORKER_BETA_PID}" \
        "${COORDINATOR_PID}"
    do
        if [[ -n "${pid}" ]]; then
            kill "${pid}" 2>/dev/null || true
            wait "${pid}" 2>/dev/null || true
        fi
    done

    if [[ ${exit_status} -ne 0 ]]; then
        echo
        echo "Delta MVP demo failed."
        echo

        for log_file in \
            "${COORDINATOR_LOG}" \
            "${COORDINATOR_RESTART_LOG}" \
            "${WORKER_ALPHA_LOG}" \
            "${WORKER_BETA_LOG}" \
            "${POLL_OUTPUT}" \
            "${LOG_RETRIEVAL_OUTPUT}"
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

# Always talks to the coordinator via the explicit address chosen
# above, exactly as a client on a separate machine would.
cli() {
    "${CLI_BINARY}" --coordinator "${COORDINATOR_ADDRESS}" "$@"
}

wait_for_coordinator_ready() {
    local max_attempts="$1"

    for ((attempt = 1; attempt <= max_attempts; ++attempt)); do
        if cli ping delta-mvp-demo >/dev/null 2>&1; then
            return 0
        fi

        sleep 0.25
    done

    return 1
}

wait_for_log_pattern() {
    local log_file="$1"
    local pattern="$2"
    local max_attempts="$3"

    for ((attempt = 1; attempt <= max_attempts; ++attempt)); do
        if grep -q "${pattern}" "${log_file}" 2>/dev/null; then
            return 0
        fi

        sleep 0.25
    done

    return 1
}

wait_for_job_state() {
    local job_id="$1"
    local expected_substring="$2"
    local max_attempts="$3"
    local sleep_seconds="$4"

    for ((attempt = 1; attempt <= max_attempts; ++attempt)); do
        if cli job get "${job_id}" >"${POLL_OUTPUT}" 2>&1; then
            if grep -q "${expected_substring}" "${POLL_OUTPUT}"; then
                return 0
            fi
        fi

        sleep "${sleep_seconds}"
    done

    return 1
}

echo "========================================"
echo "Radahn Delta MVP demo"
echo "========================================"
echo

echo "[1/15] Checking that ${COORDINATOR_ADDRESS} is free"

if lsof -nP -iTCP:"${COORDINATOR_PORT}" -sTCP:LISTEN -t \
    >/dev/null 2>&1
then
    echo "Port ${COORDINATOR_PORT} is already in use."
    echo "Stop whatever is using it and retry."
    exit 1
fi

echo "[2/15] Configuring and building Radahn"

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
echo "[3/15] Starting coordinator on an explicit listen address"

"${COORDINATOR_BINARY}" \
    "${DATABASE_PATH}" \
    --listen-address "${COORDINATOR_ADDRESS}" \
    >"${COORDINATOR_LOG}" 2>&1 &

COORDINATOR_PID=$!

echo
echo "[4/15] Waiting for coordinator readiness via ping"

if ! wait_for_coordinator_ready 40; then
    echo "Coordinator did not become ready."
    exit 1
fi

echo "Coordinator is ready at ${COORDINATOR_ADDRESS}."

echo
echo "[5/15] Starting two workers with different tags and capacities"

"${WORKER_BINARY}" \
    "${WORKER_ALPHA_ID}" \
    "${COORDINATOR_ADDRESS}" \
    --cpu 2 \
    --memory-mib 2048 \
    --disk-mib 10240 \
    --tag alpha \
    --max-concurrent-jobs 2 \
    >"${WORKER_ALPHA_LOG}" 2>&1 &

WORKER_ALPHA_PID=$!

"${WORKER_BINARY}" \
    "${WORKER_BETA_ID}" \
    "${COORDINATOR_ADDRESS}" \
    --cpu 4 \
    --memory-mib 4096 \
    --disk-mib 20480 \
    --tag beta \
    --max-concurrent-jobs 4 \
    >"${WORKER_BETA_LOG}" 2>&1 &

WORKER_BETA_PID=$!

if ! wait_for_log_pattern "${WORKER_ALPHA_LOG}" "Worker ready" 40; then
    echo "${WORKER_ALPHA_ID} did not become ready."
    exit 1
fi

if ! wait_for_log_pattern "${WORKER_BETA_LOG}" "Worker ready" 40; then
    echo "${WORKER_BETA_ID} did not become ready."
    exit 1
fi

echo "Both workers registered and ready."

echo
echo "[6/15] Submitting a command that succeeds"

cli job submit \
    "${JOB_SUCCESS_ID}" \
    "Delta demo: successful command" \
    50 1 256 512 \
    -- /bin/sh -c \
    "echo stdout-line-ok; echo stderr-line-ok 1>&2; exit 0" \
    >/dev/null 2>&1

if ! wait_for_job_state "${JOB_SUCCESS_ID}" "State: SUCCEEDED" 60 0.25; then
    echo "${JOB_SUCCESS_ID} did not reach SUCCEEDED."
    exit 1
fi

echo "${JOB_SUCCESS_ID} succeeded as expected."

echo
echo "[7/15] Submitting a command that exits with code 7"

cli job submit \
    "${JOB_EXIT7_ID}" \
    "Delta demo: exit code 7" \
    50 1 256 512 \
    -- /bin/sh -c \
    "echo about-to-fail 1>&2; exit 7" \
    >/dev/null 2>&1

if ! wait_for_job_state "${JOB_EXIT7_ID}" "State: FAILED" 60 0.25; then
    echo "${JOB_EXIT7_ID} did not reach FAILED."
    exit 1
fi

if ! grep -q "Exit code: 7" "${POLL_OUTPUT}"; then
    echo "${JOB_EXIT7_ID} did not report exit code 7."
    exit 1
fi

echo "${JOB_EXIT7_ID} failed with exit code 7 as expected."

echo
echo "[8/15] Submitting a command that times out"

cli job submit \
    "${JOB_TIMEOUT_ID}" \
    "Delta demo: timeout" \
    50 1 256 512 \
    --timeout-ms 800 \
    -- /bin/sh -c "sleep 5" \
    >/dev/null 2>&1

if ! wait_for_job_state "${JOB_TIMEOUT_ID}" "State: FAILED" 60 0.25; then
    echo "${JOB_TIMEOUT_ID} did not reach FAILED."
    exit 1
fi

if ! grep -q "Execution outcome: TIMED_OUT" "${POLL_OUTPUT}"; then
    echo "${JOB_TIMEOUT_ID} did not report a TIMED_OUT outcome."
    exit 1
fi

echo "${JOB_TIMEOUT_ID} timed out as expected."

echo
echo "[9/15] Submitting enough jobs to demonstrate concurrency"

for job_id in "${CONCURRENCY_JOB_IDS[@]}"; do
    cli job submit \
        "${job_id}" \
        "Delta demo: concurrency" \
        50 1 256 512 \
        --tag beta \
        -- /bin/sh -c "sleep 3" \
        >/dev/null 2>&1
done

for ((attempt = 1; attempt <= 80; ++attempt)); do
    remaining=0

    for job_id in "${CONCURRENCY_JOB_IDS[@]}"; do
        if ! cli job get "${job_id}" 2>/dev/null \
            | grep -q "State: SUCCEEDED"
        then
            remaining=$((remaining + 1))
        fi
    done

    if [[ ${remaining} -eq 0 ]]; then
        break
    fi

    sleep 0.25
done

for job_id in "${CONCURRENCY_JOB_IDS[@]}"; do
    if ! cli job get "${job_id}" 2>/dev/null \
        | grep -q "State: SUCCEEDED"
    then
        echo "${job_id} did not reach SUCCEEDED."
        exit 1
    fi
done

first_completion_line=$(
    grep -n "completed with state SUCCEEDED" "${WORKER_BETA_LOG}" \
        | head -1 \
        | cut -d: -f1
)

if [[ -z "${first_completion_line}" ]]; then
    echo "No concurrency job ever completed on ${WORKER_BETA_ID}."
    exit 1
fi

concurrent_starts=$(
    head -n "${first_completion_line}" "${WORKER_BETA_LOG}" \
        | grep -c "Job entered RUNNING state"
)

if [[ "${concurrent_starts}" -lt 2 ]]; then
    echo "Expected at least 2 jobs RUNNING on ${WORKER_BETA_ID} before"
    echo "its first completion; saw ${concurrent_starts}."
    exit 1
fi

echo "${concurrent_starts} jobs ran concurrently on ${WORKER_BETA_ID}."

echo
echo "[10/15] Verifying tag-constrained routing"

for job_id in "${CONCURRENCY_JOB_IDS[@]}"; do
    if ! grep -q "ID: ${job_id}$" "${WORKER_BETA_LOG}"; then
        echo "Expected ${job_id} on ${WORKER_BETA_ID}."
        exit 1
    fi

    if grep -q "ID: ${job_id}$" "${WORKER_ALPHA_LOG}"; then
        echo "${job_id} leaked onto ${WORKER_ALPHA_ID} (wrong tag)."
        exit 1
    fi
done

cli job submit \
    "${JOB_ALPHA_ONLY_ID}" \
    "Delta demo: alpha-only" \
    50 1 256 512 \
    --tag alpha \
    >/dev/null 2>&1

if ! wait_for_job_state "${JOB_ALPHA_ONLY_ID}" "State: SUCCEEDED" 60 0.25; then
    echo "${JOB_ALPHA_ONLY_ID} did not reach SUCCEEDED."
    exit 1
fi

if ! grep -q "ID: ${JOB_ALPHA_ONLY_ID}$" "${WORKER_ALPHA_LOG}"; then
    echo "Expected ${JOB_ALPHA_ONLY_ID} on ${WORKER_ALPHA_ID}."
    exit 1
fi

if grep -q "ID: ${JOB_ALPHA_ONLY_ID}$" "${WORKER_BETA_LOG}"; then
    echo "${JOB_ALPHA_ONLY_ID} leaked onto ${WORKER_BETA_ID} (wrong tag)."
    exit 1
fi

echo "Tag-constrained routing held in both directions."

echo
echo "[11/15] Retrieving stdout and stderr of the successful job"

cli job logs "${JOB_SUCCESS_ID}" \
    >"${LOG_RETRIEVAL_OUTPUT}" 2>/dev/null

if ! grep -q "stdout-line-ok" "${LOG_RETRIEVAL_OUTPUT}"; then
    echo "Did not retrieve the expected stdout content."
    exit 1
fi

cli job logs "${JOB_SUCCESS_ID}" --stderr \
    >"${LOG_RETRIEVAL_OUTPUT}" 2>/dev/null

if ! grep -q "stderr-line-ok" "${LOG_RETRIEVAL_OUTPUT}"; then
    echo "Did not retrieve the expected stderr content."
    exit 1
fi

echo "stdout and stderr both retrieved successfully."

echo
echo "[12/15] Killing one worker during a suitable test job"

# "sleep 4" is bounded and harmless: if it is orphaned by the
# worker's abrupt death below, it simply keeps running under init
# and exits on its own a few seconds later - there is no safe way to
# learn its OS pid from outside (the coordinator/worker protocol
# never exposes it), and broad process-name cleanup is off the
# table, so a self-terminating child is the deliberate choice here
# rather than something this script has to track or reap.
cli job submit \
    "${JOB_CRASH_TARGET_ID}" \
    "Delta demo: crash target" \
    50 1 256 512 \
    -- /bin/sh -c "sleep 4" \
    >/dev/null 2>&1

crash_worker_name=""
crash_worker_pid=""
crash_worker_log=""

for ((attempt = 1; attempt <= 40; ++attempt)); do
    if grep -q "ID: ${JOB_CRASH_TARGET_ID}$" "${WORKER_ALPHA_LOG}"; then
        crash_worker_name="${WORKER_ALPHA_ID}"
        crash_worker_pid="${WORKER_ALPHA_PID}"
        crash_worker_log="${WORKER_ALPHA_LOG}"
        break
    fi

    if grep -q "ID: ${JOB_CRASH_TARGET_ID}$" "${WORKER_BETA_LOG}"; then
        crash_worker_name="${WORKER_BETA_ID}"
        crash_worker_pid="${WORKER_BETA_PID}"
        crash_worker_log="${WORKER_BETA_LOG}"
        break
    fi

    sleep 0.25
done

if [[ -z "${crash_worker_name}" ]]; then
    echo "${JOB_CRASH_TARGET_ID} was never acquired by any worker."
    exit 1
fi

echo "${JOB_CRASH_TARGET_ID} landed on ${crash_worker_name};" \
    "simulating a hard crash (SIGKILL)."

# A hard kill, not graceful SIGTERM - this is the "worker just
# disappears" failure mode the coordinator's lease expiry exists
# to recover from.
kill -KILL "${crash_worker_pid}"
wait "${crash_worker_pid}" 2>/dev/null || true

surviving_worker_name=""
surviving_worker_log=""

if [[ "${crash_worker_name}" == "${WORKER_ALPHA_ID}" ]]; then
    WORKER_ALPHA_PID=""
    surviving_worker_name="${WORKER_BETA_ID}"
    surviving_worker_log="${WORKER_BETA_LOG}"
else
    WORKER_BETA_PID=""
    surviving_worker_name="${WORKER_ALPHA_ID}"
    surviving_worker_log="${WORKER_ALPHA_LOG}"
fi

echo
echo "[13/15] Verifying recovery on the surviving worker"
echo "Waiting for lease expiry, requeue, and pickup by" \
    "${surviving_worker_name}..."

if ! wait_for_job_state "${JOB_CRASH_TARGET_ID}" "State: SUCCEEDED" 90 0.5; then
    echo "${JOB_CRASH_TARGET_ID} never recovered after its worker crashed."
    exit 1
fi

if ! grep -q "Attempts: 2 /" "${POLL_OUTPUT}"; then
    echo "Expected ${JOB_CRASH_TARGET_ID} to have needed a second attempt."
    cat "${POLL_OUTPUT}"
    exit 1
fi

if ! grep -q "ID: ${JOB_CRASH_TARGET_ID}$" "${surviving_worker_log}"; then
    echo "Expected ${surviving_worker_name} to have picked up" \
        "${JOB_CRASH_TARGET_ID} after the crash."
    exit 1
fi

echo "${JOB_CRASH_TARGET_ID} survived its worker crashing: the" \
    "coordinator reclaimed its lease, requeued it, and" \
    "${surviving_worker_name} completed it on a second attempt."

echo
echo "[14/15] Restarting the coordinator using the same database"

# Simplify the restart by stopping the surviving worker first -
# nothing new is submitted after the restart, so there is no need
# for a live worker to be connected while that is verified, and
# this keeps the restart itself from depending on how a worker's
# gRPC channel behaves while its coordinator is briefly down.
if [[ "${crash_worker_name}" == "${WORKER_ALPHA_ID}" ]]; then
    kill -TERM "${WORKER_BETA_PID}"
    wait "${WORKER_BETA_PID}" 2>/dev/null || true
    WORKER_BETA_PID=""
else
    kill -TERM "${WORKER_ALPHA_PID}"
    wait "${WORKER_ALPHA_PID}" 2>/dev/null || true
    WORKER_ALPHA_PID=""
fi

kill -TERM "${COORDINATOR_PID}"
wait "${COORDINATOR_PID}" 2>/dev/null || true
COORDINATOR_PID=""

"${COORDINATOR_BINARY}" \
    "${DATABASE_PATH}" \
    --listen-address "${COORDINATOR_ADDRESS}" \
    >"${COORDINATOR_RESTART_LOG}" 2>&1 &

COORDINATOR_PID=$!

if ! wait_for_coordinator_ready 40; then
    echo "Restarted coordinator did not become ready."
    exit 1
fi

echo "Coordinator restarted against the same database and is ready."

echo
echo "[15/15] Verifying completed results and uploaded logs remain available"

if ! wait_for_job_state "${JOB_SUCCESS_ID}" "State: SUCCEEDED" 20 0.25; then
    echo "${JOB_SUCCESS_ID} did not remain SUCCEEDED after restart."
    exit 1
fi

if ! wait_for_job_state "${JOB_EXIT7_ID}" "State: FAILED" 20 0.25; then
    echo "${JOB_EXIT7_ID} did not remain FAILED after restart."
    exit 1
fi

if ! wait_for_job_state "${JOB_TIMEOUT_ID}" "State: FAILED" 20 0.25; then
    echo "${JOB_TIMEOUT_ID} did not remain FAILED after restart."
    exit 1
fi

if ! wait_for_job_state "${JOB_CRASH_TARGET_ID}" "State: SUCCEEDED" 20 0.25; then
    echo "${JOB_CRASH_TARGET_ID} did not remain SUCCEEDED after restart."
    exit 1
fi

cli job logs "${JOB_SUCCESS_ID}" \
    >"${LOG_RETRIEVAL_OUTPUT}" 2>/dev/null

if ! grep -q "stdout-line-ok" "${LOG_RETRIEVAL_OUTPUT}"; then
    echo "stdout log for ${JOB_SUCCESS_ID} did not survive the restart."
    exit 1
fi

cli job logs "${JOB_SUCCESS_ID}" --stderr \
    >"${LOG_RETRIEVAL_OUTPUT}" 2>/dev/null

if ! grep -q "stderr-line-ok" "${LOG_RETRIEVAL_OUTPUT}"; then
    echo "stderr log for ${JOB_SUCCESS_ID} did not survive the restart."
    exit 1
fi

echo "All results and uploaded logs survived the coordinator restart."

echo
echo "========================================"
echo "Delta MVP demo PASSED"
echo "========================================"
