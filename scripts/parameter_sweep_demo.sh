#!/usr/bin/env bash

set -euo pipefail

# A small, concrete demonstration of "useful parallel execution":
# submits the same real program (examples/parameter_sweep_workload.py)
# as several independent Radahn jobs, each with a different parameter
# value, across two workers with real concurrency. It then compares
# the wall-clock time of the whole sweep against the sum of each
# job's own reported runtime, which is only ever small because the
# jobs ran concurrently rather than one after another.
#
# This is a demonstration script, not a correctness test: it starts
# its own throwaway coordinator and workers (as scripts/delta_mvp_demo.sh
# does) so it can be run from a clean checkout using only the README.

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

WORKLOAD_SCRIPT="${PROJECT_ROOT}/examples/parameter_sweep_workload.py"

PYTHON3_BINARY="$(command -v python3 || true)"

if [[ -z "${PYTHON3_BINARY}" ]]; then
    echo "python3 is required to run this demo and was not found."
    exit 1
fi

COORDINATOR_ADDRESS="127.0.0.1:50081"
COORDINATOR_PORT="${COORDINATOR_ADDRESS##*:}"

TEMP_DIRECTORY="$(mktemp -d)"

DATABASE_PATH="${TEMP_DIRECTORY}/radahn-sweep.db"
COORDINATOR_LOG="${TEMP_DIRECTORY}/coordinator.log"
WORKER_ONE_LOG="${TEMP_DIRECTORY}/worker-one.log"
WORKER_TWO_LOG="${TEMP_DIRECTORY}/worker-two.log"

RUN_ID="$(date +%s)-$$"

# Six independent parameter values sweeping over the same program.
# Each takes on the order of half a second to a bit over a second,
# chosen so the sweep's parallel speedup is visible without making
# the demo slow to run.
PARAMETER_VALUES=(500000 650000 800000 900000 950000 1000000)

COORDINATOR_PID=""
WORKER_ONE_PID=""
WORKER_TWO_PID=""

cleanup() {
    local exit_status=$?

    trap - EXIT INT TERM

    for pid in "${WORKER_ONE_PID}" "${WORKER_TWO_PID}" "${COORDINATOR_PID}"; do
        if [[ -n "${pid}" ]]; then
            kill "${pid}" 2>/dev/null || true
            wait "${pid}" 2>/dev/null || true
        fi
    done

    if [[ ${exit_status} -ne 0 ]]; then
        echo
        echo "Parameter sweep demo failed."
        echo

        for log_file in "${COORDINATOR_LOG}" "${WORKER_ONE_LOG}" "${WORKER_TWO_LOG}"; do
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

cli() {
    "${CLI_BINARY}" --coordinator "${COORDINATOR_ADDRESS}" "$@"
}

wait_for_coordinator_ready() {
    local max_attempts="$1"

    for ((attempt = 1; attempt <= max_attempts; ++attempt)); do
        if cli ping parameter-sweep-demo >/dev/null 2>&1; then
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

echo "========================================"
echo "Radahn parameter-sweep demo"
echo "========================================"
echo

echo "[1/5] Checking that ${COORDINATOR_ADDRESS} is free"

if lsof -nP -iTCP:"${COORDINATOR_PORT}" -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo "Port ${COORDINATOR_PORT} is already in use."
    echo "Stop whatever is using it and retry."
    exit 1
fi

echo "[2/5] Configuring and building Radahn"

cmake --preset debug
cmake --build --preset debug

for binary in "${COORDINATOR_BINARY}" "${CLI_BINARY}" "${WORKER_BINARY}"; do
    if [[ ! -x "${binary}" ]]; then
        echo "Expected executable was not found: ${binary}"
        exit 1
    fi
done

echo
echo "[3/5] Starting a coordinator and two workers"

"${COORDINATOR_BINARY}" \
    "${DATABASE_PATH}" \
    --listen-address "${COORDINATOR_ADDRESS}" \
    >"${COORDINATOR_LOG}" 2>&1 &
COORDINATOR_PID=$!

if ! wait_for_coordinator_ready 40; then
    echo "Coordinator did not become ready."
    exit 1
fi

"${WORKER_BINARY}" \
    "sweep-worker-one" \
    "${COORDINATOR_ADDRESS}" \
    --cpu 2 \
    --max-concurrent-jobs 2 \
    >"${WORKER_ONE_LOG}" 2>&1 &
WORKER_ONE_PID=$!

"${WORKER_BINARY}" \
    "sweep-worker-two" \
    "${COORDINATOR_ADDRESS}" \
    --cpu 2 \
    --max-concurrent-jobs 2 \
    >"${WORKER_TWO_LOG}" 2>&1 &
WORKER_TWO_PID=$!

if ! wait_for_log_pattern "${WORKER_ONE_LOG}" "Worker ready" 40; then
    echo "sweep-worker-one did not become ready."
    exit 1
fi

if ! wait_for_log_pattern "${WORKER_TWO_LOG}" "Worker ready" 40; then
    echo "sweep-worker-two did not become ready."
    exit 1
fi

echo "Coordinator and both workers are ready."

echo
echo "[4/5] Submitting ${#PARAMETER_VALUES[@]} independent parameter-sweep jobs"

SWEEP_START="$(date +%s)"

job_ids=()

for n in "${PARAMETER_VALUES[@]}"; do
    job_id="sweep-${n}-${RUN_ID}"
    job_ids+=("${job_id}")

    cli job submit \
        "${job_id}" \
        "Parameter sweep n=${n}" \
        50 1 128 128 \
        -- "${PYTHON3_BINARY}" -I "${WORKLOAD_SCRIPT}" "${n}" \
        >/dev/null
done

echo "Submitted: ${job_ids[*]}"

echo
echo "Waiting for every job to reach a terminal state..."

for job_id in "${job_ids[@]}"; do
    for ((attempt = 1; attempt <= 240; ++attempt)); do
        state_output="$(cli job get "${job_id}" 2>/dev/null || true)"

        if grep -q "State: SUCCEEDED" <<<"${state_output}"; then
            break
        fi

        if grep -q "State: FAILED" <<<"${state_output}"; then
            echo "${job_id} failed unexpectedly:"
            echo "${state_output}"
            exit 1
        fi

        sleep 0.25
    done

    if ! grep -q "State: SUCCEEDED" <<<"${state_output}"; then
        echo "${job_id} did not reach SUCCEEDED in time."
        exit 1
    fi
done

SWEEP_WALL_SECONDS=$(( $(date +%s) - SWEEP_START ))

echo "All ${#job_ids[@]} jobs succeeded."

echo
echo "[5/5] Collecting each job's result from its stdout log"

sum_of_elapsed="0"

for job_id in "${job_ids[@]}"; do
    output_line="$(cli job logs "${job_id}" 2>/dev/null | tail -1)"
    echo "  ${job_id}: ${output_line}"

    job_elapsed="$(sed -n 's/.*elapsed_s=\([0-9.]*\).*/\1/p' <<<"${output_line}")"

    if [[ -n "${job_elapsed}" ]]; then
        sum_of_elapsed="$(awk -v a="${sum_of_elapsed}" -v b="${job_elapsed}" 'BEGIN { printf "%.2f", a + b }')"
    fi
done

echo
echo "Sweep wall-clock time:        ~${SWEEP_WALL_SECONDS}s"
echo "Sum of each job's own runtime: ${sum_of_elapsed}s"
echo "(the sweep's wall-clock time is well under the sum because the"
echo " jobs ran concurrently across two workers, not one after another)"

echo
echo "========================================"
echo "Parameter sweep demo PASSED"
echo "========================================"
