#!/usr/bin/env bash

set -euo pipefail

# Resolve the repository root regardless of the directory
# from which this script is called.
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

DATABASE_PATH="${TEMP_DIRECTORY}/radahn-smoke.db"

COORDINATOR_LOG="${TEMP_DIRECTORY}/coordinator.log"
SUBMIT_LOG="${TEMP_DIRECTORY}/submit.log"
WORKER_LOG="${TEMP_DIRECTORY}/worker.log"
FINAL_JOB_LOG="${TEMP_DIRECTORY}/final-job.log"

COORDINATOR_PID=""

cleanup() {
    local exit_status=$?

    # Prevent the cleanup function from recursively invoking itself.
    trap - EXIT INT TERM

    if [[ -n "${COORDINATOR_PID}" ]]; then
        kill "${COORDINATOR_PID}" 2>/dev/null || true
        wait "${COORDINATOR_PID}" 2>/dev/null || true
    fi

    if [[ ${exit_status} -ne 0 ]]; then
        echo
        echo "Milestone 2 smoke test failed."
        echo

        for log_file in \
            "${COORDINATOR_LOG}" \
            "${SUBMIT_LOG}" \
            "${WORKER_LOG}" \
            "${FINAL_JOB_LOG}"
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
echo "Radahn Milestone 2 smoke test"
echo "========================================"
echo

echo "[1/7] Checking coordinator port"

if lsof -nP -iTCP:50051 -sTCP:LISTEN -t \
    >/dev/null 2>&1
then
    echo "Port 50051 is already in use."
    echo "Stop the currently running coordinator and retry."
    exit 1
fi

echo "[2/7] Configuring debug build"

cmake --preset debug

echo
echo "[3/7] Building Radahn"

cmake --build --preset debug

echo
echo "[4/7] Running unit tests"

ctest \
    --preset debug \
    --output-on-failure

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
echo "[5/7] Starting coordinator"

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

case "$(uname -s)" in
    Darwin)
        operating_system_tag="macos"
        ;;

    Linux)
        operating_system_tag="linux"
        ;;

    *)
        echo "Unsupported operating system: $(uname -s)"
        exit 1
        ;;
esac

case "$(uname -m)" in
    arm64 | aarch64)
        architecture_tag="arm64"
        ;;

    x86_64 | amd64)
        architecture_tag="x86_64"
        ;;

    *)
        echo "Unsupported architecture: $(uname -m)"
        exit 1
        ;;
esac

job_id="milestone-2-smoke-$(date +%s)"
worker_id="milestone-2-smoke-worker"

echo
echo "[6/7] Submitting and executing ${job_id}"

"${CLI_BINARY}" job submit \
    "${job_id}" \
    "Milestone 2 automated smoke test" \
    50 \
    1 \
    512 \
    1024 \
    --tag "${operating_system_tag}" \
    --tag "${architecture_tag}" \
    >"${SUBMIT_LOG}" 2>&1

"${WORKER_BINARY}" \
    "${worker_id}" \
    localhost:50051 \
    >"${WORKER_LOG}" 2>&1

echo
echo "[7/7] Verifying final job state"

"${CLI_BINARY}" job get "${job_id}" \
    >"${FINAL_JOB_LOG}" 2>&1

if ! grep -q "State: SUCCEEDED" \
    "${FINAL_JOB_LOG}"
then
    echo "Job did not reach SUCCEEDED."
    exit 1
fi

if ! grep -q "Job entered RUNNING state" \
    "${WORKER_LOG}"
then
    echo "Worker never reported the RUNNING state."
    exit 1
fi

if ! grep -q "Executing sleep workload" \
    "${WORKER_LOG}"
then
    echo "Worker did not execute the sleep workload."
    exit 1
fi

if ! grep -q "Job completed with state SUCCEEDED" \
    "${WORKER_LOG}"
then
    echo "Worker did not report successful completion."
    exit 1
fi

if [[ ! -s "${DATABASE_PATH}" ]]; then
    echo "Coordinator did not create a SQLite database file."
    exit 1
fi

echo
cat "${WORKER_LOG}"

echo
cat "${FINAL_JOB_LOG}"

echo
echo "========================================"
echo "Milestone 2 smoke test PASSED"
echo "========================================"