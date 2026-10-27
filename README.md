# Radahn

Radahn is a small distributed job-execution cluster: one coordinator
process holds durable job and worker state and dispatches work over
gRPC to any number of independent worker processes, which execute
real commands (or an internal test workload) and report results and
captured output back. A CLI submits jobs, inspects state, and
retrieves logs.

It exists to answer one question cheaply: "I have a batch of
independent jobs and more than one machine (or core) to run them
on - schedule them, run them, retry the ones that fail for
transient reasons, and let me see what happened." It is not a
general-purpose task queue, a container orchestrator, or a
replacement for a real HPC scheduler; see
[Known limitations](#known-limitations).

## What Radahn does

- A **coordinator** accepts job submissions, tracks every job's
  state in a SQLite database, matches jobs to registered workers by
  CPU/memory/disk/GPU/tag requirements, and hands out attempt-fenced
  leases so a worker's stale or duplicate completion report can
  never corrupt a job it no longer holds authority over.
- A **worker** registers with the coordinator, advertises its own
  resource capacity and tags, acquires jobs it is eligible for, runs
  each job's command as a real subprocess (or sleeps, for the
  built-in test workload), enforces an optional timeout, and
  reports the outcome plus captured stdout/stderr back to the
  coordinator.
- A **CLI** (`radahn`) submits jobs, lists and inspects jobs and
  workers, and retrieves a job's captured logs.
- Jobs that fail for a retryable reason (worker crash, lost lease,
  nonzero exit, timeout) are automatically retried up to a
  configured attempt limit, on any eligible worker, not necessarily
  the one that ran the first attempt.
- A coordinator restart against the same database file preserves
  every job's final state, its execution result, and any uploaded
  logs.

## Supported platforms and trusted-network assumptions

- Source builds on **macOS and Linux only** (POSIX). There is no
  Windows support and none is planned for this MVP.
- There is **no authentication and no transport encryption**.
  Coordinator, workers, and the CLI all talk gRPC in the clear.
  Only run a Radahn cluster on a trusted network or over a trusted
  VPN; never expose a coordinator's listen address to an untrusted
  network.
- CPU/memory/disk/GPU/tag values a worker advertises are
  **scheduling reservations only** - Radahn never enforces them as
  OS-level resource limits (no cgroups, no `ulimit`, no container).
  A job that uses more than it declared will not be stopped by
  Radahn for that reason.
- Every simultaneously running worker process must use a **unique
  worker ID**. The coordinator does not reject a duplicate; two
  workers sharing an ID will silently overwrite each other's
  registration.

## Prerequisites

- A C++20 compiler (AppleClang, Clang, or GCC)
- CMake >= 3.25
- Ninja
- [vcpkg](https://github.com/microsoft/vcpkg), bootstrapped locally,
  with `VCPKG_ROOT` pointing at it. Radahn's dependencies (gRPC,
  Protobuf, SQLite3) are declared in `vcpkg.json` and fetched
  automatically by CMake via vcpkg's manifest mode; you do not
  install them yourself. `vcpkg-configuration.json` pins an exact
  upstream vcpkg baseline commit for reproducible dependency
  resolution - see [CI](#ci) for how a clean machine (including
  GitHub Actions) bootstraps vcpkg from scratch.
- `python3` (only to run `scripts/parameter_sweep_demo.sh`'s example
  workload; nothing else in Radahn itself depends on Python)
- `bash` and standard POSIX tools (only to run the demo scripts)

## Configure, build, and test

Radahn ships two CMake presets: `debug` (fast iteration, warnings
are not fatal) and `release` (optimized, warnings-as-errors). Both
are exercised in CI.

```sh
export VCPKG_ROOT=/path/to/your/vcpkg

cmake --preset debug
cmake --build --preset debug
ctest --preset debug

cmake --preset release
cmake --build --preset release
ctest --preset release
```

Both presets build three executables under `build/<preset>/apps/`:
`radahn-coordinator`, `radahn-worker`, and `radahn` (the CLI), plus
the unit and integration test suite (currently 38 tests).

## Starting the coordinator

```sh
build/debug/apps/coordinator/radahn-coordinator \
    radahn.db \
    --listen-address 0.0.0.0:50051
```

- The first positional argument is the SQLite database file to
  create or open (default `./radahn.db`); it is resolved to an
  absolute path and printed at startup.
- `--listen-address` is the address and port to accept connections
  on (default `0.0.0.0:50051`).
- Run `radahn-coordinator --help` for the full usage and operating
  contract it prints (no auth/encryption, trusted-network-only,
  resource values are reservations not OS limits).
- The coordinator prints a startup summary (version, listen address,
  resolved database path) before it starts serving, and logs when it
  is actually listening.

## Starting workers

```sh
build/debug/apps/worker/radahn-worker \
    worker-1 \
    localhost:50051 \
    --cpu 4 \
    --memory-mib 8192 \
    --disk-mib 51200 \
    --tag linux \
    --max-concurrent-jobs 4
```

- The two positional arguments are this worker's ID (must be unique
  across every simultaneously running worker; default
  `local-worker`) and the coordinator's address (default
  `localhost:50051`).
- `--cpu`, `--memory-mib`, `--disk-mib`, `--gpu`, and `--tag` (may be
  repeated) describe this worker's advertised capacity to the
  scheduler; they default to real detected hardware (CPU count,
  8 GiB memory, 50 GiB disk, OS/architecture tags such as `macos` /
  `linux` / `arm64` / `x86_64`), not enforced limits.
- `--max-concurrent-jobs` caps how many jobs this worker runs at
  once (default derived from `--cpu`, at most 4).
- `--shutdown-grace-ms` (default 10000) bounds how long a
  SIGINT/SIGTERM shutdown waits for in-flight jobs to finish
  normally before forcibly terminating them; see
  [Coordinator restart behavior](#coordinator-restart-behavior) for
  the analogous story on the coordinator side.
- `--log-dir` (default `./radahn-logs`, resolved to an absolute
  path) is where each execution's captured stdout/stderr is written
  locally before (bounded) upload to the coordinator.
- Run one worker process per machine, or several on one machine
  with distinct `--worker-id`s and smaller advertised capacities, to
  simulate a heterogeneous cluster locally (this is exactly what
  `scripts/delta_mvp_demo.sh` does).
- Run `radahn-worker --help` for the full usage and operating
  contract.

## Submitting a real command

```sh
build/debug/apps/cli/radahn job submit \
    my-job-1 "Compute something" \
    50 1 256 512 \
    --tag linux \
    --timeout-ms 30000 \
    -- /usr/bin/env python3 /path/to/script.py --arg value
```

Positional job-submission arguments, in order, are: `<id> <name>
<priority> <cpu> <memory-mib> <disk-mib>`, followed by any of
`--gpu`, `--tag <tag>` (repeatable), `--max-attempts <n>` (default
3), `--timeout-ms <n>`, and finally `-- <executable> [args...]`.

Everything after `--` is run as a literal subprocess on whichever
worker acquires the job - no shell is involved, so shell globs,
pipes, and `$VAR` expansion do not work there (invoke `/bin/sh -c
'...'` explicitly if you need a shell). If `--` and a command are
omitted, the job runs Radahn's internal 10-second sleep workload
instead, which is useful only for smoke-testing scheduling and has
no captured output.

**The worker that runs a job must already have that job's program
and any input files installed locally** (on its own filesystem, on
its `PATH`, or at an absolute path you pass). Radahn does not ship
code or data to workers. Submitting several independent jobs lets
them run in parallel across your workers' combined capacity, but it
does not parallelize the inside of one arbitrary program - a
single-threaded program you submit as one job still runs
single-threaded on whichever one worker executes it.

See [`examples/parameter_sweep_workload.py`](examples/parameter_sweep_workload.py)
and [`scripts/parameter_sweep_demo.sh`](scripts/parameter_sweep_demo.sh)
for a small, concrete, runnable example of real parallel work: the
same program run as several independent jobs, one per parameter
value, each an ordinary CPU-bound Python script computing a prime
count for a different N. Run it with:

```sh
scripts/parameter_sweep_demo.sh
```

It starts its own throwaway coordinator and two workers, submits six
independent jobs sweeping over six values of N, waits for all of
them to succeed, prints each job's own reported runtime, and
compares that sum against the sweep's actual wall-clock time - which
is well under the sum, because the jobs ran concurrently rather than
one after another.

## Resource requirements and tags

- `<cpu>`, `<memory-mib>`, and `<disk-mib>` are the job's declared
  resource requirements. The coordinator only dispatches a job to a
  worker that currently has at least that much of each resource
  available (CPU is a fractional core count; memory/disk are
  converted from MiB to bytes).
- `--gpu` requires a worker that advertised `--gpu` itself.
- `--tag <tag>` (repeatable) requires the worker to have **all** of
  the job's required tags (a worker may have additional tags beyond
  what the job requires); it is a subset match, not an exact match.
- None of this is an OS-enforced limit - see
  [Supported platforms](#supported-platforms-and-trusted-network-assumptions).

## Inspecting job and worker status

```sh
radahn job get <id>
radahn job list
radahn worker list
radahn worker get <worker-id>
```

`job get`/`job list` show state (`QUEUED`, `LEASED`, `RUNNING`,
`RETRY_WAIT`, `CANCELLATION_REQUESTED`, `SUCCEEDED`, `FAILED`,
`CANCELLED`), requirements, attempt count vs. max attempts, the
execution outcome and exit code/signal once known, the assigned
worker, and whether stdout/stderr logs have been uploaded yet.
`worker get`/`worker list` show online/offline state, last
heartbeat, allocated vs. total CPU/memory/disk, running jobs vs.
concurrency cap, GPU availability, and tags.

Every CLI invocation accepts `--coordinator <host:port>` before the
command (or the `RADAHN_COORDINATOR_ADDRESS` environment variable)
to target a non-default coordinator; the default is
`localhost:50051`.

## Retrieving logs

```sh
radahn job logs <id>              # stdout
radahn job logs <id> --stderr     # stderr
radahn job logs <id> --attempt 2  # a specific attempt, not just the latest
```

Logs are only available once the worker that ran the job has
uploaded them, which happens after the job reaches a terminal
outcome for that attempt. Each stream is capped at 1 MiB; a larger
capture is truncated, and the CLI prints a notice to stderr saying
so (with the true original byte count). The internal sleep workload
produces no logs at all.

## Timeout and retry behavior

- `--timeout-ms` (only valid with `-- <executable> ...`) bounds how
  long a command may run. A command that exceeds it is sent SIGTERM
  and, if it has not exited after a short grace period, SIGKILL; the
  attempt's outcome is recorded as `TIMED_OUT`.
- `--max-attempts` (default 3) bounds how many times a job may be
  attempted in total. A failed attempt - nonzero exit, signal,
  timeout, spawn failure, or the worker disappearing before
  reporting completion - returns the job to the queue for another
  attempt (on any eligible worker, not necessarily the same one), as
  long as attempts remain. Once attempts are exhausted the job
  becomes terminally `FAILED`.
- A worker holds a time-bounded **lease** on each attempt it is
  running (15 seconds by default, renewed every 2 seconds via
  heartbeat as long as the worker is reachable). If the coordinator
  does not hear from a worker in time, the lease expires and the job
  is made available for another attempt - this is how a crashed or
  partitioned worker's work gets recovered.

**Delivery guarantee:** Radahn may execute a job more than once
after failures. Attempt fencing prevents a stale attempt (one whose
lease has already expired or been superseded) from changing the
coordinator's current job state - a late completion report from an
old attempt is rejected, not applied. It does **not** provide
exactly-once external side effects: if your command writes to a
file, calls a remote API, or otherwise has an effect outside its own
exit code, that effect can happen more than once. Write
idempotent commands wherever the job's side effects matter.

## Coordinator restart behavior

The coordinator persists job state, execution results, and uploaded
stdout/stderr logs to its SQLite database file as it goes. Stopping
and restarting the coordinator process against the **same database
path** preserves all of that: `radahn job get`/`job logs` against
the jobs that already finished return the same results and logs
after a restart as before it (this is exercised directly by
`scripts/delta_mvp_demo.sh`). Workers are not required to restart
when the coordinator does; a worker's heartbeat loop keeps retrying
against a temporarily unreachable coordinator and resumes once it is
back.

Ordinary shutdown (SIGINT/SIGTERM to the worker, or simply stopping
the coordinator) is bounded, not instantaneous: see
`--shutdown-grace-ms` above for the worker side.

## Running the MVP demo

```sh
scripts/delta_mvp_demo.sh
```

This is the primary, repeatable, end-to-end demonstration: it builds
the project, starts one coordinator and two workers with distinct
tags and capacities on an explicit non-default address, and then
exercises - against real RPCs, not mocks - a successful command, a
command that fails with a specific exit code, a command that times
out, several jobs running concurrently, tag-constrained routing in
both directions, stdout/stderr retrieval, a hard worker crash with
recovery of its in-flight job on the surviving worker, and a
coordinator restart against the same database proving results and
logs survived it. It exits non-zero and dumps every component's log
on first failure.

Two earlier, narrower scripts also still exist and still pass:
`scripts/milestone_2_smoke_test.sh` and
`scripts/milestone_4g_cluster_demo.sh`. `delta_mvp_demo.sh` is the
one to run if you only run one.

## Known limitations

- No authentication, no transport encryption, no multi-tenant
  isolation: this is a single trusted-network tool, not a
  multi-user service.
- Single coordinator, no high availability: the coordinator is a
  single process backed by a single SQLite file; there is no
  coordinator clustering or failover, and the database file itself
  is a single point of failure for new scheduling (jobs already
  dispatched continue running on workers independently of a
  down coordinator, but cannot report completion until it returns).
- Resource and tag requirements are scheduling hints only; they are
  never enforced as OS-level limits, and a misbehaving job can
  consume more than it declared.
- At-least-once execution, not exactly-once: see
  [Timeout and retry behavior](#timeout-and-retry-behavior).
- Logs are captured per attempt and capped at 1 MiB per stream;
  larger output is truncated, not paginated or streamed live.
- Submitting independent jobs parallelizes across jobs, not within
  one arbitrary program; Radahn does not parallelize a
  single-threaded program for you.
- Worker IDs are not deduplicated by the coordinator; operators are
  responsible for uniqueness.
- No job cancellation CLI command exists yet, even though the
  `CANCELLATION_REQUESTED` state exists in the job state machine.

## CI

`.github/workflows/ci.yml` builds and tests both the `debug` and
`release` presets on Linux and macOS on every push and pull request,
bootstraps vcpkg from scratch at a pinned commit (matching
`vcpkg-configuration.json`'s baseline) so it never depends on a
locally pre-populated vcpkg install, runs the full CTest suite for
both presets, runs `scripts/delta_mvp_demo.sh` as a bounded
integration/smoke check, and uploads diagnostic logs as a build
artifact if anything fails.
