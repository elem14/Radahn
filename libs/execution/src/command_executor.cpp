#include "radahn/execution/command_executor.hpp"

#include <cerrno>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <crt_externs.h>
#else
extern char** environ;
#endif

namespace radahn::execution {

namespace {

void check_spawn_call(
    int error,
    const char* operation
) {
    if (error != 0) {
        throw std::system_error{
            error,
            std::generic_category(),
            operation
        };
    }
}

class SpawnFileActions {
public:
    SpawnFileActions() {
        check_spawn_call(
            posix_spawn_file_actions_init(&actions_),
            "Initialize spawn file actions"
        );
    }

    ~SpawnFileActions() {
        posix_spawn_file_actions_destroy(&actions_);
    }

    SpawnFileActions(const SpawnFileActions&) = delete;

    SpawnFileActions& operator=(
        const SpawnFileActions&
    ) = delete;

    void open(
        int descriptor,
        const std::filesystem::path& path,
        int flags,
        mode_t permissions
    ) {
        check_spawn_call(
            posix_spawn_file_actions_addopen(
                &actions_,
                descriptor,
                path.c_str(),
                flags,
                permissions
            ),
            "Configure child file descriptor"
        );
    }

    [[nodiscard]]
    const posix_spawn_file_actions_t*
    get() const noexcept {
        return &actions_;
    }

private:
    posix_spawn_file_actions_t actions_{};
};

class SpawnAttributes {
public:
    SpawnAttributes() {
        check_spawn_call(
            posix_spawnattr_init(&attributes_),
            "Initialize spawn attributes"
        );

        try {
            check_spawn_call(
                posix_spawnattr_setpgroup(
                    &attributes_,
                    0
                ),
                "Configure child process group"
            );

            check_spawn_call(
                posix_spawnattr_setflags(
                    &attributes_,
                    POSIX_SPAWN_SETPGROUP
                ),
                "Enable child process group"
            );
        } catch (...) {
            posix_spawnattr_destroy(&attributes_);
            throw;
        }
    }

    ~SpawnAttributes() {
        posix_spawnattr_destroy(&attributes_);
    }

    SpawnAttributes(const SpawnAttributes&) = delete;

    SpawnAttributes& operator=(
        const SpawnAttributes&
    ) = delete;

    [[nodiscard]]
    const posix_spawnattr_t*
    get() const noexcept {
        return &attributes_;
    }

private:
    posix_spawnattr_t attributes_{};
};

[[nodiscard]]
char** inherited_environment() noexcept {
#if defined(__APPLE__)
    return *_NSGetEnviron();
#else
    return ::environ;
#endif
}

/*
 * Signals the entire process group led by `group_leader_pid` (which
 * is exactly the direct child's pid, since it was spawned as the
 * leader of its own new group). ESRCH means every member is already
 * gone, which is a harmless race, not a failure.
 */
void signal_process_group(
    pid_t group_leader_pid,
    int signal_number,
    const char* operation
) {
    if (
        ::kill(-group_leader_pid, signal_number) == -1 &&
        errno != ESRCH
    ) {
        throw std::system_error{
            errno,
            std::generic_category(),
            operation
        };
    }
}

/*
 * Best-effort cleanup used only when something has gone wrong after
 * the child was spawned (e.g. an exception escaping the supervised
 * wait loop). Guarantees the direct child is not left running or
 * unreaped, without itself throwing.
 */
void reap_child_best_effort(
    pid_t child_pid
) noexcept {
    ::kill(-child_pid, SIGKILL);

    int status = 0;
    pid_t wait_result{};

    do {
        wait_result = waitpid(
            child_pid,
            &status,
            0
        );
    } while (
        wait_result == -1 &&
        errno == EINTR
    );
}

enum class TerminationReason {
    none,
    timeout,
    stop_requested
};

}  // namespace

ExecutionResult execute_command(
    const domain::WorkloadSpec& workload,
    const std::filesystem::path& output_directory,
    ExecutionControl control
) {
    if (
        workload.kind() !=
        domain::WorkloadKind::command
    ) {
        throw std::invalid_argument{
            "Command executor requires a command workload"
        };
    }

    const auto& command = workload.command_spec();

    const auto directory =
        std::filesystem::absolute(output_directory);

    if (!std::filesystem::create_directory(directory)) {
        throw std::invalid_argument{
            "Execution output directory already exists"
        };
    }

    SpawnFileActions actions;

    actions.open(
        STDIN_FILENO,
        "/dev/null",
        O_RDONLY,
        0
    );

    actions.open(
        STDOUT_FILENO,
        directory / "stdout.log",
        O_WRONLY | O_CREAT | O_EXCL,
        0600
    );

    actions.open(
        STDERR_FILENO,
        directory / "stderr.log",
        O_WRONLY | O_CREAT | O_EXCL,
        0600
    );

    // Own writable, NUL-terminated strings for the POSIX API.
    std::vector<std::string> owned_arguments;
    owned_arguments.reserve(command.args.size() + 1);

    // argv[0] conventionally identifies the executable.
    owned_arguments.push_back(command.executable);

    for (const auto& argument : command.args) {
        owned_arguments.push_back(argument);
    }

    std::vector<char*> argv;
    argv.reserve(owned_arguments.size() + 1);

    for (auto& argument : owned_arguments) {
        argv.push_back(argument.data());
    }

    // POSIX argv arrays end with a null pointer.
    argv.push_back(nullptr);

    SpawnAttributes attributes;

    pid_t child_pid{};

    const int spawn_error = posix_spawnp(
        &child_pid,
        command.executable.c_str(),
        actions.get(),
        attributes.get(),
        argv.data(),
        inherited_environment()
    );

    if (spawn_error != 0) {
        return ExecutionResult{
            ExecutionOutcome::spawn_failed,
            std::nullopt,
            std::nullopt,
            std::error_code{
                spawn_error,
                std::generic_category()
            }.message()
        };
    }

    try {
        const auto start_time =
            std::chrono::steady_clock::now();

        const std::optional<
            std::chrono::steady_clock::time_point
        > deadline =
            command.timeout.has_value()
                ? std::optional{start_time + *command.timeout}
                : std::nullopt;

        constexpr std::chrono::milliseconds
            poll_interval{20};

        TerminationReason reason = TerminationReason::none;

        std::optional<std::chrono::steady_clock::time_point>
            escalate_at;

        bool escalated = false;
        bool child_reaped = false;
        int status = 0;

        while (true) {
            if (!child_reaped) {
                const pid_t wait_result = waitpid(
                    child_pid,
                    &status,
                    WNOHANG
                );

                if (wait_result == child_pid) {
                    child_reaped = true;

                    if (reason == TerminationReason::none) {
                        break;
                    }
                } else if (wait_result == -1) {
                    if (errno == EINTR) {
                        continue;
                    }

                    throw std::system_error{
                        errno,
                        std::generic_category(),
                        "Wait for command process"
                    };
                }

                // wait_result == 0: the child is still running.
            }

            if (reason == TerminationReason::none) {
                const bool timed_out =
                    deadline.has_value() &&
                    std::chrono::steady_clock::now() >= *deadline;

                const bool stop_requested =
                    control.stop_token.stop_requested();

                if (timed_out || stop_requested) {
                    reason =
                        timed_out
                            ? TerminationReason::timeout
                            : TerminationReason::stop_requested;

                    signal_process_group(
                        child_pid,
                        SIGTERM,
                        "Send SIGTERM to command process group"
                    );

                    escalate_at =
                        std::chrono::steady_clock::now() +
                        control.termination_grace;
                }
            } else if (
                !escalated &&
                std::chrono::steady_clock::now() >= *escalate_at
            ) {
                /*
                 * Escalate unconditionally once the grace interval
                 * elapses. The direct child exiting promptly does
                 * not mean every descendant left in its process
                 * group has too; sending to the group again is
                 * harmless (ESRCH) for members already gone.
                 */
                signal_process_group(
                    child_pid,
                    SIGKILL,
                    "Send SIGKILL to command process group"
                );

                escalated = true;
            }

            if (
                child_reaped &&
                reason != TerminationReason::none &&
                escalated
            ) {
                break;
            }

            std::this_thread::sleep_for(poll_interval);
        }

        if (reason != TerminationReason::none) {
            return ExecutionResult{
                reason == TerminationReason::timeout
                    ? ExecutionOutcome::timed_out
                    : ExecutionOutcome::worker_stopped,
                std::nullopt,
                std::nullopt,
                reason == TerminationReason::timeout
                    ? "Execution exceeded its configured timeout"
                    : "Worker stopped execution"
            };
        }

        if (WIFEXITED(status)) {
            return ExecutionResult{
                ExecutionOutcome::exited,
                WEXITSTATUS(status),
                std::nullopt,
                {}
            };
        }

        if (WIFSIGNALED(status)) {
            return ExecutionResult{
                ExecutionOutcome::signaled,
                std::nullopt,
                WTERMSIG(status),
                {}
            };
        }

        throw std::runtime_error{
            "Unexpected child process status"
        };
    } catch (...) {
        /*
         * Whatever went wrong, the direct child must not be left
         * running or unreaped.
         */
        reap_child_best_effort(child_pid);

        throw;
    }
}

}  // namespace radahn::execution