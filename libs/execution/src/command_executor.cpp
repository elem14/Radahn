#include "radahn/execution/command_executor.hpp"

#include <cerrno>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <system_error>
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

}  // namespace

ExecutionResult execute_command(
    const domain::WorkloadSpec& workload,
    const std::filesystem::path& output_directory
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

    // Explicitly reject this until timeout enforcement exists.
    if (command.timeout.has_value()) {
        throw std::invalid_argument{
            "Execution timeouts are not implemented yet"
        };
    }

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

    if (wait_result == -1) {
        throw std::system_error{
            errno,
            std::generic_category(),
            "Wait for command process"
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
}

}  // namespace radahn::execution