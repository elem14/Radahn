#include "radahn/persistence/sqlite_job_repository.hpp"

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <sqlite3.h>

#include "radahn/domain/job.hpp"
#include "radahn/domain/job_state.hpp"
#include "radahn/domain/resource.hpp"
#include "radahn/domain/workload.hpp"

#include "radahn/persistence/sqlite_schema.hpp"

namespace radahn::persistence {

namespace {

struct StatementDeleter {
    void operator()(
        sqlite3_stmt* statement
    ) const noexcept {
        sqlite3_finalize(statement);
    }
};

using StatementPointer =
    std::unique_ptr<
        sqlite3_stmt,
        StatementDeleter
    >;

[[nodiscard]]
std::runtime_error database_error(
    sqlite3* database,
    std::string_view operation
) {
    std::string message{
        operation
    };

    message += ": ";
    message += sqlite3_errmsg(database);

    return std::runtime_error{
        std::move(message)
    };
}

[[nodiscard]]
bool is_constraint_result(
    int result
) noexcept {
    return
        (result & 0xFF) ==
        SQLITE_CONSTRAINT;
}

[[nodiscard]]
StatementPointer prepare_statement(
    sqlite3* database,
    std::string_view sql
) {
    const std::string owned_sql{
        sql
    };

    sqlite3_stmt* raw_statement = nullptr;

    const int result =
        sqlite3_prepare_v2(
            database,
            owned_sql.c_str(),
            -1,
            &raw_statement,
            nullptr
        );

    if (result != SQLITE_OK) {
        throw std::runtime_error(
            std::string("Failed SQL:\n") + 
            owned_sql + 
            "\n\nSQLite says:\n" +
            sqlite3_errmsg(database)
        );
    }

    return StatementPointer{
        raw_statement
    };
}

void require_bind_success(
    sqlite3* database,
    int result,
    std::string_view field_name
) {
    if (result == SQLITE_OK) {
        return;
    }

    throw database_error(
        database,
        std::string{
            "Could not bind "
        } + std::string{field_name}
    );
}

void bind_text(
    sqlite3* database,
    sqlite3_stmt* statement,
    int index,
    std::string_view value,
    std::string_view field_name
) {
    if (
        value.size() >
        static_cast<std::size_t>(
            std::numeric_limits<int>::max()
        )
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " is too large for SQLite"
        };
    }

    require_bind_success(
        database,
        sqlite3_bind_text(
            statement,
            index,
            value.data(),
            static_cast<int>(
                value.size()
            ),
            SQLITE_TRANSIENT
        ),
        field_name
    );
}

void bind_int64(
    sqlite3* database,
    sqlite3_stmt* statement,
    int index,
    std::int64_t value,
    std::string_view field_name
) {
    require_bind_success(
        database,
        sqlite3_bind_int64(
            statement,
            index,
            value
        ),
        field_name
    );
}

void bind_double(
    sqlite3* database,
    sqlite3_stmt* statement,
    int index,
    double value,
    std::string_view field_name
) {
    require_bind_success(
        database,
        sqlite3_bind_double(
            statement,
            index,
            value
        ),
        field_name
    );
}

void bind_optional_worker_id(
    sqlite3* database,
    sqlite3_stmt* statement,
    int index,
    const std::optional<domain::WorkerId>&
        worker_id
) {
    if (!worker_id.has_value()) {
        require_bind_success(
            database,
            sqlite3_bind_null(
                statement,
                index
            ),
            "assigned_worker_id"
        );

        return;
    }

    bind_text(
        database,
        statement,
        index,
        worker_id->value(),
        "assigned_worker_id"
    );
}

[[nodiscard]]
std::int64_t lease_to_unix_ms(
    JobLeaseTimePoint lease_time
);

void bind_optional_lease(
    sqlite3* database,
    sqlite3_stmt* statement,
    int index,
    const std::optional<JobLeaseTimePoint>&
        lease_expires_at
) {
    if (!lease_expires_at.has_value()) {
        require_bind_success(
            database,
            sqlite3_bind_null(
                statement,
                index
            ),
            "lease_expires_at_unix_ms"
        );

        return;
    }

    bind_int64(
        database,
        statement,
        index,
        lease_to_unix_ms(
            *lease_expires_at
        ),
        "lease_expires_at_unix_ms"
    );
}

[[nodiscard]]
std::int64_t checked_sql_integer(
    std::uint64_t value,
    std::string_view field_name
) {
    if (
        value >
        static_cast<std::uint64_t>(
            std::numeric_limits<
                std::int64_t
            >::max()
        )
    ) {
        throw std::invalid_argument{
            std::string{field_name} +
            " exceeds SQLite's signed integer range"
        };
    }

    return static_cast<std::int64_t>(
        value
    );
}

[[nodiscard]]
std::uint64_t checked_unsigned_column(
    sqlite3_stmt* statement,
    int column,
    std::string_view field_name
) {
    const std::int64_t value =
        sqlite3_column_int64(
            statement,
            column
        );

    if (value < 0) {
        throw std::runtime_error{
            std::string{field_name} +
            " contains a negative database value"
        };
    }

    return static_cast<std::uint64_t>(
        value
    );
}

[[nodiscard]]
std::int64_t lease_to_unix_ms(
    JobLeaseTimePoint lease_time
) {
    const auto unix_ms =
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(
            lease_time.time_since_epoch()
        ).count();

    if (unix_ms < 0) {
        throw std::invalid_argument{
            "Lease expiration cannot be before the Unix epoch"
        };
    }

    return unix_ms;
}

[[nodiscard]]
JobLeaseTimePoint lease_from_unix_ms(
    std::int64_t unix_ms
) {
    if (unix_ms < 0) {
        throw std::runtime_error{
            "Stored lease expiration timestamp is negative"
        };
    }

    return JobLeaseTimePoint{
        std::chrono::duration_cast<
            JobLeaseClock::duration
        >(
            std::chrono::milliseconds{
                unix_ms
            }
        )
    };
}

[[nodiscard]]
std::string text_column(
    sqlite3_stmt* statement,
    int column,
    std::string_view field_name
) {
    const auto* text =
        sqlite3_column_text(
            statement,
            column
        );

    if (text == nullptr) {
        throw std::runtime_error{
            std::string{field_name} +
            " unexpectedly contains NULL"
        };
    }

    return std::string{
        reinterpret_cast<
            const char*
        >(text)
    };
}

[[nodiscard]]
int job_state_to_integer(
    domain::JobState state
) {
    switch (state) {
        case domain::JobState::queued:
            return 1;

        case domain::JobState::leased:
            return 2;

        case domain::JobState::running:
            return 3;

        case domain::JobState::retry_wait:
            return 4;

        case domain::JobState::
            cancellation_requested:
            return 5;

        case domain::JobState::succeeded:
            return 6;

        case domain::JobState::failed:
            return 7;

        case domain::JobState::cancelled:
            return 8;
    }

    throw std::invalid_argument{
        "Unsupported job state"
    };
}

[[nodiscard]]
domain::JobState job_state_from_integer(
    int value
) {
    switch (value) {
        case 1:
            return domain::JobState::queued;

        case 2:
            return domain::JobState::leased;

        case 3:
            return domain::JobState::running;

        case 4:
            return domain::JobState::retry_wait;

        case 5:
            return domain::JobState::
                cancellation_requested;

        case 6:
            return domain::JobState::succeeded;

        case 7:
            return domain::JobState::failed;

        case 8:
            return domain::JobState::cancelled;

        default:
            throw std::runtime_error{
                "Database contains an unknown job state"
            };
    }
}

void save_command(
    sqlite3* database,
    const JobRecord& record
) {
    const auto& job_id = record.job.id().value();

    // Remove the previous command and its argument rows.
    auto remove = prepare_statement(
        database,
        "DELETE FROM job_commands WHERE job_id = ?;"
    );

    bind_text(
        database,
        remove.get(),
        1,
        job_id,
        "job_id"
    );

    if (sqlite3_step(remove.get()) != SQLITE_DONE) {
        throw database_error(
            database,
            "Could not clear stored command"
        );
    }

    if (
        record.job.workload().kind() !=
        domain::WorkloadKind::command
    ) {
        return;
    }

    const auto& command =
        record.job.workload().command_spec();

    auto statement = prepare_statement(
        database,
        R"sql(
            INSERT INTO job_commands (
                job_id,
                executable,
                timeout_ms
            )
            VALUES (?, ?, ?);
        )sql"
    );

    bind_text(
        database,
        statement.get(),
        1,
        job_id,
        "job_id"
    );

    bind_text(
        database,
        statement.get(),
        2,
        command.executable,
        "executable"
    );

    if (command.timeout) {
        bind_int64(
            database,
            statement.get(),
            3,
            command.timeout->count(),
            "timeout_ms"
        );
    } else {
        require_bind_success(
            database,
            sqlite3_bind_null(statement.get(), 3),
            "timeout_ms"
        );
    }

    if (sqlite3_step(statement.get()) != SQLITE_DONE) {
        throw database_error(
            database,
            "Could not save command"
        );
    }

    auto argument = prepare_statement(
        database,
        R"sql(
            INSERT INTO job_command_args (
                job_id,
                position,
                argument
            )
            VALUES (?, ?, ?);
        )sql"
    );

    for (
        std::size_t index = 0;
        index < command.args.size();
        ++index
    ) {
        sqlite3_reset(argument.get());
        sqlite3_clear_bindings(argument.get());

        bind_text(
            database,
            argument.get(),
            1,
            job_id,
            "job_id"
        );

        bind_int64(
            database,
            argument.get(),
            2,
            checked_sql_integer(index, "position"),
            "position"
        );

        bind_text(
            database,
            argument.get(),
            3,
            command.args[index],
            "argument"
        );

        if (
            sqlite3_step(argument.get()) !=
            SQLITE_DONE
        ) {
            throw database_error(
                database,
                "Could not save command argument"
            );
        }
    }
}

[[nodiscard]]
domain::WorkloadSpec load_command(
    sqlite3* database,
    const std::string& job_id
) {
    auto statement = prepare_statement(
        database,
        R"sql(
            SELECT executable, timeout_ms
            FROM job_commands
            WHERE job_id = ?;
        )sql"
    );

    bind_text(
        database,
        statement.get(),
        1,
        job_id,
        "job_id"
    );

    const int result = sqlite3_step(statement.get());

    if (result == SQLITE_DONE) {
        throw std::runtime_error{
            "Command job is missing its stored specification"
        };
    }

    if (result != SQLITE_ROW) {
        throw database_error(
            database,
            "Could not load command"
        );
    }

    domain::CommandWorkload command{
        text_column(
            statement.get(),
            0,
            "executable"
        ),
        {},
        std::nullopt
    };

    if (
        sqlite3_column_type(statement.get(), 1) !=
        SQLITE_NULL
    ) {
        command.timeout = std::chrono::milliseconds{
            sqlite3_column_int64(statement.get(), 1)
        };
    }

    auto arguments = prepare_statement(
        database,
        R"sql(
            SELECT position, argument
            FROM job_command_args
            WHERE job_id = ?
            ORDER BY position;
        )sql"
    );

    bind_text(
        database,
        arguments.get(),
        1,
        job_id,
        "job_id"
    );

    while (true) {
        const int argument_result =
            sqlite3_step(arguments.get());

        if (argument_result == SQLITE_DONE) {
            break;
        }

        if (argument_result != SQLITE_ROW) {
            throw database_error(
                database,
                "Could not load command arguments"
            );
        }

        const auto expected_position =
            checked_sql_integer(
                command.args.size(),
                "position"
            );

        if (
            sqlite3_column_int64(arguments.get(), 0) !=
            expected_position
        ) {
            throw std::runtime_error{
                "Stored argument positions are not contiguous"
            };
        }

        command.args.push_back(
            text_column(
                arguments.get(),
                1,
                "argument"
            )
        );
    }

    return domain::WorkloadSpec::command(
        std::move(command)
    );
}

[[nodiscard]]
int workload_kind_to_integer(
    domain::WorkloadKind kind
) {
    switch (kind) {
        case domain::WorkloadKind::sleep:
            return 1;

        case domain::WorkloadKind::command:
            return 2;
    }

    throw std::invalid_argument{
        "Unsupported workload type"
    };
}

[[nodiscard]]
domain::WorkloadSpec workload_from_columns(
    sqlite3* database,
    const std::string& job_id,
    int workload_kind,
    std::uint64_t sleep_duration_ms
) {
    switch (workload_kind) {
        case 2:
            return load_command(
                database,
                job_id
            );

        case 1: {
            using MillisecondsRep =
                std::chrono::milliseconds::rep;

            if (
                sleep_duration_ms >
                static_cast<std::uint64_t>(
                    std::numeric_limits<
                        MillisecondsRep
                    >::max()
                )
            ) {
                throw std::runtime_error{
                    "Stored sleep duration exceeds "
                    "the supported range"
                };
            }

            return domain::WorkloadSpec::sleep(
                std::chrono::milliseconds{
                    static_cast<MillisecondsRep>(
                        sleep_duration_ms
                    )
                }
            );
        }

        default:
            throw std::runtime_error{
                "Database contains an unknown workload type"
            };
    }
}

[[nodiscard]]
std::int64_t created_at_unix_ms(
    const domain::Job& job
) {
    return
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(
            job.created_at().time_since_epoch()
        ).count();
}

[[nodiscard]]
std::vector<std::string> load_required_tags(
    sqlite3* database,
    const domain::JobId& job_id
) {
    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT tag
                FROM job_required_tags
                WHERE job_id = ?
                ORDER BY tag;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        job_id.value(),
        "job_id"
    );

    std::vector<std::string> tags;

    while (true) {
        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result == SQLITE_DONE) {
            break;
        }

        if (result != SQLITE_ROW) {
            throw database_error(
                database,
                "Could not read job tags"
            );
        }

        tags.push_back(
            text_column(
                statement.get(),
                0,
                "tag"
            )
        );
    }

    return tags;
}

void insert_required_tags(
    sqlite3* database,
    const persistence::JobRecord& record
) {
    auto statement =
        prepare_statement(
            database,
            R"sql(
                INSERT INTO job_required_tags (
                    job_id,
                    tag
                )
                VALUES (?, ?);
            )sql"
        );

    for (
        const auto& tag :
        record.job.requirements().required_tags()
    ) {
        sqlite3_reset(
            statement.get()
        );

        sqlite3_clear_bindings(
            statement.get()
        );

        bind_text(
            database,
            statement.get(),
            1,
            record.job.id().value(),
            "job_id"
        );

        bind_text(
            database,
            statement.get(),
            2,
            tag,
            "tag"
        );

        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result == SQLITE_DONE) {
            continue;
        }

        if (is_constraint_result(result)) {
            throw std::invalid_argument{
                "Job contains duplicate or invalid tags"
            };
        }

        throw database_error(
            database,
            "Could not insert job tag"
        );
    }
}

void delete_required_tags(
    sqlite3* database,
    const domain::JobId& job_id
) {
    auto statement =
        prepare_statement(
            database,
            R"sql(
                DELETE FROM job_required_tags
                WHERE job_id = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        job_id.value(),
        "job_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result != SQLITE_DONE) {
        throw database_error(
            database,
            "Could not delete existing job tags"
        );
    }
}

[[nodiscard]]
persistence::JobRecord record_from_row(
    sqlite3* database,
    sqlite3_stmt* statement
) {
    const std::string job_id =
        text_column(
            statement,
            0,
            "job_id"
        );

    const std::string name =
        text_column(
            statement,
            1,
            "name"
        );

    const std::int64_t stored_priority =
        sqlite3_column_int64(
            statement,
            2
        );

    if (
        stored_priority <
            std::numeric_limits<int>::min() ||
        stored_priority >
            std::numeric_limits<int>::max()
    ) {
        throw std::runtime_error{
            "Stored priority is outside the supported range"
        };
    }

    const auto state =
        job_state_from_integer(
            sqlite3_column_int(
                statement,
                3
            )
        );

    const double cpu_cores =
        sqlite3_column_double(
            statement,
            4
        );

    const std::uint64_t memory_bytes =
        checked_unsigned_column(
            statement,
            5,
            "memory_bytes"
        );

    const std::uint64_t disk_bytes =
        checked_unsigned_column(
            statement,
            6,
            "disk_bytes"
        );

    const bool requires_gpu =
        sqlite3_column_int(
            statement,
            7
        ) != 0;

    const int workload_kind =
        sqlite3_column_int(
            statement,
            8
        );

    const std::uint64_t sleep_duration_ms =
        checked_unsigned_column(
            statement,
            9,
            "sleep_duration_ms"
        );

    const std::int64_t created_at_ms =
        sqlite3_column_int64(
            statement,
            10
        );

    std::optional<domain::WorkerId>
        assigned_worker_id;

    if (
        sqlite3_column_type(
            statement,
            11
        ) != SQLITE_NULL
    ) {
        assigned_worker_id =
            domain::WorkerId{
                text_column(
                    statement,
                    11,
                    "assigned_worker_id"
                )
            };
    }

    std::optional<JobLeaseTimePoint>
        lease_expires_at;

    if (
        sqlite3_column_type(
            statement,
            12
        ) != SQLITE_NULL
    ) {
        lease_expires_at =
            lease_from_unix_ms(
                sqlite3_column_int64(
                    statement,
                    12
                )
            );
    }

    const std::uint64_t
        stored_attempt_count =
            checked_unsigned_column(
                statement,
                13,
                "attempt_count"
            );

    const std::uint64_t
        stored_max_attempts =
            checked_unsigned_column(
                statement,
                14,
                "max_attempts"
            );

    if (
        stored_attempt_count >
            static_cast<std::uint64_t>(
                std::numeric_limits<
                    std::size_t
                >::max()
            ) ||
        stored_max_attempts >
            static_cast<std::uint64_t>(
                std::numeric_limits<
                    std::size_t
                >::max()
            )
    ) {
        throw std::runtime_error{
            "Stored retry metadata exceeds size_t"
        };
    }

    const domain::JobId domain_job_id{
        job_id
    };

    auto required_tags =
        load_required_tags(
            database,
            domain_job_id
        );

    domain::ResourceRequirements requirements{
        cpu_cores,
        memory_bytes,
        disk_bytes,
        requires_gpu,
        std::move(required_tags)
    };

    auto workload =
        workload_from_columns(
            database,
            job_id,
            workload_kind,
            sleep_duration_ms
        );

    const auto created_at =
        domain::Job::TimePoint{
            std::chrono::duration_cast<
                domain::Job::Clock::duration
            >(
                std::chrono::milliseconds{
                    created_at_ms
                }
            )
        };

    domain::Job job =
        domain::Job::restore(
            domain_job_id,
            name,
            static_cast<int>(
                stored_priority
            ),
            std::move(requirements),
            std::move(workload),
            state,
            created_at,
            static_cast<std::size_t>(
                stored_attempt_count
            ),
            static_cast<std::size_t>(
                stored_max_attempts
            )
        );

    return persistence::JobRecord{
        std::move(job),
        std::move(assigned_worker_id),
        std::move(lease_expires_at)
    };
}

void rollback_preserving_original_error(
    SqliteDatabase& database
) noexcept {
    try {
        database.execute(
            "ROLLBACK;"
        );
    } catch (...) {
        // Preserve the original exception.
    }
}

}  // namespace

SqliteJobRepository::SqliteJobRepository(
    SqliteDatabase& database
)
    : database_{database} {
    initialize_sqlite_schema(
        database_
    );
}

void SqliteJobRepository::insert(
    JobRecord record
) {
    sqlite3* database =
        database_.native_handle();

    database_.execute(
        "BEGIN IMMEDIATE TRANSACTION;"
    );

    try {
        auto statement =
            prepare_statement(
                database,
                R"sql(
                    INSERT INTO jobs (
                        job_id,
                        name,
                        priority,
                        state,
                        cpu_cores,
                        memory_bytes,
                        disk_bytes,
                        requires_gpu,
                        workload_kind,
                        sleep_duration_ms,
                        created_at_unix_ms,
                        assigned_worker_id,
                        lease_expires_at_unix_ms,
                        attempt_count,
                        max_attempts
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?
                    );
                )sql"
            );

        bind_text(
            database,
            statement.get(),
            1,
            record.job.id().value(),
            "job_id"
        );

        bind_text(
            database,
            statement.get(),
            2,
            record.job.name(),
            "name"
        );

        bind_int64(
            database,
            statement.get(),
            3,
            record.job.priority(),
            "priority"
        );

        bind_int64(
            database,
            statement.get(),
            4,
            job_state_to_integer(
                record.job.state()
            ),
            "state"
        );

        bind_double(
            database,
            statement.get(),
            5,
            record.job.requirements()
                .cpu_cores(),
            "cpu_cores"
        );

        bind_int64(
            database,
            statement.get(),
            6,
            checked_sql_integer(
                record.job.requirements()
                    .memory_bytes(),
                "memory_bytes"
            ),
            "memory_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            7,
            checked_sql_integer(
                record.job.requirements()
                    .disk_bytes(),
                "disk_bytes"
            ),
            "disk_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            8,
            record.job.requirements()
                    .requires_gpu()
                ? 1
                : 0,
            "requires_gpu"
        );

        bind_int64(
            database,
            statement.get(),
            9,
            workload_kind_to_integer(
                record.job.workload().kind()
            ),
            "workload_kind"
        );

        bind_int64(
            database,
            statement.get(),
            10,
            checked_sql_integer(
                static_cast<std::uint64_t>(
                    record.job.workload()
                        .sleep_duration()
                        .count()
                ),
                "sleep_duration_ms"
            ),
            "sleep_duration_ms"
        );

        bind_int64(
            database,
            statement.get(),
            11,
            created_at_unix_ms(
                record.job
            ),
            "created_at_unix_ms"
        );

        bind_optional_worker_id(
            database,
            statement.get(),
            12,
            record.assigned_worker_id
        );

        bind_optional_lease(
            database,
            statement.get(),
            13,
            record.lease_expires_at
        );

        bind_int64(
            database,
            statement.get(),
            14,
            checked_sql_integer(
                static_cast<std::uint64_t>(
                    record.job.attempt_count()
                ),
                "attempt_count"
            ),
            "attempt_count"
        );

        bind_int64(
            database,
            statement.get(),
            15,
            checked_sql_integer(
                static_cast<std::uint64_t>(
                    record.job.max_attempts()
                ),
                "max_attempts"
            ),
            "max_attempts"
        );

        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result != SQLITE_DONE) {
            if (is_constraint_result(result)) {
                throw std::invalid_argument{
                    "Job insert violated a repository constraint"
                };
            }

            throw database_error(
                database,
                "Could not insert job"
            );
        }

        insert_required_tags(
            database,
            record
        );

        save_command(
            database,
            record
        );

        database_.execute(
            "COMMIT;"
        );
    } catch (...) {
        rollback_preserving_original_error(
            database_
        );

        throw;
    }
}

void SqliteJobRepository::update(
    JobRecord record
) {
    const domain::JobId job_id{
        record.job.id()
    };

    if (!contains(job_id)) {
        throw std::invalid_argument{
            "Cannot update an unknown job"
        };
    }

    sqlite3* database =
        database_.native_handle();

    database_.execute(
        "BEGIN IMMEDIATE TRANSACTION;"
    );

    try {
        auto statement =
            prepare_statement(
                database,
                R"sql(
                    UPDATE jobs
                    SET
                        name = ?,
                        priority = ?,
                        state = ?,
                        cpu_cores = ?,
                        memory_bytes = ?,
                        disk_bytes = ?,
                        requires_gpu = ?,
                        workload_kind = ?,
                        sleep_duration_ms = ?,
                        created_at_unix_ms = ?,
                        assigned_worker_id = ?,
                        lease_expires_at_unix_ms = ?,
                        attempt_count = ?,
                        max_attempts = ?
                    WHERE job_id = ?;
                )sql"
            );

        bind_text(
            database,
            statement.get(),
            1,
            record.job.name(),
            "name"
        );

        bind_int64(
            database,
            statement.get(),
            2,
            record.job.priority(),
            "priority"
        );

        bind_int64(
            database,
            statement.get(),
            3,
            job_state_to_integer(
                record.job.state()
            ),
            "state"
        );

        bind_double(
            database,
            statement.get(),
            4,
            record.job.requirements()
                .cpu_cores(),
            "cpu_cores"
        );

        bind_int64(
            database,
            statement.get(),
            5,
            checked_sql_integer(
                record.job.requirements()
                    .memory_bytes(),
                "memory_bytes"
            ),
            "memory_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            6,
            checked_sql_integer(
                record.job.requirements()
                    .disk_bytes(),
                "disk_bytes"
            ),
            "disk_bytes"
        );

        bind_int64(
            database,
            statement.get(),
            7,
            record.job.requirements()
                    .requires_gpu()
                ? 1
                : 0,
            "requires_gpu"
        );

        bind_int64(
            database,
            statement.get(),
            8,
            workload_kind_to_integer(
                record.job.workload().kind()
            ),
            "workload_kind"
        );

        bind_int64(
            database,
            statement.get(),
            9,
            checked_sql_integer(
                static_cast<std::uint64_t>(
                    record.job.workload()
                        .sleep_duration()
                        .count()
                ),
                "sleep_duration_ms"
            ),
            "sleep_duration_ms"
        );

        bind_int64(
            database,
            statement.get(),
            10,
            created_at_unix_ms(
                record.job
            ),
            "created_at_unix_ms"
        );

        bind_optional_worker_id(
            database,
            statement.get(),
            11,
            record.assigned_worker_id
        );

        bind_optional_lease(
            database,
            statement.get(),
            12,
            record.lease_expires_at
        );

        bind_int64(
            database,
            statement.get(),
            13,
            checked_sql_integer(
                static_cast<std::uint64_t>(
                    record.job.attempt_count()
                ),
                "attempt_count"
            ),
            "attempt_count"
        );

        bind_int64(
            database,
            statement.get(),
            14,
            checked_sql_integer(
                static_cast<std::uint64_t>(
                    record.job.max_attempts()
                ),
                "max_attempts"
            ),
            "max_attempts"
        );

        bind_text(
            database,
            statement.get(),
            15,
            job_id.value(),
            "job_id"
        );

        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result != SQLITE_DONE) {
            if (is_constraint_result(result)) {
                throw std::invalid_argument{
                    "Job update violated a repository constraint"
                };
            }

            throw database_error(
                database,
                "Could not update job"
            );
        }

        delete_required_tags(
            database,
            job_id
        );

        insert_required_tags(
            database,
            record
        );

        save_command(
            database,
            record
        );

        database_.execute(
            "COMMIT;"
        );
    } catch (...) {
        rollback_preserving_original_error(
            database_
        );

        throw;
    }
}

std::optional<JobRecord>
SqliteJobRepository::get(
    const domain::JobId& job_id
) const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT
                    job_id,
                    name,
                    priority,
                    state,
                    cpu_cores,
                    memory_bytes,
                    disk_bytes,
                    requires_gpu,
                    workload_kind,
                    sleep_duration_ms,
                    created_at_unix_ms,
                    assigned_worker_id,
                    lease_expires_at_unix_ms,
                    attempt_count,
                    max_attempts
                FROM jobs
                WHERE job_id = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        job_id.value(),
        "job_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result == SQLITE_DONE) {
        return std::nullopt;
    }

    if (result != SQLITE_ROW) {
        throw database_error(
            database,
            "Could not retrieve job"
        );
    }

    return record_from_row(
        database,
        statement.get()
    );
}

std::vector<JobRecord>
SqliteJobRepository::list() const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT
                    job_id,
                    name,
                    priority,
                    state,
                    cpu_cores,
                    memory_bytes,
                    disk_bytes,
                    requires_gpu,
                    workload_kind,
                    sleep_duration_ms,
                    created_at_unix_ms,
                    assigned_worker_id,
                    lease_expires_at_unix_ms,
                    attempt_count,
                    max_attempts
                FROM jobs
                ORDER BY
                    created_at_unix_ms,
                    job_id;
            )sql"
        );

    std::vector<JobRecord> records;

    while (true) {
        const int result =
            sqlite3_step(
                statement.get()
            );

        if (result == SQLITE_DONE) {
            break;
        }

        if (result != SQLITE_ROW) {
            throw database_error(
                database,
                "Could not list jobs"
            );
        }

        records.push_back(
            record_from_row(
                database,
                statement.get()
            )
        );
    }

    return records;
}

bool SqliteJobRepository::contains(
    const domain::JobId& job_id
) const {
    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                SELECT 1
                FROM jobs
                WHERE job_id = ?
                LIMIT 1;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        job_id.value(),
        "job_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result == SQLITE_ROW) {
        return true;
    }

    if (result == SQLITE_DONE) {
        return false;
    }

    throw database_error(
        database,
        "Could not check whether job exists"
    );
}

void SqliteJobRepository::erase(
    const domain::JobId& job_id
) {
    if (!contains(job_id)) {
        throw std::invalid_argument{
            "Cannot erase an unknown job"
        };
    }

    sqlite3* database =
        database_.native_handle();

    auto statement =
        prepare_statement(
            database,
            R"sql(
                DELETE FROM jobs
                WHERE job_id = ?;
            )sql"
        );

    bind_text(
        database,
        statement.get(),
        1,
        job_id.value(),
        "job_id"
    );

    const int result =
        sqlite3_step(
            statement.get()
        );

    if (result != SQLITE_DONE) {
        throw database_error(
            database,
            "Could not erase job"
        );
    }
}

std::size_t
SqliteJobRepository::size() const {
    const std::int64_t count =
        database_.query_int64(
            "SELECT COUNT(*) FROM jobs;"
        );

    if (count < 0) {
        throw std::runtime_error{
            "SQLite returned a negative job count"
        };
    }

    if (
        static_cast<std::uint64_t>(count) >
        static_cast<std::uint64_t>(
            std::numeric_limits<
                std::size_t
            >::max()
        )
    ) {
        throw std::runtime_error{
            "Stored job count exceeds size_t"
        };
    }

    return static_cast<std::size_t>(
        count
    );
}

}  // namespace radahn::persistence