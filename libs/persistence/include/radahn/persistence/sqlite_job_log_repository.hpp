#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

#include "radahn/domain/id.hpp"

#include "radahn/persistence/job_log_repository.hpp"
#include "radahn/persistence/sqlite_database.hpp"

namespace radahn::persistence {

class SqliteJobLogRepository final
    : public IJobLogRepository {
public:
    /*
     * database object must outlive this repository
     * the constructor initializes the current schema if needed
     */
    explicit SqliteJobLogRepository(
        SqliteDatabase& database
    );

    void insert(
        JobLogRecord record
    ) override;

    [[nodiscard]]
    std::optional<JobLogRecord> get(
        const domain::JobId& job_id,
        std::uint64_t attempt_number,
        JobLogStream stream
    ) const override;

    [[nodiscard]]
    bool contains(
        const domain::JobId& job_id,
        std::uint64_t attempt_number,
        JobLogStream stream
    ) const override;

    void erase(
        const domain::JobId& job_id,
        std::uint64_t attempt_number,
        JobLogStream stream
    ) override;

    [[nodiscard]]
    std::size_t size() const override;

private:
    SqliteDatabase& database_;
};

}  // namespace radahn::persistence
