#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"

#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/job_repository.hpp"
#include "radahn/persistence/sqlite_database.hpp"

namespace radahn::persistence {

class SqliteJobRepository final
    : public IJobRepository {
public:
    /*
     * database object must outlive this repository
     * the constructor initializes the current schema if needed
     */
    explicit SqliteJobRepository(
        SqliteDatabase& database
    );

    void insert(
        JobRecord record
    ) override;

    void update(
        JobRecord record
    ) override;

    [[nodiscard]]
    std::optional<JobRecord> get(
        const domain::JobId& job_id
    ) const override;

    [[nodiscard]]
    std::vector<JobRecord>
    list() const override;

    [[nodiscard]]
    bool contains(
        const domain::JobId& job_id
    ) const override;

    void erase(
        const domain::JobId& job_id
    ) override;

    [[nodiscard]]
    std::size_t size() const override;

private:
    SqliteDatabase& database_;
};

}  // namespace radahn::persistence