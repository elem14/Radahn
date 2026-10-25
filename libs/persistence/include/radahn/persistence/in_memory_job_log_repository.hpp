#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/persistence/job_log_repository.hpp"

namespace radahn::persistence {

class InMemoryJobLogRepository final
    : public IJobLogRepository {
public:
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
    std::vector<JobLogRecord> records_;
};

}  // namespace radahn::persistence
