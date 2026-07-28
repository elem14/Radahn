#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/persistence/job_record.hpp"
#include "radahn/persistence/job_repository.hpp"

namespace radahn::persistence {

class InMemoryJobRepository final
    : public IJobRepository {
public:
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
    std::size_t size() const noexcept override;

private:
    std::vector<JobRecord> records_;
};

}  // namespace radahn::persistence