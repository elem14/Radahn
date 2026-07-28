#pragma once

#include <cstddef>
#include <optional>
#include <vector>

#include "radahn/domain/id.hpp"
#include "radahn/domain/worker_record.hpp"
#include "radahn/persistence/worker_repository.hpp"

namespace radahn::persistence {

class InMemoryWorkerRepository final
    : public IWorkerRepository {
public:
    void insert(
        domain::WorkerRecord worker
    ) override;

    void update(
        domain::WorkerRecord worker
    ) override;

    [[nodiscard]]
    std::optional<domain::WorkerRecord> get(
        const domain::WorkerId& worker_id
    ) const override;

    [[nodiscard]]
    std::vector<domain::WorkerRecord>
    list() const override;

    [[nodiscard]]
    bool contains(
        const domain::WorkerId& worker_id
    ) const override;

    void erase(
        const domain::WorkerId& worker_id
    ) override;

    [[nodiscard]]
    std::size_t size() const noexcept override;

private:
    std::vector<domain::WorkerRecord>
        records_;
};

}  // namespace radahn::persistence