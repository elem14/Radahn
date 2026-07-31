#include "radahn/persistence/in_memory_worker_repository.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace radahn::persistence {

void InMemoryWorkerRepository::insert(
    domain::WorkerRecord worker
) {
    if (contains(worker.id())) {
        throw std::invalid_argument{
            "A worker with this ID already exists"
        };
    }

    records_.push_back(
        std::move(worker)
    );
}

void InMemoryWorkerRepository::update(
    domain::WorkerRecord worker
) {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&worker](
                const domain::WorkerRecord&
                    stored_worker
            ) {
                return
                    stored_worker.id() ==
                    worker.id();
            }
        );

    if (iterator == records_.end()) {
        throw std::invalid_argument{
            "Cannot update an unknown worker"
        };
    }

    *iterator = std::move(worker);
}

std::optional<domain::WorkerRecord>
InMemoryWorkerRepository::get(
    const domain::WorkerId& worker_id
) const {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&worker_id](
                const domain::WorkerRecord&
                    worker
            ) {
                return
                    worker.id() ==
                    worker_id;
            }
        );

    if (iterator == records_.end()) {
        return std::nullopt;
    }

    return *iterator;
}

std::vector<domain::WorkerRecord>
InMemoryWorkerRepository::list() const {
    return records_;
}

bool InMemoryWorkerRepository::contains(
    const domain::WorkerId& worker_id
) const {
    return std::any_of(
        records_.begin(),
        records_.end(),
        [&worker_id](
            const domain::WorkerRecord& worker
        ) {
            return
                worker.id() ==
                worker_id;
        }
    );
}

void InMemoryWorkerRepository::erase(
    const domain::WorkerId& worker_id
) {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&worker_id](
                const domain::WorkerRecord&
                    worker
            ) {
                return
                    worker.id() ==
                    worker_id;
            }
        );

    if (iterator == records_.end()) {
        throw std::invalid_argument{
            "Cannot erase an unknown worker"
        };
    }

    records_.erase(iterator);
}

std::size_t
InMemoryWorkerRepository::size() const {
    return records_.size();
}

}  // namespace radahn::persistence