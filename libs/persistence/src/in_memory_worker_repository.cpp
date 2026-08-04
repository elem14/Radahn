#include "radahn/persistence/in_memory_worker_repository.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>
#include <vector>

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
        StoredWorker{
            std::move(worker),
            WorkerHeartbeatClock::now()
        }
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
                const StoredWorker& stored_worker
            ) {
                return
                    stored_worker.worker.id() ==
                    worker.id();
            }
        );

    if (iterator == records_.end()) {
        throw std::invalid_argument{
            "Cannot update an unknown worker"
        };
    }

    /*
     * Only replace the domain worker record.
     *
     * The heartbeat timestamp remains unchanged.
     */
    iterator->worker =
        std::move(worker);
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
                const StoredWorker& stored_worker
            ) {
                return
                    stored_worker.worker.id() ==
                    worker_id;
            }
        );

    if (iterator == records_.end()) {
        return std::nullopt;
    }

    return iterator->worker;
}

std::vector<domain::WorkerRecord>
InMemoryWorkerRepository::list() const {
    std::vector<domain::WorkerRecord> workers;

    workers.reserve(
        records_.size()
    );

    for (const auto& stored_worker : records_) {
        workers.push_back(
            stored_worker.worker
        );
    }

    return workers;
}

bool InMemoryWorkerRepository::contains(
    const domain::WorkerId& worker_id
) const {
    return std::any_of(
        records_.begin(),
        records_.end(),
        [&worker_id](
            const StoredWorker& stored_worker
        ) {
            return
                stored_worker.worker.id() ==
                worker_id;
        }
    );
}

void InMemoryWorkerRepository::record_heartbeat(
    const domain::WorkerId& worker_id,
    WorkerHeartbeatTimePoint heartbeat_time
) {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&worker_id](
                const StoredWorker& stored_worker
            ) {
                return
                    stored_worker.worker.id() ==
                    worker_id;
            }
        );

    if (iterator == records_.end()) {
        throw std::invalid_argument{
            "Cannot record a heartbeat for an unknown worker"
        };
    }

    iterator->last_heartbeat =
        heartbeat_time;
}

std::optional<WorkerHeartbeatTimePoint>
InMemoryWorkerRepository::last_heartbeat(
    const domain::WorkerId& worker_id
) const {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&worker_id](
                const StoredWorker& stored_worker
            ) {
                return
                    stored_worker.worker.id() ==
                    worker_id;
            }
        );

    if (iterator == records_.end()) {
        return std::nullopt;
    }

    return iterator->last_heartbeat;
}

void InMemoryWorkerRepository::erase(
    const domain::WorkerId& worker_id
) {
    const auto iterator =
        std::find_if(
            records_.begin(),
            records_.end(),
            [&worker_id](
                const StoredWorker& stored_worker
            ) {
                return
                    stored_worker.worker.id() ==
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