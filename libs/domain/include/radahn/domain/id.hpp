# pragma once

#include <compare>
#include <stdexcept>
#include <string>
#include <utility>

namespace radahn::domain {

template <typename Tag>
class StrongId {
public:
    explicit StrongId(std::string value)
        : value_{std::move(value)} {
        if (value_.empty()) {
            throw std::invalid_argument{"ID cannot be empty"};
        }
    }

    [[nodiscard]] const std::string& value() const noexcept {
        return value_;
    }

    auto operator<=>(const StrongId&) const = default;

private:
    std::string value_;
};

struct JobIdTag;
struct AttemptIdTag;
struct WorkerIdTag;
struct ExperimentIdTag;

using JobId = StrongId<JobIdTag>;
using AttemptId = StrongId<AttemptIdTag>;
using WorkerId = StrongId<WorkerIdTag>;
using ExperimentId = StrongId<ExperimentIdTag>;

} // namespace radahn::domain