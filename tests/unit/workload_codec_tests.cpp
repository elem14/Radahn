#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "workload_codec.hpp"

namespace {

using namespace std::chrono_literals;

using radahn::domain::WorkloadKind;
using radahn::domain::WorkloadSpec;
using radahn::rpc_codec::decode_workload;
using radahn::rpc_codec::encode_workload;

namespace rpc = radahn::rpc::v1;

int failure_count = 0;

void expect(
    bool condition,
    std::string_view description
) {
    if (condition) {
        std::cout << "[PASS] " << description << '\n';
    } else {
        std::cerr << "[FAIL] " << description << '\n';
        ++failure_count;
    }
}

template<class Action>
void expect_invalid(
    Action action,
    std::string_view description
) {
    try {
        action();
    } catch (const std::invalid_argument&) {
        expect(true, description);
        return;
    } catch (...) {
        expect(false, description);
        return;
    }

    expect(false, description);
}

void test_command_round_trip() {
    const std::vector<std::string> args{
        "simulation.py",
        "trial one",
        "",
        "repeat",
        "repeat"
    };

    const auto original = WorkloadSpec::command({
        "python3",
        args,
        30s
    });

    rpc::WorkloadSpec encoded;
    encode_workload(original, encoded);

    const auto bytes = encoded.SerializeAsString();

    rpc::WorkloadSpec parsed;

    const bool parsed_ok =
        parsed.ParseFromString(bytes);

    expect(
        parsed_ok,
        "Serialized command can be parsed"
    );

    if (!parsed_ok) {
        return;
    }

    const auto restored = decode_workload(parsed);

    expect(
        restored.kind() == WorkloadKind::command,
        "Command kind survives serialization"
    );

    expect(
        restored.command_spec().executable == "python3",
        "Executable survives serialization"
    );

    expect(
        restored.command_spec().args == args,
        "Arguments survive serialization exactly"
    );

    expect(
        restored.command_spec().timeout == 30s,
        "Timeout survives serialization"
    );
}

void test_missing_timeout() {
    const auto original = WorkloadSpec::command({
        "hostname",
        {},
        std::nullopt
    });

    rpc::WorkloadSpec encoded;
    encode_workload(original, encoded);

    expect(
        !encoded.command().has_timeout_ms(),
        "Absent timeout stays absent on the wire"
    );

    const auto restored = decode_workload(encoded);

    expect(
        !restored.command_spec().timeout.has_value(),
        "Absent timeout stays absent in the domain"
    );
}

void test_message_reuse() {
    rpc::WorkloadSpec message;

    encode_workload(
        WorkloadSpec::command({
            "python3",
            {"task.py"},
            10s
        }),
        message
    );

    encode_workload(
        WorkloadSpec::sleep(250ms),
        message
    );

    expect(
        !message.has_command(),
        "Encoding sleep clears old command fields"
    );

    expect(
        decode_workload(message).sleep_duration() == 250ms,
        "Sleep still decodes correctly"
    );

    encode_workload(
        WorkloadSpec::command({
            "hostname",
            {},
            std::nullopt
        }),
        message
    );

    expect(
        message.sleep_duration_ms() == 0,
        "Encoding command clears old sleep duration"
    );

    expect(
        message.command().args_size() == 0 &&
        !message.command().has_timeout_ms(),
        "Old arguments and timeout do not leak"
    );
}

void test_invalid_messages() {
    rpc::WorkloadSpec message;

    expect_invalid(
        [&] {
            (void)decode_workload(message);
        },
        "Missing workload kind is rejected"
    );

    message.set_kind(rpc::WORKLOAD_KIND_COMMAND);

    expect_invalid(
        [&] {
            (void)decode_workload(message);
        },
        "Missing command specification is rejected"
    );

    message.mutable_command()->set_executable("");

    expect_invalid(
        [&] {
            (void)decode_workload(message);
        },
        "Domain validation rejects empty executable"
    );

    message.mutable_command()->set_executable("python3");
    message.mutable_command()->set_timeout_ms(0);

    expect_invalid(
        [&] {
            (void)decode_workload(message);
        },
        "Explicit zero timeout is rejected"
    );

    message.mutable_command()->set_timeout_ms(
        std::numeric_limits<std::uint64_t>::max()
    );

    expect_invalid(
        [&] {
            (void)decode_workload(message);
        },
        "Overflowing timeout is rejected"
    );

    message.mutable_command()->clear_timeout_ms();
    message.set_sleep_duration_ms(100);

    expect_invalid(
        [&] {
            (void)decode_workload(message);
        },
        "Command with sleep duration is rejected"
    );

    message.set_kind(rpc::WORKLOAD_KIND_SLEEP);

    expect_invalid(
        [&] {
            (void)decode_workload(message);
        },
        "Sleep with command fields is rejected"
    );

    message.clear_command();
    message.set_sleep_duration_ms(0);

    expect_invalid(
        [&] {
            (void)decode_workload(message);
        },
        "Zero sleep duration is rejected"
    );
}

}  // namespace

int main() {
    try {
        test_command_round_trip();
        test_missing_timeout();
        test_message_reuse();
        test_invalid_messages();
    } catch (const std::exception& error) {
        std::cerr
            << "[FAIL] Unexpected exception: "
            << error.what()
            << '\n';

        return EXIT_FAILURE;
    }

    return failure_count == 0
        ? EXIT_SUCCESS
        : EXIT_FAILURE;
}