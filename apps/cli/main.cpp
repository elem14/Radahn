#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include <grpcpp/grpcpp.h>

#include "client_service.grpc.pb.h"
#include "radahn/domain/version.hpp"

namespace {

void print_usage() {
    std::cout
        << "Radahn distributed computing platform\n\n"
        << "Usage:\n"
        << "  radahn version\n"
        << "  radahn ping [message]\n"
        << "  radahn help\n";
}

int run_ping(std::string message) {
    const auto channel = grpc::CreateChannel(
        "localhost:50051",
        grpc::InsecureChannelCredentials()
    );

    const auto stub =
        radahn::rpc::v1::ClientService::NewStub(
            channel
        );

    radahn::rpc::v1::PingRequest request;
    request.set_message(std::move(message));

    radahn::rpc::v1::PingResponse response;
    grpc::ClientContext context;

    const grpc::Status status = stub->Ping(
        &context,
        request,
        &response
    );

    if (!status.ok()) {
        std::cerr
            << "Ping failed: "
            << status.error_message()
            << '\n';

        return 1;
    }

    std::cout
        << response.message()
        << '\n'
        << "Coordinator version: "
        << response.coordinator_version()
        << '\n';

    return 0;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 0;
    }

    const std::string_view command{argv[1]};

    if (command == "version") {
        std::cout
            << "Radahn "
            << radahn::domain::version()
            << '\n';

        return 0;
    }

    if (command == "ping") {
        std::string message{"hello"};

        if (argc >= 3) {
            message = argv[2];
        }

        return run_ping(std::move(message));
    }

    if (command == "help") {
        print_usage();
        return 0;
    }

    std::cerr
        << "Unknown command: "
        << command
        << '\n';

    print_usage();

    return 1;
}