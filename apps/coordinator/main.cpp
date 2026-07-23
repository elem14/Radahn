#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "client_service.grpc.pb.h"
#include "radahn/domain/version.hpp"

namespace {

class ClientServiceImpl final
    : public radahn::rpc::v1::ClientService::Service {
public:
    grpc::Status Ping(
        grpc::ServerContext* context,
        const radahn::rpc::v1::PingRequest* request,
        radahn::rpc::v1::PingResponse* response
    ) override {
        static_cast<void>(context);

        response->set_message(
            "pong: " + request->message()
        );

        response->set_coordinator_version(
            std::string{radahn::domain::version()}
        );

        return grpc::Status::OK;
    }
};

}  // namespace

int main() {
    const std::string server_address{
        "0.0.0.0:50051"
    };

    ClientServiceImpl service;

    grpc::ServerBuilder builder;

    builder.AddListeningPort(
        server_address,
        grpc::InsecureServerCredentials()
    );

    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server{
        builder.BuildAndStart()
    };

    if (!server) {
        std::cerr
            << "Failed to start Radahn Coordinator\n";

        return 1;
    }

    std::cout
        << "Radahn Coordinator "
        << radahn::domain::version()
        << " listening on "
        << server_address
        << '\n';

    server->Wait();

    return 0;
}
