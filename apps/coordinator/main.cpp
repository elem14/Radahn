#include <filesystem>
#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "client_service.grpc.pb.h"
#include "worker_service.grpc.pb.h"

#include "coordinator_service.hpp"

#include "radahn/domain/version.hpp"

int main(
    int argc,
    char* argv[]
) {
    /*
     * stdout is fully buffered (not line-buffered) whenever it's
     * redirected to a file or pipe rather than a TTY, which is
     * exactly the deployed case — logs would sit unflushed for an
     * unbounded time, invisible to anything tailing the
     * coordinator's log. Force every write to flush immediately.
     */
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    if (argc > 2) {
        std::cerr
            << "Usage: radahn-coordinator"
            << " [database-path]\n";

        return 1;
    }

    /*
     * Running without an argument creates or opens ./radahn.db
     * Tests can pass a temporary database path instead
     */
    std::filesystem::path database_path{
        "radahn.db"
    };

    if (argc == 2) {
        database_path =
            std::filesystem::path{
                argv[1]
            };
    }

    const std::string server_address{
        "0.0.0.0:50051"
    };

    try {
        radahn::coordinator_app::CoordinatorServiceImpl
            service{database_path};

        grpc::ServerBuilder builder;

        builder.AddListeningPort(
            server_address,
            grpc::InsecureServerCredentials()
        );

        builder.RegisterService(
            static_cast<
                radahn::rpc::v1::ClientService::Service*
            >(
                &service
            )
        );

        builder.RegisterService(
            static_cast<
                radahn::rpc::v1::WorkerService::Service*
            >(
                &service
            )
        );

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
            << '\n'
            << "Database: "
            << database_path.string()
            << '\n';

        server->Wait();

        return 0;
    } catch (const std::exception& error) {
        std::cerr
            << "Coordinator startup failed: "
            << error.what()
            << '\n';

        return 1;
    }
}
