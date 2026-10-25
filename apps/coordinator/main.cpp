#include <filesystem>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

#include <grpcpp/grpcpp.h>

#include "client_service.grpc.pb.h"
#include "worker_service.grpc.pb.h"

#include "coordinator_service.hpp"

#include "radahn/domain/version.hpp"

namespace {

void print_usage() {
    std::cerr
        << "Usage: radahn-coordinator"
        << " [database-path]"
        << " [--listen-address <host:port>]\n"
        << "\n"
        << "  database-path       SQLite database file to create or"
        << " open (default: ./radahn.db)\n"
        << "  --listen-address    Address and port to accept"
        << " connections on (default: 0.0.0.0:50051)\n"
        << "\n"
        << "Operating contract:\n"
        << "  - Supported platforms: source builds on macOS and"
        << " Linux only.\n"
        << "  - Workers and clients must be able to reach this"
        << " address directly, over the same network or a trusted"
        << " VPN.\n"
        << "  - There is no authentication and no transport"
        << " encryption; do not expose this address beyond a"
        << " trusted network.\n"
        << "  - Worker CPU/memory/disk capacity is used only for"
        << " scheduling reservations; it is never enforced as an OS"
        << " resource limit.\n"
        << "  - Every simultaneously running worker process must"
        << " use a unique worker ID; this is required for correct"
        << " behavior but is not rejected by the coordinator.\n";
}

struct CoordinatorConfig {
    std::filesystem::path database_path{
        "radahn.db"
    };

    std::string listen_address{
        "0.0.0.0:50051"
    };
};

[[nodiscard]] CoordinatorConfig parse_coordinator_config(
    int argc,
    char* argv[]
) {
    CoordinatorConfig config;

    bool database_path_set = false;

    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{
            argv[index]
        };

        if (argument == "--listen-address") {
            if (index + 1 >= argc) {
                throw std::invalid_argument{
                    "--listen-address requires a value"
                };
            }

            config.listen_address = argv[++index];

            continue;
        }

        if (
            !argument.empty() &&
            argument.front() == '-'
        ) {
            throw std::invalid_argument{
                "Unknown flag: " +
                std::string{argument}
            };
        }

        if (database_path_set) {
            throw std::invalid_argument{
                "Too many positional arguments"
            };
        }

        config.database_path =
            std::filesystem::path{argument};

        database_path_set = true;
    }

    return config;
}

}  // namespace

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

    for (int index = 1; index < argc; ++index) {
        const std::string_view argument{
            argv[index]
        };

        if (argument == "--help" || argument == "-h") {
            print_usage();

            return 0;
        }
    }

    CoordinatorConfig config;

    try {
        config = parse_coordinator_config(
            argc,
            argv
        );
    } catch (const std::invalid_argument& error) {
        std::cerr
            << error.what()
            << '\n';

        print_usage();

        return 1;
    }

    const auto resolved_database_path =
        std::filesystem::absolute(
            config.database_path
        );

    std::cout
        << "Radahn Coordinator "
        << radahn::domain::version()
        << '\n'
        << "Listen address: "
        << config.listen_address
        << '\n'
        << "Database path: "
        << resolved_database_path.string()
        << '\n'
        << "Authentication: none"
        << " | Transport encryption: none"
        << " (same-network or trusted-VPN connectivity required)\n"
        << "Resource requirements (CPU/memory/disk) are scheduling"
        << " reservations only; they are not enforced OS limits\n";

    try {
        radahn::coordinator_app::CoordinatorServiceImpl
            service{config.database_path};

        grpc::ServerBuilder builder;

        builder.AddListeningPort(
            config.listen_address,
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
                << "Failed to start Radahn Coordinator on "
                << config.listen_address
                << '\n';

            return 1;
        }

        std::cout
            << "Radahn Coordinator is listening on "
            << config.listen_address
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
