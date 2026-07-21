#include <iostream>
#include <string_view>

#include "radahn/domain/version.hpp"

namespace {

void print_usage() {
    std::cout
        << "Radahn distributed computing platform\n\n"
        << "Usage:\n"
        << "  radahn version\n"
        << "  radahn help\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 0;
    }

    const std::string_view command{argv[1]};

    if (command == "version") {
        std::cout << "Radahn " << radahn::domain::version() << '\n';
        return 0;
    }

    if (command == "help") {
        print_usage();
        return 0;
    }

    std::cerr << "Unknown command: " << command << '\n';
    print_usage();

    return 1;
}