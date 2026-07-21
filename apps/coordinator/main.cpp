#include <iostream>

#include "radahn/domain/version.hpp"

int main() {
    std::cout
        << "Radahn Coordinator "
        << radahn::domain::version()
        << '\n';

    return 0;
}
