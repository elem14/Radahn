add_executable(
    radahn-domain-tests
    unit/domain_tests.cpp
)

target_link_libraries(
    radahn-domain-tests
    PRIVATE
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.domain
    COMMAND radahn-domain-tests
)

add_executable(
    radahn-resource-tests
    unit/resource_tests.cpp
)

target_link_libraries(
    radahn-resource-tests
    PRIVATE
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.resources
    COMMAND radahn-resource-tests
)