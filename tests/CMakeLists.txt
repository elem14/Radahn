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


add_executable(
    radahn-scheduler-tests
    unit/scheduler_tests.cpp
)

target_link_libraries(
    radahn-scheduler-tests
    PRIVATE
        radahn_scheduler
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.scheduler
    COMMAND radahn-scheduler-tests
)


add_executable(
    radahn-job-queue-tests
    unit/job_queue_tests.cpp
)

target_link_libraries(
    radahn-job-queue-tests
    PRIVATE
        radahn_scheduler
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_queue
    COMMAND radahn-job-queue-tests
)


add_executable(
    radahn-dispatch-planner-tests
    unit/dispatch_planner_tests.cpp
)

target_link_libraries(
    radahn-dispatch-planner-tests
    PRIVATE
        radahn_scheduler
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.dispatch_planner
    COMMAND radahn-dispatch-planner-tests
)