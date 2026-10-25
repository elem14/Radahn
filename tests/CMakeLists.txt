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


add_executable(
    radahn-worker-record-tests
    unit/worker_record_tests.cpp
)

target_link_libraries(
    radahn-worker-record-tests
    PRIVATE
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.worker_record
    COMMAND radahn-worker-record-tests
)


add_executable(
    radahn-in-memory-coordinator-tests
    unit/in_memory_coordinator_tests.cpp
)

target_link_libraries(
    radahn-in-memory-coordinator-tests
    PRIVATE
        radahn_coordinator
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.in_memory_coordinator
    COMMAND radahn-in-memory-coordinator-tests
)

add_executable(
    radahn-persistence-tests
    unit/persistence_tests.cpp
)

target_link_libraries(
    radahn-persistence-tests
    PRIVATE
        radahn_persistence
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.persistence
    COMMAND radahn-persistence-tests
)

add_executable(
    radahn-coordinator-repository-tests
    unit/coordinator_repository_tests.cpp
)

target_link_libraries(
    radahn-coordinator-repository-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.coordinator_repository
    COMMAND radahn-coordinator-repository-tests
)

add_executable(
    radahn-sqlite-database-tests
    unit/sqlite_database_tests.cpp
)

target_link_libraries(
    radahn-sqlite-database-tests
    PRIVATE
        radahn_persistence
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.sqlite_database
    COMMAND radahn-sqlite-database-tests
)

add_executable(
    radahn-sqlite-job-repository-tests
    unit/sqlite_job_repository_tests.cpp
)

target_link_libraries(
    radahn-sqlite-job-repository-tests
    PRIVATE
        radahn_persistence
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.sqlite_job_repository
    COMMAND radahn-sqlite-job-repository-tests
)

add_executable(
    radahn-sqlite-worker-repository-tests
    unit/sqlite_worker_repository_tests.cpp
)

target_link_libraries(
    radahn-sqlite-worker-repository-tests
    PRIVATE
        radahn_persistence
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.sqlite_worker_repository
    COMMAND radahn-sqlite-worker-repository-tests
)

add_executable(
    radahn-job-log-repository-tests
    unit/job_log_repository_tests.cpp
)

target_link_libraries(
    radahn-job-log-repository-tests
    PRIVATE
        radahn_persistence
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_log_repository
    COMMAND radahn-job-log-repository-tests
)

add_executable(
    radahn-sqlite-coordinator-integration-tests
    unit/sqlite_coordinator_integration_tests.cpp
)

target_link_libraries(
    radahn-sqlite-coordinator-integration-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.sqlite_coordinator_integration
    COMMAND radahn-sqlite-coordinator-integration-tests
)

add_executable(
    radahn-sqlite-coordinator-recovery-tests
    unit/sqlite_coordinator_recovery_tests.cpp
)

target_link_libraries(
    radahn-sqlite-coordinator-recovery-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.sqlite_coordinator_recovery
    COMMAND radahn-sqlite-coordinator-recovery-tests
)

add_executable(
    radahn-worker-heartbeat-repository-tests
    unit/worker_heartbeat_repository_tests.cpp
)

target_link_libraries(
    radahn-worker-heartbeat-repository-tests
    PRIVATE
        radahn_persistence
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.worker_heartbeat_repository
    COMMAND radahn-worker-heartbeat-repository-tests
)

add_executable(
    radahn-worker-liveness-tests
    unit/worker_liveness_tests.cpp
)

target_link_libraries(
    radahn-worker-liveness-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.worker_liveness
    COMMAND radahn-worker-liveness-tests
)

add_executable(
    radahn-job-lease-repository-tests
    unit/job_lease_repository_tests.cpp
)

target_link_libraries(
    radahn-job-lease-repository-tests
    PRIVATE
        radahn_persistence
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_lease_repository
    COMMAND radahn-job-lease-repository-tests
)

add_executable(
    radahn-job-lease-lifecycle-tests
    unit/job_lease_lifecycle_tests.cpp
)

target_link_libraries(
    radahn-job-lease-lifecycle-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_lease_lifecycle
    COMMAND radahn-job-lease-lifecycle-tests
)

add_executable(
    radahn-job-lease-expiration-tests
    unit/job_lease_expiration_tests.cpp
)

target_link_libraries(
    radahn-job-lease-expiration-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_lease_expiration
    COMMAND radahn-job-lease-expiration-tests
)

add_executable(
    radahn-job-retry-requeue-tests
    unit/job_retry_requeue_tests.cpp
)

target_link_libraries(
    radahn-job-retry-requeue-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_retry_requeue
    COMMAND radahn-job-retry-requeue-tests
)

add_executable(
    radahn-job-retry-limit-tests
    unit/job_retry_limit_tests.cpp
)

target_link_libraries(
    radahn-job-retry-limit-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_retry_limit
    COMMAND radahn-job-retry-limit-tests
)

add_executable(
    radahn-job-reservation-lifecycle-tests
    unit/job_reservation_lifecycle_tests.cpp
)

target_link_libraries(
    radahn-job-reservation-lifecycle-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_reservation_lifecycle
    COMMAND radahn-job-reservation-lifecycle-tests
)

add_executable(
    radahn-dynamic-resource-fit-tests
    unit/dynamic_resource_fit_tests.cpp
)

target_link_libraries(
    radahn-dynamic-resource-fit-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.dynamic_resource_fit
    COMMAND radahn-dynamic-resource-fit-tests
)

add_executable(
    radahn-concurrent-job-execution-tests
    unit/concurrent_job_execution_tests.cpp
)

target_link_libraries(
    radahn-concurrent-job-execution-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.concurrent_job_execution
    COMMAND radahn-concurrent-job-execution-tests
)

add_executable(
    radahn-best-fit-policy-tests
    unit/best_fit_policy_tests.cpp
)

target_link_libraries(
    radahn-best-fit-policy-tests
    PRIVATE
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.best_fit_policy
    COMMAND radahn-best-fit-policy-tests
)

add_executable(
    radahn-command-workload-tests
    unit/command_workload_tests.cpp
)

target_link_libraries(
    radahn-command-workload-tests
    PRIVATE
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.command_workload
    COMMAND radahn-command-workload-tests
)

add_executable(
    radahn-workload-codec-tests
    unit/workload_codec_tests.cpp
)

target_link_libraries(
    radahn-workload-codec-tests
    PRIVATE
        radahn_workload_codec
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.workload_codec
    COMMAND radahn-workload-codec-tests
)

add_executable(
    radahn-command-persistence-tests
    unit/command_persistence_tests.cpp
)

target_link_libraries(
    radahn-command-persistence-tests
    PRIVATE
        radahn_persistence
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.command_persistence
    COMMAND radahn-command-persistence-tests
)

add_executable(
    radahn-command-executor-tests
    unit/command_executor_tests.cpp
)

target_link_libraries(
    radahn-command-executor-tests
    PRIVATE
        radahn_execution
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.command_executor
    COMMAND radahn-command-executor-tests
)

set_tests_properties(
    radahn.command_executor
    PROPERTIES
        TIMEOUT 20
)

add_executable(
    radahn-execution-result-tests
    unit/execution_result_tests.cpp
)

target_link_libraries(
    radahn-execution-result-tests
    PRIVATE
        radahn_persistence
        radahn_workload_codec
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.execution_result
    COMMAND radahn-execution-result-tests
)

add_executable(
    radahn-event-logging-tests
    unit/event_logging_tests.cpp
)

target_link_libraries(
    radahn-event-logging-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.event_logging
    COMMAND radahn-event-logging-tests
)

add_executable(
    radahn-job-attempt-fencing-tests
    unit/job_attempt_fencing_tests.cpp
)

target_link_libraries(
    radahn-job-attempt-fencing-tests
    PRIVATE
        radahn_coordinator
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.job_attempt_fencing
    COMMAND radahn-job-attempt-fencing-tests
)

add_executable(
    radahn-active-job-registry-tests
    unit/active_job_registry_tests.cpp
    ${CMAKE_SOURCE_DIR}/apps/worker/active_job_registry.cpp
)

target_include_directories(
    radahn-active-job-registry-tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/apps/worker
)

target_link_libraries(
    radahn-active-job-registry-tests
    PRIVATE
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.active_job_registry
    COMMAND radahn-active-job-registry-tests
)

add_executable(
    radahn-execution-log-registry-tests
    unit/execution_log_registry_tests.cpp
    ${CMAKE_SOURCE_DIR}/apps/worker/execution_log_registry.cpp
)

target_include_directories(
    radahn-execution-log-registry-tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/apps/worker
)

target_link_libraries(
    radahn-execution-log-registry-tests
    PRIVATE
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.execution_log_registry
    COMMAND radahn-execution-log-registry-tests
)

add_executable(
    radahn-sqlite-transaction-tests
    unit/sqlite_transaction_tests.cpp
)

target_link_libraries(
    radahn-sqlite-transaction-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.sqlite_transaction
    COMMAND radahn-sqlite-transaction-tests
)

add_executable(
    radahn-coordinator-rpc-integration-tests
    unit/coordinator_rpc_integration_tests.cpp
    ${CMAKE_SOURCE_DIR}/apps/coordinator/coordinator_service.cpp
)

target_include_directories(
    radahn-coordinator-rpc-integration-tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/apps/coordinator
)

target_link_libraries(
    radahn-coordinator-rpc-integration-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_proto
        radahn_workload_codec
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.coordinator_rpc_integration
    COMMAND radahn-coordinator-rpc-integration-tests
)

add_executable(
    radahn-completion-reporter-tests
    unit/completion_reporter_tests.cpp
    ${CMAKE_SOURCE_DIR}/apps/worker/completion_reporter.cpp
)

target_include_directories(
    radahn-completion-reporter-tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/apps/worker
)

target_link_libraries(
    radahn-completion-reporter-tests
    PRIVATE
        radahn_coordinator
        radahn_persistence
        radahn_scheduler
        radahn_domain
        radahn_workload_codec
        radahn_project_options
        radahn_project_warnings
)

add_test(
    NAME radahn.completion_reporter
    COMMAND radahn-completion-reporter-tests
)