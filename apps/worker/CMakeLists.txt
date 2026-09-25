add_executable(
    radahn-worker
    main.cpp
    heartbeat_loop.cpp
    active_job_registry.cpp
)

target_link_libraries(
    radahn-worker
    PRIVATE
        radahn_proto
        radahn_domain
        radahn_project_options
        radahn_project_warnings
        radahn_execution
        radahn_workload_codec
)