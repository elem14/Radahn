add_executable(
    radahn-coordinator
    main.cpp
    coordinator_service.cpp
)

target_link_libraries(
    radahn-coordinator 
    PRIVATE
        radahn_coordinator
        radahn_proto
        radahn_workload_codec
        radahn_project_options
        radahn_project_warnings
)