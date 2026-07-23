add_executable(
    radahn-coordinator
    main.cpp
)

target_link_libraries(
    radahn-coordinator 
    PRIVATE
        radahn_coordinator
        radahn_proto
        radahn_project_options
        radahn_project_warnings
)