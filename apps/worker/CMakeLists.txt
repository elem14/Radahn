add_executable(
    radahn-worker
    main.cpp
)

target_link_libraries(
    radahn-worker
    PRIVATE
        radahn_proto
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)