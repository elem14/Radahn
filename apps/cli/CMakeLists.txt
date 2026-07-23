add_executable(
    radahn
    main.cpp
)

target_link_libraries(
    radahn
    PRIVATE
        radahn_proto
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)