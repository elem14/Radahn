add_executable(
    radahn-coordinator
    main.cpp
)

target_link_libraries(
    radahn-coordinator
    PRIVATE
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)