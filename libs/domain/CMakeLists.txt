add_library(radahn_domain STATIC)

target_sources(
    radahn_domain
    PRIVATE
        src/version.cpp
        src/job_state.cpp
        src/resource.cpp
        src/worker.cpp
        src/worker_eligibility.cpp
)

target_include_directories(
    radahn_domain
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(
    radahn_domain
    PUBLIC
        radahn_project_options
        radahn_project_warnings
)