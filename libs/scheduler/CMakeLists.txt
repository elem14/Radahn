add_library(radahn_scheduler STATIC)

target_sources(
    radahn_scheduler
    PRIVATE
        src/worker_filter.cpp
        src/round_robin_policy.cpp
        src/least_loaded_policy.cpp
        src/job_queue.cpp
)

target_include_directories(
    radahn_scheduler
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(
    radahn_scheduler
    PUBLIC
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)