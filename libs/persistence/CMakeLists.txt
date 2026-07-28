add_library(
    radahn_persistence
    STATIC
        src/in_memory_job_repository.cpp
        src/in_memory_worker_repository.cpp
)

target_include_directories(
    radahn_persistence
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(
    radahn_persistence
    PUBLIC
        radahn_domain

    PRIVATE
        radahn_project_options
        radahn_project_warnings
)