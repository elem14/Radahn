find_package(
    unofficial-sqlite3
    CONFIG
    REQUIRED
)

add_library(
    radahn_persistence
    STATIC
        src/in_memory_job_repository.cpp
        src/in_memory_worker_repository.cpp
        src/sqlite_database.cpp
        src/sqlite_schema.cpp
        src/sqlite_job_repository.cpp
        src/sqlite_worker_repository.cpp
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
        unofficial::sqlite3::sqlite3
        radahn_project_options
        radahn_project_warnings
)