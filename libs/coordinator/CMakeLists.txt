add_library(radahn_coordinator STATIC)

target_sources(
    radahn_coordinator
    PRIVATE
        src/in_memory_coordinator.cpp
)

target_include_directories(
    radahn_coordinator
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(
    radahn_coordinator
    PUBLIC
        radahn_scheduler
        radahn_domain
        radahn_project_options
        radahn_project_warnings
)