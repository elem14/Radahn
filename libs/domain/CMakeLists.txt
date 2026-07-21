add_library(radahn_domain STATIC)

target_sources(
    radahn_domain
    PRIVATE
        src/version.cpp
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