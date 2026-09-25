if(NOT UNIX)
    message(
        FATAL_ERROR
        "The command executor requires a POSIX platform"
    )
endif()

add_library(
    radahn_execution
    STATIC
        src/command_executor.cpp
)

target_include_directories(
    radahn_execution
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(
    radahn_execution
    PUBLIC
        radahn_domain

    PRIVATE
        radahn_project_options
        radahn_project_warnings
)