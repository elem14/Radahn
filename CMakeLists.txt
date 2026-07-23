cmake_minimum_required(VERSION 3.25)

project(
    Radahn
    VERSION 0.1.0
    DESCRIPTION "Distributed experiment and batch-computing platform"
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(RADAHN_WARNINGS_AS_ERRORS
    "Treat compiler warnings as errors"
    OFF
)

# Shared compiler settings used by all Radahn targets.
add_library(radahn_project_options INTERFACE)

target_compile_features(
    radahn_project_options
    INTERFACE
        cxx_std_20
)

# Shared warning configuration.
add_library(radahn_project_warnings INTERFACE)

if(MSVC)
    target_compile_options(
        radahn_project_warnings
        INTERFACE
            /W4
            /permissive-
    )

    if(RADAHN_WARNINGS_AS_ERRORS)
        target_compile_options(
            radahn_project_warnings
            INTERFACE
                /WX
        )
    endif()
else()
    target_compile_options(
        radahn_project_warnings
        INTERFACE
            -Wall
            -Wextra
            -Wpedantic
            -Wconversion
            -Wshadow
    )

    if(RADAHN_WARNINGS_AS_ERRORS)
        target_compile_options(
            radahn_project_warnings
            INTERFACE
                -Werror
        )
    endif()
endif()

enable_testing()

add_subdirectory(libs/domain)
add_subdirectory(libs/scheduler)
add_subdirectory(libs/coordinator)

add_subdirectory(proto)

add_subdirectory(apps/coordinator)
add_subdirectory(apps/worker)
add_subdirectory(apps/cli)

add_subdirectory(tests)