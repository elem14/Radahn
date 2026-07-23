find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(RADAHN_PROTO_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/client_service.proto
)

set(RADAHN_PROTO_GENERATED_DIR
    ${CMAKE_CURRENT_BINARY_DIR}/generated
)

file(MAKE_DIRECTORY
    ${RADAHN_PROTO_GENERATED_DIR}
)

set(RADAHN_PROTO_SOURCES
    ${RADAHN_PROTO_GENERATED_DIR}/client_service.pb.cc
    ${RADAHN_PROTO_GENERATED_DIR}/client_service.grpc.pb.cc
)

set(RADAHN_PROTO_HEADERS
    ${RADAHN_PROTO_GENERATED_DIR}/client_service.pb.h
    ${RADAHN_PROTO_GENERATED_DIR}/client_service.grpc.pb.h
)

add_custom_command(
    OUTPUT
        ${RADAHN_PROTO_SOURCES}
        ${RADAHN_PROTO_HEADERS}

    COMMAND
        $<TARGET_FILE:protobuf::protoc>

    ARGS
        --proto_path=${CMAKE_CURRENT_SOURCE_DIR}
        --cpp_out=${RADAHN_PROTO_GENERATED_DIR}
        --grpc_out=${RADAHN_PROTO_GENERATED_DIR}
        --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
        ${RADAHN_PROTO_FILE}

    DEPENDS
        ${RADAHN_PROTO_FILE}

    COMMENT
        "Generating Radahn Protocol Buffer and gRPC sources"

    VERBATIM
)

add_library(radahn_proto STATIC)

target_sources(
    radahn_proto
    PRIVATE
        ${RADAHN_PROTO_SOURCES}
        ${RADAHN_PROTO_HEADERS}
)

target_include_directories(
    radahn_proto
    PUBLIC
        ${RADAHN_PROTO_GENERATED_DIR}
)

target_link_libraries(
    radahn_proto
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
        radahn_project_options
        radahn_project_warnings
)