cmake_minimum_required(VERSION 3.10)

set(PROTO_SRC "${PROJECT_SOURCE_DIR}/proto/data.pb.cc"
              "${PROJECT_SOURCE_DIR}/proto/data.grpc.pb.cc")

set(PROTO_HDR "${PROJECT_SOURCE_DIR}/proto/data.pb.h"
              "${PROJECT_SOURCE_DIR}/proto/data.grpc.pb.h")

add_executable(serverA serverA.cpp ${PROTO_SRC} ${PROTO_HDR})
target_include_directories(serverA PRIVATE ${PROJECT_SOURCE_DIR}/proto)
target_link_libraries(serverA
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
    config_reader  # from the top-level library
)
