# src/nodeC/CMakeLists.txt
cmake_minimum_required(VERSION 3.10)
project(nodeC)

find_package(gRPC CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)

set(PROTO_SRC
    ${CMAKE_SOURCE_DIR}/proto/data.pb.cc
    ${CMAKE_SOURCE_DIR}/proto/data.grpc.pb.cc
)
set(PROTO_HDR
    ${CMAKE_SOURCE_DIR}/proto/data.pb.h
    ${CMAKE_SOURCE_DIR}/proto/data.grpc.pb.h
)

add_executable(serverC
    serverC.cpp
    ${PROTO_SRC}
    ${PROTO_HDR}
)

target_include_directories(serverC PRIVATE
    ${CMAKE_SOURCE_DIR}/proto
    ${Protobuf_INCLUDE_DIRS}
    ${gRPC_INCLUDE_DIRS}
)

target_link_libraries(serverC
    PRIVATE
    gRPC::grpc++
    protobuf::libprotobuf
    config_reader
)
