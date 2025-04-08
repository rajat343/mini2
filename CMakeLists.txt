cmake_minimum_required(VERSION 3.10)
project(Mini2Project)

# Force a C++ standard. 14 or 17 is fine; nlohmann/json needs at least 11.
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Then the rest...
find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)

include_directories("/opt/homebrew/include")

# Optionally, if using nlohmann/json as a submodule or local header:
include_directories(${PROJECT_SOURCE_DIR}/third_party)

# Generate stubs
# (Assuming you run protoc manually, or do it here with an add_custom_command)

# read_config library
add_library(config_reader src/read_config.cpp)
target_include_directories(config_reader PUBLIC ${PROJECT_SOURCE_DIR}/src)
# If you have nlohmann/json in third_party, just ensure you can #include <nlohmann/json.hpp>

# NodeA
add_subdirectory(src/nodeA)
add_subdirectory(src/nodeB)
add_subdirectory(src/nodeC)
add_subdirectory(src/nodeD)
add_subdirectory(src/nodeE)
