cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME hdfs)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES src/hdfs_extension.cpp src/hdfs_filesystem.cpp)

# Execute Cargo build
add_custom_target(hdfs_bridge_cargo
    COMMAND cargo build --release
    WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/hdfs-bridge
    BYPRODUCTS ${CMAKE_CURRENT_LIST_DIR}/hdfs-bridge/target/release/libhdfs_bridge.a
)

# Specify static library path
set(HDFS_BRIDGE_LIB ${CMAKE_CURRENT_LIST_DIR}/hdfs-bridge/target/release/libhdfs_bridge.a)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Ensure dependency order
add_dependencies(${EXTENSION_NAME} hdfs_bridge_cargo)
add_dependencies(${LOADABLE_EXTENSION_NAME} hdfs_bridge_cargo)

# Link OpenSSL and Rust FFI bridge in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${HDFS_BRIDGE_LIB})
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${HDFS_BRIDGE_LIB})

# Link macOS system frameworks needed by Rust stdlib
if(APPLE)
    target_link_libraries(${EXTENSION_NAME} "-framework CoreFoundation" "-framework Security" "-framework SystemConfiguration")
    target_link_libraries(${LOADABLE_EXTENSION_NAME} "-framework CoreFoundation" "-framework Security" "-framework SystemConfiguration")
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
