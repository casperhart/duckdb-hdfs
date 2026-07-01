cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME hdfs)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES src/hdfs_extension.cpp src/hdfs_filesystem.cpp src/hdfs_functions.cpp)

# Build the Rust FFI bridge (hdfs-bridge) via Corrosion. Corrosion integrates
# cargo into the cmake build and, crucially for CI, honours -DRust_CARGO_TARGET
# (set by extension-ci-tools for the osx/windows cross-compile targets) so the
# static library is built for the right architecture. It also propagates the
# native system libraries cargo reports, so platform link deps (CoreFoundation /
# Security on macOS, ws2_32 / bcrypt on Windows, pthread / dl on Linux) are
# linked automatically rather than enumerated by hand.
include(FetchContent)
FetchContent_Declare(
    Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5.2
)
FetchContent_MakeAvailable(Corrosion)

corrosion_import_crate(MANIFEST_PATH ${CMAKE_CURRENT_LIST_DIR}/hdfs-bridge/Cargo.toml)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Corrosion exposes the crate's library target (cargo lib name `hdfs_bridge`).
# Linking it pulls in the static library and its native link dependencies. The
# build_*_extension helpers already used the plain target_link_libraries
# signature on these targets, so we must match it (no PUBLIC/PRIVATE keyword).
target_link_libraries(${EXTENSION_NAME} hdfs_bridge)
target_link_libraries(${LOADABLE_EXTENSION_NAME} hdfs_bridge)

# Native libraries pulled in by the bridge's third-party crates (TLS / OS
# integration). Corrosion propagates the Rust std native libs for a staticlib
# but not these, so list them explicitly. The macOS set is authoritative (from
# `cargo rustc -- --print native-static-libs`); Linux's needs (pthread/dl/m) are
# already covered by Corrosion's std libs.
if(APPLE)
    set(HDFS_NATIVE_LIBS "-framework Security" "-framework SystemConfiguration"
                         "-framework CoreFoundation" iconv)
elseif(WIN32)
    set(HDFS_NATIVE_LIBS ws2_32 bcrypt ncrypt secur32 crypt32 userenv ntdll)
else()
    set(HDFS_NATIVE_LIBS)
endif()

if(HDFS_NATIVE_LIBS)
    target_link_libraries(${EXTENSION_NAME} ${HDFS_NATIVE_LIBS})
    target_link_libraries(${LOADABLE_EXTENSION_NAME} ${HDFS_NATIVE_LIBS})
endif()

# Install the static extension archive. We intentionally omit
# EXPORT "${DUCKDB_EXPORT_SET}" (used by core extensions for find_package(DuckDB)
# consumers): exporting it would require the imported Corrosion target
# `hdfs_bridge` to also be installable, which it is not. A community extension
# distributes the loadable `.duckdb_extension`, so the export set is unneeded.
install(
  TARGETS ${EXTENSION_NAME}
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
