cmake_minimum_required(VERSION 3.12)

set(CMAKE_CXX_STANDARD 17)

# Set extension name here
set(TARGET_NAME chsql)

project(${TARGET_NAME})

if(DUCKDB_EXPLICIT_PLATFORM MATCHES "linux_arm64")
    set(ARCH_FLAGS "-march=armv8-a")
endif()

include(ExternalProject)
ExternalProject_Add(clickhouse_cpp_external
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../contribs/clickhouse-cpp
        CMAKE_ARGS
        -DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}
        -DCMAKE_OSX_DEPLOYMENT_TARGET=${CMAKE_OSX_DEPLOYMENT_TARGET}
        -DCMAKE_CXX_FLAGS=-fPIC ${ARCH_FLAGS}
        -DCMAKE_C_FLAGS=${ARCH_FLAGS}
        INSTALL_COMMAND ""  # Skip install step
        BUILD_BYPRODUCTS
            ${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/clickhouse/libclickhouse-cpp-lib.a
            ${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/contrib/absl/absl/libabsl_int128.a
            ${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/contrib/cityhash/cityhash/libcityhash.a
            ${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/contrib/lz4/lz4/liblz4.a
            ${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/contrib/zstd/zstd/libzstdstatic.a
)
add_library(clickhouse_cpp_lib STATIC IMPORTED)
set_target_properties(clickhouse_cpp_lib PROPERTIES
        IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/clickhouse/libclickhouse-cpp-lib.a
        INTERFACE_LINK_LIBRARIES "${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/contrib/absl/absl/libabsl_int128.a;${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/contrib/cityhash/cityhash/libcityhash.a;${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/contrib/lz4/lz4/liblz4.a;${CMAKE_CURRENT_BINARY_DIR}/clickhouse_cpp_external-prefix/src/clickhouse_cpp_external-build/contrib/zstd/zstd/libzstdstatic.a"
)
add_dependencies(clickhouse_cpp_lib clickhouse_cpp_external)

# Configure clickhouse-cpp options
set(CH_CPP_BUILD_SHARED OFF CACHE BOOL "Build shared library")
set(CH_CPP_BUILD_STATICALY_LINKED_LIB ON CACHE BOOL "Build static library")
set(CH_CPP_BUILD_ONLY_LIB ON CACHE BOOL "Build only library")
set(CH_CPP_WITH_OPENSSL ON CACHE BOOL "Use OpenSSL")

# Find OpenSSL package
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(
    ./src/include
    ./src
    ${CMAKE_CURRENT_SOURCE_DIR}/../duckdb/extension/parquet/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../contribs/clickhouse-cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/../contribs/clickhouse-cpp/contrib/absl
)

# Update extension sources to include new file
set(EXTENSION_SOURCES 
    src/chsql_extension.cpp 
    src/duck_flock.cpp
    src/clickhouse_scan.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link libraries using plain signature
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto clickhouse_cpp_lib)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto clickhouse_cpp_lib)

# Install targets
install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
