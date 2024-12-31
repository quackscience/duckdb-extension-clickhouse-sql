cmake_minimum_required(VERSION 3.12)

set(CMAKE_CXX_STANDARD 17)

# Set extension name here
set(TARGET_NAME chsql)

project(${TARGET_NAME})

# Configure clickhouse-cpp options
set(CH_CPP_BUILD_SHARED OFF CACHE BOOL "Build shared library")
set(CH_CPP_BUILD_STATICALY_LINKED_LIB ON CACHE BOOL "Build static library")
set(CH_CPP_BUILD_ONLY_LIB ON CACHE BOOL "Build only library")
set(CH_CPP_WITH_OPENSSL ON CACHE BOOL "Use OpenSSL")

# Add clickhouse-cpp
add_subdirectory(
    ${CMAKE_CURRENT_SOURCE_DIR}/../contribs/clickhouse-cpp
    ${CMAKE_CURRENT_BINARY_DIR}/clickhouse-cpp
    EXCLUDE_FROM_ALL
)

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
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto clickhouse-cpp-lib)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto clickhouse-cpp-lib)

# Install targets
install(
    TARGETS ${EXTENSION_NAME} clickhouse-cpp-lib absl_int128 cityhash lz4 zstdstatic
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)

# Make sure the export set includes clickhouse-cpp-lib and its dependencies
export(
    TARGETS ${EXTENSION_NAME} clickhouse-cpp-lib absl_int128 cityhash lz4 zstdstatic
    NAMESPACE duckdb::
    FILE DuckDBExports.cmake
)
