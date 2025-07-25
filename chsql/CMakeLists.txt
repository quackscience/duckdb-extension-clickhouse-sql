cmake_minimum_required(VERSION 3.5)
# Set extension name here
set(TARGET_NAME chsql)
# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(CHSQL_DUCKDB_VERSION ${DUCKDB_MAJOR_VERSION})
project(${TARGET_NAME})

include_directories(
        ./src/include
        ./src
        ${CMAKE_CURRENT_SOURCE_DIR}/../duckdb/extension/parquet/include
        ../duckdb/third_party/lz4
        ../duckdb/third_party/parquet
        ../duckdb/third_party/thrift
        ../duckdb/third_party/snappy
        ../duckdb/third_party/zstd/include
        ../duckdb/third_party/mbedtls/include
        ../duckdb/third_party/brotli/include)
set(EXTENSION_SOURCES src/chsql_extension.cpp src/duck_flock.cpp src/chsql_system.cpp src/parquet_types.cpp)
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto duckdb_mbedtls)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto duckdb_mbedtls)
install(
        TARGETS ${EXTENSION_NAME}
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
