# This file is included by DuckDB's build system. It specifies which extension to load
set(CHSQL_DUCKDB_VERSION ${DUCKDB_MAJOR_VERSION})
include_directories(
        ./src/include
        ${CMAKE_CURRENT_SOURCE_DIR}/../duckdb/extension/parquet/include
        ../duckdb/third_party/lz4
        ../duckdb/third_party/parquet
        ../duckdb/third_party/thrift
        ../duckdb/third_party/snappy
        ../duckdb/third_party/zstd/include
#        ../duckdb/third_party/mbedtls
        ../duckdb/third_party/mbedtls/include
        ../duckdb/third_party/brotli/include)

# Extension from this repo
duckdb_extension_load(chsql
    SOURCE_DIR  ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
