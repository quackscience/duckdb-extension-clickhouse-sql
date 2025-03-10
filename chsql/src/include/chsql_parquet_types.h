//
// Created by hromozeka on 10.03.25.
//

#ifndef PARQUET_TYPES_H
#define PARQUET_TYPES_H

#include "duckdb.hpp"
#include <parquet_types.h>

struct ParquetType {
  /*duckdb_parquet::ConvertedType::type -> replaced to int to support -1 nodata  value*/
  int converted_type;
  duckdb_parquet::Type::type parquet_type;
  const duckdb::LogicalType &logical_type;
  ParquetType(int converted_type, duckdb_parquet::Type::type parquet_type,
              const duckdb::LogicalType &logical_type)
      : converted_type(converted_type), parquet_type(parquet_type), logical_type(logical_type) {}
  bool check_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx);
};

class ParquetTypesManager {
  protected:
    std::vector<ParquetType> types;
    static ParquetTypesManager *instance;
    static std::mutex instance_mutex;
    ParquetTypesManager();
    static ParquetTypesManager* get_instance();
    duckdb::LogicalType derive_logical_type(const duckdb_parquet::SchemaElement &s_ele, bool binary_as_string);
  public:
    static duckdb::LogicalType get_logical_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx);
};

#endif //PARQUET_TYPES_H
