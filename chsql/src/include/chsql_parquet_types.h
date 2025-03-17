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
  /* duckdb_parquet::Type::type -> replaced to int to support -1 for no matter value */
  int parquet_type;
  const duckdb::LogicalType logical_type;
  ParquetType(int converted_type, int parquet_type, const duckdb::LogicalType &logical_type)
      : converted_type(converted_type), parquet_type(parquet_type), logical_type(logical_type) {}
  virtual bool check_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx);
  virtual duckdb::LogicalType get_logical_type(const duckdb_parquet::SchemaElement &schema);
};

struct LogicalParquetType : public ParquetType {
  bool (*get_isset)(const duckdb_parquet::SchemaElement& el);

  LogicalParquetType(bool (*get_isset) (const duckdb_parquet::SchemaElement& el),
    const duckdb::LogicalType& logical_type)
  : ParquetType(-1, duckdb_parquet::Type::type::INT32, logical_type), get_isset(get_isset) {}
  bool check_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx) override;
};

struct JSONParquetType : public ParquetType {
  JSONParquetType(): ParquetType(duckdb_parquet::ConvertedType::JSON, -1, duckdb::LogicalType::SQLNULL) {}
  duckdb::LogicalType get_logical_type(const duckdb_parquet::SchemaElement &schema) override;
};

struct DecimalParquetType : public ParquetType {
  DecimalParquetType(): ParquetType(-1, duckdb_parquet::Type::type::INT32, duckdb::LogicalType::SQLNULL) {}
  bool check_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx) override;
  duckdb::LogicalType get_logical_type(const duckdb_parquet::SchemaElement &schema) override;
};

class ParquetTypesManager {
  protected:
    static ParquetTypesManager *instance;
    static std::mutex instance_mutex;
    ParquetTypesManager();
    static ParquetTypesManager* get_instance();
    duckdb::LogicalType derive_logical_type(const duckdb_parquet::SchemaElement &s_ele, bool binary_as_string);
  public:
    static duckdb::LogicalType get_logical_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx);
};

#endif //PARQUET_TYPES_H
