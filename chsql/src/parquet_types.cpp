#include "chsql_parquet_types.h"

bool ParquetType::check_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx) {
	auto &el = schema[idx];
	if (parquet_type >= 0 && int(el.type) != parquet_type) {
		return false;
	}
	if (converted_type == -1) {
		return !el.__isset.converted_type;
	}
	return int(el.converted_type) == converted_type;
};

duckdb::LogicalType ParquetType::get_logical_type(const duckdb_parquet::SchemaElement &schema) {
	return logical_type;
}

bool LogicalParquetType::check_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx) {
	auto &el = schema[idx];
	return el.__isset.logicalType && this->get_isset(el);
}

duckdb::LogicalType JSONParquetType::get_logical_type(const duckdb_parquet::SchemaElement &schema) {
	return duckdb::LogicalType::JSON();
}

bool DecimalParquetType::check_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx) {
	auto &el = schema[idx];
	return el.__isset.converted_type && el.converted_type == duckdb_parquet::ConvertedType::DECIMAL &&
	       el.__isset.precision && el.__isset.scale && (el.precision > duckdb::DecimalType::MaxWidth() ||
	                                                    el.type == duckdb_parquet::Type::BYTE_ARRAY ||
	                                                    el.type == duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY ||
	                                                    el.type == duckdb_parquet::Type::INT32 ||
	                                                    el.type == duckdb_parquet::Type::INT64);
}

duckdb::LogicalType DecimalParquetType::get_logical_type(const duckdb_parquet::SchemaElement &el) {
	if (el.precision > duckdb::DecimalType::MaxWidth()) {
		return duckdb::LogicalType::DOUBLE;
	}
	return duckdb::LogicalType::DECIMAL(el.precision, el.scale);
}

bool isUUID(const duckdb_parquet::SchemaElement &el) {
	return el.logicalType.__isset.UUID;
}

bool isTimestampTZ(const duckdb_parquet::SchemaElement &el) {
	return el.logicalType.__isset.TIMESTAMP && el.logicalType.TIMESTAMP.isAdjustedToUTC;
}

bool isTimestampNS(const duckdb_parquet::SchemaElement &el) {
	return el.logicalType.__isset.TIMESTAMP && el.logicalType.TIMESTAMP.unit.__isset.NANOS;
}

bool isTimestamp(const duckdb_parquet::SchemaElement &el) {
	return el.logicalType.__isset.TIMESTAMP;
}

bool isTimeTZ(const duckdb_parquet::SchemaElement &el) {
	return el.logicalType.__isset.TIME && el.logicalType.TIME.isAdjustedToUTC;
}

bool isTime(const duckdb_parquet::SchemaElement &el) {
	return el.logicalType.__isset.TIME;
}

ParquetType *_types[] = {
	new LogicalParquetType(isUUID, duckdb::LogicalType::UUID),
	new LogicalParquetType(isTimestampTZ, duckdb::LogicalType::TIMESTAMP_TZ),
	new LogicalParquetType(isTimestampNS, duckdb::LogicalType::TIMESTAMP_NS),
	new LogicalParquetType(isTimestamp, duckdb::LogicalType::TIMESTAMP),
	new LogicalParquetType(isTimeTZ, duckdb::LogicalType::TIME),
	new LogicalParquetType(isTime, duckdb::LogicalType::TIME),

	new ParquetType(24, -1, duckdb::LogicalType::SQLNULL),
	new ParquetType(duckdb_parquet::ConvertedType::INT_8, duckdb_parquet::Type::INT32, duckdb::LogicalType::TINYINT),
	new ParquetType(duckdb_parquet::ConvertedType::INT_16, duckdb_parquet::Type::INT32, duckdb::LogicalType::SMALLINT),
	new ParquetType(duckdb_parquet::ConvertedType::INT_32, duckdb_parquet::Type::INT32, duckdb::LogicalType::INTEGER),
	new ParquetType(duckdb_parquet::ConvertedType::INT_64, duckdb_parquet::Type::INT64, duckdb::LogicalType::BIGINT),
	new ParquetType(duckdb_parquet::ConvertedType::UINT_8, duckdb_parquet::Type::INT32, duckdb::LogicalType::UTINYINT),
	new ParquetType(duckdb_parquet::ConvertedType::UINT_16, duckdb_parquet::Type::INT32,
	                duckdb::LogicalType::USMALLINT),
	new ParquetType(duckdb_parquet::ConvertedType::UINT_32, duckdb_parquet::Type::INT32, duckdb::LogicalType::UINTEGER),
	new ParquetType(duckdb_parquet::ConvertedType::UINT_64, duckdb_parquet::Type::INT64, duckdb::LogicalType::UBIGINT),
	new ParquetType(duckdb_parquet::ConvertedType::DATE, duckdb_parquet::Type::INT32, duckdb::LogicalType::DATE),
	new ParquetType(duckdb_parquet::ConvertedType::TIME_MICROS, duckdb_parquet::Type::INT64, duckdb::LogicalType::TIME),
	new ParquetType(duckdb_parquet::ConvertedType::TIME_MILLIS, duckdb_parquet::Type::INT64, duckdb::LogicalType::TIME),
	new ParquetType(duckdb_parquet::ConvertedType::TIMESTAMP_MILLIS, duckdb_parquet::Type::INT32,
	                duckdb::LogicalType::TIME),
	new ParquetType(duckdb_parquet::ConvertedType::TIMESTAMP_MICROS, duckdb_parquet::Type::INT64,
	                duckdb::LogicalType::TIME),
	new ParquetType(duckdb_parquet::ConvertedType::INTERVAL, -1, duckdb::LogicalType::INTERVAL),
	new ParquetType(duckdb_parquet::ConvertedType::UTF8, duckdb_parquet::Type::BYTE_ARRAY,
	                duckdb::LogicalType::VARCHAR),
	new ParquetType(duckdb_parquet::ConvertedType::ENUM, duckdb_parquet::Type::BYTE_ARRAY,
	                duckdb::LogicalType::VARCHAR),
	new ParquetType(duckdb_parquet::ConvertedType::UTF8, duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY,
	                duckdb::LogicalType::VARCHAR),
	new ParquetType(duckdb_parquet::ConvertedType::ENUM, duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY,
	                duckdb::LogicalType::VARCHAR),

	new JSONParquetType(),
	new DecimalParquetType(),

	new ParquetType(-1, duckdb_parquet::Type::BOOLEAN, duckdb::LogicalType::BOOLEAN),
	new ParquetType(-1, duckdb_parquet::Type::BOOLEAN, duckdb::LogicalType::BOOLEAN),
	new ParquetType(-1, duckdb_parquet::Type::INT32, duckdb::LogicalType::INTEGER),
	new ParquetType(-1, duckdb_parquet::Type::INT64, duckdb::LogicalType::BIGINT),
	new ParquetType(-1, duckdb_parquet::Type::INT96, duckdb::LogicalType::TIMESTAMP),
	new ParquetType(-1, duckdb_parquet::Type::FLOAT, duckdb::LogicalType::FLOAT),
	new ParquetType(-1, duckdb_parquet::Type::DOUBLE, duckdb::LogicalType::DOUBLE),
	new ParquetType(-1, duckdb_parquet::Type::BYTE_ARRAY, duckdb::LogicalType::BLOB),
	new ParquetType(-1, duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY, duckdb::LogicalType::BLOB),
};

ParquetTypesManager::ParquetTypesManager() {
};

ParquetTypesManager *ParquetTypesManager::instance = nullptr;
std::mutex ParquetTypesManager::instance_mutex;

ParquetTypesManager *ParquetTypesManager::get_instance() {
	std::lock_guard<std::mutex> lock(instance_mutex);
	if (instance == nullptr) {
		instance = new ParquetTypesManager();
	}
	return instance;
}

duckdb::LogicalType ParquetTypesManager::get_logical_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema,
                                                          idx_t idx) {
	for (auto &type: _types) {
		if (type->check_type(schema, idx)) {
			return type->get_logical_type(schema[idx]);
		}
	}
	throw std::runtime_error("Unsupported Parquet type");
}
