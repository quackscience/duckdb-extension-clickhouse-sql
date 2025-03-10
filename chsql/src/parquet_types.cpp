#include "chsql_parquet_types.h"

bool ParquetType::check_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx) {
  auto &el = schema[idx];
  if (el.type != parquet_type) {
    return false;
  }
  if (converted_type == -1) {
    return !el.__isset.converted_type;
  }
  return int(el.converted_type) == converted_type;
};

ParquetTypesManager::ParquetTypesManager() {
    /*UTF8 = 0,
     MAP = 1, -> no support
    MAP_KEY_VALUE = 2, -> no support
    LIST = 3, -> no support
    ENUM = 4, -> no support
    DECIMAL = 5, -> no support
    DATE = 6,
    TIME_MILLIS = 7
    TIME_MICROS = 8
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
    UINT_8 = 11,
    UINT_16 = 12,
    UINT_32 = 13,
    UINT_64 = 14,
    INT_8 = 15,
    INT_16 = 16,
    INT_32 = 17,
    INT_64 = 18,
    JSON = 19, -> no support
    BSON = 20, -> no support
    INTERVAL = 21 -> no support
  */
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::UTF8, duckdb_parquet::Type::BYTE_ARRAY, duckdb::LogicalType::VARCHAR));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::DATE, duckdb_parquet::Type::INT32, duckdb::LogicalType::DATE));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::TIME_MILLIS, duckdb_parquet::Type::INT32, duckdb::LogicalType::TIME));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::TIME_MICROS, duckdb_parquet::Type::INT64, duckdb::LogicalType::TIME));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::TIMESTAMP_MILLIS, duckdb_parquet::Type::INT64, duckdb::LogicalType::TIMESTAMP));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::TIMESTAMP_MICROS, duckdb_parquet::Type::INT64, duckdb::LogicalType::TIMESTAMP));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::UINT_8, duckdb_parquet::Type::INT32, duckdb::LogicalType::UTINYINT));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::UINT_16, duckdb_parquet::Type::INT32, duckdb::LogicalType::USMALLINT));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::UINT_32, duckdb_parquet::Type::INT32, duckdb::LogicalType::UINTEGER));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::UINT_64, duckdb_parquet::Type::INT64, duckdb::LogicalType::UBIGINT));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::INT_8, duckdb_parquet::Type::INT32, duckdb::LogicalType::TINYINT));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::INT_16, duckdb_parquet::Type::INT32, duckdb::LogicalType::SMALLINT));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::INT_32, duckdb_parquet::Type::INT32, duckdb::LogicalType::INTEGER));
  types.push_back(ParquetType(duckdb_parquet::ConvertedType::INT_64, duckdb_parquet::Type::INT64, duckdb::LogicalType::BIGINT));
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

duckdb::LogicalType ParquetTypesManager::get_logical_type(const duckdb::vector<duckdb_parquet::SchemaElement> &schema, idx_t idx) {
  auto inst = get_instance();
  return inst->derive_logical_type(schema[idx], false);
  /*for (auto it = inst->types.begin(); it != inst->types.end(); ++it) {
    if (it->check_type(schema, idx)) {
      return it->logical_type;
    }
  }
  throw std::runtime_error("Unsupported Parquet type");*/
}

duckdb::LogicalType ParquetTypesManager::derive_logical_type(const duckdb_parquet::SchemaElement &s_ele, bool binary_as_string) {
	// inner node
	if (s_ele.type == duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY && !s_ele.__isset.type_length) {
		throw duckdb::IOException("FIXED_LEN_BYTE_ARRAY requires length to be set");
	}
	if (s_ele.__isset.logicalType) {
		if (s_ele.logicalType.__isset.UUID) {
			if (s_ele.type == duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY) {
				return duckdb::LogicalType::UUID;
			}
		} else if (s_ele.logicalType.__isset.TIMESTAMP) {
			if (s_ele.logicalType.TIMESTAMP.isAdjustedToUTC) {
				return duckdb::LogicalType::TIMESTAMP_TZ;
			} else if (s_ele.logicalType.TIMESTAMP.unit.__isset.NANOS) {
				return duckdb::LogicalType::TIMESTAMP_NS;
			}
			return duckdb::LogicalType::TIMESTAMP;
		} else if (s_ele.logicalType.__isset.TIME) {
			if (s_ele.logicalType.TIME.isAdjustedToUTC) {
				return duckdb::LogicalType::TIME_TZ;
			}
			return duckdb::LogicalType::TIME;
		}
	}
	if (s_ele.__isset.converted_type) {
		// Legacy NULL type, does no longer exist, but files are still around of course
		if (static_cast<uint8_t>(s_ele.converted_type) == 24) {
			return duckdb::LogicalTypeId::SQLNULL;
		}
		switch (s_ele.converted_type) {
		case duckdb_parquet::ConvertedType::INT_8:
			if (s_ele.type == duckdb_parquet::Type::INT32) {
				return duckdb::LogicalType::TINYINT;
			} else {
				throw duckdb::IOException("INT8 converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::INT_16:
			if (s_ele.type == duckdb_parquet::Type::INT32) {
				return duckdb::LogicalType::SMALLINT;
			} else {
				throw duckdb::IOException("INT16 converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::INT_32:
			if (s_ele.type == duckdb_parquet::Type::INT32) {
				return duckdb::LogicalType::INTEGER;
			} else {
				throw duckdb::IOException("INT32 converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::INT_64:
			if (s_ele.type == duckdb_parquet::Type::INT64) {
				return duckdb::LogicalType::BIGINT;
			} else {
				throw duckdb::IOException("INT64 converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::UINT_8:
			if (s_ele.type == duckdb_parquet::Type::INT32) {
				return duckdb::LogicalType::UTINYINT;
			} else {
				throw duckdb::IOException("UINT8 converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::UINT_16:
			if (s_ele.type == duckdb_parquet::Type::INT32) {
				return duckdb::LogicalType::USMALLINT;
			} else {
				throw duckdb::IOException("UINT16 converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::UINT_32:
			if (s_ele.type == duckdb_parquet::Type::INT32) {
				return duckdb::LogicalType::UINTEGER;
			} else {
				throw duckdb::IOException("UINT32 converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::UINT_64:
			if (s_ele.type == duckdb_parquet::Type::INT64) {
				return duckdb::LogicalType::UBIGINT;
			} else {
				throw duckdb::IOException("UINT64 converted type can only be set for value of Type::INT64");
			}
		case duckdb_parquet::ConvertedType::DATE:
			if (s_ele.type == duckdb_parquet::Type::INT32) {
				return duckdb::LogicalType::DATE;
			} else {
				throw duckdb::IOException("DATE converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::TIMESTAMP_MICROS:
		case duckdb_parquet::ConvertedType::TIMESTAMP_MILLIS:
			if (s_ele.type == duckdb_parquet::Type::INT64) {
				return duckdb::LogicalType::TIMESTAMP;
			} else {
				throw duckdb::IOException("TIMESTAMP converted type can only be set for value of Type::INT64");
			}
		case duckdb_parquet::ConvertedType::DECIMAL:
			if (!s_ele.__isset.precision || !s_ele.__isset.scale) {
				throw duckdb::IOException("DECIMAL requires a length and scale specifier!");
			}
			if (s_ele.precision > duckdb::DecimalType::MaxWidth()) {
				return duckdb::LogicalType::DOUBLE;
			}
			switch (s_ele.type) {
			case duckdb_parquet::Type::BYTE_ARRAY:
			case duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY:
			case duckdb_parquet::Type::INT32:
			case duckdb_parquet::Type::INT64:
				return duckdb::LogicalType::DECIMAL(s_ele.precision, s_ele.scale);
			default:
				throw duckdb::IOException(
				    "DECIMAL converted type can only be set for value of Type::(FIXED_LEN_)BYTE_ARRAY/INT32/INT64");
			}
		case duckdb_parquet::ConvertedType::UTF8:
		case duckdb_parquet::ConvertedType::ENUM:
			switch (s_ele.type) {
			case duckdb_parquet::Type::BYTE_ARRAY:
			case duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY:
				return duckdb::LogicalType::VARCHAR;
			default:
				throw duckdb::IOException("UTF8 converted type can only be set for Type::(FIXED_LEN_)BYTE_ARRAY");
			}
		case duckdb_parquet::ConvertedType::TIME_MILLIS:
			if (s_ele.type == duckdb_parquet::Type::INT32) {
				return duckdb::LogicalType::TIME;
			} else {
				throw duckdb::IOException("TIME_MILLIS converted type can only be set for value of Type::INT32");
			}
		case duckdb_parquet::ConvertedType::TIME_MICROS:
			if (s_ele.type == duckdb_parquet::Type::INT64) {
				return duckdb::LogicalType::TIME;
			} else {
				throw duckdb::IOException("TIME_MICROS converted type can only be set for value of Type::INT64");
			}
		case duckdb_parquet::ConvertedType::INTERVAL:
			return duckdb::LogicalType::INTERVAL;
		case duckdb_parquet::ConvertedType::JSON:
			return duckdb::LogicalType::JSON();
		case duckdb_parquet::ConvertedType::MAP:
		case duckdb_parquet::ConvertedType::MAP_KEY_VALUE:
		case duckdb_parquet::ConvertedType::LIST:
		case duckdb_parquet::ConvertedType::BSON:
		default:
			throw duckdb::IOException("Unsupported converted type (%d)", (int32_t)s_ele.converted_type);
		}
	} else {
		// no converted type set
		// use default type for each physical type
		switch (s_ele.type) {
		case duckdb_parquet::Type::BOOLEAN:
			return duckdb::LogicalType::BOOLEAN;
		case duckdb_parquet::Type::INT32:
			return duckdb::LogicalType::INTEGER;
		case duckdb_parquet::Type::INT64:
			return duckdb::LogicalType::BIGINT;
		case duckdb_parquet::Type::INT96: // always a timestamp it would seem
			return duckdb::LogicalType::TIMESTAMP;
		case duckdb_parquet::Type::FLOAT:
			return duckdb::LogicalType::FLOAT;
		case duckdb_parquet::Type::DOUBLE:
			return duckdb::LogicalType::DOUBLE;
		case duckdb_parquet::Type::BYTE_ARRAY:
		case duckdb_parquet::Type::FIXED_LEN_BYTE_ARRAY:
			if (binary_as_string) {
				return duckdb::LogicalType::VARCHAR;
			}
			return duckdb::LogicalType::BLOB;
		default:
			return duckdb::LogicalType::INVALID;
		}
	}
}
