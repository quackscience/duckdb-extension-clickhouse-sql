#define DUCKDB_EXTENSION_MAIN

#include "chsql_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/catalog/default/default_table_functions.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>
#include "parquet_ordered_scan.cpp"
namespace duckdb {

// To add a new scalar SQL macro, add a new macro to this array!
// Copy and paste the top item in the array into the 
// second-to-last position and make some modifications. 
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, nullptr})

// Keep the DEFAULT_SCHEMA (no change needed)
// Replace "times_two" with a name for your macro
// If your function has parameters, add their names in quotes inside of the {}, with a nullptr at the end
//      If you do not have parameters, simplify to {nullptr}
// Add the text of your SQL macro as a raw string with the format R"( select 42 )"

static DefaultMacro chsql_macros[] = {
    // -- Type conversion macros
    {DEFAULT_SCHEMA, "toString", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS VARCHAR))"},
    {DEFAULT_SCHEMA, "toInt8", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS INT8))"},
    {DEFAULT_SCHEMA, "toInt16", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS INT16))"},
    {DEFAULT_SCHEMA, "toInt32", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS INT32))"},
    {DEFAULT_SCHEMA, "toInt64", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS INT64))"},
    {DEFAULT_SCHEMA, "toInt128", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS INT128))"},
    {DEFAULT_SCHEMA, "toInt256", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS HUGEINT))"},
    {DEFAULT_SCHEMA, "toInt8OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS INT8) IS NOT NULL THEN CAST(x AS INT8) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt16OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS INT16) IS NOT NULL THEN CAST(x AS INT16) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt32OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS INT32) IS NOT NULL THEN CAST(x AS INT32) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt64OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS INT64) IS NOT NULL THEN CAST(x AS INT64) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt128OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS INT128) IS NOT NULL THEN CAST(x AS INT128) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt256OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS HUGEINT) IS NOT NULL THEN CAST(x AS HUGEINT) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toInt8OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS INT8))"},
    {DEFAULT_SCHEMA, "toInt16OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS INT16))"},
    {DEFAULT_SCHEMA, "toInt32OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS INT32))"},
    {DEFAULT_SCHEMA, "toInt64OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS INT64))"},
    {DEFAULT_SCHEMA, "toInt128OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS INT128))"},
    {DEFAULT_SCHEMA, "toInt256OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS HUGEINT))"},
    // -- Unsigned integer conversion macros
    {DEFAULT_SCHEMA, "toUInt8", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS UTINYINT))"},
    {DEFAULT_SCHEMA, "toUInt16", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS USMALLINT))"},
    {DEFAULT_SCHEMA, "toUInt32", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS UINTEGER))"},
    {DEFAULT_SCHEMA, "toUInt64", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS UBIGINT))"},
    {DEFAULT_SCHEMA, "toUInt8OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS UTINYINT) IS NOT NULL THEN CAST(x AS UTINYINT) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toUInt16OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS USMALLINT) IS NOT NULL THEN CAST(x AS USMALLINT) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toUInt32OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS UINTEGER) IS NOT NULL THEN CAST(x AS UINTEGER) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toUInt64OrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS UBIGINT) IS NOT NULL THEN CAST(x AS UBIGINT) ELSE 0 END)"},
    {DEFAULT_SCHEMA, "toUInt8OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS UTINYINT))"}, // Fixed comma here
    {DEFAULT_SCHEMA, "toUInt16OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS USMALLINT))"}, // And here
    {DEFAULT_SCHEMA, "toUInt32OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS UINTEGER))"}, // Also here
    {DEFAULT_SCHEMA, "toUInt64OrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS UBIGINT))"}, // And here
    // -- Floating-point conversion macros
    {DEFAULT_SCHEMA, "toFloat", {"x", nullptr}, {{nullptr, nullptr}}, R"(CAST(x AS DOUBLE))"},
    {DEFAULT_SCHEMA, "toFloatOrNull", {"x", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST(x AS DOUBLE))"},
    {DEFAULT_SCHEMA, "toFloatOrZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(CASE WHEN TRY_CAST(x AS DOUBLE) IS NOT NULL THEN CAST(x AS DOUBLE) ELSE 0 END)"},
    // -- Arithmetic macros
    {DEFAULT_SCHEMA, "intDiv", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"((CAST(a AS BIGINT) // CAST(b AS BIGINT)))"},
    {DEFAULT_SCHEMA, "intDivOrNull", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(TRY_CAST((TRY_CAST(a AS BIGINT) // TRY_CAST(b AS BIGINT)) AS BIGINT))"},
    {DEFAULT_SCHEMA, "intDivOZero", {"x", nullptr}, {{nullptr, nullptr}}, R"(COALESCE((TRY_CAST((TRY_CAST(a AS BIGINT) // TRY_CAST(b AS BIGINT)) AS BIGINT)),0))"},
    {DEFAULT_SCHEMA, "plus", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(add(a, b))"},
    {DEFAULT_SCHEMA, "minus", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(subtract(a, b))"},
    {DEFAULT_SCHEMA, "modulo", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(CAST(a AS BIGINT) % CAST(b AS BIGINT))"},
    {DEFAULT_SCHEMA, "moduloOrZero", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(COALESCE(((TRY_CAST(a AS BIGINT) % TRY_CAST(b AS BIGINT))),0))"},
    // -- Tuple macros
    {DEFAULT_SCHEMA, "tupleIntDiv", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] // CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleIntDivByNumber", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) // CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleDivide", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] / CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleMultiply", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x,i) -> CAST(apply(b, x -> CAST(x AS BIGINT))[i] as BIGINT) * CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleMinus", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] - CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tuplePlus", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x,i) -> apply(b, x -> CAST(x AS BIGINT))[i] + CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleMultiplyByNumber", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) * CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleDivideByNumber", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) / CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleModulo", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) % CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleModuloByNumber", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(apply(a, (x) -> CAST(apply(b, x -> CAST(x AS BIGINT))[1] as BIGINT) % CAST(x AS BIGINT)))"},
    {DEFAULT_SCHEMA, "tupleConcat", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"(list_concat(a, b))"},
    // -- String matching macros
    {DEFAULT_SCHEMA, "match", {"string", "token"}, {{nullptr, nullptr}}, R"(string LIKE token)"},
    // -- Array macros
    {DEFAULT_SCHEMA, "arrayExists", {"needle", "haystack", nullptr}, {{nullptr, nullptr}}, R"(haystack @> ARRAY[needle])"},
    {DEFAULT_SCHEMA, "arrayMap", {"x", "arr", nullptr}, {{nullptr, nullptr}}, R"(array_transform(arr, x))"},
    // Date and Time Functions
    {DEFAULT_SCHEMA, "toYear", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(EXTRACT(YEAR FROM date_expression))"},
    {DEFAULT_SCHEMA, "toMonth", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(EXTRACT(MONTH FROM date_expression))"},
    {DEFAULT_SCHEMA, "toDayOfMonth", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(EXTRACT(DAY FROM date_expression))"},
    {DEFAULT_SCHEMA, "toHour", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(EXTRACT(HOUR FROM date_expression))"},
    {DEFAULT_SCHEMA, "toMinute", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(EXTRACT(MINUTE FROM date_expression))"},
    {DEFAULT_SCHEMA, "toSecond", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(EXTRACT(SECOND FROM date_expression))"},
    {DEFAULT_SCHEMA, "toYYYYMM", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(DATE_FORMAT(date_expression, '%Y%m'))"},
    {DEFAULT_SCHEMA, "toYYYYMMDD", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(DATE_FORMAT(date_expression, '%Y%m%d'))"},
    {DEFAULT_SCHEMA, "toYYYYMMDDhhmmss", {"date_expression", nullptr}, {{nullptr, nullptr}}, R"(DATE_FORMAT(date_expression, '%Y%m%d%H%M%S'))"},
    {DEFAULT_SCHEMA, "formatDateTime", {"time", "format", "timezone", nullptr}, {{nullptr, nullptr}}, R"(CASE  WHEN timezone IS NULL THEN strftime(time, format) ELSE strftime(time AT TIME ZONE timezone, format) END)"},
    // String Functions
    {DEFAULT_SCHEMA, "empty", {"str", nullptr}, {{nullptr, nullptr}}, R"(LENGTH(str) = 0)"},
    {DEFAULT_SCHEMA, "notEmpty", {"str", nullptr}, {{nullptr, nullptr}}, R"(LENGTH(str) > 0)"},
    {DEFAULT_SCHEMA, "lengthUTF8", {"str", nullptr}, {{nullptr, nullptr}}, R"(LENGTH(str))"},
    {DEFAULT_SCHEMA, "leftPad", {"str", "length", "pad_str", nullptr}, {{nullptr, nullptr}}, R"(LPAD(str, length, pad_str))"},
    {DEFAULT_SCHEMA, "rightPad", {"str", "length", "pad_str", nullptr}, {{nullptr, nullptr}}, R"(RPAD(str, length, pad_str))"},
    {DEFAULT_SCHEMA, "extractAllGroups", {"text", "pattern", nullptr}, {{nullptr, nullptr}}, R"(regexp_extract_all(text, pattern))"},
    {DEFAULT_SCHEMA, "toFixedString", {"str", "length", nullptr}, {{nullptr, nullptr}}, R"(RPAD(LEFT(str, length), length, '\0'))"},
    {DEFAULT_SCHEMA, "ifNull", {"x", "y", nullptr}, {{nullptr, nullptr}}, R"(COALESCE(x, y))"},
    {DEFAULT_SCHEMA, "arrayJoin", {"arr", nullptr}, {{nullptr, nullptr}}, R"(UNNEST(arr))"},
    {DEFAULT_SCHEMA, "splitByChar", {"separator", "str", nullptr}, {{nullptr, nullptr}}, R"(string_split(str, separator))"},
    // URL Functions
    {DEFAULT_SCHEMA, "protocol", {"url", nullptr}, {{nullptr, nullptr}}, R"(REGEXP_EXTRACT(url, '^(\w+)://', 1))"},
    {DEFAULT_SCHEMA, "domain", {"url", nullptr}, {{nullptr, nullptr}}, R"(REGEXP_EXTRACT(url, '://([^/]+)', 1))"},
    {DEFAULT_SCHEMA, "topLevelDomain", {"url", nullptr}, {{nullptr, nullptr}}, R"(REGEXP_EXTRACT(url, '\.([^./:]+)([:/]|$)', 1))"},
    {DEFAULT_SCHEMA, "path", {"url", nullptr}, {{nullptr, nullptr}}, R"(REGEXP_EXTRACT(url, '://[^/]+(/.*)', 1))"},
    // IP Address Functions
    {DEFAULT_SCHEMA, "IPv4NumToString", {"num", nullptr}, {{nullptr, nullptr}}, R"(CONCAT(CAST((num >> 24) & 255 AS VARCHAR), '.', CAST((num >> 16) & 255 AS VARCHAR), '.', CAST((num >> 8) & 255 AS VARCHAR), '.', CAST(num & 255 AS VARCHAR)))"},
    {DEFAULT_SCHEMA, "IPv4StringToNum", {"ip", nullptr}, {{nullptr, nullptr}}, R"(CAST(SPLIT_PART(ip, '.', 1) AS INTEGER) * 256 * 256 * 256 + CAST(SPLIT_PART(ip, '.', 2) AS INTEGER) * 256 * 256 + CAST(SPLIT_PART(ip, '.', 3) AS INTEGER) * 256 + CAST(SPLIT_PART(ip, '.', 4) AS INTEGER))"},
    // -- JSON Macros
    {DEFAULT_SCHEMA, "JSONExtract", {"json", "key", nullptr}, {{nullptr, nullptr}}, R"(json_extract(json, key))"},
    {DEFAULT_SCHEMA, "JSONExtractString", {"json", "key", nullptr}, {{nullptr, nullptr}}, R"(CAST(json_extract(json, key) AS VARCHAR))"},
    {DEFAULT_SCHEMA, "JSONExtractUInt", {"json", "key", nullptr}, {{nullptr, nullptr}}, R"(CAST(json_extract(json, key) AS UINTEGER))"},
    {DEFAULT_SCHEMA, "JSONExtractInt", {"json", "key", nullptr}, {{nullptr, nullptr}}, R"(CAST(json_extract(json, key) AS INT32))"},
    {DEFAULT_SCHEMA, "JSONExtractFloat", {"json", "key", nullptr}, {{nullptr, nullptr}}, R"(CAST(json_extract(json, key) AS DOUBLE))"},
    {DEFAULT_SCHEMA, "JSONExtractRaw", {"json", "key", nullptr}, {{nullptr, nullptr}}, R"(json_extract(json, key))"},
    {DEFAULT_SCHEMA, "JSONHas", {"json", "key", nullptr}, {{nullptr, nullptr}}, R"(json_extract(json, key) IS NOT NULL)"},
    {DEFAULT_SCHEMA, "JSONLength", {"json", nullptr}, {{nullptr, nullptr}}, R"(json_array_length(json))"},
    {DEFAULT_SCHEMA, "JSONType", {"json", "path", nullptr}, {{nullptr, nullptr}}, R"(CASE  WHEN path IS NULL THEN json_each(json) ELSE json_each(json,path) END)"},
    {DEFAULT_SCHEMA, "JSONExtractKeys", {"json", nullptr}, {{nullptr, nullptr}}, R"(json_object_keys(json))"},
    {DEFAULT_SCHEMA, "JSONExtractValues", {"json", nullptr}, {{nullptr, nullptr}}, R"(json_each_text(json))"},
    // -- Compare Macros
    {DEFAULT_SCHEMA, "equals", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"((a = b))"},
    {DEFAULT_SCHEMA, "notEquals", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"((a <> b))"},
    {DEFAULT_SCHEMA, "less", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"((a < b))"},
    {DEFAULT_SCHEMA, "greater", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"((a > b))"},
    {DEFAULT_SCHEMA, "lessOrEquals", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"((a <= b))"},
    {DEFAULT_SCHEMA, "greaterOrEquals", {"a", "b", nullptr}, {{nullptr, nullptr}}, R"((a >= b))"},
    // -- Misc macros
    {DEFAULT_SCHEMA, "generateUUIDv4", {nullptr}, {{nullptr, nullptr}}, R"(toString(uuid()))"},
    {DEFAULT_SCHEMA, "parseURL", {"url", "part", nullptr}, {{nullptr, nullptr}}, R"(CASE part WHEN 'protocol' THEN REGEXP_EXTRACT(url, '^(\w+)://') WHEN 'domain' THEN REGEXP_EXTRACT(url, '://([^/:]+)') WHEN 'port' THEN REGEXP_EXTRACT(url, ':(\d+)') WHEN 'path' THEN REGEXP_EXTRACT(url, '://[^/]+(/.+?)(\?|#|$)') WHEN 'query' THEN REGEXP_EXTRACT(url, '\?([^#]+)') WHEN 'fragment' THEN REGEXP_EXTRACT(url, '#(.+)$') END)"},
    {DEFAULT_SCHEMA, "bitCount", {"num", nullptr}, {{nullptr, nullptr}}, R"(BIT_COUNT(num))"},
    // Dictionary Emulation using MAP VARIABLES
    {DEFAULT_SCHEMA, "dictGet", {"dict","attr", nullptr}, {{nullptr, nullptr}}, R"(getvariable(dict)[attr][1])"},
    // -- End Macro
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}};

// To add a new table SQL macro, add a new macro to this array!
// Copy and paste the top item in the array into the
// second-to-last position and make some modifications.
// (essentially, leave the last entry in the array as {nullptr, nullptr, {nullptr}, nullptr})

// Keep the DEFAULT_SCHEMA (no change needed)
// Replace "times_two_table" with a name for your macro
// If your function has parameters without default values, add their names in quotes inside of the {}, with a nullptr at the end
//      If you do not have parameters, simplify to {nullptr}
// If your function has parameters with default values, add their names and values in quotes inside of {}'s inside of the {}.
// Be sure to keep {nullptr, nullptr} at the end
//      If you do not have parameters with default values, simplify to {nullptr, nullptr}
// Add the text of your SQL macro as a raw string with the format R"( select 42; )"

// clang-format off
static const DefaultTableMacro chsql_table_macros[] = {
        {DEFAULT_SCHEMA, "numbers", {"x", nullptr}, {{"z", "0"}, {nullptr, nullptr}},  R"(SELECT * as number FROM generate_series(z,x-1);)"},
        {DEFAULT_SCHEMA, "url", {"url", "format", nullptr}, {{nullptr, nullptr}}, R"(WITH "JSON" as (SELECT * FROM read_json_auto(url)), "PARQUET" as (SELECT * FROM read_parquet(url)), "CSV" as (SELECT * FROM read_csv_auto(url)), "BLOB" as (SELECT * FROM read_blob(url)), "TEXT" as (SELECT * FROM read_text(url)) FROM query_table(format))"},
	{DEFAULT_SCHEMA, "ch_scan", {"query", "server", nullptr}, {{"format", "JSONEachRow"}, {"user", "play"}, {nullptr, nullptr}},  R"(
	WITH
	  "json" AS (SELECT * FROM read_json_auto(concat(server, '/?default_format=jsoneachrow&user=', user, '&query=', query))),
	  "parquet" AS (SELECT * FROM read_parquet(concat(server, '/?default_format=parquet&user=', user, '&query=', query))),
	  "csv" AS (SELECT * FROM read_csv_auto(concat(server, '/?default_format=csv&user=', user, '&query=', query))),
	  "blob" AS (SELECT * FROM read_blob(concat(server, '/?default_format=blob&user=', user, '&query=', query))),
	  "text" AS (SELECT * FROM read_text(concat(server, '/?default_format=text&user=', user, '&query=', query)))
	FROM query_table(CASE
	  WHEN LOWER(format) LIKE 'json%' THEN 'json'
	  WHEN LOWER(format) LIKE 'csv%' THEN 'csv'
	  WHEN LOWER(format) = 'parquet' THEN 'parquet'
	  WHEN LOWER(format) = 'blob' THEN 'blob'
	  WHEN LOWER(format) = 'text' THEN 'text'
	  ELSE 'json'
	END)
	)"},
        {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
	};
// clang-format on

inline void ChSqlScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "ChSql "+name.GetString()+" 🐥");;
        });
}

inline void ChSqlOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "ChSql " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto chsql_scalar_function = ScalarFunction("chsql", {LogicalType::VARCHAR}, LogicalType::VARCHAR, ChSqlScalarFun);
    ExtensionUtil::RegisterFunction(instance, chsql_scalar_function);

    // Register another scalar function
    auto chsql_openssl_version_scalar_function = ScalarFunction("chsql_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, ChSqlOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, chsql_openssl_version_scalar_function);

    // Macros
	for (idx_t index = 0; chsql_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(chsql_macros[index]);
		ExtensionUtil::RegisterFunction(instance, *info);
	}
    // Table Macros
    for (idx_t index = 0; chsql_table_macros[index].name != nullptr; index++) {
		auto table_info = DefaultTableFunctionGenerator::CreateTableMacroInfo(chsql_table_macros[index]);
        ExtensionUtil::RegisterFunction(instance, *table_info);
	}
	ExtensionUtil::RegisterFunction(instance, ReadParquetOrderedFunction());
}

void ChsqlExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string ChsqlExtension::Name() {
	return "chsql";
}

std::string ChsqlExtension::Version() const {
#ifdef EXT_VERSION_CHSQL
	return EXT_VERSION_CHSQL;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void chsql_init(duckdb::DatabaseInstance &db) {
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::ChsqlExtension>();
}

DUCKDB_EXTENSION_API const char *chsql_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
