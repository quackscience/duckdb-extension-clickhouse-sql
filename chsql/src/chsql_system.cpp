#include "chsql_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/helper.hpp"

#include <fstream>
#include <sstream>
#if defined(__linux__)
#include <unistd.h>
#include <sys/sysinfo.h>
#include <iterator>
#elif defined(_WIN32)
#include <windows.h>
#elif defined(__APPLE__)
#include <unistd.h>
#include <sys/sysctl.h>
#include <sys/time.h>
#include <time.h>
#elif defined(__EMSCRIPTEN__)
// #include <emscripten.h>
#endif

namespace duckdb {
// -- System code from chsql_system
struct SystemDatabasesData : public TableFunctionData {
    vector<Value> names;
    vector<Value> engines;
    vector<Value> data_paths;
    vector<Value> metadata_paths;
    vector<Value> uuids;
    vector<Value> engine_full;
    vector<Value> comments;
    idx_t offset = 0;
};

struct SystemTablesData : public TableFunctionData {
    vector<Value> databases;
    vector<Value> names;
    vector<Value> uuids;
    vector<Value> engines;
    vector<Value> is_temporary;
    vector<Value> data_paths;
    vector<Value> metadata_paths;
    vector<Value> metadata_modification_times;
    vector<Value> metadata_versions;
    vector<Value> create_table_queries;
    idx_t offset = 0;
};

struct SystemColumnsData : public TableFunctionData {
    vector<Value> databases;
    vector<Value> tables;
    vector<Value> column_names;
    vector<Value> column_types;
    vector<Value> positions;
    vector<Value> comments;
    idx_t offset = 0;
};

struct SystemFunctionsData : public TableFunctionData {
    vector<Value> names;
    vector<Value> is_aggregate;
    vector<Value> case_insensitive;
    vector<Value> descriptions;
    idx_t offset = 0;
};

static void SystemDatabasesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (SystemDatabasesData &)*data_p.bind_data;
    if (data.offset >= data.names.size()) {
        return;
    }

    idx_t count = 0;
    while (data.offset < data.names.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, data.names[data.offset]);           // name
        output.SetValue(1, count, data.engines[data.offset]);         // engine
        output.SetValue(2, count, data.data_paths[data.offset]);      // data_path
        output.SetValue(3, count, data.metadata_paths[data.offset]);  // metadata_path
        output.SetValue(4, count, data.uuids[data.offset]);           // uuid
        output.SetValue(5, count, data.engine_full[data.offset]);     // engine_full
        output.SetValue(6, count, data.comments[data.offset]);        // comment
        
        count++;
        data.offset++;
    }

    output.SetCardinality(count);
}

static void SystemTablesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (SystemTablesData &)*data_p.bind_data;
    if (data.offset >= data.databases.size()) {
        return;
    }

    idx_t count = 0;
    while (data.offset < data.databases.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, data.databases[data.offset]);       // database
        output.SetValue(1, count, data.names[data.offset]);           // name
        output.SetValue(2, count, data.uuids[data.offset]);           // uuid
        output.SetValue(3, count, data.engines[data.offset]);         // engine
        output.SetValue(4, count, data.is_temporary[data.offset]);    // is_temporary
        output.SetValue(5, count, data.data_paths[data.offset]);      // data_path
        output.SetValue(6, count, data.metadata_paths[data.offset]);  // metadata_path
        output.SetValue(7, count, data.metadata_modification_times[data.offset]); // metadata_modification_time
        output.SetValue(8, count, data.metadata_versions[data.offset]); // metadata_version
        output.SetValue(9, count, data.create_table_queries[data.offset]); // create_table_query
        
        count++;
        data.offset++;
    }

    output.SetCardinality(count);
}

static void SystemFunctionsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (SystemFunctionsData &)*data_p.bind_data;
    if (data.offset >= data.names.size()) {
        return;
    }

    idx_t count = 0;
    while (data.offset < data.names.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, data.names[data.offset]);           // name
        output.SetValue(1, count, data.is_aggregate[data.offset]);    // is_aggregate
        output.SetValue(2, count, data.case_insensitive[data.offset]); // case_insensitive
        output.SetValue(3, count, data.descriptions[data.offset]);    // description
        
        count++;
        data.offset++;
    }

    output.SetCardinality(count);
}

static void SystemColumnsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (SystemColumnsData &)*data_p.bind_data;
    if (data.offset >= data.databases.size()) {
        return;
    }

    idx_t count = 0;
    while (data.offset < data.databases.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, data.databases[data.offset]);    // database
        output.SetValue(1, count, data.tables[data.offset]);       // table
        output.SetValue(2, count, data.column_names[data.offset]); // column name
        output.SetValue(3, count, data.column_types[data.offset]); // column type
        output.SetValue(4, count, data.positions[data.offset]);    // position
        output.SetValue(5, count, data.comments[data.offset]);     // comment
        
        count++;
        data.offset++;
    }

    output.SetCardinality(count);
}

static unique_ptr<FunctionData> SystemDatabasesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    // Define columns
    names.emplace_back("name");
    names.emplace_back("engine");
    names.emplace_back("data_path");
    names.emplace_back("metadata_path");
    names.emplace_back("uuid");
    names.emplace_back("engine_full");
    names.emplace_back("comment");

    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::UUID);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);

    auto result = make_uniq<SystemDatabasesData>();

    // Get databases using duckdb_databases()
    Connection con(Catalog::GetSystemCatalog(context).GetDatabase());
    auto database_result = con.Query("SELECT database_name, 'duckdb' as engine, path as data_path, '' as metadata_path, "
                                     "database_name as uuid, 'DuckDB' as engine_full, comment "
                                     "FROM duckdb_databases()");

    if (!database_result->HasError()) {
        for (auto &row : *database_result) {
            result->names.push_back(row.GetValue<std::string>(0));          // name
            result->engines.push_back(row.GetValue<std::string>(1));        // engine
            result->data_paths.push_back(row.GetValue<std::string>(2));     // data_path
            result->metadata_paths.push_back(row.GetValue<std::string>(3)); // metadata_path
            result->uuids.push_back(Value::UUID(row.GetValue<std::string>(4))); // uuid
            result->engine_full.push_back(row.GetValue<std::string>(5));    // engine_full
            result->comments.push_back(row.GetValue<std::string>(6));       // comment
        }
    }

    return std::move(result);
}

static unique_ptr<FunctionData> SystemTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    // Define columns
    names.emplace_back("database");
    names.emplace_back("name");
    names.emplace_back("uuid");
    names.emplace_back("engine");
    names.emplace_back("is_temporary");
    names.emplace_back("data_path");
    names.emplace_back("metadata_path");
    names.emplace_back("metadata_modification_time");
    names.emplace_back("metadata_version");
    names.emplace_back("create_table_query");

    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::UUID);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::BOOLEAN);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::TIMESTAMP);
    return_types.emplace_back(LogicalType::INTEGER);
    return_types.emplace_back(LogicalType::VARCHAR);

    auto result = make_uniq<SystemTablesData>();

    // Get tables using duckdb_tables()
    Connection con(Catalog::GetSystemCatalog(context).GetDatabase());
    auto table_result = con.Query("SELECT database_name, table_name, table_name as uuid, "
                                  "'BASE TABLE' as engine, false as is_temporary, '' as data_path, "
                                  "'' as metadata_path, '1970-01-01 00:00:00' as metadata_modification_time, "
                                  "0 as metadata_version, sql as create_table_query "
                                  "FROM duckdb_tables()");

    if (!table_result->HasError()) {
        for (auto &row : *table_result) {
            result->databases.push_back(row.GetValue<std::string>(0));      // database
            result->names.push_back(row.GetValue<std::string>(1));          // name
            result->uuids.push_back(Value::UUID(row.GetValue<std::string>(2))); // uuid (placeholder using table name hash)
            result->engines.push_back(row.GetValue<std::string>(3));        // engine
            result->is_temporary.push_back(row.GetValue<bool>(4));          // is_temporary
            result->data_paths.push_back(row.GetValue<std::string>(5));     // data_path
            result->metadata_paths.push_back(row.GetValue<std::string>(6)); // metadata_path
            result->metadata_modification_times.push_back(Value::TIMESTAMP(row.GetValue<timestamp_t>(7))); // metadata_modification_time
            result->metadata_versions.push_back(row.GetValue<int32_t>(8));  // metadata_version
            result->create_table_queries.push_back(row.GetValue<std::string>(9)); // create_table_query
        }
    }

    return std::move(result);
}

static unique_ptr<FunctionData> SystemFunctionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    // Define columns
    names.emplace_back("name");
    names.emplace_back("is_aggregate");
    names.emplace_back("case_insensitive");
    names.emplace_back("description");

    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::BOOLEAN);
    return_types.emplace_back(LogicalType::BOOLEAN);
    return_types.emplace_back(LogicalType::VARCHAR);

    auto result = make_uniq<SystemFunctionsData>();
    
    // Get functions using duckdb_functions()
    Connection con(Catalog::GetSystemCatalog(context).GetDatabase());
    auto function_result = con.Query("SELECT DISTINCT ON(function_name) "
                                     "function_name, function_type, description "
                                     "FROM duckdb_functions()");

    if (!function_result->HasError()) {
        for (auto &row : *function_result) {
            result->names.push_back(row.GetValue<std::string>(0));          // name
            result->is_aggregate.push_back(Value::BOOLEAN(false));          // is_aggregate (all false for scalar functions)
            result->case_insensitive.push_back(Value::BOOLEAN(false));      // case_insensitive (placeholder)
            result->descriptions.push_back(row.GetValue<std::string>(2));   // description
        }
    }

    return std::move(result);
}

static unique_ptr<FunctionData> SystemColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
    // Core columns
    names.emplace_back("database");
    names.emplace_back("table");
    names.emplace_back("name");
    names.emplace_back("type");
    names.emplace_back("position");
    names.emplace_back("comment");

    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::VARCHAR);
    return_types.emplace_back(LogicalType::INTEGER);
    return_types.emplace_back(LogicalType::VARCHAR);

    auto result = make_uniq<SystemColumnsData>();
    
    // Get tables using SHOW ALL TABLES
    Connection con(Catalog::GetSystemCatalog(context).GetDatabase());
    auto table_result = con.Query("SHOW ALL TABLES");
    
    if (!table_result->HasError()) {
        for (auto &row : *table_result) {
            string database = row.GetValue<std::string>(0); // database
            string schema = row.GetValue<std::string>(1);   // schema
            string table = row.GetValue<std::string>(2);    // table
            
            // Get columns for each table
            auto column_result = con.Query("PRAGMA table_info(" + schema + "." + table + ")");
            if (!column_result->HasError()) {
                for (auto &col_row : *column_result) {
                    result->databases.push_back(Value(database));          // database
                    result->tables.push_back(Value(table));                // table
                    result->column_names.push_back(col_row.GetValue<Value>(1)); // column name
                    result->column_types.push_back(col_row.GetValue<Value>(2)); // column type
                    result->positions.push_back(col_row.GetValue<Value>(0));    // position
                    result->comments.push_back(Value("")); // comment (placeholder)
                }
            }
        }
    }

    return std::move(result);
}

// Function to get the process uptime in seconds for Linux
#if defined(__linux__)
int64_t GetProcessUptime() {
    struct sysinfo s_info;
    sysinfo(&s_info);
    int64_t uptime = s_info.uptime;

    std::ifstream stat_file("/proc/self/stat");
    std::string stat_line;
    std::getline(stat_file, stat_line);
    std::istringstream iss(stat_line);
    std::vector<std::string> stats((std::istream_iterator<std::string>(iss)), std::istream_iterator<std::string>());

    int64_t start_time = std::stoll(stats[21]) / sysconf(_SC_CLK_TCK);
    return uptime - start_time;
}

// Function to get the process uptime in seconds for Windows
#elif defined(_WIN32)
int64_t GetProcessUptime() {
    FILETIME ftCreate, ftExit, ftKernel, ftUser;
    ULARGE_INTEGER li;
    SYSTEMTIME st;
    GetSystemTime(&st);
    SystemTimeToFileTime(&st, &ftExit);
    HANDLE hProcess = GetCurrentProcess();
    GetProcessTimes(hProcess, &ftCreate, &ftExit, &ftKernel, &ftUser);
    li.LowPart = ftCreate.dwLowDateTime;
    li.HighPart = ftCreate.dwHighDateTime;
    int64_t start_time = li.QuadPart / 10000000ULL - 11644473600ULL;
    GetSystemTimeAsFileTime(&ftExit);
    li.LowPart = ftExit.dwLowDateTime;
    li.HighPart = ftExit.dwHighDateTime;
    int64_t current_time = li.QuadPart / 10000000ULL - 11644473600ULL;
    return current_time - start_time;
}

// Function to get the process uptime in seconds for macOS
#elif defined(__APPLE__)
int64_t GetProcessUptime() {
    struct timeval boottime;
    size_t len = sizeof(boottime);
    sysctlbyname("kern.boottime", &boottime, &len, NULL, 0);
    time_t bsec = boottime.tv_sec, csec = time(NULL);

    return difftime(csec, bsec);
}

// Function to get the process uptime in seconds for WASM (stub implementation)
#elif defined(__EMSCRIPTEN__)
int64_t GetProcessUptime() {
    // WASM does not support system calls to get process uptime
    // This is a stub implementation
    return 0;
}

#endif

// Scalar function to return the process uptime
static void SystemUptimeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
    auto uptime = GetProcessUptime();
    auto result_data = FlatVector::GetData<int64_t>(result);
    result_data[0] = uptime;
    result.SetVectorType(VectorType::FLAT_VECTOR);
    FlatVector::SetNull(result, 0, false);
}

// system.disks
struct SystemDisksData : public TableFunctionData {
    vector<Value> names;
    vector<Value> paths;
    vector<Value> free_spaces;
    vector<Value> total_spaces;
    vector<Value> unreserved_spaces;
    vector<Value> keep_free_spaces;
    vector<Value> types;
    vector<Value> object_storage_types;
    vector<Value> metadata_types;
    vector<Value> is_encrypteds;
    vector<Value> is_read_onlys;
    vector<Value> is_write_onces;
    vector<Value> is_remotes;
    vector<Value> is_brokens;
    vector<Value> cache_paths;
    idx_t offset = 0;
};

static void SystemDisksFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = (SystemDisksData &)*data_p.bind_data;
    if (data.offset >= data.names.size()) {
        return;
    }

    idx_t count = 0;
    while (data.offset < data.names.size() && count < STANDARD_VECTOR_SIZE) {
        output.SetValue(0, count, data.names[data.offset]);                  // name
        output.SetValue(1, count, data.paths[data.offset]);                  // path
        output.SetValue(2, count, data.free_spaces[data.offset]);            // free_space
        output.SetValue(3, count, data.total_spaces[data.offset]);           // total_space
        output.SetValue(4, count, data.unreserved_spaces[data.offset]);      // unreserved_space
        output.SetValue(5, count, data.keep_free_spaces[data.offset]);       // keep_free_space
        output.SetValue(6, count, data.types[data.offset]);                  // type
        output.SetValue(7, count, data.object_storage_types[data.offset]);   // object_storage_type
        output.SetValue(8, count, data.metadata_types[data.offset]);         // metadata_type
        output.SetValue(9, count, data.is_encrypteds[data.offset]);          // is_encrypted
        output.SetValue(10, count, data.is_read_onlys[data.offset]);         // is_read_only
        output.SetValue(11, count, data.is_write_onces[data.offset]);        // is_write_once
        output.SetValue(12, count, data.is_remotes[data.offset]);            // is_remote
        output.SetValue(13, count, data.is_brokens[data.offset]);            // is_broken
        output.SetValue(14, count, data.cache_paths[data.offset]);           // cache_path
        
        count++;
        data.offset++;
    }

    output.SetCardinality(count);
}

static unique_ptr<FunctionData> SystemDisksBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    return_types.emplace_back(LogicalType::VARCHAR); // name
    names.emplace_back("name");

    return_types.emplace_back(LogicalType::VARCHAR); // path
    names.emplace_back("path");

    return_types.emplace_back(LogicalType::BIGINT); // free_space
    names.emplace_back("free_space");

    return_types.emplace_back(LogicalType::BIGINT); // total_space
    names.emplace_back("total_space");

    return_types.emplace_back(LogicalType::BIGINT); // unreserved_space
    names.emplace_back("unreserved_space");

    return_types.emplace_back(LogicalType::BIGINT); // keep_free_space
    names.emplace_back("keep_free_space");

    return_types.emplace_back(LogicalType::VARCHAR); // type
    names.emplace_back("type");

    return_types.emplace_back(LogicalType::VARCHAR); // object_storage_type
    names.emplace_back("object_storage_type");

    return_types.emplace_back(LogicalType::VARCHAR); // metadata_type
    names.emplace_back("metadata_type");

    return_types.emplace_back(LogicalType::BOOLEAN); // is_encrypted
    names.emplace_back("is_encrypted");

    return_types.emplace_back(LogicalType::BOOLEAN); // is_read_only
    names.emplace_back("is_read_only");

    return_types.emplace_back(LogicalType::BOOLEAN); // is_write_once
    names.emplace_back("is_write_once");

    return_types.emplace_back(LogicalType::BOOLEAN); // is_remote
    names.emplace_back("is_remote");

    return_types.emplace_back(LogicalType::BOOLEAN); // is_broken
    names.emplace_back("is_broken");

    return_types.emplace_back(LogicalType::VARCHAR); // cache_path
    names.emplace_back("cache_path");

    auto result = make_uniq<SystemDisksData>();
    
    // Assuming we are using the DuckDB database path
    auto &db_instance = DatabaseInstance::GetDatabase(context);
    DuckDB db(db_instance);
    Connection con(db);
    
    // Query the duckdb_databases to get the database paths
    auto db_paths_query = con.Query("SELECT * FROM duckdb_databases();");

    if (db_paths_query->HasError()) {
        throw Exception(ExceptionType::EXECUTOR, "Failed to query duckdb_databases");
    }

    auto collection_paths = db_paths_query->Fetch();

    for (idx_t row_idx = 0; row_idx < collection_paths->size(); row_idx++) {
        auto name = collection_paths->GetValue(0, row_idx).ToString(); // database_name
        auto path = collection_paths->GetValue(2, row_idx).ToString(); // path

        // Query the pragma_database_size to get the database size details
        auto size_query = con.Query("CALL pragma_database_size();");

        if (size_query->HasError()) {
            throw Exception(ExceptionType::EXECUTOR, "Failed to query pragma_database_size");
        }

        auto collection_size = size_query->Fetch();
        for (idx_t size_row_idx = 0; size_row_idx < collection_size->size(); size_row_idx++) {
            if (collection_size->GetValue(0, size_row_idx).ToString() == name) {
                int64_t block_size = collection_size->GetValue(2, size_row_idx).GetValue<int64_t>();
                int64_t total_blocks = collection_size->GetValue(3, size_row_idx).GetValue<int64_t>();
                int64_t free_blocks = collection_size->GetValue(5, size_row_idx).GetValue<int64_t>();

                result->names.push_back(name);
                result->paths.push_back(path.empty() ? Value("") : Value(path));
                result->free_spaces.push_back(Value::BIGINT(free_blocks * block_size)); // free_blocks * block_size
                result->total_spaces.push_back(Value::BIGINT(total_blocks * block_size)); // total_blocks * block_size
                result->unreserved_spaces.push_back(Value::BIGINT(free_blocks * block_size)); // free_blocks * block_size
                result->keep_free_spaces.push_back(Value::BIGINT(0)); // No equivalent in DuckDB, setting to 0
                result->types.push_back(Value("Local")); // Assuming Local type
                result->object_storage_types.push_back(Value("None")); // No equivalent in DuckDB, setting to None
                result->metadata_types.push_back(Value("None")); // No equivalent in DuckDB, setting to None
                result->is_encrypteds.push_back(Value::BOOLEAN(false)); // Assuming no encryption
                result->is_read_onlys.push_back(Value::BOOLEAN(false)); // Assuming not read-only
                result->is_write_onces.push_back(Value::BOOLEAN(false)); // Assuming not write-once
                result->is_remotes.push_back(Value::BOOLEAN(false)); // Assuming not remote
                result->is_brokens.push_back(Value::BOOLEAN(false)); // Assuming not broken
                result->cache_paths.push_back(Value("")); // No equivalent in DuckDB, setting to empty
                break;
            }
        }
    }

    return std::move(result);
}

// -- Registration function
void RegisterSystemFunctions(DatabaseInstance &instance) {
    // Register system.databases table function
    auto databases_func = TableFunction("system_databases", {}, SystemDatabasesFunction, SystemDatabasesBind);
    ExtensionUtil::RegisterFunction(instance, databases_func);

    // Register system.tables table function
    auto tables_func = TableFunction("system_tables", {}, SystemTablesFunction, SystemTablesBind);
    ExtensionUtil::RegisterFunction(instance, tables_func);

    // Register system.columns table function
    auto columns_func = TableFunction("system_columns", {}, SystemColumnsFunction, SystemColumnsBind);
    ExtensionUtil::RegisterFunction(instance, columns_func);

    // Register system.functions table function
    auto functions_func = TableFunction("system_functions", {}, SystemFunctionsFunction, SystemFunctionsBind);
    ExtensionUtil::RegisterFunction(instance, functions_func);

    // Register system.uptime scalar function
    auto uptime_func = ScalarFunction("uptime", {}, LogicalType::BIGINT, SystemUptimeFunction);
    ExtensionUtil::RegisterFunction(instance, uptime_func);

    // Register system.disks table function
    auto disks_func = TableFunction("system_disks", {}, SystemDisksFunction, SystemDisksBind);
    ExtensionUtil::RegisterFunction(instance, disks_func);
}

void CreateSystemViews(Connection &con) {
    // Create system schema if it doesn't exist
    con.Query("CREATE SCHEMA IF NOT EXISTS system;");

    // Create views in system schema
    con.Query("CREATE OR REPLACE VIEW system.databases AS SELECT * FROM system_databases();");
    con.Query("CREATE OR REPLACE VIEW system.tables AS SELECT * FROM system_tables();");
    con.Query("CREATE OR REPLACE VIEW system.columns AS SELECT * FROM system_columns();");
    con.Query("CREATE OR REPLACE VIEW system.functions AS SELECT * FROM system_functions();");
    con.Query("CREATE OR REPLACE VIEW system.uptime AS SELECT uptime();");
    con.Query("CREATE OR REPLACE VIEW system.disks AS SELECT * FROM system_disks();");
}

} // namespace duckdb
