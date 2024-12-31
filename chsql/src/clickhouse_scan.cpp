#include "clickhouse_scan.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_util.hpp"
#include <clickhouse/client.h>

namespace duckdb {

struct ClickHouseBindData : public TableFunctionData {
    string query;
    string host;
    string port;
    string user;
    string password;
    string database;
    bool finished;
    vector<LogicalType> types;
    vector<string> names;
    
    ClickHouseBindData(string query, string host, string port, string user, string password, string database) 
        : query(query), host(host), port(port), user(user), password(password), database(database), finished(false) {}
};

// Convert ClickHouse type to DuckDB LogicalType
static LogicalType ConvertClickHouseType(const clickhouse::ColumnRef column) {
    switch (column->Type()->GetCode()) {
        // Integer types
        case clickhouse::Type::Int8:
            return LogicalType::TINYINT;
        case clickhouse::Type::Int16:
            return LogicalType::SMALLINT;
        case clickhouse::Type::Int32:
            return LogicalType::INTEGER;
        case clickhouse::Type::Int64:
            return LogicalType::BIGINT;
            
        // Unsigned integer types    
        case clickhouse::Type::UInt8:
            return LogicalType::UTINYINT;
        case clickhouse::Type::UInt16:
            return LogicalType::USMALLINT;
        case clickhouse::Type::UInt32:
            return LogicalType::UINTEGER;
        case clickhouse::Type::UInt64:
            return LogicalType::UBIGINT;
            
        // Floating point types
        case clickhouse::Type::Float32:
            return LogicalType::FLOAT;
        case clickhouse::Type::Float64:
            return LogicalType::DOUBLE;
            
        // String types    
        case clickhouse::Type::String:
        case clickhouse::Type::FixedString:
            return LogicalType::VARCHAR;
            
        // Date and Time types
        case clickhouse::Type::Date:
        case clickhouse::Type::Date32:
            return LogicalType::DATE;
        case clickhouse::Type::DateTime:
        case clickhouse::Type::DateTime64:
            return LogicalType::TIMESTAMP;
            
        // Default to VARCHAR for unsupported types
        default:
            return LogicalType::VARCHAR;
    }
}

static void ClickHouseScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = const_cast<ClickHouseBindData&>(data_p.bind_data->Cast<ClickHouseBindData>());
    
    if (bind_data.finished) {
        return;
    }

    try {
        // Initialize ClickHouse client
        clickhouse::Client client(clickhouse::ClientOptions()
            .SetHost(bind_data.host)
            .SetPort(std::stoi(bind_data.port))
            .SetUser(bind_data.user)
            .SetPassword(bind_data.password)
            .SetDefaultDatabase(bind_data.database)
            .SetPingBeforeQuery(true));

        // Execute query
        client.Select(bind_data.query, [&](const clickhouse::Block& block) {
            idx_t row_count = block.GetRowCount();
            output.SetCardinality(row_count);

            for (idx_t col_idx = 0; col_idx < block.GetColumnCount(); col_idx++) {
                const auto source = block[col_idx];
                auto &target = output.data[col_idx];

                // Convert and copy data based on type
                switch (bind_data.types[col_idx].id()) {
                    case LogicalTypeId::VARCHAR: {
                        const auto strings = source->As<clickhouse::ColumnString>();
                        auto target_vector = FlatVector::GetData<string_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            auto sv = strings->At(row_idx);
                            target_vector[row_idx] = StringVector::AddString(target, sv.data(), sv.size());
                        }
                        break;
                    }
                    case LogicalTypeId::INTEGER: {
                        const auto integers = source->As<clickhouse::ColumnInt32>();
                        auto target_vector = FlatVector::GetData<int32_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    // Add remaining type conversions here
                    default:
                        throw NotImplementedException("Type not yet implemented in scan function");
                }
            }
        });

        bind_data.finished = true;

    } catch (const std::exception& e) {
        throw IOException("ClickHouse error: " + string(e.what()));
    }
}

static unique_ptr<FunctionData> ClickHouseScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    auto query = input.inputs[0].GetValue<string>();

    // Get ClickHouse connection details from secrets
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    auto secret_match = secret_manager.LookupSecret(transaction, "clickhouse", "clickhouse");

    if (!secret_match.HasMatch()) {
        throw InvalidInputException("No 'clickhouse' secret found. Please create a secret with CREATE SECRET first.");
    }

    auto &secret = secret_match.GetSecret();
    const auto *kv_secret = dynamic_cast<const KeyValueSecret*>(&secret);
    if (!kv_secret) {
        throw InvalidInputException("Invalid secret format for 'clickhouse' secret");
    }

    // Extract connection parameters from secret
    string host, port, user, password, database;
    Value val;

    if (kv_secret->TryGetValue("host", val)) host = val.ToString();
    if (kv_secret->TryGetValue("port", val)) port = val.ToString();
    if (kv_secret->TryGetValue("user", val)) user = val.ToString();
    if (kv_secret->TryGetValue("password", val)) password = val.ToString();
    if (kv_secret->TryGetValue("database", val)) database = val.ToString();

    // Create bind data
    auto result = make_uniq<ClickHouseBindData>(query, host, port, user, password, database);

    // Initialize client to fetch schema
    try {
        clickhouse::Client client(clickhouse::ClientOptions()
            .SetHost(host)
            .SetPort(std::stoi(port))
            .SetUser(user)
            .SetPassword(password)
            .SetDefaultDatabase(database));

        // Execute query to get schema
        client.Select(query, [&](const clickhouse::Block& block) {
            for (size_t i = 0; i < block.GetColumnCount(); i++) {
                auto column = block[i];
                return_types.push_back(ConvertClickHouseType(column));
                names.push_back(block.GetColumnName(i));
            }
        });

        result->types = return_types;
        result->names = names;

        return std::move(result);
    } catch (const std::exception& e) {
        throw IOException("ClickHouse error during bind: " + string(e.what()));
    }
}

void RegisterClickHouseScanFunction(DatabaseInstance &instance) {
    TableFunction clickhouse_scan("clickhouse_scan", {LogicalType::VARCHAR}, ClickHouseScanFunction, ClickHouseScanBind);
    ExtensionUtil::RegisterFunction(instance, clickhouse_scan);
}

} // namespace duckdb
