#include "clickhouse_scan.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
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
static LogicalType ConvertClickHouseType(const clickhouse::ColumnRef& column) {
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
        case clickhouse::Type::Int128:
            return LogicalType::HUGEINT;
            
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
            return LogicalType::DATE;
        case clickhouse::Type::Date32:
            return LogicalType::DATE;
        case clickhouse::Type::DateTime:
            return LogicalType::TIMESTAMP;
        case clickhouse::Type::DateTime64:
            return LogicalType::TIMESTAMP;
            
        // Boolean type
        case clickhouse::Type::Nothing:
            return LogicalType::BOOLEAN;
            
        // Decimal types
        case clickhouse::Type::Decimal:
        case clickhouse::Type::Decimal32:
        case clickhouse::Type::Decimal64:
        case clickhouse::Type::Decimal128:
            // Get precision and scale from the type
            auto decimal_type = static_cast<const clickhouse::DecimalType*>(column->Type().get());
            return LogicalType::DECIMAL(decimal_type->GetPrecision(), decimal_type->GetScale());
    }
}

static void ClickHouseScanFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<ClickHouseBindData>();
    
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
                auto& target = output.data[col_idx];
                auto& source = block[col_idx];

                // Convert and copy data based on type
                switch (bind_data.types[col_idx].id()) {
                    // String types
                    case LogicalTypeId::VARCHAR: {
                        if (source->Type()->GetCode() == clickhouse::Type::FixedString) {
                            auto& strings = source->As<clickhouse::ColumnFixedString>();
                            auto& target_vector = FlatVector::GetData<string_t>(target);
                            for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                target_vector[row_idx] = StringVector::AddString(target, strings->At(row_idx));
                            }
                        } else {
                            auto& strings = source->As<clickhouse::ColumnString>();
                            auto& target_vector = FlatVector::GetData<string_t>(target);
                            for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                target_vector[row_idx] = StringVector::AddString(target, strings->At(row_idx));
                            }
                        }
                        break;
                    }
                    
                    // Integer types
                    case LogicalTypeId::TINYINT: {
                        auto& integers = source->As<clickhouse::ColumnInt8>();
                        auto& target_vector = FlatVector::GetData<int8_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    case LogicalTypeId::SMALLINT: {
                        auto& integers = source->As<clickhouse::ColumnInt16>();
                        auto& target_vector = FlatVector::GetData<int16_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    case LogicalTypeId::INTEGER: {
                        auto& integers = source->As<clickhouse::ColumnInt32>();
                        auto& target_vector = FlatVector::GetData<int32_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    case LogicalTypeId::BIGINT: {
                        auto& integers = source->As<clickhouse::ColumnInt64>();
                        auto& target_vector = FlatVector::GetData<int64_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    case LogicalTypeId::HUGEINT: {
                        auto& integers = source->As<clickhouse::ColumnInt128>();
                        auto& target_vector = FlatVector::GetData<hugeint_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            // Assuming ClickHouse returns Int128 as two 64-bit integers
                            auto value = integers->At(row_idx);
                            target_vector[row_idx] = hugeint_t(value.high, value.low);
                        }
                        break;
                    }
                    
                    // Unsigned integer types
                    case LogicalTypeId::UTINYINT: {
                        auto& integers = source->As<clickhouse::ColumnUInt8>();
                        auto& target_vector = FlatVector::GetData<uint8_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    case LogicalTypeId::USMALLINT: {
                        auto& integers = source->As<clickhouse::ColumnUInt16>();
                        auto& target_vector = FlatVector::GetData<uint16_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    case LogicalTypeId::UINTEGER: {
                        auto& integers = source->As<clickhouse::ColumnUInt32>();
                        auto& target_vector = FlatVector::GetData<uint32_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    case LogicalTypeId::UBIGINT: {
                        auto& integers = source->As<clickhouse::ColumnUInt64>();
                        auto& target_vector = FlatVector::GetData<uint64_t>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = integers->At(row_idx);
                        }
                        break;
                    }
                    
                    // Floating point types
                    case LogicalTypeId::FLOAT: {
                        auto& floats = source->As<clickhouse::ColumnFloat32>();
                        auto& target_vector = FlatVector::GetData<float>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = floats->At(row_idx);
                        }
                        break;
                    }
                    case LogicalTypeId::DOUBLE: {
                        auto& doubles = source->As<clickhouse::ColumnFloat64>();
                        auto& target_vector = FlatVector::GetData<double>(target);
                        for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                            target_vector[row_idx] = doubles->At(row_idx);
                        }
                        break;
                    }
                    
                    // Date and Time types
                    case LogicalTypeId::DATE: {
                        if (source->Type()->GetCode() == clickhouse::Type::Date32) {
                            auto& dates = source->As<clickhouse::ColumnDate32>();
                            auto& target_vector = FlatVector::GetData<date_t>(target);
                            for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                // Convert from days since epoch
                                target_vector[row_idx] = date_t(dates->At(row_idx));
                            }
                        } else {
                            auto& dates = source->As<clickhouse::ColumnDate>();
                            auto& target_vector = FlatVector::GetData<date_t>(target);
                            for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                target_vector[row_idx] = date_t(dates->At(row_idx));
                            }
                        }
                        break;
                    }
                    case LogicalTypeId::TIMESTAMP: {
                        if (source->Type()->GetCode() == clickhouse::Type::DateTime64) {
                            auto& timestamps = source->As<clickhouse::ColumnDateTime64>();
                            auto& target_vector = FlatVector::GetData<timestamp_t>(target);
                            for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                // Convert from microseconds since epoch
                                target_vector[row_idx] = timestamp_t(timestamps->At(row_idx));
                            }
                        } else {
                            auto& timestamps = source->As<clickhouse::ColumnDateTime>();
                            auto& target_vector = FlatVector::GetData<timestamp_t>(target);
                            for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                // Convert from seconds since epoch
                                target_vector[row_idx] = timestamp_t(timestamps->At(row_idx) * Interval::MICROS_PER_SEC);
                            }
                        }
                        break;
                    }
                    
                    // Decimal types
                    case LogicalTypeId::DECIMAL: {
                        switch (source->Type()->GetCode()) {
                            case clickhouse::Type::Decimal32: {
                                auto& decimals = source->As<clickhouse::ColumnDecimal32>();
                                auto& target_vector = FlatVector::GetData<hugeint_t>(target);
                                for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                    target_vector[row_idx] = hugeint_t(decimals->At(row_idx));
                                }
                                break;
                            }
                            case clickhouse::Type::Decimal64: {
                                auto& decimals = source->As<clickhouse::ColumnDecimal64>();
                                auto& target_vector = FlatVector::GetData<hugeint_t>(target);
                                for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                    target_vector[row_idx] = hugeint_t(decimals->At(row_idx));
                                }
                                break;
                            }
                            case clickhouse::Type::Decimal128: {
                                auto& decimals = source->As<clickhouse::ColumnDecimal128>();
                                auto& target_vector = FlatVector::GetData<hugeint_t>(target);
                                for (idx_t row_idx = 0; row_idx < row_count; row_idx++) {
                                    auto value = decimals->At(row_idx);
                                    target_vector[row_idx] = hugeint_t(value.high, value.low);
                                }
                                break;
                            }
                            default:
                                throw NotImplementedException("Uns
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
