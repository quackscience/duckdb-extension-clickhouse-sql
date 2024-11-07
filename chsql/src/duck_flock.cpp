#ifndef DUCK_FLOCK_H
#define DUCK_FLOCK_H
#include "chsql_extension.hpp"

namespace duckdb {
    struct DuckFlockData : FunctionData {
        vector<unique_ptr<Connection>> conn;
        vector<unique_ptr<QueryResult>> results;
        unique_ptr<FunctionData> Copy() const override {
            throw std::runtime_error("not implemented");
        }
        bool Equals(const FunctionData &other) const override {
            throw std::runtime_error("not implemented");
        };
    };

    unique_ptr<FunctionData> DuckFlockBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
        auto data = make_uniq<DuckFlockData>();

        // Check for NULL input parameters
        if (input.inputs.empty() || input.inputs.size() < 2) {
            throw std::runtime_error("url_flock: missing required parameters");
        }
        if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
            throw std::runtime_error("url_flock: NULL parameters are not allowed");
        }

        auto strQuery = input.inputs[0].GetValue<string>();
        if (strQuery.empty()) {
            throw std::runtime_error("url_flock: empty query string");
        }

        auto &raw_flock = ListValue::GetChildren(input.inputs[1]);
        if (raw_flock.empty()) {
            throw std::runtime_error("url_flock: empty flock list");
        }

        bool has_valid_result = false;
        // Process each connection
        for (auto &duck : raw_flock) {
            if (duck.IsNull() || duck.ToString().empty()) {
                continue;
            }

            try {
                auto conn = make_uniq<Connection>(*context.db);
                if (!conn) {
                    continue;
                }

                auto settingResult = conn->Query("SET autoload_known_extensions=1;SET autoinstall_known_extensions=1;");
                if (settingResult->HasError()) {
                    continue;
                }

                auto req = conn->Prepare("SELECT * FROM read_json($2 || '/?default_format=JSONEachRow&query=' || url_encode($1::VARCHAR))");
                if (req->HasError()) {
                    continue;
                }

                auto queryResult = req->Execute(strQuery.c_str(), duck.ToString());
                if (!queryResult || queryResult->HasError()) {
                    continue;
                }

                // Store the first valid result's types and names
                if (!has_valid_result) {
                    return_types.clear();
                    copy(queryResult->types.begin(), queryResult->types.end(), back_inserter(return_types));
                    names.clear();
                    copy(queryResult->names.begin(), queryResult->names.end(), back_inserter(names));

                    if (return_types.empty()) {
                        throw std::runtime_error("url_flock: query must return at least one column");
                    }
                    has_valid_result = true;
                }

                data->conn.push_back(std::move(conn));
                data->results.push_back(std::move(queryResult));
            } catch (const std::exception &e) {
                continue;
            }
        }

        // Verify we have at least one valid result
        if (!has_valid_result || data->results.empty()) {
            throw std::runtime_error("url_flock: invalid or no results");
        }

        return std::move(data);
    }

    void DuckFlockImplementation(ClientContext &context, TableFunctionInput &data_p,
                               DataChunk &output) {
        auto &data = data_p.bind_data->Cast<DuckFlockData>();
        
        if (data.results.empty()) {
            return;
        }

        for (const auto &res : data.results) {
            if (!res) {
                continue;
            }

            ErrorData error_data;
            unique_ptr<DataChunk> data_chunk = make_uniq<DataChunk>();
            
            try {
                if (res->TryFetch(data_chunk, error_data)) {
                    if (data_chunk && !data_chunk->size() == 0) {
                        output.Append(*data_chunk);
                        return;
                    }
                }
            } catch (...) {
                continue;
            }
        }
    }

    TableFunction DuckFlockTableFunction() {
        TableFunction f(
            "url_flock",
            {LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
            DuckFlockImplementation,
            DuckFlockBind,
            nullptr,
            nullptr
        );
        return f;
    }
}
#endif
