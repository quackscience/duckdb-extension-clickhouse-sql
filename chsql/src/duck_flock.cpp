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
        
        // Set default schema in case all results fail
        return_types = {LogicalType::VARCHAR};
        names = {"result"};
        
        // Check for NULL input parameters
        if (input.inputs.empty() || input.inputs.size() < 2 || 
            input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
            return data;  // Return with default schema
        }

        auto strQuery = input.inputs[0].GetValue<string>();
        if (strQuery.empty()) {
            return data;  // Return with default schema
        }

        vector<string> flock;
        auto &raw_flock = ListValue::GetChildren(input.inputs[1]);
        if (raw_flock.empty()) {
            return data;  // Return with default schema
        }

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

                data->conn.push_back(std::move(conn));
                data->results.push_back(std::move(queryResult));
            } catch (...) {
                continue;
            }
        }

        // If we have valid results, use their schema instead of default
        if (!data->results.empty() && !data->results[0]->HasError()) {
            return_types.clear();
            copy(data->results[0]->types.begin(), data->results[0]->types.end(), back_inserter(return_types));
            names.clear();
            copy(data->results[0]->names.begin(), data->results[0]->names.end(), back_inserter(names));
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
                    if (data_chunk && data_chunk->size() != 0) {
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
