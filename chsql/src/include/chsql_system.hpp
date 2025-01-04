#pragma once

#include "duckdb.hpp"

namespace duckdb {

void RegisterSystemFunctions(DatabaseInstance &instance);
void CreateSystemViews(Connection &con);

} // namespace duckdb
