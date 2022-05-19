#include <stdio.h>
#include <algorithm>
#include <iostream>
#include "duckdb.hpp"

extern "C" {
    ArrowArrayStream get_arrow_array_stream();
}

std::unique_ptr<duckdb::ArrowArrayStreamWrapper>
CreateStream(uintptr_t, std::pair<std::unordered_map<duckdb::idx_t, duckdb::string>, std::vector<duckdb::string>> &project_columns,
             duckdb::TableFilterCollection *filters = nullptr) {
    for (duckdb::string i: project_columns.second)
        printf("%s\n", i.c_str());

    auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream = get_arrow_array_stream();
    stream_wrapper->number_of_rows = 10000000000;
    return stream_wrapper;
}

int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn{db};
    duckdb::vector<duckdb::Value> params;
    params.push_back(duckdb::Value::POINTER((uintptr_t) NULL));
    params.push_back(duckdb::Value::POINTER((uintptr_t) &CreateStream));
    params.push_back(duckdb::Value::UBIGINT(1000000));
    auto rel_t = conn.TableFunction("arrow_scan", params)->CreateView("t");
    conn.Query("SET threads TO 1;")->Print();
    conn.Query("SELECT * FROM t LIMIT 10")->Print();
//    conn.Query("SELECT SUM(u.b) FROM t JOIN t u ON t.b = u.a")->Print();
    for (std::string line; std::getline(std::cin, line);) {
        conn.Query(line)->Print();
    }
    return 0;
}
