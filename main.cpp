#include <stdio.h>
#include <algorithm>
#include <iostream>
#include "duckdb.hpp"
#include <chrono>

// run "cargo install cbindgen && cbindgen rust_arrow_client/" for a hint how this line should look if the Rust side changes
extern "C" ArrowArrayStream get_arrow_array_stream(const char *base_url, const char *table_name);

std::unique_ptr<duckdb::ArrowArrayStreamWrapper>
CreateStream(uintptr_t, std::pair<std::unordered_map<duckdb::idx_t, duckdb::string>, std::vector<duckdb::string>> &project_columns,
             duckdb::TableFilterCollection *filters = nullptr) {
//    for (duckdb::string i: project_columns.second)
//        printf("%s\n", i.c_str());

    auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream = get_arrow_array_stream("http://localhost:6789", "users");
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
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        conn.Query(line)->Print();
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "ms" << std::endl;
    }
    return 0;
}

//select sum(id) from t group by id limit 1