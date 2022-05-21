#include <stdio.h>
#include <algorithm>
#include <iostream>
#include "duckdb.hpp"
#include <chrono>

// run "cargo install cbindgen && cbindgen rust_arrow_client/" for a hint how this line should look if the Rust side changes
extern "C" ArrowArrayStream get_arrow_array_stream(const char *base_url, const char *table_name);

std::unique_ptr<duckdb::ArrowArrayStreamWrapper>
CreateStream(uintptr_t table, std::pair<std::unordered_map<duckdb::idx_t, duckdb::string>, std::vector<duckdb::string>> &project_columns,
             duckdb::TableFilterCollection *filters = nullptr) {
//    for (duckdb::string i: project_columns.second)
//        printf("%s\n", i.c_str());

    auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream = get_arrow_array_stream("http://localhost:6789", (char *)table);
    stream_wrapper->number_of_rows = 10000000000;
    return stream_wrapper;
}

int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn{db};

    std::vector<std::shared_ptr<duckdb::Relation>> tables;
    std::vector<const char *> tables_names{"users", "transactions"};
    for (const auto& name: tables_names) {
        duckdb::vector<duckdb::Value> params;
        params.push_back(duckdb::Value::POINTER((uintptr_t) name));
        params.push_back(duckdb::Value::POINTER((uintptr_t) &CreateStream));
        params.push_back(duckdb::Value::UBIGINT(1000000));
        auto relation = conn.TableFunction("arrow_scan", params)->CreateView(name);
        tables.push_back(relation);
    }

    conn.Query("SET threads TO 1;")->Print();
    conn.Query("SELECT * FROM users LIMIT 3")->Print();
    conn.Query("SELECT * FROM transactions LIMIT 3")->Print();
//    conn.Query("SELECT SUM(u.b) FROM t JOIN t u ON t.b = u.a")->Print();
    for (std::string line; std::getline(std::cin, line);) {
        std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
        conn.Query(line)->Print();
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << "ms" << std::endl;
    }
    return 0;
}

//SELECT * FROM transactions JOIN users ON transactions."user" = users.id LIMIT 10