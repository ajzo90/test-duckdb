#include <stdio.h>
#include <algorithm>
#include <iostream>
#include "arrow/api.h"
#include "arrow/c/bridge.h"
#include "arrow/c/abi.h"
#include "duckdb.hpp"

void abort_not_ok(arrow::Status s) {
    if (!s.ok()) {
        s.Abort();
    }
}

std::shared_ptr<arrow::Schema> schema() {
    return arrow::schema({
                                 arrow::field("a", arrow::int32()),
                                 arrow::field("b", arrow::int32()),
                         });
}

int get_next(struct ArrowArrayStream *self, struct ArrowArray *out) {
    const size_t batch_size = 3;
    const size_t total_size = 10;
    auto progress = size_t(uintptr_t(self->private_data));
    if (progress >= uintptr_t(total_size)) {
        out->release = NULLPTR;
        return 0;
    }
    self->private_data = (void *) (uintptr_t(progress + batch_size));

    int64_t this_batch_size = std::min(int64_t(batch_size), int64_t(total_size - progress));
    printf("NEXT off: %lu, len: %ld\n", progress, this_batch_size);

    arrow::NumericBuilder<arrow::Int32Type> builder;
    abort_not_ok(builder.Reserve(this_batch_size));
    for (int32_t i = progress; i < progress + this_batch_size; i++)
        abort_not_ok(builder.Append(i));
    auto int32_array = builder.Finish().ValueOrDie();

    auto array_vector = arrow::ArrayVector({int32_array, int32_array});
    auto struct_array = arrow::StructArray::Make(array_vector, schema()->fields()).ValueOrDie();
    abort_not_ok(ExportArray(*struct_array, out));
    return 0;
}

int get_schema(struct ArrowArrayStream *, struct ArrowSchema *out) {
    abort_not_ok(ExportSchema(*schema(), out));
    return 0;
};

extern "C" {
    void hello_world();
}

std::unique_ptr<duckdb::ArrowArrayStreamWrapper>
CreateStream(uintptr_t, std::pair<std::unordered_map<duckdb::idx_t, duckdb::string>, std::vector<duckdb::string>> &project_columns,
             duckdb::TableFilterCollection *filters = nullptr) {
    for (duckdb::string i: project_columns.second)
        printf("%s\n", i.c_str());

    printf("CREATE\n");
    auto stream_wrapper = duckdb::make_unique<duckdb::ArrowArrayStreamWrapper>();
    stream_wrapper->arrow_array_stream.release = nullptr;
    stream_wrapper->arrow_array_stream.get_schema = get_schema;
    stream_wrapper->arrow_array_stream.get_next = get_next;
    stream_wrapper->arrow_array_stream.private_data = NULLPTR;
    stream_wrapper->number_of_rows = 10000000000;
    return stream_wrapper;
}

int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn{db};
    duckdb::vector<duckdb::Value> params;
    params.push_back(duckdb::Value::POINTER((uintptr_t) NULLPTR));
    params.push_back(duckdb::Value::POINTER((uintptr_t) &CreateStream));
    params.push_back(duckdb::Value::UBIGINT(1000000));
    auto rel_t = conn.TableFunction("arrow_scan", params)->CreateView("t");
    conn.Query("SET threads TO 1;")->Print();
    conn.Query("SELECT SUM(u.b) FROM t JOIN t u ON t.b = u.a")->Print();
    return 0;
}
