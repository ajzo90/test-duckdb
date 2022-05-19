#/bin/bash
set -e

go build test_server.go
./test_server &
trap "kill $(jobs -pr)" EXIT

mkdir -p build
(cd build && cmake -DCMAKE_BUILD_TYPE=Release .. )
cmake --build build --config Release -t arrow_duckdb_test
./build/arrow_duckdb_test
