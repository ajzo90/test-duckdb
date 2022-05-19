#/bin/bash
set -e

go build test_server.go
./test_server &
trap "kill $(jobs -pr)" EXIT

cmake -S . -B build
cmake --build build
./build/arrow_duckdb_test
