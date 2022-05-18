#/bin/bash
set -e

trap "go run test_server.go" EXIT

cmake -S . -B build
cmake --build build
./build/arrow_duckdb_test
