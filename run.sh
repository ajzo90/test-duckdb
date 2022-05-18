#/bin/bash
set -e

cmake -S . -B build
cmake --build build
./build/arrow_duckdb_test