#!/bin/bash
set -e

LIBDUCKDB=$(pwd)/libduckdb
echo ${LIBDUCKDB}

#CGO_LDFLAGS="-L/Users/christianpersson/repos/duckdb/build/release/src" CGO_CFLAGS="-I/Users/christianpersson/repos/duckdb/src/include" DYLD_LIBRARY_PATH="/Users/christianpersson/repos/duckdb/build/release/src" go get github.com/marcboeker/go-duckdb
export CGO_LDFLAGS="-L${LIBDUCKDB} -Wl,-rpath=${LIBDUCKDB}"
export CGO_CFLAGS="-I${LIBDUCKDB}"
go build