#!/bin/zsh

DUCK=/Users/christianpersson/repos/duckdb

#CGO_LDFLAGS="-L/Users/christianpersson/repos/duckdb/build/release/src" CGO_CFLAGS="-I/Users/christianpersson/repos/duckdb/src/include" DYLD_LIBRARY_PATH="/Users/christianpersson/repos/duckdb/build/release/src" go get github.com/marcboeker/go-duckdb
 cat users.csv | CGO_LDFLAGS="-L/$DUCK/build/release/src" CGO_CFLAGS="-I/$DUCK/src/include" DYLD_LIBRARY_PATH="/$DUCK/build/release/src" go run main.go