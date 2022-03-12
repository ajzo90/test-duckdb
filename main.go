// Run with:
// CGO_LDFLAGS="-L<path to libduckdb_static.a>" CGO_CFLAGS="-I<path to duckdb.h>" DYLD_LIBRARY_PATH="<path to libduckdb.dylib>" go run examples/test.go

package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

func errPanic(err error) {
	if err != nil {
		panic(err)
	}
}

var nextFifoIndex uint64 = 0

func csvFifo(records uint64, columns []uint64) string {
	var idx = atomic.AddUint64(&nextFifoIndex, 1) - 1
	var fname = fmt.Sprintf("./csv%d", idx)
	errPanic(syscall.Mkfifo(fname, 0666))
	var file, err = os.OpenFile(fname, os.O_RDWR, 0666)
	errPanic(err)
	var w = bufio.NewWriter(file)
	var rnd = rand.New(rand.NewSource(int64(idx)))
	go func() {
		var buf []byte
		for record := uint64(0); records == 0 || record < records; record++ {
			buf = strconv.AppendUint(buf[:0], record, 10)
			for _, max := range columns {
				buf = append(buf, ',')
				buf = strconv.AppendUint(buf, rnd.Uint64()%max, 10)
			}
			buf = append(buf, '\n')
			_, err = w.Write(buf)
			errPanic(err)
		}
		errPanic(w.Flush())
		errPanic(file.Close())
	}()
	return fname
}

func main() {

	defer func() {
		if err := recover(); err != nil {
			log.Fatalf("panicked with err:%v, perhaps you forgot to 'rm /var/tmp/duck.db' or 'rm csv*'?\n", err)
		}
	}()

	var db, err = sql.Open("duckdb", "/var/tmp/duck.db")
	errPanic(err)
	defer db.Close()

	var readCsv = "read_csv('%s', DELIM=',', HEADER=False, COLUMNS={'id': 'INT', 'usr': 'INT', 'item': 'INT', 'ts': 'INT', 'quantity':'INT'})"
	var key, val = "usr", "quantity"

	var renderQ = func(q string, optionalTbl ...interface{}) string {
		q = strings.ReplaceAll(q, "{key}", key)
		q = strings.ReplaceAll(q, "{val}", val)
		q = strings.ReplaceAll(q, "{csv}", fmt.Sprintf(readCsv, optionalTbl...))
		return q
	}

	var transactions = func() string {
		// id,user,item,ts,quantity
		return csvFifo(100_000_000, []uint64{1_000_000, 1_000, 1_000, 10})
	}

	if f := os.Getenv("CSVFILE"); f != "" {
		// use external file instead
		readCsv = "read_csv('%s', DELIM=',', HEADER=True, COLUMNS={'usr': 'UINT64', 'item': 'UINT64', 'ts': 'INT32', 'quantity': 'INT32'})"
		transactions = func() string {
			return f
		}
	}

	var exec = func(format string, optionalTbl ...interface{}) {
		var t0 = time.Now()
		var q = renderQ(format, optionalTbl...)
		_, err = db.Exec(q)
		errPanic(err)
		fmt.Println(q, time.Since(t0))
	}

	var query = func(format string, optionalTbl ...interface{}) {
		var t0 = time.Now()
		var q = renderQ(format, optionalTbl...)
		var rows *sql.Rows
		rows, err = db.Query(q)
		errPanic(err)

		var nRows []string
		nRows, err = rows.Columns()
		errPanic(err)
		var bufferPointers = make([]interface{}, len(nRows))
		var buffer = make([]interface{}, len(nRows))
		for i := range buffer {
			bufferPointers[i] = &buffer[i]
		}
		for rows.Next() {
			errPanic(rows.Scan(bufferPointers...))
			for i := range buffer {
				fmt.Printf("%v ", buffer[i])
			}
			fmt.Println()
		}
		errPanic(rows.Err())
		fmt.Println(q, time.Since(t0))
	}

	exec("create table x(usr INT NOT NULL,item INT NOT NULL, ts INT NOT NULL, quantity INT NOT NULL)")
	exec("insert into x select (usr>>34)::int, (item>>34)::int, ts, quantity from {csv}", transactions()) // x>>34: tried to use smaller types, not a lot of improvements..
	exec("COPY x TO 'x.parquet' (FORMAT 'PARQUET', CODEC 'ZSTD')")

	// from db
	for i := 0; i < 3; i++ {
		query("select {key}, count({val}) v from x where {key} < 10 group by {key} order by v desc limit 3")
	}

	// from parquet file
	for i := 0; i < 3; i++ {
		query("select {key}, count({val}) v from 'x.parquet' where {key} < 10 group by {key} order by v desc limit 3")
	}

	// from csv (slow)
	//query("select {key}, count({val}) v from {csv} where {key} > 10 group by {key} order by v desc limit 1", transactions())

}
