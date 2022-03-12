// Run with:
// CGO_LDFLAGS="-L<path to libduckdb_static.a>" CGO_CFLAGS="-I<path to duckdb.h>" DYLD_LIBRARY_PATH="<path to libduckdb.dylib>" go run examples/test.go

package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/marcboeker/go-duckdb"
	"math/rand"
	"os"
	"strconv"
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
	var db, err = sql.Open("duckdb", "/var/tmp/duck.db")
	errPanic(err)
	defer db.Close()

	//var users = csvFifo(10000, []uint64{1000})

	var exec = func(format string, a ...interface{}) {
		var t0 = time.Now()
		var q = fmt.Sprintf(format, a...)
		_, err = db.Exec(q)
		errPanic(err)
		fmt.Println(q, time.Since(t0))
	}

	var query = func(format string, a ...interface{}) {
		var t0 = time.Now()
		var q = fmt.Sprintf(format, a...)

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
	
	var transactions = func() string {
		return csvFifo(100_000_000, []uint64{1_000_000})
	}

	exec("create table x as select * from read_csv('%s', DELIM=',', HEADER=False, COLUMNS={'column0': 'INT32', 'column1': 'INT32'})", transactions())

	exec("COPY x TO 'x.parquet' (FORMAT 'PARQUET', CODEC 'ZSTD')")

	for i := 0; i < 3; i++ {
		query("select column1, fsum(column0) v from x where column1 > 10 group by column1 order by v desc limit 1")
	}
	for i := 0; i < 3; i++ {
		query("select column1, fsum(column0) v from 'x.parquet' where column1 > 10 group by column1 order by v desc limit 1")
	}

	query("select  column1, fsum(column0) v from read_csv('%s', DELIM=',', HEADER=False, COLUMNS={'column0': 'INT32', 'column1': 'INT32'}) where column1 > 10 group by column1 order by v desc limit 1", transactions())

}
