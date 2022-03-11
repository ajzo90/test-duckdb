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
	"sync/atomic"
	"syscall"
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
	var f = bufio.NewWriter(file)
	var rnd = rand.New(rand.NewSource(int64(idx)))
	go func() {
		for record := uint64(0); records == 0 || record < records; record++ {
			_, err = fmt.Fprintf(f, "%d", record)
			errPanic(err)
			for _, max := range columns {
				_, err = fmt.Fprintf(f, ",%d", rnd.Uint64()%max)
				errPanic(err)
			}
			_, err = fmt.Fprint(f, "\n")
			errPanic(err)
		}
		errPanic(f.Flush())
		errPanic(file.Close())
	}()
	return fname
}

func main() {
	var db, err = sql.Open("duckdb", "/var/tmp/duck.db")
	errPanic(err)
	defer db.Close()

	var transactions = csvFifo(100000, []uint64{1000, 1000})
	var users = csvFifo(10000, []uint64{1000})
	//var item = csvFifo(1000, []uint64{1000})

	//time.Sleep(time.Second * 1000)
	var query = fmt.Sprintf(`SELECT AVG(U.column1::INT) from read_csv_auto('%[1]s') T JOIN read_csv_auto('%[2]s') U ON T.column1 = U.column0`, transactions, users)

	var rows *sql.Rows
	rows, err = db.Query(query)
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
}
