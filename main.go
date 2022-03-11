// Run with:
// CGO_LDFLAGS="-L<path to libduckdb_static.a>" CGO_CFLAGS="-I<path to duckdb.h>" DYLD_LIBRARY_PATH="<path to libduckdb.dylib>" go run examples/test.go

package main

import (
	"bufio"
	"database/sql"
	_ "github.com/marcboeker/go-duckdb"
	"io"
	"log"
	"os"
	"strconv"
	"time"
)

var (
	db *sql.DB
)

type row struct {
	k   int
	sum float64
}

func main() {

	if false {
		// dump files
		main2()
		return
	}

	var err error
	db, err = sql.Open("duckdb", "/var/tmp/duck.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	check(db.Ping())

	var exec = func(q string) {
		var t0 = time.Now()
		check(db.Exec(q))
		log.Println(q, time.Since(t0))
	}

	exec("SET memory_limit='2GB';")

	for i := 0; i < 1; i++ {
		func() {
			var t0 = time.Now()
			rows, err := db.Query(`
		SELECT u_i3, count(*) from read_csv_auto('/dev/stdin') group by u_i3 order by count(*) desc limit 3`,
			)
			check(err)
			defer rows.Close()
			defer func() {
				log.Println(time.Since(t0))
			}()

			for rows.Next() {
				u := new(row)
				err := rows.Scan(&u.k, &u.sum)
				if err != nil {
					log.Fatal(err)
				}
				log.Println(u.k, u.sum)
			}
			check(rows.Err())
		}()
	}

	//res, err := db.Exec("DELETE FROM users")
	//check(err)

	//ra, _ := res.RowsAffected()
	//log.Printf("Deleted %d rows\n", ra)

	//runTransaction()
	//testPreparedStmt()
}

func check(args ...interface{}) {
	err := args[len(args)-1]
	if err != nil {
		panic(err)
	}
}

func runTransaction() {
	log.Println("Starting transaction...")
	tx, err := db.Begin()
	check(err)

	check(tx.Exec("INSERT INTO users VALUES('gru', 25, 1.35, false, '1996-04-03')"))
	row := tx.QueryRow("SELECT COUNT(*) FROM users WHERE name = ?", "gru")
	var count int64
	check(row.Scan(&count))
	if count > 0 {
		log.Println("User Gru was inserted")
	}

	log.Println("Rolling back transaction...")
	check(tx.Rollback())

	row = db.QueryRow("SELECT COUNT(*) FROM users WHERE name = ?", "gru")
	check(row.Scan(&count))
	if count > 0 {
		log.Println("Found user Gru")
	} else {
		log.Println("Didn't find user Gru")
	}
}

//func testPreparedStmt() {
//	stmt, err := db.Prepare("INSERT INTO users VALUES(?, ?, ?, ?, ?)")
//	check(err)
//	defer stmt.Close()
//
//	check(stmt.Exec("Kevin", 11, 0.55, true, "2013-07-06"))
//	check(stmt.Exec("Bob", 12, 0.73, true, "2012-11-04"))
//	check(stmt.Exec("Stuart", 13, 0.66, true, "2014-02-12"))
//
//	stmt, err = db.Prepare("SELECT * FROM users WHERE age > ?")
//	check(err)
//
//	rows, err := stmt.Query(1)
//	check(err)
//	defer rows.Close()
//
//	for rows.Next() {
//		u := new(user)
//		err := rows.Scan(&u.name, &u.age, &u.height, &u.awesome, &u.bday)
//		if err != nil {
//			log.Fatal(err)
//		}
//		log.Printf(
//			"%s is %d years old, %.2f tall, bday on %s and has awesomeness: %t\n",
//			u.name, u.age, u.height, u.bday.Format(time.RFC3339), u.awesome,
//		)
//	}
//}

func main2() {
	var w = bufio.NewWriter(os.Stdout)
	defer w.Flush()

	if err := Main(w, 100*1000*1000, "t_id,t_i1,t_i2,t_i3,t_s1\n"); err != nil {
		log.Fatalln(err)
	}

	//if err := Main(w, 1*1000*1000, "u_id,u_i1,u_i2,u_i3,u_s1\n"); err != nil {
	//	log.Fatalln(err)
	//}
}

func Main(w io.Writer, n int, s string) error {
	if _, err := w.Write([]byte(s)); err != nil {
		return err
	}
	var buf []byte
	for i := 0; i < n; i++ {
		buf = strconv.AppendInt(buf[:0], int64(i), 10)
		buf = append(buf, ',')
		buf = strconv.AppendInt(buf, int64(i%1000000), 10)
		buf = append(buf, ',')
		buf = strconv.AppendInt(buf, int64(i%10), 10)
		buf = append(buf, ',')
		buf = strconv.AppendInt(buf, int64(i%100), 10)
		buf = append(buf, `,"xxx-`...)
		buf = strconv.AppendInt(buf, int64(i%100), 10)
		buf = append(buf, '"', '\n')
		if _, err := w.Write(buf); err != nil {
			return err
		}
	}
	return nil
}
