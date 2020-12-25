package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	help bool

	host string
	port string
	dbname string

	query string

	concurrency int
	times int
)


func init() {
	flag.BoolVar(&help, "h", false, "this help")

	flag.StringVar(&host, "H", "172.31.12.171", "specify tidb host")
	flag.StringVar(&port, "P", "4000", "specify tidb port")
	flag.StringVar(&dbname, "D", "tpcc", "specify test db name")

	flag.StringVar(&query, "e", "", "sql to be executed")

	flag.IntVar(&concurrency, "c", 1, "query concurrency")
	flag.IntVar(&times, "t", 1, "query times")

	flag.Usage = usage
}

func main() {
	fmt.Println("TPC Test")
	flag.Parse()

	if help {
		flag.Usage()
		return
	}

	if query == "" {
		fmt.Println("error: please specify a sql to execute")
		return
	}
	fmt.Printf("try to run sql: %s\n", query)

	if concurrency <= 0 {
		concurrency = 1
	}
	if times <= 0 {
		times = 1
	}

	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(clientId int) {
			defer wg.Done()

			db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s:%s)/%s", host, port, dbname))
			if err != nil {
				panic(err.Error())
			}
			defer db.Close()

			for j := 0; j < times; j++ {
				start := time.Now()
				results, err := db.Query(query)
				t := time.Now()
				elapsed := t.Sub(start)
				if err != nil {
					panic(err.Error())
				}

				rows := 0
				for results.Next() {
					rows += 1
				}
				log.Printf("client %d, got %d rows, cose time %d ms", clientId, rows, elapsed / (1000 * 1000))
			}
		}(i)
	}
	wg.Wait()
}

func usage() {
	fmt.Fprintf(os.Stderr, `tpcc-test
Usage: [bin] [-hHPDect]

Options:
`)
	flag.PrintDefaults()
}