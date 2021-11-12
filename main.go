package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/riete/db-compare/scan"

	"github.com/riete/db-compare/mysql"
)

func run(pk mysql.PK, ms mysql.MySql) []scan.Scan {
	var sr []scan.Scan
	for _, database := range pk.Databases {
		for _, table := range database.Tables {
			s := scan.Scan{
				Mysql:    ms,
				Database: database.Name,
				Table:    table.Name,
				HasPK:    table.HasPK,
				PKColumn: table.PKColumn,
				PKType:   table.PKType,
			}
			if err := s.GetCount(); err != nil {
				log.Fatalln(err)
			}
			log.Println(fmt.Sprintf("full scan %s %s %s done", ms.Host, database.Name, table.Name))
			sr = append(sr, s)
		}
	}
	return sr
}

func fullCheck(pk mysql.PK, src, tgt mysql.MySql) {
	srcScan := run(pk, src)
	tgtScan := run(pk, tgt)
	if err := scan.SaveFull(srcScan, tgtScan); err != nil {
		log.Fatalln(err)
	}
}

func diffCheck(src, tgt mysql.MySql) {
	srcScan, err := scan.LoadAndScan(src)
	if err != nil {
		log.Fatalln(err)
	}
	tgtScan, err := scan.LoadAndScan(tgt)
	if err != nil {
		log.Fatalln(err)
	}
	if err := scan.SaveDiff(srcScan, tgtScan); err != nil {
		log.Fatalln(err)
	}
}

func main() {
	source := flag.String("source", "", "host:port:username:password")
	target := flag.String("target", "", "host:port:username:password")
	fullScan := flag.Bool("full-scan", false, "full scan, default false")
	flag.Parse()
	if *source == "" || *target == "" {
		panic(errors.New("source and target is required"))
	}
	src := strings.Split(*source, ":")
	tgt := strings.Split(*target, ":")

	srcMysql := mysql.MySql{Host: src[0], Port: src[1], Username: src[2], Password: src[3]}
	tgtMysql := mysql.MySql{Host: tgt[0], Port: tgt[1], Username: tgt[2], Password: tgt[3]}

	pk := mysql.PK{MySql: srcMysql}
	if err := pk.Parse(); err != nil {
		log.Fatalln(err)
	}
	if err := pk.Save(); err != nil {
		log.Fatalln(err)
	}
	if *fullScan {
		fullCheck(pk, srcMysql, tgtMysql)
	} else {
		diffCheck(srcMysql, tgtMysql)
	}
}
