// Copyright 2016 The Ssdbcl Author. All Rights Reserved.

package main

import (
	"log"

	"github.com/lifezq/sqlcl"
)

func arrayRemove(a []string, s string) []string {

	for i, v := range a {

		if v == s {
			a = append(a[:i], a[i+1:]...)
		}
	}

	return a
}

func main() {

	qset := sqlcl.NewQuerySet()
	qset.Select("*").From("table_bench").Where("id").Eq("30000").
		And("id").Gt("40000").Or("title").Neq("title_01").Limit(100, 20).Sql()

	qset = sqlcl.NewQuerySet()
	qset.InsertTable("test_temp").InsertFields("(title,content)").InsertValues("('fdsfds','fdsfd'),('vvvvvv','ddddd')").Sql()

	qset = sqlcl.NewQuerySet()
	qset.UpdateTable("test_temp").UpdateSet("title='fffff',content='ccccccccccccccccccc'").Where("id").Eq("30000").Or("id").Gt("100000").Sql()
	return
	conn, err := sqlcl.New(sqlcl.Config{
		Driver:   "mysql",
		Addr:     "127.0.0.1:3306",
		User:     "root",
		Pass:     "123456",
		DbName:   "test",
		Protocol: "tcp",
		Params:   "charset=utf8",
	})

	if err != nil {
		log.Printf("Db conn err:%s\n", err.Error())
		return
	}
	defer conn.Close()

	tbs, err := conn.Query("show tables")
	log.Printf("tbs:%v err:%v\n", tbs, err)

	rst, err := conn.PrepareQuery("select * from table_bench where id in(?,?,?)", 30001, 30002, 30003)
	log.Printf("#0001 rst:%v err:%v\n", rst, err)
}
