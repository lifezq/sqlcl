// Copyright 2016 The Sqlcl Author. All Rights Reserved.

package main

import (
	"log"

	"github.com/lifezq/sqlcl"
)

func main() {

	qset := sqlcl.NewQuerySet()
	qset.Select("*").From("test_temp").Where("id").Eq("30000").
		And("id").Gt("40000").Or("title").Neq("title_01").Limit(100, 20)
	log.Printf("sql:%s\n", qset.Sql(false))

	qset.Clear()

	qset.InsertTable("test_temp").InsertFields("title,content").InsertValues("('fdsfds','fdsfd'),('vvvvvv','ddddd')")

	log.Printf("sql:%s\n", qset.Sql(false))

	qset.Clear()

	qset.UpdateTable("test_temp").UpdateSet("title='fffff',content='ccccccccccccccccccc'").Where("id").Eq("30000").Or("id").Gt("100000")
	log.Printf("sql:%s\n", qset.Sql(false))

	qset.Clear()

	qset.Select("*").From("test_temp").Where("id").In("31,32,33,100")

	log.Printf("sql:%s\n", qset.Sql(false))

	qset.Clear()

	qset.Delete().From("test_temp").Where("id").In("31,32,33,100")

	log.Printf("sql:%s\n", qset.Sql(false))

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

	qset.Clear()

	qset.InsertTable("test_temp").InsertFields("title,content").InsertValues("('title_01','value_01'),('title_02','value_02'),('title_03','content_03')")
	//conn.Exec(qset.Sql(false))
	log.Printf("sql:%s\n", qset.Sql(false))

	qset.Clear()

	qset.Select("*").From("test_temp").Where("id").In("?,?,?")

	log.Printf("sql:%s\n", qset.Sql(true))

	rst, err := conn.PrepareQuery(qset, 1, 2, 3)
	log.Printf("#0001 rst:%v err:%v\n", rst, err)
}
