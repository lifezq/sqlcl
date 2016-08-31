// Copyright 2016 The Sqlcl Author. All Rights Reserved.

package sqlcl

import "testing"

func TestSqlite3(t *testing.T) {

	db, err := New(Config{
		Driver: "sqlite3",
		//Addr:   ":memory:",
		Addr: "./foo.db",
	})
	if err != nil {
		t.Fatalf("db conn err:%s", err.Error())
	}
	defer db.Close()

	rs, err := db.Exec("create temporary table if not exists foo(id integer not null primary key autoincrement, name text)")
	if err != nil {
		t.Fatalf("#000 rs:%v err:%v\n", rs, err)
	}
	// t.Logf("#000 rs:%v err:%v\n", rs, err)

	qset := NewQuerySet()
	err = db.Prepare(qset.InsertTable("foo").InsertFields("name").InsertValues("(?)"))
	if err != nil {
		t.Fatalf("#001  err:%v\n", err)
	}
	t.Logf("#001 sql:%s  err:%v\n", qset.Sql(true), err)

	for i := 0; i < 1000; i++ {

		rs, err = db.PrepareExec(qset, i)
		if err != nil {
			t.Errorf("#002 rs:%v err:%v\n", rs, err)
		}

		// lid, err := rs.LastInsertId()
		// afs, err := rs.RowsAffected()

		//t.Logf("#002 lid:%d afs:%d err:%v\n", lid, afs, err)
	}

	db.PrepareClose(qset)

	qset.Clear()
	err = db.Prepare(qset.Select("*").From("foo").LimitString("?,?"))
	if err != nil {
		t.Fatalf("#003 db.Prepare err:%s", err.Error())
	}

	offset, limit := uint64(0), uint64(20)
	for {

		rs, err := db.PrepareQuery(qset, offset, limit)
		if err != nil {
			t.Errorf("#003 rs:%v err:%v", rs, err)
		}

		t.Logf("#003 rs:%v err:%v", rs, err)

		if len(rs.Data) < 1 {
			break
		}

		offset += limit
	}

	db.PrepareClose(qset)

	//rs, err = db.Exec("drop table if exists foo")
	//if err != nil {
	//	t.Errorf("#004 rs:%v err:%v", rs, err)
	//}

	// t.Logf("#004 rs:%v err:%v", rs, err)
}
