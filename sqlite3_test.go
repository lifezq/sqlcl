// Copyright 2016 The Sqlcl Author. All Rights Reserved.
//
// -----------------------------------------------------

package sqlcl

import (
	"fmt"
	"testing"
)

func TestSqlite3DB(t *testing.T) {

	db, err := New(Config{
		Driver: "sqlite3",
		//Addr:   ":memory:",
		Addr: "./foo.db",
	})
	if err != nil {
		t.Fatalf("db conn err:%s", err.Error())
	}
	defer db.Close()

	rs, err := db.ExecString("create table if not exists foo(id integer not null primary key autoincrement, name text)")
	if err != nil {
		t.Fatalf("#000 rs:%v err:%v\n", rs, err)
	}
	// t.Logf("#000 rs:%v err:%v\n", rs, err)

	qset := NewQuerySet()
	err = db.Prepare(qset.InsertTable("foo").InsertFields("name").InsertValues("(?)"))
	if err != nil {
		t.Fatalf("#001  err:%v\n", err)
	}
	t.Logf("#001 sql:%s  err:%v\n", qset.sql(), err)

	for i := 0; i < 1000; i++ {

		rs, err = db.PrepareExec(qset, i)
		if err != nil {
			t.Errorf("#002 rs:%v err:%v\n", rs, err)
			break
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

	offset, limit, total := uint64(0), uint64(20), 0
	for {

		rs, err := db.PrepareQuery(qset, offset, limit)
		if err != nil {
			t.Errorf("#003 rs:%v err:%v", rs, err)
			break
		}

		total += len(rs.Data)

		if len(rs.Data) < 1 {
			break
		}

		offset += limit
	}

	t.Logf("#003 rs.len:%d err:%v", total, err)

	db.PrepareClose(qset)

	//rs, err = db.Exec("drop table if exists foo")
	//if err != nil {
	//	t.Errorf("#004 rs:%v err:%v", rs, err)
	//}

	// t.Logf("#004 rs:%v err:%v", rs, err)
}

func TestSqlite3Tx(t *testing.T) {

	db, err := New(Config{
		Driver: "sqlite3",
		Addr:   "file::memory:?mode=memory&cache=shared",
		// Addr:   ":memory:",
	})
	if err != nil {
		t.Fatalf("db conn err:%s", err.Error())
	}
	defer db.Close()

	rs, err := db.ExecString("create temporary table if not exists foo(id integer not null primary key autoincrement, name text)")
	if err != nil {
		t.Fatalf("Tx.#000 rs:%v err:%v\n", rs, err)
	}

	qset := NewQuerySet()
	qset.InsertTable("foo").InsertFields("name").InsertValues("(?)")

	if err = db.Prepare(qset); err != nil {
		t.Fatalf("Tx.db.Prepare err:%s\n", err.Error())
	}
	t.Logf("Tx.sql:%s\n", qset.sql())

	if err = db.TxBegin(qset); err != nil {
		t.Fatalf("Tx.db.Begin err:%s\n", err.Error())
	}

	for i := 0; i < 1000; i++ {

		exec_rst, err := db.TxStmtExec(qset, fmt.Sprintf("#%d_tx_test_value", i))
		if err != nil {
			t.Errorf("Tx.db.TxStmtExec err:%s\n", err.Error())
			break
		}

		_, err = exec_rst.LastInsertId()
		if err != nil {
			t.Errorf("Tx.exec_rst.LastInsertId err:%s\n", err.Error())
			break
		}

		_, err = exec_rst.RowsAffected()
		if err != nil {
			t.Errorf("Tx.exec_rst.RowsAffected err:%s\n", err.Error())
			break
		}

		// t.Logf("Tx.db.TxStmtExec lid:%d aft:%d\n", lid, aft)
	}

	if tx_rollback() {

		if err = db.TxRollBack(qset); err != nil {
			t.Fatalf("Tx.db.TxRollBack err:%s\n", err.Error())
		}

		t.Fatalf("Exit Info Tx.RollBack Args Is Specified")
	}

	if err = db.TxCommit(qset); err != nil {
		t.Fatalf("Tx.db.TxCommit err:%s\n", err.Error())
	}

	db.PrepareClose(qset)

	rst, err := db.Query(qset.Clear().Select("*").From("foo"))
	if err != nil {
		t.Fatalf("Tx.db.query err:%s\n", err.Error())
	}

	t.Logf("Tx.rst.len:%d err:%v\n", len(rst.Data), err)
}
