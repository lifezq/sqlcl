// Copyright 2016 The Sqlcl Author. All Rights Reserved.
//
// -----------------------------------------------------

package sqlcl

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestMysqlSql(t *testing.T) {

	var (
		qset  = NewQuerySet()
		qneed = strings.TrimSpace("SELECT *  FROM `test_temp`  WHERE id   = \"30000\"   AND id   > \"40000\"   OR title   != \"title_01\"  LIMIT 100,20")
	)

	// ==========================================================
	qset.Clear().Select("*").From("test_temp").Where("id").Eq("30000").And("id").Gt("40000").Or("title").Neq("title_01").Limit(100, 20)

	do_sql_test(qneed, qset, t)

	// ==========================================================
	qneed = strings.TrimSpace("INSERT INTO  `test_temp`  (title,content)  VALUES ('fdsfds','fdsfd'),('vvvvvv','ddddd')")

	qset.Clear().InsertTable("test_temp").InsertFields("title,content").InsertValues("('fdsfds','fdsfd'),('vvvvvv','ddddd')")
	do_sql_test(qneed, qset, t)

	// ==========================================================
	qneed = strings.TrimSpace("UPDATE  `test_temp`  SET title='fffff',content='ccccccccccccccccccc'  WHERE id   = \"30000\"   OR id   > \"100000\"")

	qset.Clear().UpdateTable("test_temp").UpdateSet("title='fffff',content='ccccccccccccccccccc'").Where("id").Eq("30000").Or("id").Gt("100000")
	do_sql_test(qneed, qset, t)

	// ==========================================================
	qneed = strings.TrimSpace("SELECT *  FROM `test_temp`  WHERE id   IN (31,32,33,100)")
	qset.Clear().Select("*").From("test_temp").Where("id").In("31,32,33,100")
	do_sql_test(qneed, qset, t)

	// ==========================================================
	qneed = strings.TrimSpace("DELETE  FROM `test_temp`  WHERE id   IN (31,32,33,500,1000)")
	qset.Clear().Delete().From("test_temp").Where("id").In("31,32,33,500,1000")
	do_sql_test(qneed, qset, t)

}

func TestMysqlDB(t *testing.T) {

	db, err := New(Config{
		Driver:   "mysql",
		Addr:     "127.0.0.1:3306",
		User:     "root",
		Pass:     "",
		DbName:   "test",
		Protocol: "tcp",
		Params:   "charset=utf8",
	})

	if err != nil {
		t.Fatalf("Db db err:%s\n", err.Error())
	}
	defer db.Close()

	db.ExecString(`charset utf8`)

	db.ExecString(`DROP TABLE IF EXISTS test_temp`)

	db.ExecString(`CREATE TABLE IF NOT EXISTS test_temp(id int(11) primary key auto_increment, 
		title varchar(30) not null default '', content varchar(100) not null default '',
		num float(9,2) not null default 0)engine=innodb default charset utf8`)

	qset := NewQuerySet()

	for i := 0; i < 100; i++ {

		qset.Clear().InsertTable("test_temp").InsertFields("title,content").InsertValues("('title_01','value_01'),('title_02','value_02'),('title_03','content_03')")
		if _, err = db.Exec(qset); err != nil {
			t.Errorf("db.Exec err:%v", err)
			break
		}
	}

	// t.Logf("sql:%s\n", qset.sql())

	qset.Clear().Select("*").From("test_temp").Where("id").In("?,?,?")

	// t.Logf("sql:%s\n", qset.sql())

	for i := 0; i < 100; i++ {

		if _, err := db.PrepareQuery(qset, 1, 2, 3); err != nil {
			t.Errorf("db.PrepareQuery err:%v", err)
			break
		}
	}

	db.PrepareClose(qset)

	// Tx
	if err := db.TxBegin(qset.Clear()); err != nil {
		t.Fatalf("db.TxBegin err:%s", err.Error())
	}

	if err := db.TxPrepare(qset.InsertTable("test_temp").InsertFields("title,content").InsertValues("(?,?)")); err != nil {
		t.Fatalf("db.TxPrepare err:%s", err.Error())
	}

	for i := 0; i < 100; i++ {

		rst, err := db.TxStmtExec(qset, fmt.Sprintf("#%d_tx_test_title", i), fmt.Sprintf("#%d_tx_test_content", i))
		if err != nil {
			t.Fatalf("db.TxStmtExec err:%s", err.Error())
		}

		_, err = rst.LastInsertId()
		if err != nil {
			t.Fatalf("rst.LastInsertId err:%s", err.Error())
		}

		_, err = rst.RowsAffected()
		if err != nil {
			t.Fatalf("rst.RowsAffected err:%s", err.Error())
		}

		// t.Logf("db.Tx lid:%d aft:%d\n", lid, aft)
	}

	if tx_rollback() {

		if err := db.TxRollBack(qset); err != nil {
			t.Fatalf("db.TxRollBack err:%s", err.Error())
		}

		t.Fatalf("Exit Info Tx.RollBack Args Is Specified")
	}

	if err := db.TxCommit(qset); err != nil {
		t.Fatalf("db.TxCommit err:%s", err.Error())
	}

	db.PrepareClose(qset)
}

func TestMysqlRollBack(t *testing.T) {

	db, err := New(Config{
		Driver:   "mysql",
		Addr:     "127.0.0.1:3306",
		User:     "root",
		Pass:     "",
		DbName:   "test",
		Protocol: "tcp",
		Params:   "charset=utf8",
	})

	if err != nil {
		t.Fatalf("Db db err:%s\n", err.Error())
	}
	defer db.Close()

	qset := NewQuerySet()

	db.ExecString(`charset utf8`)

	db.ExecString(`DROP TABLE IF EXISTS test_temp`)

	db.ExecString(`CREATE TABLE IF NOT EXISTS test_temp(id int(11) primary key auto_increment, 
		title varchar(30) not null default '', content varchar(100) not null default '',
		num float(9,2) not null default 0)engine=innodb default charset utf8`)

	// Tx
	if err := db.TxBegin(qset); err != nil {
		t.Fatalf("db.TxBegin err:%s", err.Error())
	}

	if err := db.TxPrepare(qset.InsertTable("test_temp").InsertFields("title,content").InsertValues("(?,?)")); err != nil {
		t.Fatalf("db.TxPrepare err:%s", err.Error())
	}

	var (
		lid int64
		aft int64
	)

	for i := 1; i <= 100; i++ {

		rst, err := db.TxStmtExec(qset, fmt.Sprintf("#%d_tx_test_title", i), fmt.Sprintf("#%d_tx_test_content", i))
		if err != nil {
			t.Fatalf("db.TxStmtExec err:%s", err.Error())
		}

		lid, err = rst.LastInsertId()
		if err != nil {
			t.Fatalf("rst.LastInsertId err:%s", err.Error())
		}

		aft, err = rst.RowsAffected()
		if err != nil {
			t.Fatalf("rst.RowsAffected err:%s", err.Error())
		}

		// t.Logf("New.db.Tx lid:%d aft:%d\n", lid, aft)
	}

	if err := db.TxPrepare(qset.Clear().UpdateTable("test_temp").UpdateSet("title=?,content=?").Where("id").Eq("?")); err != nil {
		t.Fatalf("db.TxPrepare err:%s", err.Error())
	}

	for i := lid - 100 + 1; i <= lid; i++ {

		rst, err := db.TxStmtExec(qset, fmt.Sprintf("#%d_tx_test_title_updated", i), fmt.Sprintf("#%d_tx_test_content_updated", i), i)
		if err != nil {
			t.Fatalf("db.TxStmtExec err:%s", err.Error())
		}

		aft, err = rst.RowsAffected()
		if err != nil {
			t.Fatalf("rst.RowsAffected err:%s", err.Error())
		}

		if aft < 1 {
			t.Fatalf("rst.RowsAffected aft:%d", aft)
		}

		// t.Logf("Update.db.Tx lid:%d aft:%d\n", i, aft)
	}

	rst, err := db.TxExec(qset.Clear().UpdateTable("test_temp").UpdateSet("num=num+?").Where("id").Eq(fmt.Sprintf("%d", lid)), fmt.Sprintf("%.2f", 10.07))
	if err != nil {
		t.Fatalf("db.TxPrepare err:%s", err.Error())
	}

	aft, err = rst.RowsAffected()
	if err != nil {
		t.Fatalf("rst.RowsAffected err:%s", err.Error())
	}
	// t.Logf("Update.db.Tx lid:%d aft:%d\n", lid, aft)

	db.TxPrepareClose(qset)

	if tx_rollback() {

		if err := db.TxRollBack(qset); err != nil {
			t.Fatalf("db.TxRollBack err:%s", err.Error())
		}

		t.Fatalf("Exit Info Tx.RollBack Args Is Specified")
	}

	if err := db.TxCommit(qset); err != nil {
		t.Fatalf("db.TxCommit err:%s", err.Error())
	}

	db.ExecString(`drop table test_temp`)
}

func do_sql_test(qneed string, q *QuerySet, t *testing.T) {

	//	pass := true
	qneed = strings.TrimSpace(qneed)

	for i := 0; i < 1000; i++ {

		if strings.TrimSpace(q.sql()) != qneed {
			//			pass = false
			t.Errorf("Sql not matched. sql:%s", q.sql())
			break
		}
	}

	//	t.Logf("pass:%v sql:%s\n", pass, qneed)
}

func tx_rollback() bool {

	for i := 1; i < len(os.Args); i++ {

		if os.Args[i] == "rollback" {
			return true
		}
	}

	return false
}
