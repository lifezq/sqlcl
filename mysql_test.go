// Copyright 2016 The Author. All Rights Reserved.

package sqlcl

import (
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
		Pass:     "123456",
		DbName:   "test",
		Protocol: "tcp",
		Params:   "charset=utf8",
	})

	if err != nil {
		t.Fatalf("Db db err:%s\n", err.Error())
	}
	defer db.Close()

	qset := NewQuerySet()

	for i := 0; i < 100; i++ {

		qset.Clear().InsertTable("test_temp").InsertFields("title,content").InsertValues("('title_01','value_01'),('title_02','value_02'),('title_03','content_03')")
		if _, err = db.Exec(qset.sql()); err != nil {
			t.Errorf("db.Exec err:%v", err)
			break
		}
	}

	// t.Logf("sql:%s\n", qset.sql())

	qset.Clear().Select("*").From("test_temp").Where("id").In("?,?,?")

	// t.Logf("sql:%s\n", qset.sql())

	for i := 0; i < 100; i++ {

		if _, err := db.PrepareQuery(qset, 1, 2, 3); err != nil {
			t.Errorf("db.PrepareQuery rst:%v err:%v", err)
			break
		}
	}

	db.PrepareClose(qset)
}

func do_sql_test(qneed string, q *QuerySet, t *testing.T) {

	pass := true
	qneed = strings.TrimSpace(qneed)

	for i := 0; i < 1000; i++ {

		if strings.TrimSpace(q.sql()) != qneed {
			pass = false
			t.Errorf("Sql not matched. sql:%s", q.sql())
			break
		}
	}

	t.Logf("pass:%v sql:%s\n", pass, qneed)
}
