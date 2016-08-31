// Copyright 2016 The Sqlcl Author. All Rights Reserved.

package sqlcl

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

type Config struct {
	Driver   string // mysql/sqlite3
	Addr     string // mysql:127.0.0.1:3306/sqlite3:/tmp/foo.db or :memory:
	User     string
	Pass     string
	DbName   string
	Protocol string
	Params   string
}

type Server struct {
	DB *sql.DB
}

type RowColumn map[string]string

type Result struct {
	Data []RowColumn
}

func New(c Config) (*Server, error) {

	dsn := ""

	switch c.Driver {
	case "mysql":
		if len(c.Protocol) < 3 {
			c.Protocol = "tcp"
		}
		dsn = fmt.Sprintf("%s:%s@%s(%s)/%s?%s", c.User, c.Pass, c.Protocol, c.Addr, c.DbName, c.Params)

	case "sqlite3":
		dsn = c.Addr

	default:

		return nil, fmt.Errorf("Unknow db driver:%s", c.Driver)
	}

	db, err := sql.Open(c.Driver, dsn)
	if err != nil {
		return nil, err
	}

	return &Server{DB: db}, nil
}

func (s *Server) Close() error {
	return s.DB.Close()
}

func (s *Server) Query(q *QuerySet, args ...interface{}) (*Result, error) {

	rows, err := s.DB.Query(q.Sql(false), args...)
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) QueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	rows, err := s.DB.Query(q.Sql(false), args...)
	if err != nil {
		return nil, err
	}

	rst, err := parseRows(rows)
	if err != nil {
		return nil, err
	}

	if len(rst.Data) < 1 {
		return nil, fmt.Errorf("Not Found")
	}

	return &rst.Data[0], err
}

func (s *Server) Prepare(q *QuerySet) error {

	var err error
	q.Stmt, err = s.DB.Prepare(q.Sql(true))
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) PrepareQuery(q *QuerySet, args ...interface{}) (*Result, error) {

	if len(args) < 1 {
		return nil, fmt.Errorf("No Args")
	}

	if q.Stmt == nil {

		var err error
		q.Stmt, err = s.DB.Prepare(q.Sql(true))
		if err != nil {
			return nil, err
		}
	}

	rows, err := q.Stmt.Query(args...)
	// defer q.Stmt.Close()
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) PrepareQueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	if len(args) < 1 {
		return nil, fmt.Errorf("No Args")
	}

	if q.Stmt == nil {

		var err error
		q.Stmt, err = s.DB.Prepare(q.Sql(true))
		if err != nil {
			return nil, err
		}
	}

	rows, err := q.Stmt.Query(args...)
	//	defer q.Stmt.Close()
	if err != nil {
		return nil, err
	}

	rst, err := parseRows(rows)
	if err != nil {
		return nil, err
	}

	if len(rst.Data) < 1 {
		return nil, fmt.Errorf("Not Found")
	}

	return &rst.Data[0], nil
}

func (s *Server) PrepareExec(q *QuerySet, args ...interface{}) (sql.Result, error) {

	if len(args) < 1 {
		return nil, fmt.Errorf("No Args")
	}

	if q.Stmt == nil {

		var err error
		q.Stmt, err = s.DB.Prepare(q.Sql(true))
		if err != nil {
			return nil, err
		}
	}

	return q.Stmt.Exec(args...)
}

func (s *Server) PrepareClose(q *QuerySet) {

	if q.Stmt != nil {
		q.Stmt.Close()
	}
}

func (s *Server) Exec(q string) (sql.Result, error) {
	return s.DB.Exec(q)
}

func parseRows(rows *sql.Rows) (*Result, error) {

	columes, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var (
		value    = ""
		rst      = &Result{}
		values   = make([]sql.RawBytes, len(columes))
		row_dest = make([]interface{}, len(columes))
	)

	for i, _ := range values {
		row_dest[i] = &values[i]
	}

	for rows.Next() {

		err := rows.Scan(row_dest...)
		if err != nil {
			continue
		}

		rdt := RowColumn{}

		for i, col := range values {

			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}

			rdt[columes[i]] = value
		}

		rst.Data = append(rst.Data, rdt)
	}

	return rst, nil
}
