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

	rows, err := s.DB.Query(q.sql(), args...)
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) QueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	rows, err := s.DB.Query(q.sql(), args...)
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

	q.strip = true

	var err error
	q.stmt, err = s.DB.Prepare(q.sql())
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) PrepareQuery(q *QuerySet, args ...interface{}) (*Result, error) {

	if len(args) < 1 {
		return nil, fmt.Errorf("No Args")
	}

	if q.stmt == nil {

		q.strip = true

		var err error
		q.stmt, err = s.DB.Prepare(q.sql())
		if err != nil {
			return nil, err
		}

	}

	rows, err := q.stmt.Query(args...)
	// defer q.stmt.Close()
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) PrepareQueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	if len(args) < 1 {
		return nil, fmt.Errorf("No Args")
	}

	if q.stmt == nil {

		q.strip = true

		var err error
		q.stmt, err = s.DB.Prepare(q.sql())
		if err != nil {
			return nil, err
		}

	}

	rows, err := q.stmt.Query(args...)
	//	defer q.stmt.Close()
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

	if q.stmt == nil {

		q.strip = true

		var err error
		q.stmt, err = s.DB.Prepare(q.sql())
		if err != nil {
			return nil, err
		}

	}

	return q.stmt.Exec(args...)
}

func (s *Server) PrepareClose(q *QuerySet) {

	if q.stmt != nil {
		q.stmt.Close()
	}
}

func (s *Server) Exec(q string) (sql.Result, error) {
	return s.DB.Exec(q)
}

func (s *Server) TxBegin(q *QuerySet) error {

	var err error
	q.tx, err = s.DB.Begin()
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) TxCommit(q *QuerySet) error {

	if q.tx == nil {
		return fmt.Errorf("Client Error")
	}

	return q.tx.Commit()
}

func (s *Server) TxExec(q *QuerySet, args ...interface{}) (sql.Result, error) {

	if q.tx == nil {
		return nil, fmt.Errorf("Client Error")
	}

	return q.tx.Exec(q.sql(), args...)
}

func (s *Server) TxPrepare(q *QuerySet) error {

	if q.tx == nil {
		return fmt.Errorf("Client Error")
	}

	q.strip = true

	var err error
	q.stmt, err = q.tx.Prepare(q.sql())

	return err
}

func (s *Server) TxQuery(q *QuerySet, args ...interface{}) (*Result, error) {

	if q.tx == nil {
		return nil, fmt.Errorf("Client Error")
	}

	rows, err := q.tx.Query(q.sql(), args...)
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) TxQueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	if q.tx == nil {
		return nil, fmt.Errorf("Client Error")
	}

	rows, err := q.tx.Query(q.sql(), args...)

	rst, err := parseRows(rows)
	if err != nil {
		return nil, err
	}

	if len(rst.Data) < 1 {
		return nil, fmt.Errorf("Not Found")
	}

	return &rst.Data[0], err
}

func (s *Server) TxRollBack(q *QuerySet) error {

	if q.tx == nil {
		return fmt.Errorf("Client Error")
	}

	return q.tx.Rollback()
}

func (s *Server) TxStmtQuery(q *QuerySet, args ...interface{}) (*Result, error) {

	if q.tx == nil || q.stmt == nil {
		return nil, fmt.Errorf("Client Error")
	}

	rows, err := q.tx.Stmt(q.stmt).Query(args...)
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) TxStmtQueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	if q.tx == nil || q.stmt == nil {
		return nil, fmt.Errorf("Client Error")
	}

	rows, err := q.tx.Stmt(q.stmt).Query(args...)
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

func (s *Server) TxStmtExec(q *QuerySet, args ...interface{}) (sql.Result, error) {

	if q.tx == nil || q.stmt == nil {
		return nil, fmt.Errorf("Client Error")
	}

	return q.tx.Stmt(q.stmt).Exec(args...)
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
