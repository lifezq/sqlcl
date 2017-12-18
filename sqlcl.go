// Copyright 2016 The Sqlcl Author. All Rights Reserved.
//
// -----------------------------------------------------

package sqlcl

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

type Config struct {
	Driver      string // mysql/sqlite3
	Addr        string // mysql:127.0.0.1:3306/sqlite3:/tmp/foo.db or :memory:
	User        string
	Pass        string
	DbName      string
	Protocol    string
	Params      string
	MaxLifetime time.Duration
	MaxIdleConn int
	MaxConn     int
}

type Server struct {
	db *sql.DB
}

type RowColumn map[string]string

type Result struct {
	Data []*RowColumn
}

func (r *RowColumn) Get(k string) string {

	if v, ok := (*r)[k]; ok {
		return v
	}
	return ""
}

func (r *RowColumn) Int8(k string) int8 {
	return int8(r.Int(k))
}

func (r *RowColumn) Uint8(k string) uint8 {
	return uint8(r.Int8(k))
}

func (r *RowColumn) Int16(k string) int16 {
	return int16(r.Int(k))
}

func (r *RowColumn) Uint16(k string) uint16 {
	return uint16(r.Int16(k))
}

func (r *RowColumn) Int(k string) int {
	i, _ := strconv.Atoi(r.Get(k))
	return i
}

func (r *RowColumn) Int32(k string) int32 {
	i, _ := strconv.ParseInt(r.Get(k), 10, 32)
	return int32(i)
}

func (r *RowColumn) Uint32(k string) uint32 {
	i, _ := strconv.ParseInt(r.Get(k), 10, 64)
	return uint32(i)
}

func (r *RowColumn) Int64(k string) int64 {
	i, _ := strconv.ParseInt(r.Get(k), 10, 64)
	return i
}

func (r *RowColumn) Uint64(k string) uint64 {
	i, _ := strconv.ParseUint(r.Get(k), 10, 64)
	return i
}

func (r *RowColumn) Float32(k string) float32 {
	i, _ := strconv.ParseFloat(r.Get(k), 32)
	return float32(i)
}

func (r *RowColumn) Float64(k string) float64 {
	i, _ := strconv.ParseFloat(r.Get(k), 64)
	return i
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

	db_link, err := sql.Open(c.Driver, dsn)
	if err != nil {
		return nil, err
	}

	db_link.SetConnMaxLifetime(c.MaxLifetime)
	db_link.SetMaxIdleConns(c.MaxIdleConn)
	db_link.SetMaxOpenConns(c.MaxConn)

	return &Server{db: db_link}, nil
}

func (s *Server) Close() error {
	return s.db.Close()
}

func (s *Server) Ping() error {
	return s.db.Ping()
}

func (s *Server) QueryString(sql string) (*Result, error) {

	rows, err := s.db.Query(sql)
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) Query(q *QuerySet, args ...interface{}) (*Result, error) {

	rows, err := s.db.Query(q.sql(), args...)
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) QueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	rows, err := s.db.Query(q.sql(), args...)
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

	return rst.Data[0], err
}

func (s *Server) Prepare(q *QuerySet) error {

	var err error
	q.stmt, err = s.db.Prepare(q.sql())
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

		var err error
		q.stmt, err = s.db.Prepare(q.sql())
		if err != nil {
			return nil, err
		}

	}

	rows, err := q.stmt.Query(args...)
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

		var err error
		q.stmt, err = s.db.Prepare(q.sql())
		if err != nil {
			return nil, err
		}

	}

	rows, err := q.stmt.Query(args...)
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

	return rst.Data[0], nil
}

func (s *Server) PrepareExec(q *QuerySet, args ...interface{}) (sql.Result, error) {

	if len(args) < 1 {
		return nil, fmt.Errorf("No Args")
	}

	if q.stmt == nil {

		var err error
		q.stmt, err = s.db.Prepare(q.sql())
		if err != nil {
			return nil, err
		}

	}

	return q.stmt.Exec(args...)
}

func (s *Server) PrepareClose(q *QuerySet) {

	if q.stmt != nil {
		q.stmt.Close()
		q.stmt = nil
	}
}

func (s *Server) Exec(q *QuerySet) (sql.Result, error) {
	return s.db.Exec(q.sql())
}

func (s *Server) ExecString(sql string) (sql.Result, error) {
	return s.db.Exec(sql)
}

func (s *Server) TxBegin(q *QuerySet) error {

	var err error
	q.tx, err = s.db.Begin()
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

	var err error
	q.stmt, err = q.tx.Prepare(q.sql())

	return err
}

func (s *Server) TxPrepareExec(q *QuerySet, args ...interface{}) (sql.Result, error) {

	if q.tx == nil {
		return nil, fmt.Errorf("Client Error")
	}

	if q.stmt == nil {

		var err error
		q.stmt, err = q.tx.Prepare(q.sql())
		if err != nil {
			return nil, err
		}
	}

	return q.tx.Stmt(q.stmt).Exec(args...)
}

func (s *Server) TxPrepareClose(q *QuerySet) error {

	if q.tx == nil || q.stmt == nil {
		return fmt.Errorf("Client Error")
	}

	err := q.stmt.Close()
	q.stmt = nil
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

	return rst.Data[0], err
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

	return rst.Data[0], err
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

		rdt := &RowColumn{}

		for i, col := range values {

			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}

			(*rdt)[columes[i]] = value
		}

		rst.Data = append(rst.Data, rdt)
	}

	return rst, nil
}
