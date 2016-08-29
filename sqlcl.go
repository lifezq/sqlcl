// Copyright 2016 The Sqlcl Author. All Rights Reserved.

package sqlcl

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	Driver   string
	Addr     string
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

	if c.Driver != "mysql" {
		return nil, fmt.Errorf("Unknow db driver:%s", c.Driver)
	}

	if len(c.Protocol) < 3 {
		c.Protocol = "tcp"
	}

	db, err := sql.Open(c.Driver, fmt.Sprintf("%s:%s@%s(%s)/%s?%s", c.User, c.Pass, c.Protocol, c.Addr, c.DbName, c.Params))
	if err != nil {
		return nil, err
	}

	return &Server{DB: db}, nil
}

func (s *Server) Close() error {
	return s.DB.Close()
}

func (s *Server) Query(q *QuerySet, args ...interface{}) (*Result, error) {

	rows, err := s.DB.Query(q.Sql(), args...)
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) QueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	rows, err := s.DB.Query(q.Sql(), args...)
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

func (s *Server) PrepareQuery(q *QuerySet, args ...interface{}) (*Result, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("No Args")
	}

	stmt, err := s.DB.Prepare(q.Sql())
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(args...)
	defer stmt.Close()
	if err != nil {
		return nil, err
	}

	return parseRows(rows)
}

func (s *Server) PrepareQueryRow(q *QuerySet, args ...interface{}) (*RowColumn, error) {

	if len(args) < 1 {
		return nil, fmt.Errorf("No Args")
	}

	stmt, err := s.DB.Prepare(q.Sql())
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query(args...)
	defer stmt.Close()
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
