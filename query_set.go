// Copyright 2016 The Sqlcl Author. All Rights Reserved.
//
// -----------------------------------------------------

package sqlcl

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	QINSERTTABLE  = "0INSERT INTO "
	QINSERTFIELDS = "1FIELDS"
	QINSERTVALUES = "2VALUES"
	QUPDATE       = "0UPDATE "
	QUPDATESET    = "1SET"
	QDELETE       = "0DELETE"
	QSELECT       = "0SELECT"
	QFROM         = "1FROM"
	QWHERE        = "3WHERE"
	QAND          = "4AND"
	QOR           = "4OR"
	QGROUPBY      = "5GROUP BY"
	QHAVING       = "6HAVING"
	QORDERBY      = "7ORDER BY"
	QLIMIT        = "8LIMIT"
)

type QuerySet struct {
	stmt    *sql.Stmt
	tx      *sql.Tx
	filters []string
	set     map[string]string
}

func NewQuerySet() *QuerySet {
	return &QuerySet{
		filters: []string{},
		set:     make(map[string]string),
	}
}

func (q *QuerySet) Clear() *QuerySet {
	*q = *NewQuerySet()
	return q
}

func (q *QuerySet) InsertTable(table string) *QuerySet {
	q.set[QINSERTTABLE] = fmt.Sprintf(" %s `%s` ", QINSERTTABLE[1:], table)
	return q
}

func (q *QuerySet) InsertFields(fields string) *QuerySet {
	q.set[QINSERTFIELDS] = fmt.Sprintf(" (%s) ", fields)
	return q
}

func (q *QuerySet) InsertValues(values string) *QuerySet {
	q.set[QINSERTVALUES] = fmt.Sprintf(" %s %s ", QINSERTVALUES[1:], values)
	return q
}

func (q *QuerySet) UpdateTable(table string) *QuerySet {
	q.set[QUPDATE] = fmt.Sprintf(" %s `%s` ", QUPDATE[1:], table)
	return q
}

func (q *QuerySet) UpdateSet(values string) *QuerySet {
	q.set[QUPDATESET] = fmt.Sprintf(" %s %s ", QUPDATESET[1:], values)
	return q
}

func (q *QuerySet) Delete() *QuerySet {
	q.set[QDELETE] = fmt.Sprintf(" %s ", QDELETE[1:])
	return q
}

func (q *QuerySet) Select(fields string) *QuerySet {
	q.set[QSELECT] = fmt.Sprintf(" %s %s ", QSELECT[1:], fields)
	return q
}

func (q *QuerySet) From(table string) *QuerySet {
	q.set[QFROM] = fmt.Sprintf(" %s `%s` ", QFROM[1:], table)
	return q
}

func (q *QuerySet) Where(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" %s %s ", QWHERE[1:], name))
	return q
}

func (q *QuerySet) And(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" %s %s ", QAND[1:], name))
	return q
}

func (q *QuerySet) Or(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" %s %s ", QOR[1:], name))
	return q
}

func (q *QuerySet) In(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" IN (%s) ", name))
	return q
}

func (q *QuerySet) Eq(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" = \"%s\" ", name))
	return q
}

func (q *QuerySet) Neq(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" != \"%s\" ", name))
	return q
}

func (q *QuerySet) Gt(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" > \"%s\" ", name))
	return q
}

func (q *QuerySet) Ge(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" >= \"%s\" ", name))
	return q
}

func (q *QuerySet) Lt(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" < \"%s\" ", name))
	return q
}

func (q *QuerySet) Le(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" <= \"%s\" ", name))
	return q
}

func (q *QuerySet) Like(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.filters = append(q.filters, fmt.Sprintf(" LIKE \"%s\" ", name))
	return q
}

func (q *QuerySet) GroupBy(name string) *QuerySet {
	q.set[QGROUPBY] = fmt.Sprintf(" %s %s", QGROUPBY[1:], name)
	return q
}

func (q *QuerySet) Having(name string) *QuerySet {
	q.set[QHAVING] = fmt.Sprintf(" %s %s", QHAVING[1:], name)
	return q
}

func (q *QuerySet) OrderBy(name string) *QuerySet {
	q.set[QORDERBY] = fmt.Sprintf(" %s %s", QORDERBY[1:], name)
	return q
}

func (q *QuerySet) Limit(offset, num uint64) *QuerySet {
	q.set[QLIMIT] = fmt.Sprintf(" %s %d,%d", QLIMIT[1:], offset, num)
	return q
}

func (q *QuerySet) LimitString(limit string) *QuerySet {
	q.set[QLIMIT] = fmt.Sprintf(" %s %s", QLIMIT[1:], limit)
	return q
}

func (q *QuerySet) sql() string {

	var (
		sql     string
		qss     = qscores{}
		filters = strings.Replace(strings.Join(q.filters, " "), "\"?\"", "?", -1)
	)

	for k, v := range q.set {

		score, _ := strconv.Atoi(fmt.Sprintf("%d", k[0]))
		qss = append(qss, qscore{
			score: score,
			value: v,
		})
	}

	qss = append(qss, qscore{
		score: 0x35,
		value: filters,
	})

	sort.Sort(qss)

	for _, v := range qss {

		sql += v.value
	}

	return sql
}

type qscore struct {
	score int
	value string
}

type qscores []qscore

func (s qscores) Len() int {
	return len(s)
}

func (s qscores) Less(i, j int) bool {
	return s[i].score < s[j].score
}

func (s qscores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
