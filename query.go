// Copyright 2016 The Sqlcl Author. All Rights Reserved.

package sqlcl

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	QINSERTTABLE  = "0INSERT INTO "
	QINSERTFIELDS = "1INSERTFIELDS"
	QINSERTVALUES = "2VALUES"
	QUPDATE       = "0UPDATE TABLE"
	QUPDATESET    = "1SET"
	QSELECT       = "0SELECT"
	QFROM         = "1FROM"
	QWHERE        = "3WHERE"
	QAND          = "4AND"
	QOR           = "4OR"
	QLIMIT        = "6LIMIT"
)

type QuerySet struct {
	Filters []string
	Set     map[string]string
}

type QScore struct {
	Score int
	Value string
}

type QScores []QScore

func (s QScores) Len() int {
	return len(s)
}

func (s QScores) Less(i, j int) bool {
	return s[i].Score < s[j].Score
}

func (s QScores) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func NewQuerySet() *QuerySet {
	return &QuerySet{Set: make(map[string]string)}
}

func (q *QuerySet) InsertTable(table string) *QuerySet {
	q.Set[QINSERTTABLE] = fmt.Sprintf(" %s `%s` ", QINSERTTABLE[1:], table)
	return q
}

func (q *QuerySet) InsertFields(fields string) *QuerySet {
	q.Set[QINSERTFIELDS] = fmt.Sprintf(" %s ", fields)
	return q
}

func (q *QuerySet) InsertValues(values string) *QuerySet {
	q.Set[QINSERTVALUES] = fmt.Sprintf(" %s %s ", QINSERTVALUES[1:], values)
	return q
}

func (q *QuerySet) UpdateTable(table string) *QuerySet {
	q.Set[QUPDATE] = fmt.Sprintf(" %s `%s` ", QUPDATE[1:], table)
	return q
}

func (q *QuerySet) UpdateSet(values string) *QuerySet {
	q.Set[QUPDATESET] = fmt.Sprintf(" %s %s ", QUPDATESET[1:], values)
	return q
}

func (q *QuerySet) Select(fields string) *QuerySet {
	q.Set[QSELECT] = fmt.Sprintf(" %s %s ", QSELECT[1:], fields)
	return q
}

func (q *QuerySet) From(table string) *QuerySet {
	q.Set[QFROM] = fmt.Sprintf(" %s `%s` ", QFROM[1:], table)
	return q
}

func (q *QuerySet) Where(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" %s %s ", QWHERE[1:], name))
	return q
}

func (q *QuerySet) And(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" %s %s ", QAND[1:], name))
	return q
}

func (q *QuerySet) Or(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" %s %s ", QOR[1:], name))
	return q
}

func (q *QuerySet) In(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" IN (%s) ", name))
	return q
}

func (q *QuerySet) Eq(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" = \"%s\" ", name))
	return q
}

func (q *QuerySet) Neq(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" != \"%s\" ", name))
	return q
}

func (q *QuerySet) Gt(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" > \"%s\" ", name))
	return q
}

func (q *QuerySet) Ge(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" >= \"%s\" ", name))
	return q
}

func (q *QuerySet) Lt(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" < \"%s\" ", name))
	return q
}

func (q *QuerySet) Le(name string) *QuerySet {

	if strings.ContainsAny(name, "=><") {
		return q
	}

	q.Filters = append(q.Filters, fmt.Sprintf(" <= \"%s\" ", name))
	return q
}

func (q *QuerySet) Limit(offset, num uint64) *QuerySet {
	q.Set[QLIMIT] = fmt.Sprintf(" %s %d,%d", QLIMIT[1:], offset, num)
	return q
}

func (q *QuerySet) Sql() string {

	var (
		sql string
		qss = QScores{}
	)

	for k, v := range q.Set {

		score, _ := strconv.Atoi(fmt.Sprintf("%d", k[0]))
		qss = append(qss, QScore{
			Score: score,
			Value: v,
		})
	}

	qss = append(qss, QScore{
		Score: 53,
		Value: strings.Join(q.Filters, " "),
	})

	sort.Sort(qss)

	for _, v := range qss {

		sql += v.Value
	}

	return sql
}
