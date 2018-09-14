package pgqb

import (
	"reflect"
)

type clause interface {
	toSQL(ctx *buildContext)
	collectColSources(collector colSrcMap)
	isClause()
	copyTo(cp clause)
}

type baseClause struct{}

func (baseClause) toSQL(ctx *buildContext) {}

func (baseClause) collectColSources(collector colSrcMap) {}

func (baseClause) isClause() {}

func (baseClause) copyTo(cp clause) {}

// Base class for clauses that involve a list of ColExp.
type baseColExpListClause struct {
	baseClause
	colExpList []ColExp
}

func (c *baseColExpListClause) toSQL(ctx *buildContext) {
	for i, colExp := range c.colExpList {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		colExp.toSQL(ctx)
	}
}

func (c *baseColExpListClause) toSQLWithKeyword(keyword string, ctx *buildContext) {
	if len(c.colExpList) == 0 {
		return
	}
	ctx.buf.WriteString(keyword + " ")
	c.toSQL(ctx)
	ctx.buf.WriteByte(' ')
}

func (c *baseColExpListClause) collectColSources(collector colSrcMap) {
	for _, colExp := range c.colExpList {
		colExp.collectColSources(collector)
	}
}

func (c *baseColExpListClause) addColExp(exps ... interface{}) {
	c.colExpList = append(c.colExpList, getExpList(exps)...)
}

func (c *baseColExpListClause) copyTo(cp clause) {
	var colExpList []ColExp
	copy(colExpList, c.colExpList)
	clause := baseColExpListClause{colExpList: colExpList}
	cp = &clause
}

// Select clause.
type selectClause struct {
	baseColExpListClause
}

func (c *selectClause) toSQL(ctx *buildContext) {
	c.baseColExpListClause.toSQLWithKeyword("SELECT", ctx)
}

func (c *selectClause) copyTo(cp clause) {
	clause := selectClause{}
	c.baseColExpListClause.copyTo(&clause.baseColExpListClause)
	cp = &clause
}

// Group by clause.
type groupByClause struct {
	baseColExpListClause
}

func (c *groupByClause) toSQL(ctx *buildContext) {
	c.baseColExpListClause.toSQLWithKeyword("GROUP BY", ctx)
}

func (c *groupByClause) copyTo(cp clause) {
	clause := groupByClause{}
	c.baseColExpListClause.copyTo(&clause.baseColExpListClause)
	cp = &clause
}

// Order by clause.
type orderByClause struct {
	baseColExpListClause
}

func (c *orderByClause) toSQL(ctx *buildContext) {
	c.baseColExpListClause.toSQLWithKeyword("ORDER BY", ctx)
}

func (c *orderByClause) copyTo(cp clause) {
	clause := orderByClause{}
	c.baseColExpListClause.copyTo(&clause.baseColExpListClause)
	cp = &clause
}

func (c *orderByClause) addColExp(exps ... interface{}) {
	order := getExpList(exps)
	for i, colExp := range order {
		if _, ok := colExp.(*OrderExpNode); !ok {
			order[i] = Asc(colExp)
		}
	}
	c.colExpList = append(c.colExpList, order...)
}

// Base class for all clauses that involve a predicate.
type basePredicateClause struct {
	baseClause
	predicate ColExp
}

func (c *basePredicateClause) toSQL(ctx *buildContext) {
	// Trust sub-classes only call this method when predicate is not nil.
	c.predicate.toSQL(ctx)
}

func (c *basePredicateClause) toSQLWithKeyword(keyword string, ctx *buildContext) {
	if isNull(c.predicate) {
		return
	}
	ctx.buf.WriteString(keyword + " ")
	c.toSQL(ctx)
	ctx.buf.WriteByte(' ')
}

func (c *basePredicateClause) collectColSources(collector colSrcMap) {
	c.predicate.collectColSources(collector)
}

func (c *basePredicateClause) addPredicate(predicates ... interface{}) {
	var tmp = make([]interface{}, 0, len(predicates)+1)
	if c.predicate != nil {
		tmp = append(tmp, c.predicate)
	}
	tmp = append(tmp, predicates...)
	if len(predicates) > 0 {
		c.predicate = And(tmp...)
	}
}

func (c basePredicateClause) copyTo(cp clause) {
	cp = &c
}

// Where clause.
type whereClause struct {
	basePredicateClause
}

func (c *whereClause) toSQL(ctx *buildContext) {
	c.basePredicateClause.toSQLWithKeyword("WHERE", ctx)
}

func (c *whereClause) copyTo(cp clause) {
	res := &whereClause{}
	c.basePredicateClause.copyTo(&res.basePredicateClause)
	cp = res
}

// Having clause.
type havingClause struct {
	basePredicateClause
}

func (c *havingClause) toSQL(ctx *buildContext) {
	c.basePredicateClause.toSQLWithKeyword("HAVING", ctx)
}

func (c *havingClause) copyTo(cp clause) {
	res := &whereClause{}
	c.basePredicateClause.copyTo(&res.basePredicateClause)
	cp = res
}

// From clause.
type fromClause struct {
	tbExpList []TableExp
}

func (c *fromClause) toSQL(ctx *buildContext) {
	if len(c.tbExpList) == 0 {
		return
	}
	ctx.buf.WriteString("FROM ")
	for i, tbExp := range c.tbExpList {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		tbExp.toSQL(ctx)
	}
	ctx.buf.WriteByte(' ')
}

func (c *fromClause) collectColSources(collector colSrcMap) {
	for _, tbExp := range c.tbExpList {
		tbExp.collectColSources(collector)
	}
}

func (c *fromClause) addTableExp(exps ... TableExp) {
	c.tbExpList = append(c.tbExpList, exps...)
}

func (c *fromClause) fillMissingColSrc(colSrcMap colSrcMap) {
	fromColSrcMap := collectColSourcesFromClauses(c)
	difference := colSrcMap.Subtract(fromColSrcMap)
	for _, colSrc := range difference {
		if tbExp, ok := colSrc.(TableExp); ok {
			c.addTableExp(tbExp)
		}
	}
}

func (c *fromClause) copyTo(cp clause) {
	var tbExpList []TableExp
	copy(tbExpList, c.tbExpList)
	clause := fromClause{tbExpList: tbExpList}
	cp = &clause
}

func (fromClause) isClause() {}

// Set clause.
type setClause struct {
	// Column names -> ColExp
	setExpMap map[string]ColExp
}

func (c *setClause) toSQL(ctx *buildContext) {
	if len(c.setExpMap) == 0 {
		return
	}
	ctx.buf.WriteString("SET ")
	i := 0
	for cname, exp := range c.setExpMap {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		ctx.buf.WriteString(ctx.QuoteObject(cname) + " = ")
		exp.toSQL(ctx)
		i++
	}
	ctx.buf.WriteByte(' ')
}

func (c *setClause) collectColSources(collector colSrcMap) {
	for _, exp := range c.setExpMap {
		exp.collectColSources(collector)
	}
}

func (c *setClause) copyTo(cp clause) {
	setExpMap := make(map[string]ColExp, len(c.setExpMap))
	for k, v := range c.setExpMap {
		setExpMap[k] = v
	}
	clause := setClause{setExpMap: setExpMap}
	cp = &clause
}

func (setClause) isClause() {}

// Insert clause.
type insertClause struct {
	table      *TableNode
	columns    []string // List of column names.
	valuesList [][]ColExp
}

func (c *insertClause) toSQL(ctx *buildContext) {
	if len(c.columns) == 0 || c.table == nil || len(c.valuesList) == 0 {
		return
	}
	ctx.buf.WriteString("INSERT INTO ")
	c.table.toSQL(ctx)
	ctx.buf.WriteString(" (")
	for i, col := range c.columns {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		ctx.buf.WriteString(ctx.QuoteObject(col))
	}
	ctx.buf.WriteString(") VALUES ")
	for i, valList := range c.valuesList {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		ctx.buf.WriteByte('(')
		for j, val := range valList {
			if j > 0 {
				ctx.buf.WriteString(", ")
			}
			val.toSQL(ctx)
		}
		ctx.buf.WriteByte(')')
	}
	ctx.buf.WriteByte(' ')
}

func (c *insertClause) collectColSources(collector colSrcMap) {
	for _, valList := range c.valuesList {
		for _, val := range valList {
			val.collectColSources(collector)
		}
	}
}

func (c *insertClause) copyTo(cp clause) {
	var columns []string
	var valuesList [][]ColExp
	copy(columns, c.columns)
	copy(valuesList, c.valuesList)
	clause := insertClause{table: c.table, columns: columns, valuesList: valuesList}
	cp = &clause
}

func (insertClause) isClause() {}

// TODO: GROUP BY, HAVING, RETURNING clauses

func collectColSourcesFromClauses(clauses ... clause) colSrcMap {
	res := colSrcMap{}
	for _, clause := range clauses {
		if !isNull(clause) {
			clause.collectColSources(res)
		}
	}
	return res
}

func clauseToSQL(clause clause, ctx *buildContext) {
	if !isNull(clause) {
		clause.toSQL(ctx)
	}
}

func isNull(c interface{}) bool {
	return c == nil || !reflect.ValueOf(c).Elem().IsValid()
}
