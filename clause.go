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

// newSelect clause.
type selectClause struct {
	colExpList []ColExp
}

func (c *selectClause) toSQL(ctx *buildContext) {
	if len(c.colExpList) == 0 {
		return
	}
	ctx.buf.WriteString("SELECT ")
	for i, colExp := range c.colExpList {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		colExp.toSQL(ctx)
	}
	ctx.buf.WriteByte(' ')
}

func (c *selectClause) collectColSources(collector colSrcMap) {
	for _, colExp := range c.colExpList {
		colExp.collectColSources(collector)
	}
}

func (c *selectClause) addColExp(exps ... interface{}) {
	c.colExpList = append(c.colExpList, getExpList(exps)...)
}

func (c *selectClause) copyTo(cp clause) {
	var colExpList []ColExp
	copy(colExpList, c.colExpList)
	clause := selectClause{colExpList: colExpList}
	cp = &clause
}

func (selectClause) isClause() {}

// Where clause.
type whereClause struct {
	predicate ColExp
}

func (c *whereClause) toSQL(ctx *buildContext) {
	if isNull(c.predicate) {
		return
	}
	ctx.buf.WriteString("WHERE ")
	c.predicate.toSQL(ctx)
	ctx.buf.WriteByte(' ')
}

func (c *whereClause) collectColSources(collector colSrcMap) {
	c.predicate.collectColSources(collector)
}

func (c *whereClause) addPredicate(predicates ... interface{}) {
	var tmp = make([]interface{}, 0, len(predicates)+1)
	if c.predicate != nil {
		tmp = append(tmp, c.predicate)
	}
	tmp = append(tmp, predicates...)
	if len(tmp) > 1 {
		c.predicate = And(tmp...)
	}
}

func (c whereClause) copyTo(cp clause) {
	cp = &c
}

func (whereClause) isClause() {}

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

// INSERT clause.
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
