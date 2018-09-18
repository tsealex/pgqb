package pgqb

import (
	"reflect"
)

type clause interface {
	toSQL(ctx *buildContext)
	collectColSources(collector colSrcMap)
	isClause()
	deepcopy() clause
}

type baseClause struct{}

func (baseClause) toSQL(ctx *buildContext) {}

func (baseClause) collectColSources(collector colSrcMap) {}

func (baseClause) isClause() {}

func (baseClause) deepcopy() clause {
	return nil
}

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

func (c *baseColExpListClause) deepcopy() clause {
	var colExpList = make([]ColExp, len(c.colExpList))
	copy(colExpList, c.colExpList)
	return &baseColExpListClause{colExpList: colExpList}
}

// Select clause.
type selectClause struct {
	baseColExpListClause
}

func (c *selectClause) toSQL(ctx *buildContext) {
	c.baseColExpListClause.toSQLWithKeyword("SELECT", ctx)
}

func (c *selectClause) deepcopy() clause {
	var baseColExpListClause = c.baseColExpListClause.deepcopy().(*baseColExpListClause)
	return &selectClause{baseColExpListClause: *baseColExpListClause}
}

// Returning clause.
type returningClause struct {
	baseColExpListClause
}

func (c *returningClause) toSQL(ctx *buildContext) {
	c.baseColExpListClause.toSQLWithKeyword("RETURNING", ctx)
}

func (c *returningClause) deepcopy() clause {
	var baseColExpListClause = c.baseColExpListClause.deepcopy().(*baseColExpListClause)
	return &returningClause{baseColExpListClause: *baseColExpListClause}
}

// Group by clause.
type groupByClause struct {
	baseColExpListClause
}

func (c *groupByClause) toSQL(ctx *buildContext) {
	c.baseColExpListClause.toSQLWithKeyword("GROUP BY", ctx)
}

func (c *groupByClause) deepcopy() clause {
	var baseColExpListClause = c.baseColExpListClause.deepcopy().(*baseColExpListClause)
	return &groupByClause{baseColExpListClause: *baseColExpListClause}
}

// Order by clause.
type orderByClause struct {
	baseColExpListClause
}

func (c *orderByClause) toSQL(ctx *buildContext) {
	c.baseColExpListClause.toSQLWithKeyword("ORDER BY", ctx)
}

func (c *orderByClause) deepcopy() clause {
	var baseColExpListClause = c.baseColExpListClause.deepcopy().(*baseColExpListClause)
	return &orderByClause{baseColExpListClause: *baseColExpListClause}
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

// Default values clause.
type defaultValuesClause struct {
	baseColExpListClause
}

func (c *defaultValuesClause) toSQL(ctx *buildContext) {
	c.baseColExpListClause.toSQLWithKeyword("DEFAULT VALUES", ctx)
}

func (c *defaultValuesClause) deepcopy() clause {
	var baseColExpListClause = c.baseColExpListClause.deepcopy().(*baseColExpListClause)
	return &defaultValuesClause{baseColExpListClause: *baseColExpListClause}
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

func (c basePredicateClause) deepcopy() clause {
	return &c
}

// Where clause.
type whereClause struct {
	basePredicateClause
}

func (c *whereClause) toSQL(ctx *buildContext) {
	c.basePredicateClause.toSQLWithKeyword("WHERE", ctx)
}

func (c *whereClause) deepcopy() clause {
	var basePredicateClause = c.basePredicateClause.deepcopy().(*basePredicateClause)
	return &whereClause{basePredicateClause: *basePredicateClause}
}

// Having clause.
type havingClause struct {
	basePredicateClause
}

func (c *havingClause) toSQL(ctx *buildContext) {
	c.basePredicateClause.toSQLWithKeyword("HAVING", ctx)
}

func (c *havingClause) deepcopy() clause {
	var basePredicateClause = c.basePredicateClause.deepcopy().(*basePredicateClause)
	return &havingClause{basePredicateClause: *basePredicateClause}
}

// Base clause that involves a list of table expressions.
type baseTbExpListClause struct {
	baseClause
	tbExpList []TableExp
}

func (c *baseTbExpListClause) toSQL(ctx *buildContext) {
	for i, tbExp := range c.tbExpList {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		tbExp.toSQL(ctx)
	}
}

func (c *baseTbExpListClause) toSQLWithKeyword(keyword string, ctx *buildContext) {
	if len(c.tbExpList) == 0 {
		return
	}
	ctx.buf.WriteString(keyword + " ")
	c.toSQL(ctx)
	ctx.buf.WriteByte(' ')
}

func (c *baseTbExpListClause) collectColSources(collector colSrcMap) {
	for _, tbExp := range c.tbExpList {
		tbExp.collectColSources(collector)
	}
}

func (c *baseTbExpListClause) addTableExp(exps ... TableExp) {
	c.tbExpList = append(c.tbExpList, exps...)
}

func (c *baseTbExpListClause) fillMissingColSrc(colSrcMap colSrcMap) {
	fromColSrcMap := collectColSourcesFromClauses(c)
	difference := colSrcMap.Subtract(fromColSrcMap)
	for _, colSrc := range difference {
		if tbExp, ok := colSrc.(TableExp); ok {
			c.addTableExp(tbExp)
		}
	}
}

func (c *baseTbExpListClause) deepcopy() clause {
	var tbExpList = make([]TableExp, len(c.tbExpList))
	copy(tbExpList, c.tbExpList)
	return &baseTbExpListClause{tbExpList: tbExpList}
}

// From clause.
type fromClause struct {
	baseTbExpListClause
}

func (c *fromClause) toSQL(ctx *buildContext) {
	c.baseTbExpListClause.toSQLWithKeyword("FROM", ctx)
}

func (c *fromClause) deepcopy() clause {
	var baseTbExpListClause = *c.baseTbExpListClause.deepcopy().(*baseTbExpListClause)
	return &fromClause{baseTbExpListClause: baseTbExpListClause}
}

// From clause.
type usingClause struct {
	baseTbExpListClause
}

func (c *usingClause) toSQL(ctx *buildContext) {
	c.baseTbExpListClause.toSQLWithKeyword("USING", ctx)
}

func (c *usingClause) deepcopy() clause {
	var baseTbExpListClause = *c.baseTbExpListClause.deepcopy().(*baseTbExpListClause)
	return &usingClause{baseTbExpListClause: baseTbExpListClause}
}

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

func (c *setClause) deepcopy() clause {
	setExpMap := make(map[string]ColExp, len(c.setExpMap))
	for k, v := range c.setExpMap {
		setExpMap[k] = v
	}
	return &setClause{setExpMap: setExpMap}
}

func (setClause) isClause() {}

// Conflict clause.
type conflictClause struct {
	*setClause
	cols []*ColumnNode
}

func (c *conflictClause) toSQL(ctx *buildContext) {
	origState := ctx.setState(buildContextStateNoColumnSource)
	ctx.buf.WriteString("ON CONFLICT (")
	for i, col := range c.cols {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		col.toSQL(ctx)
	}
	ctx.buf.WriteString(") ")
	ctx.setState(origState)
	if isNull(c.setClause) {
		ctx.buf.WriteString("DO NOTHING")
	} else {
		ctx.buf.WriteString("DO UPDATE ")
		c.setClause.toSQL(ctx)
	}
}

func (c *conflictClause) deepcopy() clause {
	return &conflictClause{setClause: c.setClause.deepcopy().(*setClause)}
}

// Insert clause.
type insertClause struct {
	table   *TableNode
	columns []*ColumnNode
}

func (c *insertClause) toSQL(ctx *buildContext) {
	origState := ctx.setState(buildContextStateNoColumnSource)
	ctx.buf.WriteString("INSERT INTO ")
	c.table.toSQL(ctx)
	if len(c.columns) > 0 {
		ctx.buf.WriteString(" (")
		for i, col := range c.columns {
			if i > 0 {
				ctx.buf.WriteString(", ")
			}
			col.toSQL(ctx)
		}
		ctx.buf.WriteByte(')')
	}
	ctx.buf.WriteByte(' ')
	ctx.setState(origState)
}

func (c *insertClause) collectColSources(collector colSrcMap) {}

func (c *insertClause) deepcopy() clause {
	var columns = make([]*ColumnNode, len(c.columns))
	copy(columns, c.columns)
	return &insertClause{table: c.table, columns: columns}
}

func (insertClause) isClause() {}

// Values clause.
type valueSourceClause interface {
	clause
	isValueSource()
}

type valuesClause struct {
	valuesList [][]ColExp
}

func (c *valuesClause) toSQL(ctx *buildContext) {
	ctx.buf.WriteString("VALUES ")
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

func (c *valuesClause) collectColSources(collector colSrcMap) {
	for _, valList := range c.valuesList {
		for _, val := range valList {
			val.collectColSources(collector)
		}
	}
}

func (c *valuesClause) deepcopy() clause {
	var valuesList = make([][]ColExp, len(c.valuesList))
	copy(valuesList, c.valuesList)
	return &valuesClause{valuesList: valuesList}
}

func (valuesClause) isClause() {}

func (valuesClause) isValueSource() {}

// Subquery clause.
type subqueryClause struct {
	selectStmt *SelectStmt
}

func (c *subqueryClause) toSQL(ctx *buildContext) {
	c.selectStmt.toSQL(ctx)
}

func (c *subqueryClause) collectColSources(collector colSrcMap) {}

func (c *subqueryClause) isClause() {}

func (c *subqueryClause) deepcopy() clause {
	return &subqueryClause{selectStmt: c.selectStmt.Make()}
}

func (c *subqueryClause) isValueSource() {}

// Helper functions.
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
