package pgqb

import (
	"strconv"
	"reflect"
)

type Stmt interface {
	isStmt()
	toSQL(ctx *buildContext)
}

// Select statement.
type SelectStmt struct {
	selectClause  *selectClause
	fromClause    *fromClause
	whereClause   *whereClause
	groupByClause *groupByClause
	havingClause  *havingClause
	orderByClause *orderByClause
	limit         int
	offset        int
}

func (SelectStmt) isStmt() {}

func (s *SelectStmt) Select(exps ... interface{}) *SelectStmt {
	if len(exps) == 0 {
		return s
	}
	if s.selectClause == nil {
		s.selectClause = &selectClause{}
	}
	s.selectClause.addColExp(exps...)
	return s
}

func (s *SelectStmt) From(exps ... TableExp) *SelectStmt {
	if len(exps) == 0 {
		return s
	}
	if s.fromClause == nil {
		s.fromClause = &fromClause{}
	}
	s.fromClause.addTableExp(exps...)
	return s
}

func (s *SelectStmt) Where(exps ... interface{}) *SelectStmt {
	if len(exps) == 0 {
		return s
	}
	if s.whereClause == nil {
		s.whereClause = &whereClause{}
	}
	s.whereClause.addPredicate(exps...)
	return s
}

func (s *SelectStmt) GroupBy(exps ... interface{}) *SelectStmt {
	if len(exps) == 0 {
		return s
	}
	if s.groupByClause == nil {
		s.groupByClause = &groupByClause{}
	}
	s.groupByClause.addColExp(exps...)
	return s
}

func (s *SelectStmt) Having(exps ... interface{}) *SelectStmt {
	if len(exps) == 0 {
		return s
	}
	if s.havingClause == nil {
		s.havingClause = &havingClause{}
	}
	s.havingClause.addPredicate(exps...)
	return s
}

func (s *SelectStmt) OrderBy(exps ... interface{}) *SelectStmt {
	if len(exps) == 0 {
		return s
	}
	if s.orderByClause == nil {
		s.orderByClause = &orderByClause{}
	}
	s.orderByClause.addColExp(exps...)
	return s
}

func (s *SelectStmt) Limit(n int) *SelectStmt {
	s.limit = n
	return s
}

func (s *SelectStmt) Offset(n int) *SelectStmt {
	s.offset = n
	return s
}

func (s *SelectStmt) toSQL(ctx *buildContext) {
	if ctx.AutoFrom() {
		usedColSrc := collectColSourcesFromClauses(
			s.selectClause, s.whereClause, s.groupByClause, s.havingClause,
			s.orderByClause)
		if s.fromClause == nil {
			s.fromClause = &fromClause{}
		}
		s.fromClause.fillMissingColSrc(usedColSrc)
	}
	origState := ctx.state
	ctx.state = buildContextStateColumnDeclaration
	clauseToSQL(s.selectClause, ctx)
	ctx.state = origState
	clauseToSQL(s.fromClause, ctx)
	clauseToSQL(s.whereClause, ctx)
	clauseToSQL(s.groupByClause, ctx)
	clauseToSQL(s.havingClause, ctx)
	clauseToSQL(s.orderByClause, ctx)
	if s.limit > 0 {
		ctx.buf.WriteString("LIMIT " + strconv.FormatInt(int64(s.limit), 10) + " ")
	}
	if s.offset > 0 {
		ctx.buf.WriteString("OFFSET " + strconv.FormatInt(int64(s.offset), 10) + " ")
	}
}

// Create a snapshot (deep-copy) of the Stmt object.
func (s *SelectStmt) Make() *SelectStmt {
	res := &SelectStmt{limit: s.limit, offset: s.offset}
	res.selectClause = deepcopyClause(s.selectClause).(*selectClause)
	res.whereClause = deepcopyClause(s.whereClause).(*whereClause)
	res.fromClause = deepcopyClause(s.fromClause).(*fromClause)
	res.groupByClause = deepcopyClause(s.groupByClause).(*groupByClause)
	res.havingClause = deepcopyClause(s.havingClause).(*havingClause)
	res.orderByClause = deepcopyClause(s.orderByClause).(*orderByClause)
	return res
}

func Select(exps ... interface{}) *SelectStmt {
	res := &SelectStmt{}
	res.Select(exps...)
	return res
}

// Insert statement.
type InsertStmt struct {
	insertClause        *insertClause
	defaultValuesClause *defaultValuesClause
	valuesClause        valueSourceClause
	conflictClause      *conflictClause
	returningClause     *returningClause
}

func (s *InsertStmt) toSQL(ctx *buildContext) {
	clauseToSQL(s.insertClause, ctx)
	clauseToSQL(s.defaultValuesClause, ctx)
	clauseToSQL(s.valuesClause, ctx)
	clauseToSQL(s.conflictClause, ctx)
	clauseToSQL(s.returningClause, ctx)
}

// Should only be called once.
func (s *InsertStmt) into(table *TableNode, cols ... *ColumnNode) {
	s.insertClause = &insertClause{table: table, columns: cols}
}

func (s *InsertStmt) DefaultValues(exps ... interface{}) *InsertStmt {
	if len(exps) == 0 {
		return s
	}
	if s.defaultValuesClause == nil {
		s.defaultValuesClause = &defaultValuesClause{}
	}
	s.defaultValuesClause.addColExp(exps...)
	return s
}

func (s *InsertStmt) getValueClause() *valuesClause {
	valClause, ok := s.valuesClause.(*valuesClause)
	if !ok {
		valClause = &valuesClause{}
		s.valuesClause = valClause
	}
	return valClause
}

func (s *InsertStmt) Values(exps ... interface{}) *InsertStmt {
	valClause := s.getValueClause()
	valClause.valuesList = append(valClause.valuesList, getExpList(exps))
	return s
}

func (s *InsertStmt) ValuesInBulk(tuples ... []interface{}) *InsertStmt {
	valClause := s.getValueClause()
	for _, tuple := range tuples {
		valClause.valuesList = append(valClause.valuesList, getExpList(tuple))
	}
	return s
}

func (s *InsertStmt) ClearValues() *InsertStmt {
	s.valuesClause = nil
	return s
}

func (s *InsertStmt) From(stmt *SelectStmt) *InsertStmt {
	s.valuesClause = &subqueryClause{selectStmt: stmt}
	return s
}

func (s *InsertStmt) Returning(exps ... interface{}) *InsertStmt {
	if len(exps) == 0 {
		return s
	}
	if s.returningClause == nil {
		s.returningClause = &returningClause{}
	}
	s.returningClause.addColExp(exps...)
	return s
}

func (s *InsertStmt) On(target ConflictTarget, action *conflictClause) *InsertStmt {
	s.conflictClause = action
	action.cols = target
	return s
}

func (s *InsertStmt) Make() *InsertStmt {
	res := &InsertStmt{}
	res.conflictClause = deepcopyClause(s.conflictClause).(*conflictClause)
	res.defaultValuesClause = deepcopyClause(s.defaultValuesClause).(*defaultValuesClause)
	res.returningClause = deepcopyClause(s.returningClause).(*returningClause)
	res.insertClause = deepcopyClause(s.insertClause).(*insertClause)
	if !isNull(s.valuesClause) {
		switch s.valuesClause.(type) {
		case *valuesClause:
			res.valuesClause = deepcopyClause(s.valuesClause.(*valuesClause)).(*valuesClause)
		case *subqueryClause:
			res.valuesClause = deepcopyClause(s.valuesClause.(*subqueryClause)).(*subqueryClause)
		}
	}
	return res
}

func (s *InsertStmt) isStmt() {}

func DoNothing() *conflictClause {
	return &conflictClause{}
}

func DoUpdate(setter Set) *conflictClause {
	return &conflictClause{setClause: &setClause{setExpMap: setter.mapNamesToColExps()}}
}

func InsertInto(table *TableNode, cols ... *ColumnNode) *InsertStmt {
	stmt := &InsertStmt{}
	stmt.into(table, cols...)
	return stmt
}

// Update statement.
type UpdateStmt struct {
	table           *TableNode
	setClause       *setClause
	fromClause      *fromClause
	whereClause     *whereClause
	returningClause *returningClause
}

func (s *UpdateStmt) isStmt() {}

func (s *UpdateStmt) toSQL(ctx *buildContext) {
	panic("implement me")
}

// Helper for deep-copying a clause.
func deepcopyClause(src clause) interface{} {
	if !isNull(src) {
		return src.deepcopy()
	}
	return reflect.New(reflect.TypeOf(src)).Elem().Interface()
}

type Set map[*ColumnNode]interface{}

func (s Set) mapNamesToColExps() map[string]ColExp {
	var res = make(map[string]ColExp, len(s))
	for col, exp := range s {
		res[col.name] = getExp(exp)
	}
	return res
}

type ConflictTarget []*ColumnNode

func Conflict(cols ... *ColumnNode) ConflictTarget {
	return ConflictTarget(cols)
}
