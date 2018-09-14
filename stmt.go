package pgqb

import "strconv"

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
}

// Create a snapshot (deep-copy) of the Stmt object.
func (s *SelectStmt) Fix() *SelectStmt {
	res := &SelectStmt{limit: s.limit}
	copyClauseTo(s.selectClause, res.selectClause)
	copyClauseTo(s.whereClause, res.whereClause)
	copyClauseTo(s.fromClause, res.fromClause)
	copyClauseTo(s.groupByClause, res.groupByClause)
	copyClauseTo(s.havingClause, res.havingClause)
	copyClauseTo(s.orderByClause, res.orderByClause)
	return res
}

func Select(exps ... interface{}) *SelectStmt {
	res := &SelectStmt{}
	res.Select(exps...)
	return res
}

// Helper for deep-copying a clause.
func copyClauseTo(src clause, dst clause) {
	if !isNull(src) {
		src.copyTo(dst)
	}
}
