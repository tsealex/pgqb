package pgqb

type Stmt interface {
	isStmt()
	toSQL(ctx *buildContext)
}

type SelectStmt struct {
	selectClause *selectClause
	fromClause   *fromClause
	whereClause  *whereClause
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

func (s *SelectStmt) toSQL(ctx *buildContext) {
	// TODO: When GROUP BY, HAVING are added, modify this list
	if ctx.AutoFrom() {
		usedColSrc := collectColSourcesFromClauses(s.selectClause, s.whereClause)
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
}

// Create a snapshot (deep-copy) of the Stmt object.
func (s *SelectStmt) Fix() *SelectStmt {
	res := &SelectStmt{}
	s.selectClause.copyTo(res.selectClause)
	s.whereClause.copyTo(res.whereClause)
	s.fromClause.copyTo(res.fromClause)
	return res
}

func Select(exps ... interface{}) *SelectStmt {
	res := &SelectStmt{}
	res.Select(exps...)
	return res
}
