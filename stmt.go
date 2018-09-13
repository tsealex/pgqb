package pgqb

type SelectStmt struct {
	ctx          *Context
	selectClause *selectClause
	fromClause   *fromClause
	whereClause  *whereClause
}

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
	ctx.state = buildContextStateColumnDeclaration
	clauseToSQL(s.selectClause, ctx)
	ctx.state = buildContextStateNone
	clauseToSQL(s.fromClause, ctx)
	clauseToSQL(s.whereClause, ctx)
}

func (s *SelectStmt) ToSQL() string {
	bCtx := s.ctx.createBuildContext()
	s.toSQL(bCtx)
	return bCtx.buf.String()
}

func newSelect(ctx *Context, exps ... interface{}) *SelectStmt {
	res := &SelectStmt{ctx: ctx}
	res.Select(exps...)
	return res
}
