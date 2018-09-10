package pgqb

type selectClause struct {
	expList []ColExp
}

type whereClause struct {
	predicate ColExp
}

type fromClause struct {
	expList []TableExp
}
