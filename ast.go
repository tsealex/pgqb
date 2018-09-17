package pgqb

import (
	"strconv"
	"strings"
	"regexp"
)

// Node in the abstract syntax tree.
type astNode interface {
	isAstNode()
	toSQL(ctx *buildContext)
}

type ColSource interface {
	name() string

	As(alias string) *TableAliasNode
	Column(cname string) *ColumnNode
}

type TableExp interface {
	astNode
	isTableExp()
	collectColSources(collector colSrcMap)

	Join(joinType JoinType, dst TableExp, onExp ColExp) TableExp
	InnerJoin(dst TableExp, onExp ColExp) TableExp
	LeftOuterJoin(dst TableExp, onExp ColExp) TableExp
	RightOuterJoin(dst TableExp, onExp ColExp) TableExp
	NaturalJoin(dst TableExp) TableExp
}

type BaseTableExpNode struct {
	TableExp
}

func (BaseTableExpNode) isAstNode() {}

func (BaseTableExpNode) toSQL(ctx *buildContext) {
	panic("not implemented")
}

func (BaseTableExpNode) collectColSources(collector colSrcMap) {}

func (n *BaseTableExpNode) Join(joinType JoinType, dst TableExp, onExp ColExp) TableExp {
	return Join(joinType, n.TableExp, dst, onExp)
}

func (n *BaseTableExpNode) InnerJoin(dst TableExp, onExp ColExp) TableExp {
	return n.Join(InnerJoin, dst, onExp)
}

func (n *BaseTableExpNode) LeftOuterJoin(dst TableExp, onExp ColExp) TableExp {
	return n.Join(LeftOuterJoin, dst, onExp)
}

func (n *BaseTableExpNode) RightOuterJoin(dst TableExp, onExp ColExp) TableExp {
	return n.Join(RightOuterJoin, dst, onExp)
}

func (n *BaseTableExpNode) NaturalJoin(dst TableExp) TableExp {
	return n.Join(NaturalJoin, dst, nil)
}

func (BaseTableExpNode) isTableExp() {}

// Table/View.
type TableNode struct {
	BaseTableExpNode
	schema string
	tbname string
}

func (n *TableNode) name() string {
	return n.tbname
}

func (n *TableNode) collectColSources(collector colSrcMap) {
	id := n.name()
	collector[id] = n
}

func (n *TableNode) toSQL(ctx *buildContext) {
	// TODO: This opIs Postgres-specific.
	tbname := ctx.QuoteObject(n.tbname)
	if n.schema != "" {
		ctx.buf.WriteString(ctx.QuoteObject(n.schema) + "." + tbname)
	} else {
		ctx.buf.WriteString(tbname)
	}
}

func (n *TableNode) As(alias string) *TableAliasNode {
	return TableAlias(n, alias)
}

// Return the column of this dst.
func (n *TableNode) Column(cname string) *ColumnNode {
	return Column(n, cname)
}

func Table(schema, tbname string) *TableNode {
	node := &TableNode{schema: schema, tbname: tbname}
	node.TableExp = node
	return node
}

// Alias of a table/view.
type TableAliasNode struct {
	BaseTableExpNode
	table ColSource
	alias string
}

func (n *TableAliasNode) collectColSources(collector colSrcMap) {
	id := n.name()
	collector[id] = n
}

func (n *TableAliasNode) toSQL(ctx *buildContext) {
	n.table.(astNode).toSQL(ctx)
	// TODO: This may be Postgres-specific.
	ctx.buf.WriteString(" " + ctx.QuoteObject(n.alias))
}

func (n *TableAliasNode) name() string {
	return n.alias
}

func (n *TableAliasNode) As(alias string) *TableAliasNode {
	return n.table.As(alias)
}

// Return the Column of this table.
func (n *TableAliasNode) Column(cname string) *ColumnNode {
	return Column(n, cname)
}

func TableAlias(table ColSource, alias string) *TableAliasNode {
	node := &TableAliasNode{table: table, alias: alias}
	node.TableExp = node
	return node
}

// Join expression.
type JoinType string

const (
	InnerJoin      JoinType = "INNER JOIN"
	LeftOuterJoin           = "LEFT OUTER JOIN"
	RightOuterJoin          = "RIGHT OUTER JOIN"
	NaturalJoin             = "NATURAL JOIN"
)

type JoinNode struct {
	BaseTableExpNode
	src      TableExp
	dst      TableExp
	exp      ColExp
	joinType JoinType
}

func (JoinNode) isTableExp() {}

func (JoinNode) isAstNode() {}

func (n *JoinNode) collectColSources(collector colSrcMap) {
	n.src.collectColSources(collector)
	n.dst.collectColSources(collector)
}

func (n *JoinNode) toSQL(ctx *buildContext) {
	n.src.toSQL(ctx)
	ctx.buf.WriteString(" " + string(n.joinType) + " ")
	n.dst.toSQL(ctx)
	if n.joinType != NaturalJoin {
		ctx.buf.WriteString(" ON (")
		n.exp.toSQL(ctx)
		ctx.buf.WriteByte(')')
	}
}

func Join(joinType JoinType, src, dst TableExp, onExp ColExp) *JoinNode {
	return &JoinNode{src: src, exp: onExp, dst: dst, joinType: joinType}
}

// Abstract expression.
type ColExp interface {
	astNode
	isColExp()
	collectColSources(collector colSrcMap)
	// Binary operations
	Add(right interface{}) ColExp
	Sub(right interface{}) ColExp
	Mul(right interface{}) ColExp
	Div(right interface{}) ColExp
	Mod(right interface{}) ColExp
	Exp(right interface{}) ColExp
	Is(right interface{}) ColExp
	IsNot(right interface{}) ColExp
	Gt(right interface{}) ColExp
	Gte(right interface{}) ColExp
	Lt(right interface{}) ColExp
	Lte(right interface{}) ColExp
	Eq(right interface{}) ColExp
	Ne(right interface{}) ColExp
	Like(right interface{}) ColExp
	NotLike(right interface{}) ColExp
	Similar(right interface{}) ColExp
	NotSimilar(right interface{}) ColExp
	Match(right interface{}, caseInSen bool) ColExp
	NotMatch(right interface{}, caseInSen bool) ColExp
	Contains(right interface{}) ColExp
	ContainedBy(right interface{}) ColExp
	Union(right interface{}) ColExp
	Intersect(right interface{}) ColExp
	BitAnd(right interface{}) ColExp
	BitOr(right interface{}) ColExp
	BitXor(right interface{}) ColExp
	LeftShift(right interface{}) ColExp
	RightShift(right interface{}) ColExp

	As(alias string) ColExp
}

// Base expression.
type BaseColExpNode struct {
	ColExp
}

func (n *BaseColExpNode) Add(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opAdd, getExp(right))
}

func (n *BaseColExpNode) Sub(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opSub, getExp(right))
}

func (n *BaseColExpNode) Mul(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opMul, getExp(right))
}

func (n *BaseColExpNode) Div(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opDiv, getExp(right))
}

func (n *BaseColExpNode) Gt(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opGt, getExp(right))
}
func (n *BaseColExpNode) Gte(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opGte, getExp(right))
}
func (n *BaseColExpNode) Lt(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opLt, getExp(right))
}
func (n *BaseColExpNode) Lte(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opLte, getExp(right))
}

func (n *BaseColExpNode) Eq(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opEq, getExp(right))
}

func (n *BaseColExpNode) Ne(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opNe, getExp(right))
}

func (n *BaseColExpNode) Mod(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opMod, getExp(right))
}

func (n *BaseColExpNode) Exp(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opExp, getExp(right))
}

func (n *BaseColExpNode) Is(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opIs, getExp(right))
}

func (n *BaseColExpNode) IsNot(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opIsNot, getExp(right))
}

func (n *BaseColExpNode) Like(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opLike, getExp(right))
}

func (n *BaseColExpNode) NotLike(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opNotLike, getExp(right))
}

func (n *BaseColExpNode) Match(right interface{}, caseInSen bool) ColExp {
	if caseInSen {
		return BinaryExp(n.ColExp, opInsMatch, getExp(right))
	} else {
		return BinaryExp(n.ColExp, opMatch, getExp(right))
	}
}

func (n *BaseColExpNode) NotMatch(right interface{}, caseInSen bool) ColExp {
	if caseInSen {
		return BinaryExp(n.ColExp, opNotInsMatch, getExp(right))
	} else {
		return BinaryExp(n.ColExp, opNotMatch, getExp(right))
	}
}

func (n *BaseColExpNode) Similar(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opSimilar, getExp(right))
}

func (n *BaseColExpNode) NotSimilar(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opNotSimilar, getExp(right))
}

func (n *BaseColExpNode) Contains(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opContains, getExp(right))
}

func (n *BaseColExpNode) ContainedBy(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opContainedBy, getExp(right))
}

func (n *BaseColExpNode) Union(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opUnion, getExp(right))
}

func (n *BaseColExpNode) Intersect(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opIntersect, getExp(right))
}

func (n *BaseColExpNode) In(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opIn, getExp(right))
}

func (n *BaseColExpNode) BitAnd(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opBitAnd, getExp(right))
}

func (n *BaseColExpNode) BitOr(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opBitOr, getExp(right))
}

func (n *BaseColExpNode) BitXor(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opBitXor, getExp(right))
}

func (n *BaseColExpNode) LeftShift(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opLeftShift, getExp(right))
}

func (n *BaseColExpNode) RightShift(right interface{}) ColExp {
	return BinaryExp(n.ColExp, opRightShift, getExp(right))
}

func (BaseColExpNode) toSQL(ctx *buildContext) {
	panic("not implemented")
}

func (n *BaseColExpNode) As(alias string) ColExp {
	return ColumnAlias(n.ColExp, alias)
}

func (BaseColExpNode) isAstNode()                            {}
func (BaseColExpNode) isColExp()                             {}
func (BaseColExpNode) collectColSources(collector colSrcMap) {}

// SQL. TODO: Test this.
type SQLNode struct {
	BaseColExpNode
	sql  string
	args []ColExp
}

func (n *SQLNode) toSQL(ctx *buildContext) {
	quoted := false
	slash := false
	argIdx := 0
	for _, c := range n.sql {
		if c == '"' && !slash {
			quoted = !quoted
		}
		if c == '?' && !quoted {
			n.args[argIdx].toSQL(ctx)
		} else {
			ctx.buf.WriteByte(byte(c))
		}
		slash = c == '\\'
	}
}

// Make sure you pass as many arguments as the placeholders
func SQL(sql string, args ... ColExp) *SQLNode {
	node := &SQLNode{sql: sql, args: args}
	node.ColExp = node
	return node
}

var Default = SQL("DEFAULT")

// Placeholder for an argument.
type ArgumentNode struct {
	BaseColExpNode
	tag string
}

func (n *ArgumentNode) toSQL(ctx *buildContext) {
	if ctx.NamedArgumentMode() {
		if n.tag == "" {
			panic("empty tag opIs not allowed in NamedArgument mode")
		}
		ctx.buf.WriteString(":" + n.tag)
	} else {
		argNum := ctx.getArgNum(n.tag)
		ctx.buf.WriteString("$" + strconv.FormatInt(int64(argNum), 10))
	}
}

func Argument(tag string) *ArgumentNode {
	tag = strings.TrimSpace(tag)
	node := &ArgumentNode{tag: tag}
	node.ColExp = node
	return node
}

func Arg(tag string) *ArgumentNode {
	return Argument(tag)
}

// Single SQL literal.
type SQLLiteral interface {
	GetSQLRepr() string
}

func convertValueToLiteral(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	var res string
	switch value.(type) {
	case int:
		res = strconv.FormatInt(int64(value.(int)), 10)
	case int8:
		res = strconv.FormatInt(int64(value.(int8)), 10)
	case int16:
		res = strconv.FormatInt(int64(value.(int16)), 10)
	case int32:
		res = strconv.FormatInt(int64(value.(int32)), 10)
	case int64:
		res = strconv.FormatInt(value.(int64), 10)
	case uint:
		res = strconv.FormatUint(uint64(value.(uint)), 10)
	case uint8:
		res = strconv.FormatUint(uint64(value.(uint8)), 10)
	case uint16:
		res = strconv.FormatUint(uint64(value.(uint16)), 10)
	case uint32:
		res = strconv.FormatUint(uint64(value.(uint32)), 10)
	case uint64:
		res = strconv.FormatUint(value.(uint64), 10)
	case float32:
		res = strconv.FormatFloat(float64(value.(float32)), 'f', -1, 32)
	case float64:
		res = strconv.FormatFloat(value.(float64), 'f', -1, 64)
	case bool:
		res = strconv.FormatBool(value.(bool))
	case string:
		// TODO: This opIs Postgres-specific
		res = "'" + value.(string) + "'"
	default:
		if l, ok := value.(SQLLiteral); ok {
			res = l.GetSQLRepr()
		} else {
			panic("unrecognizable value type")
		}
	}
	return res
}

type LiteralNode struct {
	BaseColExpNode
	value string
}

var Null = Literal(nil)

func (n *LiteralNode) toSQL(ctx *buildContext) {
	ctx.buf.WriteString(n.value)
}

func Literal(value interface{}) *LiteralNode {
	node := &LiteralNode{value: convertValueToLiteral(value)}
	node.ColExp = node
	return node
}

func getExp(exp interface{}) ColExp {
	if res, ok := exp.(ColExp); ok {
		return res
	}
	return Literal(exp)
}

// Array.
type ArrayNode struct {
	BaseColExpNode
	values []string
}

func (n *ArrayNode) toSQL(ctx *buildContext) {
	ctx.buf.WriteString("'{")
	for i, value := range n.values {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		ctx.buf.WriteString(value)
	}
	ctx.buf.WriteString("}'")
}

func Array(values ... interface{}) *ArrayNode {
	node := &ArrayNode{values: make([]string, len(values))}
	for i, value := range values {
		node.values[i] = convertValueToLiteral(value)
	}
	node.ColExp = node
	return node
}

// Tuple.
type TupleNode struct {
	BaseColExpNode
	values []string
}

func (n *TupleNode) toSQL(ctx *buildContext) {
	ctx.buf.WriteByte('(')
	for i, value := range n.values {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		ctx.buf.WriteString(value)
	}
	ctx.buf.WriteByte(')')
}

func Tuple(values ... interface{}) *TupleNode {
	node := &TupleNode{values: make([]string, len(values))}
	for i, value := range values {
		node.values[i] = convertValueToLiteral(value)
	}
	node.ColExp = node
	return node
}

// Node that involves a column source.
type BaseColumnSourceNode struct {
	BaseColExpNode
	source ColSource
}

func (n *BaseColumnSourceNode) collectColSources(collector colSrcMap) {
	id := n.source.name()
	collector[id] = n.source
}

// Column.
type ColumnNode struct {
	BaseColumnSourceNode
	name string
}

func (n *ColumnNode) toSQL(ctx *buildContext) {
	if ctx.state != buildContextStateNoColumnSource {
		ctx.buf.WriteString(ctx.QuoteObject(n.source.name()) + ".")
	}
	ctx.buf.WriteString(ctx.QuoteObject(n.name))
}

func Column(src ColSource, cname string) *ColumnNode {
	node := &ColumnNode{name: cname}
	node.source = src
	node.ColExp = node
	return node
}

// Alias of ColExp.
type ColExpAliasNode struct {
	BaseColExpNode
	exp   ColExp
	alias string
}

func (n *ColExpAliasNode) toSQL(ctx *buildContext) {
	name := ctx.QuoteObject(n.alias)
	if ctx.state == buildContextStateColumnDeclaration {
		n.exp.toSQL(ctx)
		ctx.buf.WriteByte(' ')
	}
	ctx.buf.WriteString(name)
}

func (n *ColExpAliasNode) collectColSources(collector colSrcMap) {
	n.exp.collectColSources(collector)
}

func ColumnAlias(src ColExp, alias string) *ColExpAliasNode {
	node := &ColExpAliasNode{exp: src, alias: alias}
	node.ColExp = node
	return node
}

// All columns.
type StarNode struct {
	BaseColumnSourceNode
}

func (n *StarNode) toSQL(ctx *buildContext) {
	if ctx.state != buildContextStateNoColumnSource {
		ctx.buf.WriteString(ctx.QuoteObject(n.source.name()) + ".*")
	} else {
		ctx.buf.WriteByte('*')
	}
}

func Star(src ColSource) *StarNode {
	node := &StarNode{}
	node.source = src
	node.ColExp = node
	return node
}

// Expression surrounded by parenthesis.
type GroupExpNode struct {
	BaseColExpNode
	exp ColExp
}

func (n *GroupExpNode) toSQL(ctx *buildContext) {
	ctx.buf.WriteByte('(')
	n.exp.toSQL(ctx)
	ctx.buf.WriteByte(')')
}

func (n *GroupExpNode) collectColSources(collector colSrcMap) {
	n.exp.collectColSources(collector)
}

func Group(exp ColExp) *GroupExpNode {
	node := &GroupExpNode{exp: exp}
	node.ColExp = node
	return node
}

func G(exp ColExp) *GroupExpNode {
	return Group(exp)
}

// Compound expressions
type compoundExp interface {
	isCompoundExp()
}

func compoundExpToSQL(exp ColExp, ctx *buildContext) {
	_, isCompound := exp.(compoundExp)
	if isCompound {
		ctx.buf.WriteByte('(')
		exp.toSQL(ctx)
		ctx.buf.WriteByte(')')
	} else {
		exp.toSQL(ctx)
	}
}

// Unary expressions
type operatorPosition bool

const (
	posLeft  operatorPosition = false
	posRight                  = true
)

type UnaryExpNode struct {
	BaseColExpNode
	exp      ColExp
	op       string
	position operatorPosition
	spaceSep bool
}

func (UnaryExpNode) isCompoundExp() {}

func (n *UnaryExpNode) collectColSources(collector colSrcMap) {
	n.exp.collectColSources(collector)
}

func (n *UnaryExpNode) toSQL(ctx *buildContext) {
	if n.position == posLeft {
		ctx.buf.WriteString(n.op)
		if n.spaceSep {
			ctx.buf.WriteByte(' ')
		}
		compoundExpToSQL(n.exp, ctx)
	} else {
		compoundExpToSQL(n.exp, ctx)
		if n.spaceSep {
			ctx.buf.WriteByte(' ')
		}
		ctx.buf.WriteString(n.op)
	}
}

type UnaryExpFactory func(exp interface{}) *UnaryExpNode

func createUnaryExpFactory(op string, pos operatorPosition) UnaryExpFactory {
	return func(exp interface{}) *UnaryExpNode {
		return UnaryExp(getExp(exp), op, pos)
	}
}

func CreateLeftUnaryExpFactory(op string) UnaryExpFactory {
	return createUnaryExpFactory(op, posLeft)
}

func CreateRightUnaryExpFactory(op string) UnaryExpFactory {
	return createUnaryExpFactory(op, posRight)
}

var isAlphanumeric = regexp.MustCompile(`^[a-zA-Z0-9]+$`).MatchString

func (n *UnaryExpNode) isOpAlphanumeric() bool {
	op := n.op
	var c string
	if n.position == posLeft {
		c = string(op[len(op)-1])
	} else {
		c = string(op[0])
	}
	return isAlphanumeric(c)
}

func UnaryExp(exp ColExp, op string, pos operatorPosition) *UnaryExpNode {
	node := &UnaryExpNode{exp: exp, op: op, position: pos}
	node.ColExp = node
	node.spaceSep = node.isOpAlphanumeric()
	return node
}

const (
	opNeg       string = "-"
	opAbs              = "@"
	opFactorial        = "!"
)

func Neg(exp interface{}) *UnaryExpNode {
	return UnaryExp(getExp(exp), opNeg, posLeft)
}

func Abs(exp interface{}) *UnaryExpNode {
	return UnaryExp(getExp(exp), opAbs, posLeft)
}

func Factorial(exp interface{}) *UnaryExpNode {
	return UnaryExp(getExp(exp), opFactorial, posRight)
}

const (
	opNot string = "NOT"
)

func Not(exp interface{}) *UnaryExpNode {
	return UnaryExp(Group(getExp(exp)), opNot, posLeft)
}

// Order expression
type OrderExpNode struct {
	UnaryExpNode
}

func (OrderExpNode) As(alias string) ColExp {
	panic("invalid operation")
}

const (
	orderAsc  string = "ASC"
	orderDesc string = "DESC"
)

func OrderExp(exp interface{}, order string) *OrderExpNode {
	node := &OrderExpNode{UnaryExpNode: *UnaryExp(getExp(exp), order, posRight)}
	node.ColExp = node
	return node
}

func Asc(exp interface{}) *OrderExpNode {
	return OrderExp(exp, orderAsc)
}

func Desc(exp interface{}) *OrderExpNode {
	return OrderExp(exp, orderDesc)
}

// Binary expressions
type BinaryExpNode struct {
	BaseColExpNode
	left  ColExp
	right ColExp
	op    string
}

func (BinaryExpNode) isCompoundExp() {}

func (n *BinaryExpNode) toSQL(ctx *buildContext) {
	compoundExpToSQL(n.left, ctx)
	ctx.buf.WriteString(" " + n.op + " ")
	compoundExpToSQL(n.right, ctx)
}

func (n *BinaryExpNode) collectColSources(collector colSrcMap) {
	n.left.collectColSources(collector)
	n.right.collectColSources(collector)
}

type BinaryExpFactory func(left, right interface{}) *BinaryExpNode

func CreateBinaryExpFactory(op string) BinaryExpFactory {
	return func(left, right interface{}) *BinaryExpNode {
		return BinaryExp(getExp(left), op, getExp(right))
	}
}

func BinaryExp(left ColExp, op string, right ColExp) *BinaryExpNode {
	node := &BinaryExpNode{left: left, right: right, op: op}
	node.ColExp = node
	return node
}

const (
	// TODO: Some of these are Postgres-specific.
	opGt          string = ">"
	opGte                = ">="
	opEq                 = "="
	opNe                 = "!="
	opLt                 = "<"
	opLte                = "<="
	opMod                = "%"
	opExp                = "^"
	opBitAnd             = "&"
	opBitOr              = "|"
	opBitXor             = "#"
	opLeftShift          = "<<"
	opRightShift         = ">>"
	opIs                 = "IS"
	opIsNot              = "IS NOT"
	opAdd         string = "+"
	opSub                = "-"
	opMul                = "*"
	opDiv                = "/"
	opLike        string = "LIKE"
	opNotLike            = "NOT LIKE"
	opSimilar            = "SIMILAR TO"
	opNotSimilar         = "NOT SIMILAR TO"
	opMatch              = "~"
	opInsMatch           = "~*"
	opNotMatch           = "!~"
	opNotInsMatch        = "!~*"
	opContains    string = "@>"
	opContainedBy        = "<@"
	opUnion              = "||"
	opIntersect          = "&&"
	opIn                 = "IN"
	// TODO: More array operators
)

// Multi-expression base node.
type MultiExpNode struct {
	BaseColExpNode
	expList []ColExp
}

func (n *MultiExpNode) collectColSources(collector colSrcMap) {
	for _, exp := range n.expList {
		exp.collectColSources(collector)
	}
}

func MultiExp(expList []ColExp) *MultiExpNode {
	node := &MultiExpNode{expList: expList}
	node.ColExp = node
	return node
}

// Logical operations.
type logicalExp interface {
	isLogicalExp()
}

type LogicalExpNode struct {
	MultiExpNode
	op string
}

func logicalExpToSQL(exp ColExp, ctx *buildContext) {
	_, isLogical := exp.(logicalExp)
	if isLogical {
		ctx.buf.WriteByte('(')
		exp.toSQL(ctx)
		ctx.buf.WriteByte(')')
	} else {
		exp.toSQL(ctx)
	}
}

func (LogicalExpNode) isLogicalExp() {}

func (LogicalExpNode) isCompoundExp() {}

func (n *LogicalExpNode) toSQL(ctx *buildContext) {
	for i, exp := range n.expList {
		if i > 0 {
			ctx.buf.WriteString(" " + n.op + " ")
		}
		logicalExpToSQL(exp, ctx)
	}
}

func LogicalExp(op string, expList []ColExp) *LogicalExpNode {
	if len(expList) == 0 {
		panic("must have at least one sub-expression")
	}
	node := &LogicalExpNode{MultiExpNode: *MultiExp(expList), op: op}
	node.ColExp = node
	return node
}

const (
	opOr  string = "OR"
	opAnd string = "AND"
)

func getExpList(exps []interface{}) []ColExp {
	var expList = make([]ColExp, len(exps))
	for i, exp := range exps {
		expList[i] = getExp(exp)
	}
	return expList
}

func And(exps ... interface{}) *LogicalExpNode {
	return LogicalExp(opAnd, getExpList(exps))
}

func Or(exps ... interface{}) *LogicalExpNode {
	return LogicalExp(opOr, getExpList(exps))
}

// Function call expression.
type FuncCallNode struct {
	MultiExpNode
	name string
}

func (n *FuncCallNode) toSQL(ctx *buildContext) {
	ctx.buf.WriteString(n.name)
	ctx.buf.WriteByte('(')
	for i, exp := range n.expList {
		if i > 0 {
			ctx.buf.WriteString(", ")
		}
		exp.toSQL(ctx)
	}
	ctx.buf.WriteByte(')')
}

func FuncCall(name string, args ... interface{}) *FuncCallNode {
	exps := getExpList(args)
	node := &FuncCallNode{MultiExpNode: *MultiExp(exps), name: name}
	node.ColExp = node
	return node
}

// Return a function that can generate function calls to a specific function.
type FuncCallFactory func(...interface{}) *FuncCallNode

func CreateFuncCallFactory(name string) FuncCallFactory {
	return func(args ... interface{}) *FuncCallNode {
		return FuncCall(name, args...)
	}
}

// Expressions that involve sub-queries (i.e. EXISTS, ALL, SOME).
// TODO: Add tests.
type SubQueryColExpNode struct {
	BaseColExpNode
	op         string
	selectStmt *SelectStmt
}

func (n *SubQueryColExpNode) toSQL(ctx *buildContext) {
	ctx.buf.WriteString(n.op + " (")
	origMode := ctx.mode
	ctx.mode &= ^ContextModeAutoFrom
	n.selectStmt.toSQL(ctx)
	ctx.mode = origMode
	ctx.buf.WriteByte(')')
}

func (n *SubQueryColExpNode) collectColSources(collector colSrcMap) {
	// TODO: Update this list as we add more clauses to SELECT stmt.
	usedSrcMap := collectColSourcesFromClauses(n.selectStmt.whereClause, n.selectStmt.selectClause)
	fromSrcMap := collectColSourcesFromClauses(n.selectStmt.fromClause)
	difference := usedSrcMap.Subtract(fromSrcMap)
	// Include all the column sources not specified in the subquery
	for _, colSrc := range difference {
		collector[colSrc.name()] = colSrc
	}
}

func SubQueryExp(op string, stmt *SelectStmt) *SubQueryColExpNode {
	n := &SubQueryColExpNode{op: op, selectStmt: stmt}
	n.ColExp = n
	return n
}

const (
	opExists string = "EXISTS"
	opALL           = "ALL"
	opSome          = "SOME"
	// TODO: More operators
)

func Exists(stmt *SelectStmt) *SubQueryColExpNode {
	return SubQueryExp(opExists, stmt)
}

func All(stmt *SelectStmt) *SubQueryColExpNode {
	return SubQueryExp(opALL, stmt)
}

func Some(stmt *SelectStmt) *SubQueryColExpNode {
	return SubQueryExp(opSome, stmt)
}

// TODO: Subquery alias (ColumnSrc, TableExp)
type SubQueryTableExpNode struct {
	BaseTableExpNode
	alias      string
	selectStmt *SelectStmt
}

func (n *SubQueryTableExpNode) name() string {
	return n.alias
}

func (n *SubQueryTableExpNode) As(alias string) *TableAliasNode {
	return TableAlias(SubQueryTableExp(n.selectStmt, alias), alias)
}

func (n *SubQueryTableExpNode) Column(cname string) *ColumnNode {
	return Column(n, cname)
}

func (n *SubQueryTableExpNode) toSQL(ctx *buildContext) {
	ctx.buf.WriteByte('(')
	n.selectStmt.toSQL(ctx)
	ctx.buf.WriteString(") " + ctx.QuoteObject(n.alias))
}

func SubQueryTableExp(stmt *SelectStmt, alias string) *SubQueryTableExpNode {
	node := &SubQueryTableExpNode{selectStmt: stmt, alias: alias}
	node.TableExp = node
	return node
}

// TODO: Array accessor (i.e. '{2, 7, 3}'[1]).

// TODO: Nested arrays.
