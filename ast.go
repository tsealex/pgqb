package pgqb

import (
	"strconv"
	"strings"
)

// Node in the abstract syntax tree.
type astNode interface {
	isAstNode()
	toSQL(ctx *BuildContext)
}

type ColSource interface {
	name() string

	As(alias string) ColSource
	Column(cname string) *ColumnNode
}

type TableExp interface {
	astNode
	isTableExp()

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

func (BaseTableExpNode) toSQL(ctx *BuildContext) {
	panic("not implemented")
}

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

func (n *TableNode) toSQL(ctx *BuildContext) {
	// TODO: This opIs Postgres-specific.
	tbname := ctx.QuoteObject(n.tbname)
	if n.schema != "" {
		ctx.buf.WriteString(ctx.QuoteObject(n.schema) + "." + tbname)
	} else {
		ctx.buf.WriteString(tbname)
	}
}

func (n *TableNode) As(alias string) ColSource {
	return &TableAliasNode{table: n, alias: alias}
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
	table *TableNode
	alias string
}

func (n *TableAliasNode) toSQL(ctx *BuildContext) {
	n.table.toSQL(ctx)
	// TODO: This may be Postgres-specific.
	ctx.buf.WriteString(" " + ctx.QuoteObject(n.alias))
}

func (n *TableAliasNode) name() string {
	return n.alias
}

func (n *TableAliasNode) As(alias string) ColSource {
	return n.table.As(alias)
}

// Return the Column of this table.
func (n *TableAliasNode) Column(cname string) *ColumnNode {
	return Column(n, cname)
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

func (n *JoinNode) toSQL(ctx *BuildContext) {
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
	collectColSources(collector map[string]ColSource)
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

func (BaseColExpNode) toSQL(ctx *BuildContext) {
	panic("not implemented")
}

func (BaseColExpNode) isAstNode()                                       {}
func (BaseColExpNode) isColExp()                                        {}
func (BaseColExpNode) collectColSources(collector map[string]ColSource) {}

// Placeholder for an argument.
type ArgumentNode struct {
	BaseColExpNode
	tag string
}

func (n *ArgumentNode) toSQL(ctx *BuildContext) {
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
type LiteralNode struct {
	BaseColExpNode
	value string
}

var Null = Literal(nil)

func (n *LiteralNode) toSQL(ctx *BuildContext) {
	ctx.buf.WriteString(n.value)
}

func Literal(value interface{}) *LiteralNode {
	var node *LiteralNode
	if value == nil {
		node = &LiteralNode{value: "NULL"}
	} else {
		var litVal string
		switch value.(type) {
		case int:
			litVal = strconv.FormatInt(int64(value.(int)), 10)
		case int8:
			litVal = strconv.FormatInt(int64(value.(int8)), 10)
		case int16:
			litVal = strconv.FormatInt(int64(value.(int16)), 10)
		case int32:
			litVal = strconv.FormatInt(int64(value.(int32)), 10)
		case int64:
			litVal = strconv.FormatInt(value.(int64), 10)
		case uint:
			litVal = strconv.FormatUint(uint64(value.(uint)), 10)
		case uint8:
			litVal = strconv.FormatUint(uint64(value.(uint8)), 10)
		case uint16:
			litVal = strconv.FormatUint(uint64(value.(uint16)), 10)
		case uint32:
			litVal = strconv.FormatUint(uint64(value.(uint32)), 10)
		case uint64:
			litVal = strconv.FormatUint(value.(uint64), 10)
		case float32:
			litVal = strconv.FormatFloat(float64(value.(float32)), 'f', 8, 32)
		case float64:
			litVal = strconv.FormatFloat(value.(float64), 'f', 11, 64)
		case bool:
			litVal = strconv.FormatBool(value.(bool))
		case string:
			// TODO: This opIs Postgres-specific
			litVal = "'" + litVal + "'"
		default:
			panic("unrecognizable literal value type")
		}
		node = &LiteralNode{value: litVal}
	}
	node.ColExp = node
	return node
}

func getExp(exp interface{}) ColExp {
	if res, ok := exp.(ColExp); ok {
		return res
	}
	return Literal(exp)
}

// Column.
type ColumnNode struct {
	BaseColExpNode
	source ColSource
	name   string
}

func (n *ColumnNode) toSQL(ctx *BuildContext) {
	// TODO: This opIs Postgres-specific
	ctx.buf.WriteString(ctx.QuoteObject(n.source.name()) + "." + ctx.QuoteObject(n.name))
}

func (n *ColumnNode) collectColSources(collector map[string]ColSource) {
	id := n.source.name()
	collector[id] = n.source
}

func (n *ColumnNode) As(alias string) *ColumnAliasNode {
	return columnAlias(n, alias)
}

func Column(src ColSource, cname string) *ColumnNode {
	node := &ColumnNode{source: src, name: cname}
	node.ColExp = node
	return node
}

// Alias of column.
type ColumnAliasNode struct {
	BaseColExpNode
	column *ColumnNode
	alias  string
}

func (n *ColumnAliasNode) ID() string {
	return n.alias
}

func (n *ColumnAliasNode) toSQL(ctx *BuildContext) {
	name := ctx.QuoteObject(n.alias)
	if ctx.state == buildContextStateDeclaration {
		n.column.toSQL(ctx)
		ctx.buf.WriteByte(' ')
	}
	ctx.buf.WriteString(name)
}

func (n *ColumnAliasNode) collectColSources(collector map[string]ColSource) {
	col := n.column
	col.collectColSources(collector)
}

func columnAlias(src *ColumnNode, alias string) *ColumnAliasNode {
	node := &ColumnAliasNode{column: src, alias: alias}
	node.ColExp = node
	return node
}

// Expression surrounded by parenthesis.
type GroupExpNode struct {
	BaseColExpNode
	exp ColExp
}

func (n *GroupExpNode) toSQL(ctx *BuildContext) {
	ctx.buf.WriteByte('(')
	n.exp.toSQL(ctx)
	ctx.buf.WriteByte(')')
}

func (n *GroupExpNode) collectColSources(collector map[string]ColSource) {
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
}

func (n *UnaryExpNode) collectColSources(collector map[string]ColSource) {
	n.exp.collectColSources(collector)
}

func (n *UnaryExpNode) toSQL(ctx *BuildContext) {
	if !n.position {
		ctx.buf.WriteString(n.op + " ")
		n.exp.toSQL(ctx)
	} else {
		n.exp.toSQL(ctx)
		ctx.buf.WriteString(" " + n.op)
	}
}

func UnaryExp(exp ColExp, op string, pos operatorPosition) *UnaryExpNode {
	node := &UnaryExpNode{exp: exp, op: op, position: pos}
	node.ColExp = node
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

// Binary expressions
type BinaryExpNode struct {
	BaseColExpNode
	left  ColExp
	right ColExp
	op    string
}

func (n *BinaryExpNode) toSQL(ctx *BuildContext) {
	n.left.toSQL(ctx)
	ctx.buf.WriteString(" " + n.op + " ")
	n.right.toSQL(ctx)
}

func (n *BinaryExpNode) collectColSources(collector map[string]ColSource) {
	n.left.collectColSources(collector)
	n.right.collectColSources(collector)
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
	// TODO: More array operators
)

// Multi-expression base node.
type MultiExpNode struct {
	BaseColExpNode
	expList []ColExp
}

func (n *MultiExpNode) collectColSources(collector map[string]ColSource) {
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
type LogicalExpNode struct {
	MultiExpNode
	op string
}

func (n *LogicalExpNode) toSQL(ctx *BuildContext) {
	for i, exp := range n.expList {
		if i > 0 {
			ctx.buf.WriteString(" " + n.op + " ")
		}
		ctx.buf.WriteByte('(')
		exp.toSQL(ctx)
		ctx.buf.WriteByte(')')
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
type FunctionCallNode struct {
	MultiExpNode
	name string
}

func (n *FunctionCallNode) toSQL(ctx *BuildContext) {
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

func FunctionCall(name string, args ... interface{}) *FunctionCallNode {
	exps := getExpList(args)
	node := &FunctionCallNode{MultiExpNode: *MultiExp(exps), name: name}
	node.ColExp = node
	return node
}

// Return a function that can generate function calls to a specific function.
func FunctionCallFactory(name string) func(...interface{}) *FunctionCallNode {
	return func(args ... interface{}) *FunctionCallNode {
		return FunctionCall(name, args...)
	}
}

// TODO: Expressions that involve sub-queries (i.e. EXISTS, ALL, SOME).
