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
type ColumnSource interface {
	astNode
	isColumnSource()
	Name() string
	As(alias string) ColumnSource
	Column(cname string) *ColumnNode
}

// Table/View.
type TableNode struct {
	schema string
	name   string
}

func (TableNode) isAstNode()      {}
func (TableNode) isColumnSource() {}

func (n *TableNode) Name() string {
	return n.name
}

func (n *TableNode) toSQL(ctx *BuildContext) {
	// TODO: This opIs Postgres-specific.
	tbname := ctx.QuoteObject(n.name)
	if n.schema != "" {
		ctx.buf.WriteString(ctx.QuoteObject(n.schema) + "." + tbname)
	} else {
		ctx.buf.WriteString(tbname)
	}
}

func (n *TableNode) As(alias string) ColumnSource {
	return &TableAliasNode{table: n, alias: alias}
}

// Return the column of this table.
func (n *TableNode) Column(cname string) *ColumnNode {
	return Column(n, cname)
}

func Table(schema, tbname string) *TableNode {
	return &TableNode{schema: schema, name: tbname}
}

// Alias of a table/view.
type TableAliasNode struct {
	table *TableNode
	alias string
}

func (TableAliasNode) isAstNode() {}

func (n *TableAliasNode) toSQL(ctx *BuildContext) {
	n.table.toSQL(ctx)
	// TODO: This may be Postgres-specific.
	ctx.buf.WriteString(" " + ctx.QuoteObject(n.alias))
}

func (TableAliasNode) isColumnSource() {}

func (n *TableAliasNode) Name() string {
	return n.alias
}

func (n *TableAliasNode) As(alias string) ColumnSource {
	return n.table.As(alias)
}

// Return the Column of this table.
func (n *TableAliasNode) Column(cname string) *ColumnNode {
	return Column(n, cname)
}

// Abstract expression.
type ExpressionNode interface {
	astNode
	isExpression()
	collectColumnSources(collector map[string]ColumnSource)
	// Binary operations
	Add(right interface{}) ExpressionNode
	Sub(right interface{}) ExpressionNode
	Mul(right interface{}) ExpressionNode
	Div(right interface{}) ExpressionNode
	Mod(right interface{}) ExpressionNode
	Exp(right interface{}) ExpressionNode
	Is(right interface{}) ExpressionNode
	IsNot(right interface{}) ExpressionNode
	Gt(right interface{}) ExpressionNode
	Gte(right interface{}) ExpressionNode
	Lt(right interface{}) ExpressionNode
	Lte(right interface{}) ExpressionNode
	Eq(right interface{}) ExpressionNode
	Ne(right interface{}) ExpressionNode
	Like(right interface{}) ExpressionNode
	NotLike(right interface{}) ExpressionNode
	Similar(right interface{}) ExpressionNode
	NotSimilar(right interface{}) ExpressionNode
	Match(right interface{}, caseInSen bool) ExpressionNode
	NotMatch(right interface{}, caseInSen bool) ExpressionNode
	Contains(right interface{}) ExpressionNode
	ContainedBy(right interface{}) ExpressionNode
	Union(right interface{}) ExpressionNode
	Intersect(right interface{}) ExpressionNode
	BitAnd(right interface{}) ExpressionNode
	BitOr(right interface{}) ExpressionNode
	BitXor(right interface{}) ExpressionNode
	LeftShift(right interface{}) ExpressionNode
	RightShift(right interface{}) ExpressionNode
}

// Base expression.
type BaseExpressionNode struct {
	ExpressionNode
}

func (n *BaseExpressionNode) Add(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opAdd, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Sub(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opSub, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Mul(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opMul, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Div(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opDiv, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Gt(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opGt, getExpressionOrLiteral(right))
}
func (n *BaseExpressionNode) Gte(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opGte, getExpressionOrLiteral(right))
}
func (n *BaseExpressionNode) Lt(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opLt, getExpressionOrLiteral(right))
}
func (n *BaseExpressionNode) Lte(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opLte, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Eq(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opEq, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Ne(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opNe, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Mod(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opMod, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Exp(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opExp, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Is(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opIs, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) IsNot(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opIsNot, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Like(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opLike, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) NotLike(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opNotLike, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Match(right interface{}, caseInSen bool) ExpressionNode {
	if caseInSen {
		return BinaryExpression(n.ExpressionNode, opInsMatch, getExpressionOrLiteral(right))
	} else {
		return BinaryExpression(n.ExpressionNode, opMatch, getExpressionOrLiteral(right))
	}
}

func (n *BaseExpressionNode) NotMatch(right interface{}, caseInSen bool) ExpressionNode {
	if caseInSen {
		return BinaryExpression(n.ExpressionNode, opNotInsMatch, getExpressionOrLiteral(right))
	} else {
		return BinaryExpression(n.ExpressionNode, opNotMatch, getExpressionOrLiteral(right))
	}
}

func (n *BaseExpressionNode) Similar(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opSimilar, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) NotSimilar(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opNotSimilar, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Contains(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opContains, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) ContainedBy(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opContainedBy, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Union(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opUnion, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) Intersect(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opIntersect, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) BitAnd(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opBitAnd, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) BitOr(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opBitOr, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) BitXor(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opBitXor, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) LeftShift(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opLeftShift, getExpressionOrLiteral(right))
}

func (n *BaseExpressionNode) RightShift(right interface{}) ExpressionNode {
	return BinaryExpression(n.ExpressionNode, opRightShift, getExpressionOrLiteral(right))
}

func (BaseExpressionNode) toSQL(ctx *BuildContext) {
	panic("not implemented")
}

func (BaseExpressionNode) isAstNode()                                             {}
func (BaseExpressionNode) isExpression()                                          {}
func (BaseExpressionNode) collectColumnSources(collector map[string]ColumnSource) {}

// Placeholder for an argument.
type ArgumentNode struct {
	BaseExpressionNode
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
	node.ExpressionNode = node
	return node
}

// Single SQL literal.
type LiteralNode struct {
	BaseExpressionNode
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
	node.ExpressionNode = node
	return node
}

func getExpressionOrLiteral(exp interface{}) ExpressionNode {
	if res, ok := exp.(ExpressionNode); ok {
		return res
	}
	return Literal(exp)
}

// Column.
type ColumnNode struct {
	BaseExpressionNode
	source ColumnSource
	name   string
}

func (n *ColumnNode) toSQL(ctx *BuildContext) {
	// TODO: This opIs Postgres-specific
	ctx.buf.WriteString(ctx.QuoteObject(n.source.Name()) + "." + ctx.QuoteObject(n.name))
}

func (n *ColumnNode) collectColumnSources(collector map[string]ColumnSource) {
	id := n.source.Name()
	collector[id] = n.source
}

func (n *ColumnNode) As(alias string) *ColumnAliasNode {
	return columnAlias(n, alias)
}

func Column(src ColumnSource, cname string) *ColumnNode {
	node := &ColumnNode{source: src, name: cname}
	node.ExpressionNode = node
	return node
}

// Alias of column.
type ColumnAliasNode struct {
	BaseExpressionNode
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

func (n *ColumnAliasNode) collectColumnSources(collector map[string]ColumnSource) {
	col := n.column
	col.collectColumnSources(collector)
}

func columnAlias(src *ColumnNode, alias string) *ColumnAliasNode {
	node := &ColumnAliasNode{column: src, alias: alias}
	node.ExpressionNode = node
	return node
}

// Expression surrounded by parenthesis.
type GroupExpression struct {
	BaseExpressionNode
	exp ExpressionNode
}

func (n *GroupExpression) toSQL(ctx *BuildContext) {
	ctx.buf.WriteByte('(')
	n.exp.toSQL(ctx)
	ctx.buf.WriteByte(')')
}

func (n *GroupExpression) collectColumnSources(collector map[string]ColumnSource) {
	n.exp.collectColumnSources(collector)
}

func Group(exp ExpressionNode) *GroupExpression {
	node := &GroupExpression{exp: exp}
	node.ExpressionNode = node
	return node
}

func G(exp ExpressionNode) *GroupExpression {
	return Group(exp)
}

// Unary expressions
type operatorPosition bool

const (
	posLeft  operatorPosition = false
	posRight                  = true
)

type UnaryExpressionNode struct {
	BaseExpressionNode
	exp      ExpressionNode
	op       string
	position operatorPosition
}

func (n *UnaryExpressionNode) collectColumnSources(collector map[string]ColumnSource) {
	n.exp.collectColumnSources(collector)
}

func (n *UnaryExpressionNode) toSQL(ctx *BuildContext) {
	if !n.position {
		ctx.buf.WriteString(n.op + " ")
		n.exp.toSQL(ctx)
	} else {
		n.exp.toSQL(ctx)
		ctx.buf.WriteString(" " + n.op)
	}
}

func UnaryExpression(exp ExpressionNode, op string, pos operatorPosition) *UnaryExpressionNode {
	node := &UnaryExpressionNode{exp: exp, op: op, position: pos}
	node.ExpressionNode = node
	return node
}

const (
	opNeg       string = "-"
	opAbs              = "@"
	opFactorial        = "!"
)

func Neg(exp interface{}) *UnaryExpressionNode {
	return UnaryExpression(getExpressionOrLiteral(exp), opNeg, posLeft)
}

func Abs(exp interface{}) *UnaryExpressionNode {
	return UnaryExpression(getExpressionOrLiteral(exp), opAbs, posLeft)
}

func Factorial(exp interface{}) *UnaryExpressionNode {
	return UnaryExpression(getExpressionOrLiteral(exp), opFactorial, posRight)
}

const (
	opNot string = "NOT"
)

func Not(exp interface{}) *UnaryExpressionNode {
	return UnaryExpression(Group(getExpressionOrLiteral(exp)), opNot, posLeft)
}

// Binary expressions
type BinaryExpressionNode struct {
	BaseExpressionNode
	left  ExpressionNode
	right ExpressionNode
	op    string
}

func (n *BinaryExpressionNode) toSQL(ctx *BuildContext) {
	n.left.toSQL(ctx)
	ctx.buf.WriteString(" " + n.op + " ")
	n.right.toSQL(ctx)
}

func (n *BinaryExpressionNode) collectColumnSources(collector map[string]ColumnSource) {
	n.left.collectColumnSources(collector)
	n.right.collectColumnSources(collector)
}

func BinaryExpression(left ExpressionNode, op string, right ExpressionNode) *BinaryExpressionNode {
	node := &BinaryExpressionNode{left: left, right: right, op: op}
	node.ExpressionNode = node
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
type MultiExpressionNode struct {
	BaseExpressionNode
	expList []ExpressionNode
}

func (n *MultiExpressionNode) collectColumnSources(collector map[string]ColumnSource) {
	for _, exp := range n.expList {
		exp.collectColumnSources(collector)
	}
}

func MultiExpression(expList []ExpressionNode) *MultiExpressionNode {
	node := &MultiExpressionNode{expList: expList}
	node.ExpressionNode = node
	return node
}

// Logical operations.
type LogicalExpressionNode struct {
	MultiExpressionNode
	op string
}

func (n *LogicalExpressionNode) toSQL(ctx *BuildContext) {
	for i, exp := range n.expList {
		if i > 0 {
			ctx.buf.WriteString(" " + n.op + " ")
		}
		ctx.buf.WriteByte('(')
		exp.toSQL(ctx)
		ctx.buf.WriteByte(')')
	}
}

func BinaryLogicalExpression(op string, expList []ExpressionNode) *LogicalExpressionNode {
	if len(expList) == 0 {
		panic("must have at least one sub-expression")
	}
	node := &LogicalExpressionNode{MultiExpressionNode: *MultiExpression(expList), op: op}
	node.ExpressionNode = node
	return node
}

const (
	opOr  string = "OR"
	opAnd string = "AND"
)

func getExpressionOrLiteralList(exps []interface{}) []ExpressionNode {
	var expList = make([]ExpressionNode, len(exps))
	for i, exp := range exps {
		expList[i] = getExpressionOrLiteral(exp)
	}
	return expList
}

func And(exps ... interface{}) *LogicalExpressionNode {
	return BinaryLogicalExpression(opAnd, getExpressionOrLiteralList(exps))
}

func Or(exps ... interface{}) *LogicalExpressionNode {
	return BinaryLogicalExpression(opOr, getExpressionOrLiteralList(exps))
}

// Function call expression.
type FunctionCallNode struct {
	MultiExpressionNode
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
	exps := getExpressionOrLiteralList(args)
	node := &FunctionCallNode{MultiExpressionNode: *MultiExpression(exps), name: name}
	node.ExpressionNode = node
	return node
}

// Return a function that can generate function calls to a specific function.
func FunctionCallFactory(name string) func(...interface{}) *FunctionCallNode {
	return func(args ... interface{}) *FunctionCallNode {
		return FunctionCall(name, args...)
	}
}

// TODO: Expressions that involve sub-queries (i.e. EXISTS, ALL, SOME).
