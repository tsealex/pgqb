package pgqb

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"fmt"
)

// Props
var myTb *TableNode
var myTbSchema, myTbTable string

// Helpers
func AstToSQL(node astNode) string {
	return AstToSQLWithState(node, 0)
}

func AstToSQLWithState(node astNode, state buildContextState) string {
	ctx := NewBuildContext(BuildContextModeNone)
	ctx.state = state
	node.toSQL(ctx)
	return ctx.buf.String()
}

// Setup & Teardown
func TestMain(m *testing.M) {
	// Setup
	myTbSchema = "public"
	myTbTable = "testTable"
	myTb = Table(myTbSchema, myTbTable)
	// Run tests
	m.Run()

	// Teardown
}

// Test Table Expressions.
func TestTableNode(t *testing.T) {
	tb := Table("public", "tb")
	assert.Equal(t, `"public"."tb"`, AstToSQL(tb))
}

func TestTableNode_InnerJoin(t *testing.T) {
	tb := Table("public", "tb")
	tb2 := Table("public", "tb2")
	assert.Equal(t, `"public"."tb" NATURAL JOIN "public"."tb2"`,
		AstToSQL(tb.NaturalJoin(tb2)))
}

func TestTableNode_As(t *testing.T) {
	tb := Table("public", "tb")
	assert.Equal(t, `"public"."tb" "newTb"`, AstToSQL(tb.As("newTb")))
}

func TestTableAliasNode(t *testing.T) {
	tba := TableAlias(Table("public", "tb"), "newTb")
	assert.Equal(t, `"public"."tb" "newTb"`, AstToSQL(tba))
}

func TestTableAliasNode_As(t *testing.T) {
	tba := TableAlias(Table("public", "tb"), "newTb")
	assert.Equal(t, `"public"."tb" "No"`, AstToSQL(tba.As("No")))
}

// Test Column Expressions.
func TestLiteral_Integer(t *testing.T) {
	l := Literal(2)
	assert.Equal(t, "2", AstToSQL(l))
}

func TestLiteral_Float(t *testing.T) {
	l := Literal(3.33)
	assert.Equal(t, "3.33", AstToSQL(l))
}

func TestLiteral_String(t *testing.T) {
	l := Literal("22")
	assert.Equal(t, "'22'", AstToSQL(l))
}

func TestLiteral_Bool(t *testing.T) {
	l := Literal(false)
	assert.Equal(t, "false", AstToSQL(l))
}

func TestLiteral_Null(t *testing.T) {
	l := Literal(nil)
	assert.Equal(t, "NULL", AstToSQL(l))
}

func TestLiteral_Error(t *testing.T) {
	assert.Panics(t, func() {
		Literal(struct{ A int }{})
	})
}

func TestColumn(t *testing.T) {
	col := Column(myTb, "Col")
	assert.Equal(t, fmt.Sprintf(`"%s"."Col"`, myTbTable), AstToSQL(col))

	col = Column(myTb.As("NewTb"), "Col")
	assert.Equal(t, `"NewTb"."Col"`, AstToSQL(col))
}

func TestColumnAlias(t *testing.T) {
	col := Column(myTb, "Col").As("SomeCol")
	assert.Equal(t, `"SomeCol"`, AstToSQL(col))

	assert.Equal(t, fmt.Sprintf(`"%s"."Col" "SomeCol"`, myTbTable),
		AstToSQLWithState(col, buildContextStateDeclaration))
}

func TestBaseColExpNode_Add(t *testing.T) {
	x := Literal(2)
	y := Literal(50)
	assert.Equal(t, "(2 + 50) + 50", AstToSQL(x.Add(y).Add(y)))
	assert.Equal(t, "(2 + 50) + (50 + 2)", AstToSQL(x.Add(y).Add(y.Add(x))))
	assert.Equal(t, "((2 + 50) + 50) + 2", AstToSQL(x.Add(y).Add(y).Add(x)))
}

func TestBaseColExpNode_Gte(t *testing.T) {
	col := Column(myTb, "Col")
	assert.Equal(t, fmt.Sprintf(`"%s"."Col" >= 34`, myTbTable),
		AstToSQL(col.Gte(34)))
}

// TODO: More tests for different operators.
func TestBaseColExpNode_Intersect(t *testing.T) {
	col := Column(myTb, "Col")
	arr := Array(3, 50, 70, 80)
	assert.Equal(t, fmt.Sprintf(`"%s"."Col" && '{3, 50, 70, 80}'`, myTbTable),
		AstToSQL(col.Intersect(arr)))
}

type Dummy struct{}

func (Dummy) GetSQLRepr() string {
	return "Okay"
}

func TestSQLLiteral(t *testing.T) {
	l := Literal(&Dummy{})
	assert.Equal(t, "Okay", AstToSQL(l))
}

func TestAnd(t *testing.T) {
	col := Column(myTb, "Col").As("NewCol")
	a := And(true, col.Gt(75), col.Lte(100), col.Ne(88))
	assert.Equal(t, `true AND ("NewCol" > 75) AND ("NewCol" <= 100) AND ("NewCol" != 88)`, AstToSQL(a))

	a = And(true, col.Gt(75), And(col.Lte(100), col.Ne(88)))
	assert.Equal(t, `true AND ("NewCol" > 75) AND (("NewCol" <= 100) AND ("NewCol" != 88))`, AstToSQL(a))

	assert.Panics(t, func() {
		And()
	})
}

func TestFuncCall(t *testing.T) {
	fn := FuncCall("MyFunc", 23, "test", Column(myTb, "Col"))
	assert.Equal(t, fmt.Sprintf(`MyFunc(23, 'test', "%s"."Col")`, myTbTable), AstToSQL(fn))

	fn = FuncCall("now")
	assert.Equal(t, "now()", AstToSQL(fn))
}

func TestFuncCallFactory(t *testing.T) {
	ff := CreateFuncCallFactory("new")
	assert.Equal(t, "new()", AstToSQL(ff()))
	assert.Equal(t, "new(2, 4, 6)", AstToSQL(ff(2, 4, 6)))
}

func TestNeg(t *testing.T) {
	n := Neg(5.78)
	assert.Equal(t, " - 5.78", AstToSQL(n))

	a := n.Add(30)
	assert.Equal(t, "( - 5.78) + 30", AstToSQL(a))
}