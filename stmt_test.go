package pgqb

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"strings"
	"fmt"
)

func stmtToSQL(ctx *Context, s Stmt) string {
	return strings.Trim(ctx.ToSQL(s), " ")
}

func TestSelectStmt(t *testing.T) {
	t1 := Table("public", "school")
	c1 := Column(t1, "name")
	c2 := Column(t1, "city")
	c3 := Column(t1, "enrollment")

	ctx := NewContext()
	sql := stmtToSQL(ctx, Select(c1, c2).Where(c2.Ne("New York City")))
	assert.Equal(t, `SELECT "school"."name", "school"."city" FROM "public"."school" WHERE "school"."city" != 'New York City'`, sql)

	t2 := Table("public", "city")
	e1 := Column(t2, "name")
	e2 := Column(t2, "state")
	sql = stmtToSQL(ctx, Select(c1, e1.As("city"), e2).From(t1))
	assert.Equal(t, `SELECT "school"."name", "city"."name" "city", "city"."state" FROM "public"."school", "public"."city"`, sql)

	sql = stmtToSQL(ctx, Select(c1, e1.As("city"), e2).From(t1.InnerJoin(t2, c2.Eq(e1))))
	assert.Equal(t, `SELECT "school"."name", "city"."name" "city", "city"."state" FROM "public"."school" INNER JOIN "public"."city" ON ("school"."city" = "city"."name")`, sql)

	// Test auto-including tables behavior when SubQueryExp presents
	sql = stmtToSQL(ctx, Select(Exists(Select(e1.Eq(c2)).From(t2))))
	assert.Equal(t, `SELECT EXISTS (SELECT "city"."name" = "school"."city" FROM "public"."city" ) FROM "public"."school"`, sql)

	// Test GroupBy
	max := CreateFuncCallFactory("max")
	sql = stmtToSQL(ctx, Select(max(c3), e2).Where(c2.Eq(e1)).GroupBy(e2))
	expSQLTmpl := `SELECT max("school"."enrollment"), "city"."state" FROM "public"."%s", "public"."%s" WHERE "school"."city" = "city"."name" GROUP BY "city"."state"`
	// The order of table inclusions may be different for each run
	assert.True(t, sql == fmt.Sprintf(expSQLTmpl, "school", "city") ||
		sql == fmt.Sprintf(expSQLTmpl, "city", "school"))

	sql = stmtToSQL(ctx, Select(max(c3).As("maxEnrollment"), c1, e2).
		From(t1.InnerJoin(t2, c2.Eq(e1))).GroupBy(e2).Having(max(c3).Gt(1000)))
	assert.Equal(t, `SELECT max("school"."enrollment") "maxEnrollment", "school"."name", "city"."state" FROM "public"."school" INNER JOIN "public"."city" ON ("school"."city" = "city"."name") GROUP BY "city"."state" HAVING max("school"."enrollment") > 1000`, sql)

	sql = stmtToSQL(ctx, Select(c1).OrderBy(Desc(c3), c1).Limit(30))
	assert.Equal(t, `SELECT "school"."name" FROM "public"."school" ORDER BY "school"."enrollment" DESC, "school"."name" ASC LIMIT 30`, sql)
}

func TestSelectStmt_Make(t *testing.T) {
	ctx := NewContext()
	sel1 := Select(1, 2, 3)
	sel2 := sel1.Make()
	sel2.Select(4, 5, 6)
	assert.Equal(t, `SELECT 1, 2, 3`, stmtToSQL(ctx, sel1))
	assert.Equal(t, `SELECT 1, 2, 3, 4, 5, 6`, stmtToSQL(ctx, sel2))
}

func TestInsertStmt(t *testing.T) {
	t1 := Table("public", "school")
	c1 := Column(t1, "name")
	c2 := Column(t1, "city")
	//c3 := Column(t1, "enrollment")

	ctx := NewContext()
	stmt := InsertInto(t1, c1, c2).Values("Abc", Arg("city"))
	sql := stmtToSQL(ctx, stmt)
	exp := `INSERT INTO "public"."school" ("name", "city") VALUES ('Abc', $1)`
	assert.Equal(t, exp, sql)

	stmt.On(Conflict(c1), DoNothing())
	sql = stmtToSQL(ctx, stmt)
	exp2 := exp + ` ON CONFLICT ("name") DO NOTHING`
	assert.Equal(t, exp2, sql)

	stmt.On(Conflict(c2, c1), DoUpdate(Set{
		c2: "Seattle",
	}))
	sql = stmtToSQL(ctx, stmt)
	exp2 = exp + ` ON CONFLICT ("city", "name") DO UPDATE SET "city" = 'Seattle'`
	assert.Equal(t, exp2, sql)

	stmt.Returning(c2.Eq(c1))
	exp2 = exp2 + ` RETURNING "school"."city" = "school"."name"`
	sql = stmtToSQL(ctx, stmt)
	assert.Equal(t, exp2, sql)

	// TODO: More tests
}

func TestUpdateStmt(t *testing.T) {
	t1 := Table("public", "school")
	c1 := Column(t1, "name")
	c2 := Column(t1, "city")
	c3 := Column(t1, "enrollment")

	ctx := NewContext()
	stmt := Update(t1, Set{
		c2: "Madison",
	}).Where(c3.Gt(50000), c1.Ne("University of Wisconsin")).Returning(Star(t1))
	sql := stmtToSQL(ctx, stmt)
	assert.Equal(t, `UPDATE "public"."school" SET "city" = 'Madison' WHERE "school"."enrollment" > 50000 AND "school"."name" != 'University of Wisconsin' RETURNING "school".*`, sql)
}

func TestDeleteStmt(t *testing.T) {
	t1 := Table("public", "school")
	c1 := Column(t1, "name")
	c2 := Column(t1, "city")
	c3 := Column(t1, "enrollment")

	t2 := t1.As("school2")
	e2 := Column(t1, "city")
	e3 := Column(t2, "enrollment")

	ctx := NewContext()
	stmt := DeleteFrom(t1).Using(t2).Where(e3.Gt(40000), e2.Eq(c2), c3.Lte(40000))
	sql := stmtToSQL(ctx, stmt)
	assert.Equal(t, `DELETE FROM "public"."school" USING "public"."school" "school2" WHERE "school2"."enrollment" > 40000 AND "school"."city" = "school"."city" AND "school"."enrollment" <= 40000`, sql)

	t3 := Table("public", "city")
	p1 := Column(t3, "name")
	p2 := Column(t3, "state")
	stmt = DeleteFrom(t1).Where(p2.Eq(Arg("state")), p1.Eq(c2)).Returning(c1, c3.Gt(40000))
	sql = stmtToSQL(ctx, stmt)
	assert.Equal(t, `DELETE FROM "public"."school" USING "public"."city" WHERE "city"."state" = $1 AND "city"."name" = "school"."city" RETURNING "school"."name", "school"."enrollment" > 40000`, sql)

}
