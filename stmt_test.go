package pgqb

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"strings"
	"fmt"
)

func TestSelect(t *testing.T) {
	t1 := Table("public", "school")
	c1 := Column(t1, "name")
	c2 := Column(t1, "city")
	c3 := Column(t1, "enrollment")

	ctx := NewContext()
	sql := strings.Trim(ctx.ToSQL(Select(c1, c2).Where(c2.Ne("New York City"))), " ")
	assert.Equal(t, `SELECT "school"."name", "school"."city" FROM "public"."school" WHERE "school"."city" != 'New York City'`, sql)

	t2 := Table("public", "city")
	e1 := Column(t2, "name")
	e2 := Column(t2, "state")
	sql = strings.Trim(ctx.ToSQL(Select(c1, e1.As("city"), e2).From(t1)), " ")
	assert.Equal(t, `SELECT "school"."name", "city"."name" "city", "city"."state" FROM "public"."school", "public"."city"`, sql)

	sql = strings.Trim(ctx.ToSQL(Select(c1, e1.As("city"), e2).
		From(t1.InnerJoin(t2, c2.Eq(e1)))), " ")
	assert.Equal(t, `SELECT "school"."name", "city"."name" "city", "city"."state" FROM "public"."school" INNER JOIN "public"."city" ON ("school"."city" = "city"."name")`, sql)

	// Test auto-including tables behavior when SubQueryExp presents
	sql = strings.Trim(ctx.ToSQL(Select(Exists(Select(e1.Eq(c2)).From(t2)))), " ")
	assert.Equal(t, `SELECT EXISTS (SELECT "city"."name" = "school"."city" FROM "public"."city" ) FROM "public"."school"`, sql)

	// Test GroupBy
	max := CreateFuncCallFactory("max")
	sql = strings.Trim(ctx.ToSQL(Select(max(c3), e2).Where(c2.Eq(e1)).GroupBy(e2)), " ")
	expSQLTmpl := `SELECT max("school"."enrollment"), "city"."state" FROM "public"."%s", "public"."%s" WHERE "school"."city" = "city"."name" GROUP BY "city"."state"`
	// The order of table inclusions may be different for each run
	assert.True(t, sql == fmt.Sprintf(expSQLTmpl, "school", "city") ||
		sql == fmt.Sprintf(expSQLTmpl, "city", "school"))

	sql = strings.Trim(ctx.ToSQL(Select(max(c3).As("maxEnrollment"), c1, e2).
		From(t1.InnerJoin(t2, c2.Eq(e1))).GroupBy(e2).Having(max(c3).Gt(1000))), " ")
	assert.Equal(t, `SELECT max("school"."enrollment") "maxEnrollment", "school"."name", "city"."state" FROM "public"."school" INNER JOIN "public"."city" ON ("school"."city" = "city"."name") GROUP BY "city"."state" HAVING max("school"."enrollment") > 1000`, sql)

	sql = strings.Trim(ctx.ToSQL(Select(c1).OrderBy(Desc(c3), c1).Limit(30)), " ")
	assert.Equal(t, `SELECT "school"."name" FROM "public"."school" ORDER BY "school"."enrollment" DESC, "school"."name" ASC LIMIT 30`, sql)
}
