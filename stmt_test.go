package pgqb

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"strings"
)

func TestSelect(t *testing.T) {
	t1 := Table("public", "school")
	c1 := Column(t1, "name")
	c2 := Column(t1, "city")

	sql := strings.Trim(NewContext().Select(c1, c2).Where(c2.Ne("New York City")).ToSQL(), " ")
	assert.Equal(t, `SELECT "school"."name", "school"."city" FROM "public"."school" WHERE "school"."city" != 'New York City'`, sql)

	t2 := Table("public", "city")
	e1 := Column(t2, "name")
	e2 := Column(t2, "state")
	sql = strings.Trim(NewContext().Select(c1, e1.As("city"), e2).From(t1).ToSQL(), " ")
	assert.Equal(t, `SELECT "school"."name", "city"."name" "city", "city"."state" FROM "public"."school", "public"."city"`, sql)


	sql = strings.Trim(NewContext().Select(c1, e1.As("city"), e2).
		From(t1.InnerJoin(t2, c2.Eq(e1))).ToSQL(), " ")
	assert.Equal(t, `SELECT "school"."name", "city"."name" "city", "city"."state" FROM "public"."school" INNER JOIN "public"."city" ON ("school"."city" = "city"."name")`, sql)

}