package pgqb

import (
	"time"
	"testing"
	"github.com/stretchr/testify/assert"
	"strings"
)

type restaurantModel struct {
	Model
	Id          *ColumnNode
	Name        *ColumnNode
	Location    *ColumnNode
	OpenTime    *ColumnNode
	CloseTime   *ColumnNode
	OwnerId     *ColumnNode
	NumCustomer *ColumnNode
}

func (m *restaurantModel) As(alias string) *restaurantModel {
	return newRestaurantModel(m.Model.As(alias))
}

func (m *restaurantModel) all() []ColExp {
	return []ColExp{
		m.Id, m.Name, m.Location, m.OpenTime, m.CloseTime, m.OwnerId, m.NumCustomer,
	}
}

func newRestaurantModel(src Model) *restaurantModel {
	return &restaurantModel{
		Model:       src,
		Id:          Column(src, "Id"),
		Name:        Column(src, "Name"),
		Location:    Column(src, "Location"),
		OpenTime:    Column(src, "OpenTime"),
		CloseTime:   Column(src, "CloseTime"),
		OwnerId:     Column(src, "OwnerId"),
		NumCustomer: Column(src, "NumCustomer"),
	}
}

var restaurantTable = Table("public", "Restaurant")

func RestaurantModel() *restaurantModel {
	return newRestaurantModel(restaurantTable)
}

type Restaurant struct {
	Id          int64
	Name        string
	Location    string
	OpenTime    time.Time
	CloseTime   time.Time
	NumCustomer int
	OwnerId     int
}

func (Restaurant) Model() *restaurantModel {
	return RestaurantModel()
}

func TestModel(t *testing.T) {
	restA := RestaurantModel()
	restB := RestaurantModel().As("RestaurantB")
	cnode := restA.NumCustomer.Add(restB.NumCustomer).Gt(50)
	assert.Equal(t, `("Restaurant"."NumCustomer" + "RestaurantB"."NumCustomer") > 50`, AstToSQL(cnode))

	tnode := restA.InnerJoin(restB, restA.Name.Eq(restB.Name))
	assert.Equal(t, `"public"."Restaurant" INNER JOIN "public"."Restaurant" "RestaurantB" ON ("Restaurant"."Name" = "RestaurantB"."Name")`,AstToSQL(tnode))
}

func TestSelectModel(t *testing.T) {
	restA := RestaurantModel()
	restB := RestaurantModel().As("RestaurantB")
	sql := strings.Trim(NewContext().ToSQL(Select(Star(restB.Model)).Where(
		restB.OwnerId.Eq(restA.OwnerId), restB.OwnerId.Eq(200))), " ")
	assert.Equal(t, `SELECT "RestaurantB".* FROM "public"."Restaurant" "RestaurantB", "public"."Restaurant" WHERE "RestaurantB"."OwnerId" = "Restaurant"."OwnerId" AND "RestaurantB"."OwnerId" = 200`, sql)
}