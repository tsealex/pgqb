package model

import (
	"pgqb"
	"time"
)

type restaurantModel struct {
	source      pgqb.ColSource
	Id          *pgqb.ColumnNode
	Name        *pgqb.ColumnNode
	Location    *pgqb.ColumnNode
	OpenTime    *pgqb.ColumnNode
	CloseTime   *pgqb.ColumnNode
	OwnerId     *pgqb.ColumnNode
	NumCustomer *pgqb.ColumnNode
}

func (m *restaurantModel) As(alias string) *restaurantModel {
	return newRestaurantModel(m.source.As(alias))
}

func newRestaurantModel(src pgqb.ColSource) *restaurantModel {
	return &restaurantModel{
		source:      src,
		Id:          pgqb.Column(src, "Id"),
		Name:        pgqb.Column(src, "name"),
		Location:    pgqb.Column(src, "Location"),
		OpenTime:    pgqb.Column(src, "OpenTime"),
		CloseTime:   pgqb.Column(src, "CloseTime"),
		OwnerId:     pgqb.Column(src, "OwnerId"),
		NumCustomer: pgqb.Column(src, "NumCustomer"),
	}
}

var restaurantTable = pgqb.Table("public", "Restaurant")

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

func test() {
	restA := RestaurantModel()
	restB := RestaurantModel().As("RestaurantB")
	restA.NumCustomer.Add(restB.NumCustomer).Gt(50)
	now := pgqb.FunctionCallFactory("now")
	now()
}
