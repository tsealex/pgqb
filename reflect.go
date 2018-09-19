package pgqb

import (
	"database/sql"
	"fmt"
	"strings"
)

var informationSchema = "information_schema"

var pgCatalog = "pg_catalog"

var columnsTable = Table(informationSchema, "columns")

// Subset of Columns table's attributes.
var columnsModel = struct {
	Model
	ColumnName      *ColumnNode
	TableName       *ColumnNode
	TableSchema     *ColumnNode
	DataType        *ColumnNode
	IsNullable      *ColumnNode
	OrdinalPosition *ColumnNode
}{
	Model:           columnsTable,
	ColumnName:      Column(columnsTable, "column_name"),
	TableName:       Column(columnsTable, "table_name"),
	TableSchema:     Column(columnsTable, "table_schema"),
	DataType:        Column(columnsTable, "data_type"),
	IsNullable:      Column(columnsTable, "is_nullable"),
	OrdinalPosition: Column(columnsTable, "ordinal_position"),
}

type columns struct {
	ColumnName      string `db:"column_name"`
	TableName       string `db:"table_name"`
	TableSchema     string `db:"table_schema"`
	DataType        string `db:"data_type"`
	IsNullable      bool   `db:"is_nullable"`
	OrdinalPosition int    `db:"ordinal_position"`
}

func (c *columns) MemberName() string {
	// TODO: Should always be in CamelCase.
	return makeCamelCase(c.ColumnName)
}

// Return mappings between qualified table names and lists of columns
func GetAllTables(db *sql.DB, exclSchema ... interface{}) map[string][]*columns {
	res := map[string][]*columns{}
	cols := columnsModel
	stmt := Select(cols.ColumnName, cols.TableName, cols.TableSchema, cols.DataType, cols.IsNullable).
		Where(cols.TableSchema.NotIn(Tuple(exclSchema...)))
	query := NewContext().ToSQL(stmt)
	rows, err := db.Query(query)
	if err != nil {
		panic(fmt.Sprint("error occurred", err))
	}
	for rows.Next() {
		col := columns{}
		rows.Scan(&col.ColumnName, &col.TableName, &col.TableSchema, &col.DataType, &col.IsNullable)
		// TODO: Do something with the column info
		fullName := col.TableSchema + "." + col.TableName
		if _, in := res[fullName]; !in {
			res[fullName] = []*columns{}
		}
		res[fullName] = append(res[fullName], &col)
	}
	return res
}

// TODO: Import pgqb
// TODO: Object
var modelCodeTemplate = `
var {{.LowerTable}}Table = pgqb.Table({{.Schema}}, {{.Table}})

type {{.LowerTable}}Model struct {
	pgqb.Model
{{range _, column := .Columns}}
	{{call $column.MemberName}} *pgqb.ColumnNode
{{end}}
}

func (m *{{.LowerTable}}Model) As(alias string) *{{.LowerTable}}Model {
	return new{{.UpperTable}}Model(m.Model.As(alias))
}

func new{{.UpperTable}}Model(src pgqb.Model) *{{.LowerTable}}Model {
	return &{{.LowerTable}}Model{
		Model: src,
{{range _, column := .Columns}}
		{{call $column.MemberName}}: &pgqb.ColumnNode{src, "{{$column.ColumnName}}"},
{{end}}
	}
}

func {{.UpperTable}}Model() *{{.LowerTable}}Model {
	return new{{.UpperTable}}Model({{.LowerTable}}Table)
}

type {{.UpperTable}} struct {
{{range _, column := .Columns}}
		{{call $column.MemberName}} // TODO <------------------------------ (DataType)
{{end}}
}

`

func CreateModels(map[string][]*columns) {

}

// Helper functions
// Uppercase the first character of the string.
func capitalize(s string) string {
	return strings.ToUpper(string(s[0])) + s[1:]
}

// Lowercase the first character of the string.
func uncapitalize(s string) string {
	return strings.ToLower(string(s[0])) + s[1:]
}

// Convert a string to CamelCase form.
func makeCamelCase(s string) string {
	res := ""
	parts := strings.Split(s, "_")
	for _, part := range parts {
		res += capitalize(part)
	}
	return res
}
