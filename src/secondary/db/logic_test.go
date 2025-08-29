package db

import (
	"fmt"
	"testing"
)

func TestCreateTable(t *testing.T) {
	_ = CreateTable("table", "./", &Table{Columns: []string{"id", "name"}, Name: "test-table"})
}
func TestSelectTable(t *testing.T) {
	table, _ := SelectTable("table", "./")
	fmt.Println(table)
}
