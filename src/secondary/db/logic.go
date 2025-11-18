package db

import (
	"fmt"
	"os"
	"path/filepath"
	"raselper/src/secondary/utils"
)

// 创建表(三类文件, 数据文件、表信息、主索引)
func CreateTable(tableName string, tablePath string, tableInfo *Table) error {
	// 转换为byte数组
	data := []byte(tableInfo.Name)
	data = append(data, byte(tableInfo.ColumnCount))
	for _, column := range tableInfo.Columns {
		data = append(data, byte(column.NameLength))
		data = append(data, byte(column.Type))
		data = append(data, []byte(column.Name)...)
	}

	// 一次性写入文件（如果文件存在会被覆盖）
	err := os.WriteFile(filepath.Join(tablePath, tableName+".idb"), data, 0644)
	if err != nil {
		fmt.Printf("create table fail: %v\n", err)
		return nil
	}

	return nil
}

func SelectTable(tableName string, tablePath string) (*Table, []*TableRow, error) {
	tableInfo := &Table{}
	var tableRows []*TableRow
	length := getColumnLength(tableInfo.Columns)
	if offset, err := utils.StreamFromOffset(filepath.Join(tablePath, tableName+".idb"), 0, 8192); err != nil {
		return nil, make([]*TableRow, 0), err
	} else {
		tableInfo = readTable(offset)
	}
	if offset, err := utils.StreamFromOffset(filepath.Join(tablePath, tableName+".db"), 0, length); err != nil {
		return nil, make([]*TableRow, 0), err
	} else {
		tableRows = append(tableRows, readRow(offset))
	}

	return tableInfo, tableRows, nil
}

func InsertTable(tableName string, tablePath string) (int, error) {
	tableInfo := &Table{}
	readTable()
	if offset, err := utils.StreamFromOffset(filepath.Join(tablePath, tableName+".idb"), 0, 8192); err != nil {
		return 0, err
	} else {
		tableInfo = readTable(offset)
	}
}

//func UpdateTable(tableName string, tablePath string) (int, error) {
//
//}
//
//func DeleteTable(tableName string, tablePath string) (int, error) {
//
//}
