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

	// 一次性写入文件（如果文件存在会被覆盖）
	err := os.WriteFile(filepath.Join(tablePath, tableName+".db"), data, 0644)
	if err != nil {
		fmt.Printf("create table fail: %v\n", err)
		return nil
	}

	return nil
}

func SelectTable(tableName string, tablePath string) (*Table, error) {
	tableInfo := &Table{}
	if offset, err := utils.StreamFromOffset(filepath.Join(tablePath, tableName+".db"), 0, 1024); err != nil {
		return nil, err
	} else {
		tableInfo.Name = string(offset)
	}
	if offset, err := utils.StreamFromOffset(filepath.Join(tablePath, tableName+".db"), 1024, 1024); err != nil {
		return nil, err
	} else {
		tableInfo.Columns = string(offset)
		return tableInfo, nil
	}

	return tableInfo, nil
}
