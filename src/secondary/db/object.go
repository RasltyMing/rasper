package db

import (
	"encoding/binary"
	"path/filepath"
	"raselper/src/secondary/utils"
)

type Table struct {
	Name        string // 偏移32位作为表名
	ColumnCount uint16 // 偏移16位作为表字段数
	Columns     []Column
}

func (r Table) GetByteArray() []byte {
	data := []byte(r.Name)
	data = append(data, byte(r.ColumnCount))
	for _, column := range r.Columns {
		data = append(data, byte(column.NameLength))
		data = append(data, byte(column.Type))
		data = append(data, []byte(column.Name)...)
	}
	return data
}

func GetTableFromFile(tableName string, tablePath string) (*Table, error) {
	if offset, err := utils.StreamFromOffset(filepath.Join(tablePath, tableName+".idb"), 0, 8192); err != nil {
		return
	} else {
		tableInfo = readTable(offset)
	}
}

func readTable(data []byte) *Table {
	table := &Table{
		Name:        string(data[0:32]),
		ColumnCount: binary.LittleEndian.Uint16(data[32:48]),
		Columns:     []Column{},
	}

	data = data[48:]
	for i := 0; i < int(table.ColumnCount); i++ {
		readColumnRef, dataAfterRead := readColumn(data)
		table.Columns = append(table.Columns, *readColumnRef)
		data = dataAfterRead
	}

	return table
}

type Column struct {
	NameLength uint16 // 偏移16位作为名称长度
	Type       uint16 // 偏移16位作为类型 0: string, 1: int, 2: float, 3: bool, 4: varchar
	TypeLength uint16 // 偏移16位作为类型长度
	Name       string
}

type TableRow struct {
	MeatData []byte // 偏移40位作为头, 1位为删除标记, 3位空位, 36位作为id
	Data     []byte
}

func readColumn(data []byte) (*Column, []byte) {
	nameLength := binary.LittleEndian.Uint16(data[0:16])
	columnType := binary.LittleEndian.Uint16(data[16:32])
	columnTypeLength := binary.LittleEndian.Uint16(data[32:48])
	name := string(data[48:nameLength])

	return &Column{
		NameLength: nameLength,
		Type:       columnType,
		TypeLength: columnTypeLength,
		Name:       name,
	}, data[(48 + nameLength):]
}

func readRow(data []byte) *TableRow {
	return &TableRow{
		MeatData: data[0:40],
		Data:     data[40:],
	}
}

func getColumnLength(columnInfos []Column) int {
	totalLength := 0
	for _, info := range columnInfos {
		totalLength += int(info.TypeLength)
	}
	return totalLength
}
