package main

import (
	"encoding/json"
	"fmt"

	"github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
)

func main() {
	options := map[string]string{
		"schema":         "DKYPW",
		"appName":        "GORM 连接达梦数据库示例",
		"connectTimeout": "30000",
	}

	// dm://user:password@host:port?schema=SYSDBA[&...]
	dsn := dameng.BuildUrl("SYSDBA", "SYSDBA001", "127.0.0.1", 5236, options)
	// VARCHAR 类型大小为字符长度
	//db, err := gorm.Open(dameng.New(dameng.Config{DSN: dsn, VarcharSizeIsCharLength: true}))
	// VARCHAR 类型大小为字节长度（默认）
	db, err := gorm.Open(dameng.Open(dsn), &gorm.Config{})
	if err != nil {
		// panic error or log error info
	}

	// do somethings
	var versionInfo []map[string]interface{}
	db.Table("SYS.V$VERSION").Find(&versionInfo)
	if err := db.Error; err == nil {
		versionBytes, _ := json.MarshalIndent(versionInfo, "", "  ")
		fmt.Printf("达梦数据库版本信息：\n%s\n", versionBytes)
	}
}
