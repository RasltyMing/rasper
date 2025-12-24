package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"raselper/src/forwork/read_model/data"
	"strings"

	dameng "github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
)

func main() {
	newConfig, err := data.ReadAppConfig("app.yaml")
	data.Config = *newConfig
	if err != nil {
		log.Printf("读取配置失败: %v", err)
	}

	// 连接达梦数据库
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s", data.Config.DB.Username, data.Config.DB.Password, data.Config.DB.IP, data.Config.DB.Port)
	data.DB, err = gorm.Open(dameng.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Print("连接数据库失败:", err)
		return
	}

	args := os.Args
	fmt.Println("args:", args)

	path := args[1]

	var graphNameList []string
	if result := data.DB.Table(data.Config.DB.Database + ".SG_CON_FEEDERLINE_B").
		Select("GRAPH_NAME").
		Find(&graphNameList); result.Error != nil {
		fmt.Printf("Error: %v\n", result.Error)
	}
	for _, graphName := range graphNameList {
		graphNameSvg := strings.ReplaceAll(graphName, "dx.pic.g", "svg")
		if !fileExists(filepath.Join(path, graphNameSvg)) {
			fmt.Println("file not exist: ", graphNameSvg)
		}
	}
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
