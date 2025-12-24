package main

import (
	"fmt"
	"log"
	"os"
	"raselper/src/forwork/read_model/data"
	"raselper/src/forwork/read_model/util"

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

	cimxml, err := util.ParseCIMXML(os.Args[1])
	if err != nil {
		fmt.Println(err)
	}
	idNodeMap, _, _ := util.GetTopoMap(cimxml)
	var nodeListEntity []util.NodeMap
	for id, nodeList := range idNodeMap {
		if len(nodeList) > 2 {
			fmt.Println("id:", id)
			for _, node := range nodeList {
				data.DB.Table("DKYPW.NODE_MAP").
					Where("id = ?", "350100"+node).
					Find(&nodeListEntity)
				fmt.Println("nodeListEntity Len:", len(nodeListEntity))
				for _, entity := range nodeListEntity {
					fmt.Println("entity:", entity)
				}
			}
		}
	}
}
