package main

import (
	"fmt"
	"log"
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
	{
		// 连接达梦数据库
		dsn := fmt.Sprintf("dm://%s:%s@%s:%s", data.Config.DB2.Username, data.Config.DB2.Password, data.Config.DB2.IP, data.Config.DB2.Port)
		data.DB2, err = gorm.Open(dameng.Open(dsn), &gorm.Config{})
		if err != nil {
			log.Print("连接数据库失败:", err)
			return
		}
	}

	var db1FeederCList []util.FeederC
	data.DB.Table(data.Config.DB.Database + ".SG_CON_FEEDERLINE_B").
		Find(&db1FeederCList)
	var db2FeederCList []util.FeederC
	data.DB2.Table(data.Config.DB2.Database + ".SG_CON_FEEDERLINE_B").
		Find(&db2FeederCList)

	var db1FeederMap map[string]string
	for _, feederC := range db1FeederCList {
		db1FeederMap[feederC.PmsRdfID] = feederC.DCloudID
	}
	var db2FeederMap map[string]string
	for _, feederC := range db2FeederCList {
		db2FeederMap[feederC.PmsRdfID] = feederC.DCloudID
	}

	var db2Db1Map map[string]string
	for rdf, cloud := range db2FeederMap {
		db2Db1Map[cloud] = db1FeederMap[rdf]
	}
}
