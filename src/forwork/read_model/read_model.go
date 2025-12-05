package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"raselper/src/forwork/read_model/data"
	"raselper/src/forwork/read_model/util"
	"strings"

	dameng "github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
)

var config util.Config
var db *gorm.DB
var owner string

// 主函数
func main() {
	newConfig, err := util.ReadAppConfig("app.yaml")
	config = *newConfig
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	// 连接达梦数据库
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s", config.DB.Username, config.DB.Password, config.DB.IP, config.DB.Port)
	db, err = gorm.Open(dameng.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("连接数据库失败:", err)
		return
	}

	args := os.Args
	fmt.Println("args:", args)

	path := args[1]
	fileInfo, err := os.Stat(args[1])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if fileInfo.IsDir() {
		fmt.Printf("%s 是一个文件夹\n", path)
		err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if !strings.HasSuffix(filepath.Base(path), ".xml") {
				log.Println("pass file:" + path)
				return nil
			}

			if err = ReadOneFileAndDeal(path); err != nil {
				log.Fatal(err)
				return nil
			}
			return nil
		})
	} else {
		fmt.Printf("%s 是一个文件\n", path)
		// 还可以获取更多信息
		fmt.Printf("文件大小: %d bytes\n", fileInfo.Size())
		fmt.Printf("修改时间: %v\n", fileInfo.ModTime())
		if err := ReadOneFileAndDeal(path); err != nil {
			fmt.Println(err)
		}
	}

}

func ReadOneFileAndDeal(sourcePath string) error {
	// 方法1: 使用解析
	fmt.Println("=== 方法1: 解析 ===")
	simpleRdf, err := util.ParseCIMXML(sourcePath)

	if simpleRdf == nil {
		log.Fatal("xml read fail!")
		return err
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("解析成功\n")

		fmt.Printf("\n统计信息:\n")
		fmt.Printf("Terminal: %d\n", len(simpleRdf.Terminals))
	}

	idNodeMap, nodeIdMap, deviceFeederMap := util.GetTopoMap(simpleRdf)
	fmt.Println("idNodeMap:", len(idNodeMap))
	fmt.Println(idNodeMap)
	fmt.Println("nodeIdMap:", len(nodeIdMap))
	fmt.Println(nodeIdMap)

	// 获取ID对应的云ID
	rdfDCloudMap, dcloudList := util.GetDCloudIDList(config, db, idNodeMap)
	nodeMap := util.GetDCloudNodeIDList(config, db, nodeIdMap, owner)
	// 获取馈线id和主馈线标识todo:转换配网id中应该连接主网的部分
	for _, circuit := range simpleRdf.Circuits {
		var feeder util.FeederC
		if result := db.Table(config.DB.Database+".SG_CON_FEEDERLINE_C").
			Where("PMS_RDF_ID = ?", circuit.ID).
			Find(&feeder); result.Error != nil {
			log.Fatal(result.Error)
		}
		data.CircuitFeederMap[circuit.ID] = feeder.DCloudID
		if feeder.DCloudID != "" {
			// todo: feat
			//modelJoin := util.GetFeederJoin(db, config, feeder.DCloudID)
		}
		if circuit.IsCurrentFeeder == "1" { // 主馈线
			owner = feeder.Owner
			data.CircuitMainFeederMap[circuit.ID] = true
			log.Println("Read File: " + sourcePath + ", owner:" + owner)
			break
		}
	}
	// 获取ID相关的Topo
	var topoList []util.Topo
	result := db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
		Where("ID in ?", dcloudList).
		Find(&topoList)
	if result.Error != nil {
		log.Printf("❌ 查询拓扑数据失败: error=%v", result.Error)
	}

	fmt.Println("topoList:", len(topoList))

	util.HandleTopo(idNodeMap, nodeIdMap, topoList, rdfDCloudMap, nodeMap, deviceFeederMap, db, config, owner)

	return nil
}
