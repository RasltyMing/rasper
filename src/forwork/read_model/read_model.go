package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"raselper/src/forwork/read_model/data"
	"raselper/src/forwork/read_model/util"
	"raselper/src/secondary/utils"
	"strings"

	dameng "github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
)

var db *gorm.DB

// 主函数
func main() {
	newConfig, err := data.ReadAppConfig("app.yaml")
	data.Config = *newConfig
	if err != nil {
		log.Printf("读取配置失败: %v", err)
	}

	// 连接达梦数据库
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s", data.Config.DB.Username, data.Config.DB.Password, data.Config.DB.IP, data.Config.DB.Port)
	db, err = gorm.Open(dameng.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Print("连接数据库失败:", err)
		return
	}
	data.DB = db

	args := os.Args
	fmt.Println("args:", args)

	path := args[1]
	fileInfo, err := os.Stat(args[1])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	if fileInfo.IsDir() {
		// 创建线程池，3个worker，任务队列大小100
		pool := utils.NewWorkerPool(data.Config.ThreadPool, 5000)
		defer pool.Close()

		fmt.Printf("%s 是一个文件夹\n", path)
		err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
			if !strings.HasSuffix(filepath.Base(path), ".xml") {
				log.Println("pass file:" + path)
				return nil
			}

			pool.Submit(func() {
				fmt.Println("read file:" + path)
				if err = ReadOneFileAndDeal(path); err != nil {
					log.Println(err)
				} else {
					fmt.Println("file done:", path)
				}
			})

			return nil
		})

		// 等待所有任务完成
		pool.Wait()
		fmt.Println("All tasks completed")
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
	var owner string

	defer func() {
		err := recover()
		if err != nil {
			log.Println(err)
		}
	}()

	// 方法1: 使用解析
	fmt.Println("=== 方法1: 解析 ===")
	simpleRdf, err := util.ParseCIMXML(sourcePath)

	if simpleRdf == nil {
		log.Println("xml read fail!")
		return err
	}

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("解析成功\n")

		fmt.Printf("\n统计信息:\n")
		fmt.Printf("Terminal: %d\n", len(simpleRdf.Terminals))
	}

	// 获取馈线id和主馈线标识
	var circuitDCloudMap = make(map[string]string)
	var circuitMainFeederMap = make(map[string]bool) // 源端馈线ID - 是否主馈线
	fmt.Println("circuit length:", len(simpleRdf.Circuits))
	for i, circuit := range simpleRdf.Circuits {
		fmt.Println("Feeder:", i, ":", circuit.ID)
		var feeder util.FeederC
		if result := db.Table(data.Config.DB.Database+".SG_CON_FEEDERLINE_C").
			Where("PMS_RDF_ID = ?", circuit.ID).
			Find(&feeder); result.Error != nil {
			log.Println(result.Error)
		}
		circuitDCloudMap[circuit.ID] = feeder.DCloudID
		fmt.Println("Feeder:", circuit.ID, ", DCloud:", feeder.DCloudID)
		if circuit.IsCurrentFeeder == "1" { // 主馈线
			owner = feeder.Owner
			circuitMainFeederMap[circuit.ID] = true
			log.Println("Read File: " + sourcePath + ", owner:" + owner)
		}
	}

	topoList := util.GetDeviceTopoMap(simpleRdf)
	fmt.Println("topoMap:", len(topoList))

	cloud := util.GetTopoInfoInDCloud(topoList, circuitDCloudMap, owner)
	fmt.Println("topoInfo:", len(cloud))

	//util.HandleTopo(idNodeMap, nodeIdMap, topoList, rdfDCloudMap, nodeMap, deviceFeederMap, db, data.Config, owner, simpleRdf, circuitDCloudMap)
	util.HandleDBTopo(cloud, circuitDCloudMap, owner, simpleRdf)
	util.HandleMultiplyNode(cloud, circuitDCloudMap, owner, simpleRdf)
	util.HandleEmptyTopo(cloud)
	util.MainSubConnect(circuitDCloudMap, circuitMainFeederMap)                               // 只处理果老师拼接成功的
	util.MainSubConnectIfNotConnect(circuitDCloudMap, circuitMainFeederMap, cloud, simpleRdf) // 处理没有拼接成功的
	//util.ConnectMultiplyNode(simpleRdf, owner)

	// 提示图库程序更新图库馈线
	for _, circuit := range simpleRdf.Circuits {
		var feeder util.FeederC
		if result := db.Table(data.Config.DB.Database+".SG_CON_FEEDERLINE_C").
			Where("PMS_RDF_ID = ?", circuit.ID).
			Find(&feeder); result.Error != nil {
			log.Print(result.Error)
		}
		if circuit.IsCurrentFeeder == "1" { // 主馈线
			owner = feeder.Owner
			circuitMainFeederMap[circuit.ID] = true
			log.Println("Update Feeder: " + sourcePath + ", owner:" + owner + ", feeder:" + circuit.ID)
			if _, err := httpGet(data.Config.UpdateUrl + "/" + feeder.DCloudID + "/" + owner); err != nil {
				log.Println(err)
			}
		}
	}

	log.Println("ReadFileDone: ", sourcePath)

	if data.Config.Delete {
		if err := os.Remove(sourcePath); err != nil {
			log.Print(err)
		}
		log.Println("Remove File: " + sourcePath)
	}

	return nil
}

// 请求http
func httpGet(url string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}
