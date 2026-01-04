package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"raselper/src/forwork/read_model/data"
	"strconv"
	"time"

	"io/ioutil"

	dameng "github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
)

// 配置文件结构
type Config struct {
	DB struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
		IP       string `yaml:"ip"`
		Port     string `yaml:"port"`
	} `yaml:"db"`
	API struct {
		URL string `yaml:"url"`
	} `yaml:"api"`
}

// SGDevDBreakerHNStatus 开关状态历史表
type HNStatus struct {
	UpdateTime   time.Time `gorm:"column:UPDATE_TIME" json:"updateTime"`
	DatasourceID string    `gorm:"column:DATASOURCE_ID" json:"datasourceId"`
	ID           string    `gorm:"column:ID;primaryKey" json:"id"`
	Name         string    `gorm:"column:NAME" json:"name"`
	FeederID     string    `gorm:"column:FEEDER_ID" json:"feederId"`
	StatusValue  int       `gorm:"column:STATUS_VALUE" json:"statusValue"`
	ChangeReason int       `gorm:"column:CHANGE_REASON" json:"changeReason"`
}

// API请求结构
type BreakerYXRequest struct {
	Data []BreakerYXData `json:"data"`
}

type BreakerYXData struct {
	DataSourceID string `json:"dataSourceId"`
	ID           string `json:"id"`
	StatusValue  int    `json:"statusValue"`
	UpdateTime   string `json:"updateTime"`
}

func main() {
	// 1. 读取配置文件
	config, err := data.ReadAppConfig("application.yaml")
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	// 2. 连接达梦数据库
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s", config.DB.Username, config.DB.Password, config.DB.IP, config.DB.Port)
	data.DB, err = gorm.Open(dameng.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}

	args := os.Args
	if len(args) < 3 {
		fmt.Println("需要同步的表名和时间差作为参数! (./programName SG_DEV_DBREAKER_H_NSTATUS -10)")
	}

	// 3. 获取当前日期并生成表名
	timeGap, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Println("time format fail! use -10 as default")
		timeGap = -10
	}
	queryTime := time.Now().Add(time.Duration(timeGap) * time.Minute)
	queryTimeFormat := queryTime.Format("2006-01-02 15:04:05")
	fmt.Println("queryTimeFormat:", queryTimeFormat)
	fmt.Println("queryTable:", config.DB.Database+"."+args[1])

	// 4. 查询数据
	var records []HNStatus
	result := data.DB.Table(config.DB.Database+"."+args[1]).
		Where("UPDATE_TIME > ?", queryTimeFormat).
		Find(&records)
	if result.Error != nil {
		log.Fatalf("查询数据失败: %v", result.Error)
	}

	log.Printf("查询到 %d 条记录", len(records))

	if len(records) == 0 {
		log.Println("没有找到需要处理的数据")
		return
	}

	// 5. 转换为API请求格式
	var apiDataList [][]BreakerYXData
	index := 0
	for _, record := range records {
		apiData := apiDataList[index]
		if len(apiData) > config.Slice {
			index++
			fmt.Println("apiData new index:", index)
		}
		apiData = append(apiData, BreakerYXData{
			DataSourceID: record.DatasourceID,
			ID:           record.ID,
			StatusValue:  record.StatusValue,
			UpdateTime:   record.UpdateTime.Format("2006-01-02 15:04:05"),
		})
	}

	for _, apiData := range apiDataList {
		requestBody := BreakerYXRequest{
			Data: apiData,
		}

		// 6. 转换为JSON
		jsonData, err := json.Marshal(requestBody)
		if err != nil {
			log.Fatalf("JSON序列化失败: %v", err)
		}

		log.Printf("请求数据: %s", string(jsonData))

		// 7. 发送POST请求
		resp, err := http.Post("http://"+config.UpdateUrl+"/DataSaveGuoDiao/DataSaveController/savebreakerYX", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Fatalf("发送POST请求失败: %v", err)
		}
		defer resp.Body.Close()

		// 8. 处理响应
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("读取响应失败: %v", err)
		}

		log.Printf("响应状态码: %d", resp.StatusCode)
		log.Printf("响应内容: %s", string(body))

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Println("数据保存成功")
		} else {
			log.Println("数据保存失败")
		}
	}
}
