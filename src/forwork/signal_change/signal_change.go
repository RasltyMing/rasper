package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"io/ioutil"

	dameng "github.com/godoes/gorm-dameng"
	"gopkg.in/yaml.v3"
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

// 数据库表结构
type SgDevDfuseH struct {
	OccurTime    time.Time `gorm:"column:OCCUR_TIME"`
	DatasourceID string    `gorm:"column:DATASOURCE_ID"`
	ID           string    `gorm:"column:ID"`
	Name         string    `gorm:"column:NAME"`
	FeederID     string    `gorm:"column:FEEDER_ID"`
	StatusValue  int       `gorm:"column:STATUS_VALUE"`
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

// 读取配置文件
func ReadAppConfig(filename string) (*Config, error) {
	config := &Config{}

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %v", err)
	}

	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %v", err)
	}

	return config, nil
}

func main() {
	// 1. 读取配置文件
	config, err := ReadAppConfig("app.yaml")
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	// 2. 连接达梦数据库
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s", config.DB.Username, config.DB.Password, config.DB.IP, config.DB.Port)
	db, err := gorm.Open(dameng.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接数据库失败: %v", err)
	}

	// 3. 获取当前日期并生成表名
	now := time.Now()
	year := now.Year()
	tableName := fmt.Sprintf("SG_DEV_DFUSE_H_SCHANGE_%d", year)

	// 设置查询的开始时间（今天凌晨）
	startTime := time.Date(year, now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	startTimeStr := startTime.Format("2006-01-02")

	// 4. 查询数据
	var records []SgDevDfuseH
	query := fmt.Sprintf("SELECT * FROM %s WHERE OCCUR_TIME > ?", tableName)

	result := db.Raw(query, startTimeStr).Scan(&records)
	if result.Error != nil {
		log.Fatalf("查询数据失败: %v", result.Error)
	}

	log.Printf("查询到 %d 条记录", len(records))

	if len(records) == 0 {
		log.Println("没有找到需要处理的数据")
		return
	}

	// 5. 转换为API请求格式
	var apiData []BreakerYXData
	for _, record := range records {
		apiData = append(apiData, BreakerYXData{
			DataSourceID: record.DatasourceID,
			ID:           record.ID,
			StatusValue:  record.StatusValue,
			UpdateTime:   record.OccurTime.Format("2006-01-02 15:04:05"),
		})
	}

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
	apiURL := config.API.URL
	if apiURL == "" {
		apiURL = "http://your-api-server/DataSaveGuoDiao/DataSaveController/savebreakerYX"
	}

	resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(jsonData))
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

// 可选：定时执行任务
func startScheduledTask() {
	ticker := time.NewTicker(1 * time.Hour) // 每小时执行一次
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			log.Println("开始执行定时任务...")
			// 这里可以调用main函数中的逻辑
		}
	}
}
