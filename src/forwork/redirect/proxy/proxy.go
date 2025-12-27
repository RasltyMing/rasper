package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"raselper/src/forwork/read_model/data"

	dameng "github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
)

// 数据库模型
type SGConFeederC struct {
	DcloudID string `gorm:"column:DCLOUD_ID"`
	PmsRdfID string `gorm:"column:PMS_RDF_ID"`
}

func (SGConFeederC) TableName() string {
	return "SG_CON_FEEDERLINE_C"
}

var db *gorm.DB

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

	// 目标服务地址
	targetURL := "http://localhost:8081"
	target, _ := url.Parse(targetURL)
	proxy := httputil.NewSingleHostReverseProxy(target)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		devType := r.URL.Query().Get("devType")
		devID := r.URL.Query().Get("devId")

		log.Printf("收到请求 - devType: %s, devId: %s", devType, devID)

		// 如果不是feeder类型，直接转发
		if devType != "feeder" || devID == "" {
			proxy.ServeHTTP(w, r)
			return
		}

		// 查询映射关系
		var feeder SGConFeederC
		result := db.Where("DCLOUD_ID = ?", devID).First(&feeder)
		if result.Error != nil {
			log.Printf("映射失败: devId=%s, error=%v", devID, result.Error)
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "❌ 设备未找到: devId=%s", devID)
			return
		}

		log.Printf("映射成功: devId=%s -> feederId=%s", devID, feeder.PmsRdfID)

		// 创建新请求
		newReq := r.Clone(r.Context())
		query := newReq.URL.Query()
		query.Del("devType")
		query.Del("devId")
		query.Set("feederId", feeder.PmsRdfID)
		newReq.URL.RawQuery = query.Encode()

		// 转发请求
		proxy.ServeHTTP(w, newReq)
	})

	log.Println("转发服务启动在 :8080")
	log.Println("测试URL: http://localhost:8080/?devType=feeder&devId=1701555")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
