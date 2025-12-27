package main

import (
	"database/sql"
	"fmt"
	_ "fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"raselper/src/forwork/read_model/data"
	_ "strings"

	dameng "github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
	// _ "github.com/go-sql-driver/mysql" // MySQL
)

var db *sql.DB

func initDB() error {
	var err error
	// 根据你的数据库配置调整
	connStr := "host=localhost port=5432 user=postgres password=123456 dbname=yourdb sslmode=disable"
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return err
	}
	return db.Ping()
}

func getPMSRdfID(dcloudID string) (string, error) {
	var pmsRdfID string
	query := `SELECT pms_rdf_id FROM SG_CON_FEEDER_C WHERE dcloud_id = $1`
	err := db.QueryRow(query, dcloudID).Scan(&pmsRdfID)
	return pmsRdfID, err
}

func proxyHandler(targetURL string) http.HandlerFunc {
	target, _ := url.Parse(targetURL)
	proxy := httputil.NewSingleHostReverseProxy(target)

	return func(w http.ResponseWriter, r *http.Request) {
		// 只处理特定的devType
		devType := r.URL.Query().Get("devType")
		if devType != "feeder" {
			http.Error(w, "Invalid devType", http.StatusBadRequest)
			return
		}

		// 获取原始devId
		devID := r.URL.Query().Get("devId")
		if devID == "" {
			http.Error(w, "devId is required", http.StatusBadRequest)
			return
		}

		// 查询映射关系
		pmsRdfID, err := getPMSRdfID(devID)
		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "Device not found", http.StatusNotFound)
			} else {
				http.Error(w, "Database error", http.StatusInternalServerError)
			}
			log.Printf("Error querying database: %v", err)
			return
		}

		// 创建新的请求URL
		newURL := *r.URL
		newURL.RawQuery = ""

		// 移除旧的查询参数
		q := newURL.Query()
		q.Set("feederId", pmsRdfID)
		newURL.RawQuery = q.Encode()

		// 修改原始请求
		r.URL = &newURL
		r.Host = target.Host

		log.Printf("Forwarding request: %s -> %s", devID, pmsRdfID)

		// 转发请求
		proxy.ServeHTTP(w, r)
	}
}

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

	if err := initDB(); err != nil {
		log.Fatal("Failed to connect to database:", err)
	}
	defer db.Close()

	// 目标服务地址
	targetURL := "http://target-service:8088"

	http.HandleFunc("/api/", proxyHandler(targetURL))

	log.Println("Proxy server started on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
