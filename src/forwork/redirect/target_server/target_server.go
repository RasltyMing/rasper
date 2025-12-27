package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()

		// 创建响应
		response := map[string]interface{}{
			"timestamp": time.Now().Format("2006-01-02 15:04:05"),
			"method":    r.Method,
			"url":       r.URL.String(),
			"query":     query,
		}

		// 特别处理feederId
		if feederID := query.Get("feederId"); feederID != "" {
			response["message"] = fmt.Sprintf("✅ 成功接收feeder设备，ID: %s", feederID)
			response["status"] = "success"
		} else {
			response["message"] = "⚠️ 未接收到feederId参数"
			response["status"] = "warning"
		}

		// 返回JSON
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)

		// 日志
		log.Printf("目标服务收到: %s %s", r.Method, r.URL.String())
	})

	log.Println("目标服务启动在 :8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
