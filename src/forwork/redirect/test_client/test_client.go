package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func main() {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()

	// 首页 - 显示使用说明
	router.GET("/", func(c *gin.Context) {
		output := `🚀 代理转发测试客户端

📌 使用说明:
1. 测试单个转发:
   GET /test?devId=1701555

2. 批量测试:
   GET /batch

3. 查看映射关系:
   GET /mapping/:devId

4. 直接请求目标服务:
   GET /direct?feederId=1001_11

📊 服务状态:
• 转发服务: http://localhost:8080
• 目标服务: http://localhost:8081

🔍 测试示例:
curl "http://localhost:8082/test?devId=1701555"
`
		c.String(200, output)
	})

	// 测试单个转发
	router.GET("/test", func(c *gin.Context) {
		devID := c.Query("devId")
		if devID == "" {
			c.String(400, "❌ 请提供 devId 参数\n\n示例: /test?devId=1701555")
			return
		}

		url := fmt.Sprintf("http://localhost:8080/?devType=feeder&devId=%s", devID)

		// 发送请求到转发服务
		resp, err := http.Get(url)
		if err != nil {
			c.String(500, fmt.Sprintf("❌ 请求失败: %v", err))
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		var result map[string]interface{}
		json.Unmarshal(body, &result)

		prettyJSON, _ := json.MarshalIndent(result, "", "  ")

		output := fmt.Sprintf(`✅ 转发测试完成

📤 请求信息:
• devId: %s
• devType: feeder
• 请求URL: %s

📥 响应信息:
• 状态码: %d
• 响应内容:
%s

🔗 目标服务收到参数: %v
`, devID, url, resp.StatusCode, string(prettyJSON), result["query"])

		c.String(200, output)
	})

	// 批量测试
	router.GET("/batch", func(c *gin.Context) {
		testCases := []string{"1701555", "1701556", "1701557", "not_found"}

		output := "🧪 批量测试开始\n\n"

		for _, devID := range testCases {
			url := fmt.Sprintf("http://localhost:8080/?devType=feeder&devId=%s", devID)

			resp, err := http.Get(url)
			if err != nil {
				output += fmt.Sprintf("❌ %s: 请求失败 - %v\n", devID, err)
				continue
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var result map[string]interface{}
			json.Unmarshal(body, &result)

			if resp.StatusCode == 200 {
				query := result["query"].(map[string]interface{})
				feederID := ""
				if q, ok := query["feederId"].([]interface{}); ok && len(q) > 0 {
					feederID = q[0].(string)
				}
				output += fmt.Sprintf("✅ %s → %s (状态码: %d)\n", devID, feederID, resp.StatusCode)
			} else {
				output += fmt.Sprintf("❌ %s: 失败 (状态码: %d) - %s\n", devID, resp.StatusCode, string(body))
			}
		}

		output += "\n📊 批量测试完成"
		c.String(200, output)
	})

	// 直接查询映射关系
	router.GET("/mapping/:devId", func(c *gin.Context) {
		devID := c.Param("devId")

		// 直接查询转发服务
		url := fmt.Sprintf("http://localhost:8080/?devType=feeder&devId=%s", devID)
		resp, err := http.Get(url)
		if err != nil {
			c.String(500, fmt.Sprintf("❌ 查询失败: %v", err))
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == 404 {
			c.String(404, fmt.Sprintf("❌ 映射关系不存在\n\n设备ID: %s\n错误: 未找到对应记录", devID))
			return
		}

		body, _ := io.ReadAll(resp.Body)
		var result map[string]interface{}
		json.Unmarshal(body, &result)

		query := result["query"].(map[string]interface{})
		feederID := ""
		if q, ok := query["feederId"].([]interface{}); ok && len(q) > 0 {
			feederID = q[0].(string)
		}

		output := fmt.Sprintf(`📋 映射关系查询

🔑 输入参数:
• dcloud_id (devId): %s

🎯 输出结果:
• pms_rdf_id (feederId): %s

📊 完整响应:
状态码: %d
消息: %s
URL: %s
`, devID, feederID, resp.StatusCode, result["message"], result["url"])

		c.String(200, output)
	})

	// 直接请求目标服务（绕过代理）
	router.GET("/direct", func(c *gin.Context) {
		feederID := c.Query("feederId")
		if feederID == "" {
			c.String(400, "❌ 请提供 feederId 参数\n\n示例: /direct?feederId=1001_11")
			return
		}

		url := fmt.Sprintf("http://localhost:8081/?feederId=%s", feederID)
		resp, err := http.Get(url)
		if err != nil {
			c.String(500, fmt.Sprintf("❌ 请求失败: %v", err))
			return
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		var result map[string]interface{}
		json.Unmarshal(body, &result)

		prettyJSON, _ := json.MarshalIndent(result, "", "  ")

		output := fmt.Sprintf(`🎯 直接请求目标服务

📤 请求信息:
• feederId: %s
• 目标URL: %s

📥 响应结果:
%s
`, feederID, url, string(prettyJSON))

		c.String(200, output)
	})

	// 健康检查
	router.GET("/health", func(c *gin.Context) {
		services := map[string]string{
			"转发服务 (8080)": "http://localhost:8080",
			"目标服务 (8081)": "http://localhost:8081",
		}

		output := "🏥 服务健康检查\n\n"
		allHealthy := true

		for name, url := range services {
			resp, err := http.Get(url)
			if err != nil || resp.StatusCode >= 500 {
				output += fmt.Sprintf("❌ %s: 异常 - %v\n", name, err)
				allHealthy = false
			} else {
				output += fmt.Sprintf("✅ %s: 正常 (状态码: %d)\n", name, resp.StatusCode)
			}
			if resp != nil {
				resp.Body.Close()
			}
		}

		if allHealthy {
			output += "\n🎉 所有服务运行正常！"
		} else {
			output += "\n⚠️ 部分服务异常，请检查！"
		}

		c.String(200, output)
	})

	// 显示日志
	router.GET("/log", func(c *gin.Context) {
		// 测试几个请求以生成日志
		testURLs := []string{
			"http://localhost:8080/?devType=feeder&devId=1701555",
			"http://localhost:8080/?devType=feeder&devId=not_found",
			"http://localhost:8081/?feederId=test",
		}

		output := "📝 最近请求日志\n\n"

		for _, url := range testURLs {
			resp, err := http.Get(url)
			if err != nil {
				output += fmt.Sprintf("❌ %s\n   错误: %v\n\n", url, err)
			} else {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()

				var result map[string]interface{}
				json.Unmarshal(body, &result)

				status := "✅"
				if resp.StatusCode >= 400 {
					status = "❌"
				}

				output += fmt.Sprintf("%s %s\n   状态码: %d\n   响应: %s\n\n",
					status, url, resp.StatusCode,
					strings.ReplaceAll(string(body), "\n", " "))
			}
		}

		c.String(200, output)
	})

	log.Println("测试客户端启动在 :8082")
	log.Println("访问 http://localhost:8082 查看使用说明")
	router.Run(":8082")
}
