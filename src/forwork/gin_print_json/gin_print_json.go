package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// 定义固定的响应结构
type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func main() {
	// 创建 Gin 引擎
	r := gin.Default()

	// 定义 POST 接口
	r.POST("/api/echo", func(c *gin.Context) {
		// 读取请求体的原始数据
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, Response{
				Code:    400,
				Message: "读取请求体失败",
			})
			return
		}

		// 恢复请求体以便后续可能的处理
		c.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		// 打印接收到的原始 JSON 字符串
		log.Println("=== 接收到请求 ===")
		log.Printf("请求路径: %s", c.Request.URL.Path)
		log.Printf("请求方法: %s", c.Request.Method)
		log.Println("请求头:")
		for key, values := range c.Request.Header {
			for _, value := range values {
				log.Printf("  %s: %s", key, value)
			}
		}
		log.Println("请求体 (原始):")
		fmt.Println(string(body))

		// 尝试解析为 JSON 并格式化输出
		if len(body) > 0 {
			var jsonData interface{}
			if err := json.Unmarshal(body, &jsonData); err == nil {
				log.Println("请求体 (格式化 JSON):")
				prettyJSON, _ := json.MarshalIndent(jsonData, "", "  ")
				fmt.Println(string(prettyJSON))
			} else {
				log.Printf("非 JSON 格式数据: %s", string(body))
			}
		}
		log.Println("=== 请求结束 ===")

		// 返回固定的 JSON 响应
		c.JSON(http.StatusOK, Response{
			Code:    0,
			Message: "操作成功",
		})
	})

	// 可以处理任意请求方法和路径的通用接口
	r.Any("/api/debug/*path", func(c *gin.Context) {
		// 读取请求体的原始数据
		body, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, Response{
				Code:    400,
				Message: "读取请求体失败",
			})
			return
		}

		// 恢复请求体
		c.Request.Body = io.NopCloser(bytes.NewBuffer(body))

		// 详细打印所有信息
		log.Println("\n=== 调试信息 ===")
		log.Printf("时间: %v", c.GetHeader("Date"))
		log.Printf("客户端IP: %s", c.ClientIP())
		log.Printf("请求URL: %s", c.Request.URL.String())
		log.Printf("请求方法: %s", c.Request.Method)
		log.Printf("完整路径: %s", c.FullPath())

		log.Println("请求参数:")
		for key, values := range c.Request.URL.Query() {
			log.Printf("  %s: %v", key, values)
		}

		log.Println("请求头:")
		for key, values := range c.Request.Header {
			for _, value := range values {
				log.Printf("  %s: %s", key, value)
			}
		}

		if len(body) > 0 {
			log.Printf("Content-Type: %s", c.GetHeader("Content-Type"))
			log.Println("请求体:")

			// 尝试根据 Content-Type 处理
			contentType := c.GetHeader("Content-Type")
			if contentType == "application/json" || contentType == "text/json" {
				var jsonData interface{}
				if err := json.Unmarshal(body, &jsonData); err == nil {
					prettyJSON, _ := json.MarshalIndent(jsonData, "", "  ")
					fmt.Println(string(prettyJSON))
				} else {
					fmt.Printf("无效的JSON: %s\n", string(body))
				}
			} else if contentType == "application/x-www-form-urlencoded" {
				if err := c.Request.ParseForm(); err == nil {
					for key, values := range c.Request.PostForm {
						for _, value := range values {
							log.Printf("  %s: %s", key, value)
						}
					}
				}
			} else {
				fmt.Println("json:")
				var jsonData interface{}
				if err := json.Unmarshal(body, &jsonData); err == nil {
					prettyJSON, _ := json.MarshalIndent(jsonData, "", "  ")
					fmt.Println(string(prettyJSON))
				}
				fmt.Printf("原始数据: %s\n", string(body))
			}
		} else {
			log.Println("请求体: 空")
		}
		log.Println("=== 调试结束 ===\n")

		// 返回固定的 JSON 响应
		c.JSON(http.StatusOK, Response{
			Code:    0,
			Message: "操作成功",
		})
	})

	// 启动服务器
	log.Println("JSON 回显服务启动")
	log.Println("服务器地址: http://localhost:7780")
	log.Println("测试接口: POST http://localhost:7780/api/echo")
	log.Println("调试接口: ANY http://localhost:7780/api/debug/任意路径")

	if err := r.Run(":7780"); err != nil {
		log.Fatal("服务器启动失败:", err)
	}
}
