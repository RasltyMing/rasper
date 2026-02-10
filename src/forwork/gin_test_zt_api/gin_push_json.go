package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

func main() {
	// 初始化 Gin 路由
	router := gin.Default()

	// 添加 CORS 支持（可选）
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	})

	// 默认路由：返回所有可用的 JSON 文件列表
	router.GET("/", func(c *gin.Context) {
		files, err := ioutil.ReadDir(".")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to read directory",
			})
			return
		}

		var jsonFiles []string
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
				jsonFiles = append(jsonFiles, file.Name())
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"available_json_files": jsonFiles,
			"usage":                "Use /json?file=filename (without .json extension) to get JSON content",
		})
	})

	// 主要功能：通过 file 参数读取对应的 ${file}.json
	router.GET("/json", func(c *gin.Context) {
		// 1. 获取 file 参数
		fileName := c.Query("file")
		page := c.Query("pageNum")
		if fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "Missing 'file' query parameter",
				"example": "/json?file=data",
				"note":    "The .json extension will be automatically added",
			})
			return
		}

		// 2. 清理文件名，防止目录遍历攻击
		fileName = strings.TrimSpace(fileName)

		// 移除可能的 .json 后缀（如果有的话，我们会自动添加）
		fileName = strings.TrimSuffix(fileName, ".json")

		// 检查文件名是否包含路径分隔符或其他不安全字符
		if strings.Contains(fileName, "/") || strings.Contains(fileName, "\\") ||
			strings.Contains(fileName, "..") || strings.Contains(fileName, ":") ||
			fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid file name",
			})
			return
		}

		// 3. 构建完整的文件路径
		jsonFilePath := filepath.Join("src/forwork/gin_test_zt_api", fileName+page+".json")

		// 4. 检查文件是否存在
		fileInfo, err := os.Stat(jsonFilePath)
		if os.IsNotExist(err) {
			// 提供更详细的错误信息
			c.JSON(http.StatusNotFound, gin.H{
				"error":          "JSON file not found",
				"requested_file": fileName,
				"full_path":      jsonFilePath,
				"tip":            "Make sure the file exists in the same directory as the server",
			})
			return
		}

		// 5. 检查是否是目录
		if fileInfo.IsDir() {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Requested path is a directory, not a file",
			})
			return
		}

		// 6. 读取文件内容
		fileContent, err := ioutil.ReadFile(jsonFilePath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "Failed to read JSON file",
				"details": err.Error(),
			})
			return
		}

		// 7. 验证是否为有效的 JSON
		var jsonData interface{}
		if err := json.Unmarshal(fileContent, &jsonData); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "Invalid JSON format",
				"file":    jsonFilePath,
				"details": err.Error(),
			})
			return
		}

		// 8. 返回 JSON 内容
		c.JSON(http.StatusOK, jsonData)
	})

	// 扩展功能：支持直接指定完整文件名（包含 .json 后缀）
	router.GET("/json/raw", func(c *gin.Context) {
		fileName := c.Query("filename")
		if fileName == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Missing 'filename' query parameter",
			})
			return
		}

		// 检查文件扩展名
		if !strings.HasSuffix(strings.ToLower(fileName), ".json") {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Only .json files are allowed",
			})
			return
		}

		// 安全检查和读取文件
		absPath, err := filepath.Abs(fileName)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Invalid file path",
			})
			return
		}

		// 读取并返回文件
		fileContent, err := ioutil.ReadFile(absPath)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":   "Failed to read file",
				"details": err.Error(),
			})
			return
		}

		var jsonData interface{}
		if err := json.Unmarshal(fileContent, &jsonData); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "Invalid JSON format",
				"details": err.Error(),
			})
			return
		}

		c.JSON(http.StatusOK, jsonData)
	})

	// 批量获取多个 JSON 文件
	router.GET("/json/batch", func(c *gin.Context) {
		filesParam := c.Query("files")
		if filesParam == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error":   "Missing 'files' query parameter",
				"example": "/json/batch?files=file1,file2,file3",
			})
			return
		}

		fileNames := strings.Split(filesParam, ",")
		results := make(map[string]interface{})
		errors := make(map[string]string)

		for _, fileName := range fileNames {
			fileName = strings.TrimSpace(fileName)
			fileName = strings.TrimSuffix(fileName, ".json")

			if fileName == "" {
				continue
			}

			jsonFilePath := fileName + ".json"
			fileContent, err := ioutil.ReadFile(jsonFilePath)
			if err != nil {
				errors[fileName] = err.Error()
				continue
			}

			var jsonData interface{}
			if err := json.Unmarshal(fileContent, &jsonData); err != nil {
				errors[fileName] = "Invalid JSON format: " + err.Error()
				continue
			}

			results[fileName] = jsonData
		}

		response := gin.H{
			"successful": results,
		}

		if len(errors) > 0 {
			response["errors"] = errors
		}

		c.JSON(http.StatusOK, response)
	})

	// 列出所有可用的 JSON 文件
	router.GET("/json/list", func(c *gin.Context) {
		files, err := ioutil.ReadDir(".")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to read directory",
			})
			return
		}

		var jsonFiles []gin.H
		for _, file := range files {
			if !file.IsDir() && strings.HasSuffix(file.Name(), ".json") {
				jsonFiles = append(jsonFiles, gin.H{
					"name":     file.Name(),
					"size":     file.Size(),
					"modified": file.ModTime().Format("2006-01-02 15:04:05"),
					"url":      "/json?file=" + strings.TrimSuffix(file.Name(), ".json"),
				})
			}
		}

		c.JSON(http.StatusOK, gin.H{
			"count":             len(jsonFiles),
			"files":             jsonFiles,
			"current_directory": getCurrentDir(),
		})
	})

	// 健康检查端点
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":  "healthy",
			"service": "json-file-server",
			"endpoints": gin.H{
				"get_json":   "/json?file=filename",
				"list_files": "/json/list",
				"batch_get":  "/json/batch?files=file1,file2",
			},
		})
	})

	// 启动服务器
	log.Println("Starting JSON File Server on :8080")
	log.Println("Available endpoints:")
	log.Println("  GET /              - Show usage information")
	log.Println("  GET /json?file=    - Get specific JSON file (without .json extension)")
	log.Println("  GET /json/list     - List all available JSON files")
	log.Println("  GET /json/batch    - Get multiple JSON files at once")
	log.Println("  GET /health        - Health check")

	if err := router.Run(":8080"); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

// 辅助函数：获取当前目录
func getCurrentDir() string {
	dir, err := os.Getwd()
	if err != nil {
		return "unknown"
	}
	return dir
}
