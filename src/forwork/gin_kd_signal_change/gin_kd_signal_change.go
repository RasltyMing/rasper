package main

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
)

// 请求数据结构
type DataItem struct {
	DataSourceId string `json:"dataSourceId"`
	ID           string `json:"id"`
	Name         string `json:"name"`
	ChangeReason int    `json:"changeReason"`
	FeederId     string `json:"feederId"`
	StatusValue  int    `json:"statusValue"`
	OccurTime    string `json:"occurTime"`
}

// StatusUpdateRequest 状态更新请求
type StatusUpdateRequest struct {
	Data []DeviceStatus `json:"data"`
}

// DeviceStatus 设备状态
type DeviceStatus struct {
	DataSourceID string `json:"dataSourceId"`
	ID           string `json:"id"`
	StatusValue  int    `json:"statusValue"`
	UpdateTime   string `json:"updateTime"`
}

type RequestData struct {
	Data []DataItem `json:"data"`
}

// 响应数据结构
type ResponseData struct {
	SuccessCount int      `json:"successCount"`
	AllCount     int      `json:"allCount"`
	AllSuccess   bool     `json:"allSuccess"`
	ErrorArray   []string `json:"errorArray"`
}

func main() {
	r := gin.Default()

	// 统一的处理函数
	handleSave := func(c *gin.Context) {
		var req RequestData

		// 绑定JSON请求体
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON format"})
			return
		}

		// 处理数据（这里可以根据实际需求进行业务逻辑处理）
		allCount := len(req.Data)
		successCount := allCount // 假设全部成功，实际应根据处理结果调整
		allSuccess := true       // 假设全部成功，实际应根据处理结果调整
		errorArray := []string{} // 假设没有错误，实际应根据处理结果调整

		// 构建响应
		resp := ResponseData{
			SuccessCount: successCount,
			AllCount:     allCount,
			AllSuccess:   allSuccess,
			ErrorArray:   errorArray,
		}

		c.JSON(http.StatusOK, resp)
	}

	// 注册所有路由
	routes := []string{
		"/DataSaveGuoDiao/DataSaveController/saveDbreakerSchange",
		"/DataSaveGuoDiao/DataSaveController/saveDLoadSwitchSchange",
		"/DataSaveGuoDiao/DataSaveController/saveDFuseSchange",
		"/DataSaveGuoDiao/DataSaveController/saveDDisSchange",
		"/DataSaveGuoDiao/DataSaveController/saveDGroundDisSchange",
	}

	for _, route := range routes {
		r.POST(route, handleSave)
	}

	r.POST("/DataSaveGuoDiao/DataSaveController/savebreakerYX", func(c *gin.Context) {
		var req StatusUpdateRequest
		// 绑定JSON请求体
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON format"})
			return
		}
		fmt.Println("req:", req)

		c.JSON(http.StatusOK, map[string]interface{}{
			"success": true,
		})
	})

	// 启动服务器
	r.Run(":18080")
}
