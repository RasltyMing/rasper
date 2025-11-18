package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// SG186ZpqEntity 对应Java中的实体类
type SG186ZpqEntity struct {
	DistStaId     string `json:"dist_sta_id"`
	MgtOrgCode    string `json:"mgt_org_code"`
	PublClgFlag   string `json:"publ_clg_flag"`
	ResrcSuplCode string `json:"resrc_supl_code"`
	StatYm        string `json:"stat_ym"`
	ZNum          string `json:"z_num"`
	Ds            string `json:"ds"`
}

func main() {
	r := gin.Default()

	// 定义路由，参考你提供的URL路径
	r.GET("/cst/yx2/rds/get_ads_cst_yx2_k_zpq_tqsum", getDataHandler)

	// 启动服务
	r.Run(":8080")
}

// getDataHandler 处理数据请求
func getDataHandler(c *gin.Context) {
	// 获取查询参数
	appCode := c.Query("appCode")
	returnTotalNum := c.Query("returnTotalNum")

	// 验证appCode（简单示例）
	if appCode == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "缺少appCode参数",
		})
		return
	}

	// 模拟数据查询
	data := getMockData()

	// 根据returnTotalNum参数决定返回格式
	if returnTotalNum == "true" {
		c.JSON(http.StatusOK, gin.H{
			"data":        data,
			"totalNumber": len(data),
		})
	} else {
		c.JSON(http.StatusOK, data)
	}
}

// getMockData 生成模拟数据
func getMockData() []SG186ZpqEntity {
	return []SG186ZpqEntity{
		{
			DistStaId:     "001",
			MgtOrgCode:    "org001",
			PublClgFlag:   "1",
			ResrcSuplCode: "tg001",
			StatYm:        "202301",
			ZNum:          "100",
			Ds:            time.Now().Format("2006-01-02"),
		},
		{
			DistStaId:     "002",
			MgtOrgCode:    "org002",
			PublClgFlag:   "0",
			ResrcSuplCode: "tg002",
			StatYm:        "202302",
			ZNum:          "200",
			Ds:            time.Now().Format("2006-01-02"),
		},
	}
}
