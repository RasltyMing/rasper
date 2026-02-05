package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// 定义响应结构体
type ApiResponse struct {
	Data struct {
		TotalNum int   `json:"totalNum"`
		PageSize int   `json:"pageSize"`
		Rows     []Row `json:"rows"`
		PageNum  int   `json:"pageNum"`
	} `json:"data"`
	ErrCode   int         `json:"errCode"`
	RequestId string      `json:"requestId"`
	ErrMsg    string      `json:"errMsg"`
	ApiLog    interface{} `json:"apiLog"`
}

// 定义行数据结构体
type Row struct {
	EndTime               string      `json:"end_time"`
	ExtendField1          interface{} `json:"extend_field_1"`
	ExtendField10         interface{} `json:"extend_field_10"`
	ExtendField2          interface{} `json:"extend_field_2"`
	ExtendField3          interface{} `json:"extend_field_3"`
	ExtendField4          interface{} `json:"extend_field_4"`
	ExtendField5          interface{} `json:"extend_field_5"`
	ExtendField6          interface{} `json:"extend_field_6"`
	ExtendField7          interface{} `json:"extend_field_7"`
	ExtendField8          interface{} `json:"extend_field_8"`
	ExtendField9          interface{} `json:"extend_field_9"`
	ExtendFieldOrgCode    string      `json:"extend_field_org_code"`
	ExtendFieldSrcSystem  string      `json:"extend_field_src_system"`
	ExtendFieldTimeStamp  string      `json:"extend_field_time_stamp"`
	ExtendFieldUpdateFlag string      `json:"extend_field_update_flag"`
	ExtendFieldUpdateTime string      `json:"extend_field_update_time"`
	ExtendFieldValidFlag  string      `json:"extend_field_valid_flag"`
	Ghxmmc                interface{} `json:"ghxmmc"`
	Ghxmzt                interface{} `json:"ghxmzt"`
	ID                    string      `json:"id"`
	Kyxmmc                interface{} `json:"kyxmmc"`
	PColName              string      `json:"p_col_name"`
	PColPsrID             string      `json:"p_col_psr_id"`
	StartTime             string      `json:"start_time"`
	Wtqdlx                string      `json:"wtqdlx"`
	WtqdID                string      `json:"wtqd_id"`
	Xmjgsj                interface{} `json:"xmjgsj"`
	XmkyTimeVersion       string      `json:"xmky_time_version"`
	Ds                    string      `json:"ds"`
}

// 生成示例数据
func generateSampleData(param string) ApiResponse {
	var response ApiResponse

	// 设置基本数据
	response.Data.TotalNum = 261613
	response.Data.PageSize = 10
	response.Data.PageNum = 1
	response.ErrCode = 0
	response.ErrMsg = "success"
	response.RequestId = "1401237417688144694846887e6606"
	response.ApiLog = nil

	// 示例行数据
	rows := []Row{
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "00002d19-9fbb-40f6-8e77-8f68670b3248_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "���Ŷ�D��#2��䣨�����",
			PColPsrID:             "00002d19-9fbb-40f6-8e77-8f68670b3248",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "00002d19-9fbb-40f6-8e77-8f68670b3248",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "00003da5-2a84-4ec1-87ef-9f30e303157b_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "�����´�",
			PColPsrID:             "00003da5-2a84-4ec1-87ef-9f30e303157b",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "00003da5-2a84-4ec1-87ef-9f30e303157b",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		// 可以继续添加更多行数据...
	}

	// 添加更多示例数据
	additionalRows := []Row{
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "0000C32D-81ED-4169-A3A9-D4CC4277CE1C_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "10kV������#142֧2����߱�",
			PColPsrID:             "0000C32D-81ED-4169-A3A9-D4CC4277CE1C",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "0000C32D-81ED-4169-A3A9-D4CC4277CE1C",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "0000F33B4DD5488FA780312E7A7AC3214104_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "���¯���",
			PColPsrID:             "0000F33B4DD5488FA780312E7A7AC3214104",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "0000F33B4DD5488FA780312E7A7AC3214104",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "000115a4-a890-42f0-8fe3-2226333a4377_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "����С��",
			PColPsrID:             "000115a4-a890-42f0-8fe3-2226333a4377",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "000115a4-a890-42f0-8fe3-2226333a4377",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "00019981-d6e0-462b-95cf-1566d9063289_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "���´�B3��¥����䣨�����",
			PColPsrID:             "00019981-d6e0-462b-95cf-1566d9063289",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "00019981-d6e0-462b-95cf-1566d9063289",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "0001BA5BC10B4252B4769BE96E0B638B4104_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "����#8����",
			PColPsrID:             "0001BA5BC10B4252B4769BE96E0B638B4104",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "0001BA5BC10B4252B4769BE96E0B638B4104",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "0002C55CFDA544538DF4BB3A3483B6EE0302_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "��������1�ű�",
			PColPsrID:             "0002C55CFDA544538DF4BB3A3483B6EE0302",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "0002C55CFDA544538DF4BB3A3483B6EE0302",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "0003C2E788C74017B0BC96436E5338450110_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "����#5",
			PColPsrID:             "0003C2E788C74017B0BC96436E5338450110",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "0003C2E788C74017B0BC96436E5338450110",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
		{
			EndTime:               "20260117",
			ExtendField1:          nil,
			ExtendField10:         nil,
			ExtendField2:          nil,
			ExtendField3:          nil,
			ExtendField4:          nil,
			ExtendField5:          nil,
			ExtendField6:          nil,
			ExtendField7:          nil,
			ExtendField8:          nil,
			ExtendField9:          nil,
			ExtendFieldOrgCode:    "10220116",
			ExtendFieldSrcSystem:  "pdwghzx",
			ExtendFieldTimeStamp:  "2026-01-19 03:21:56",
			ExtendFieldUpdateFlag: "0",
			ExtendFieldUpdateTime: "2026-01-19 03:21:56",
			ExtendFieldValidFlag:  "1",
			Ghxmmc:                nil,
			Ghxmzt:                nil,
			ID:                    "0005546827AC4AA28FC2DED377D0F0684104_20250118_20260117",
			Kyxmmc:                nil,
			PColName:              "�㸻��䣨�����",
			PColPsrID:             "0005546827AC4AA28FC2DED377D0F0684104",
			StartTime:             "20250118",
			Wtqdlx:                "TQ",
			WtqdID:                "0005546827AC4AA28FC2DED377D0F0684104",
			Xmjgsj:                nil,
			XmkyTimeVersion:       "2026-01-18 00:00:00",
			Ds:                    "20260118",
		},
	}

	// 合并所有行数据
	rows = append(rows, additionalRows...)
	response.Data.Rows = rows

	if param != "1" {
		fmt.Println("param:", param)
		response.Data.Rows = make([]Row, 0)
	}

	return response
}

// API处理函数
func apiHandler(w http.ResponseWriter, r *http.Request) {
	// 设置响应头
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	// 生成响应数据
	pageNum := r.URL.Query().Get("pageNum")
	response := generateSampleData(pageNum)

	// 生成请求ID
	response.RequestId = time.Now().Format("20060102150405") + "_" + "46887e6606"

	// 编码JSON
	jsonData, err := json.MarshalIndent(response, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 写入响应
	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)
}

func main() {
	// 注册路由
	http.HandleFunc("/api/data", apiHandler)

	// 启动服务器
	port := ":8080"
	fmt.Printf("服务器启动，监听端口 %s\n", port)
	fmt.Printf("访问地址：http://localhost%s/api/data\n", port)

	if err := http.ListenAndServe(port, nil); err != nil {
		fmt.Printf("服务器启动失败: %v\n", err)
	}
}
