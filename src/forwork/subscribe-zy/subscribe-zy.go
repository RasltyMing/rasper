package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"raselper/src/forwork/first"
	"raselper/src/forwork/read_model/data"
	"strings"

	"github.com/go-redis/redis/v8"
	dameng "github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
	"gorm.io/gorm/utils"
)

var (
	rdb *redis.Client
	ctx = context.Background()
)

// 初始化Redis连接
func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr:     data.Config.Redis.Url,      // Redis地址
		Username: data.Config.Redis.Username, // 用户名
		Password: data.Config.Redis.Password, // 密码
		DB:       data.Config.Redis.DB,       // 数据库
	})

	// 测试连接
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Printf("Failed to connect to Redis: %v", err)
		// 这里不直接退出，允许程序继续运行，但Redis操作会失败
	} else {
		log.Println("Connected to Redis successfully")
	}
}

// 获取Redis set中的所有值
func getRedisSetValues(key string) ([]string, error) {
	if rdb == nil {
		return nil, fmt.Errorf("redis client not initialized")
	}

	// 获取set中的所有成员
	members, err := rdb.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get set values for key %s: %v", key, err)
	}

	return members, nil
}

// 遍历检查9个地市的缓存
func isItemInRedisSetRound(setKey, item string) (bool, error) {
	organList := []string{"350100", "350200", "350300", "350400", "350500", "350600", "350700", "350800", "350900"}
	for _, organ := range organList {
		exist, err := isItemInRedisSet(organ+":"+setKey, "\""+item+"\"")
		if exist {
			return exist, nil
		}
		if err != nil {
			return false, err
		}
	}

	return false, nil
}

// 检查item是否在Redis set中存在
func isItemInRedisSet(setKey, item string) (bool, error) {
	if rdb == nil {
		return false, fmt.Errorf("redis client not initialized")
	}

	// 使用SIsMember命令检查成员是否存在
	exists, err := rdb.SIsMember(ctx, setKey, item).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check set membership for %s in %s: %v", item, setKey, err)
	}

	return exists, nil
}

// 请求结构体
type StartRequest struct {
	BreakerIds    []string `json:"breakerIds"`
	BusIds        []string `json:"busIds"`
	DeviceIds     []string `json:"deviceIds"`
	ParentSvgName *string  `json:"parentSvgName"`
	SvgName       string   `json:"svgName"`
	Type          string   `json:"type"`
}

// 响应结构体
type StartResponse struct {
	OnDevices        []string `json:"onDevices"`
	OffDevices       []string `json:"offDevices"`
	PointOffDevices3 []string `json:"pointOffDevices3"`
	PointOffDevices  []string `json:"pointOffDevices"`
	PointOffDevices2 []string `json:"pointOffDevices2"`
	GroundDevices    []string `json:"groundDevices"`
	PointOnDevices   []string `json:"pointOnDevices"`
}

func zyStartHandler(w http.ResponseWriter, r *http.Request) {
	// 只处理POST请求
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 解析请求体
	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// 合并三个字段到onDevices
	onDevices := make([]string, 0)

	// 添加breakerIds
	onDevices = append(onDevices, req.BreakerIds...)

	// 添加busIds
	onDevices = append(onDevices, req.BusIds...)

	// 添加deviceIds
	onDevices = append(onDevices, req.DeviceIds...)

	// 初始化响应
	response := StartResponse{
		OnDevices:        onDevices, // 这里是合并后的设备列表
		OffDevices:       []string{},
		PointOffDevices3: []string{},
		PointOffDevices:  []string{},
		PointOffDevices2: []string{},
		GroundDevices:    []string{},
		PointOnDevices:   []string{},
	}

	// Redis set的key
	pointOffSetKey := "POINTOFF"

	// 检查每个onDevice是否在POINTOFF set中
	for _, device := range onDevices {
		exists, err := isItemInRedisSetRound(pointOffSetKey, device)
		if err != nil {
			log.Printf("Warning: Failed to check device %s in Redis set: %v", device, err)
			// 如果检查失败，可以选择将设备保留在onDevices中
			continue
		}

		if exists {
			// 如果设备在POINTOFF set中，则添加到OffDevices
			response.OffDevices = append(response.OffDevices, device)
			response.PointOffDevices = response.OffDevices
			log.Printf("Device %s found in POINTOFF set, moved to OffDevices", device)
		}
	}

	// 从OnDevices中移除已经移动到OffDevices的设备
	// 创建一个map来快速查找需要移除的设备
	offDeviceMap := make(map[string]bool)
	for _, device := range response.OffDevices {
		offDeviceMap[device] = true
	}

	// 过滤掉已经在OffDevices中的设备
	filteredOnDevices := make([]string, 0)
	for _, device := range response.OnDevices {
		if !offDeviceMap[device] {
			filteredOnDevices = append(filteredOnDevices, device)
		}
	}
	response.OnDevices = removeRdfId(filteredOnDevices)

	// 查找拓扑, 找出相关馈线的所有拓扑
	list := getTopoList(filteredOnDevices)
	feederList := getTopoFeederList(list) // 找有关的馈线
	var totalTopoList []first.Topo
	for _, feeder := range feederList {
		topoList := queryTopoData(feeder)
		totalTopoList = append(totalTopoList, topoList...)
	}
	{
		filteredTotalTopoList := make([]first.Topo, 0)
		// 过滤掉OffDevices的设备
		for _, topo := range totalTopoList {
			if offDeviceMap[topo.ID] {
				continue
			}
			filteredTotalTopoList = append(filteredTotalTopoList, topo)
		}
		totalTopoList = filteredTotalTopoList
	}
	graph := first.BuildGraph(totalTopoList)
	for key, connected := range graph {
		fmt.Println(key, ": ", connected)
		if isAllAcLine(connected) {
			fmt.Println("All AcLine - Pass")
			response.OnDevices = removeDeviceList(response.OnDevices, connected)
			//response.OffDevices = append(response.OffDevices, connected...)
			//response.PointOffDevices = response.OffDevices
		}
	}
	log.Printf("topoList list: %v", list)

	// 设置响应头
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if data.Config.Test.On {
		log.Println("test...")
		response.OnDevices = data.Config.Test.OnDevices
		response.OffDevices = data.Config.Test.OffDevices
		response.PointOffDevices3 = data.Config.Test.PointOffDevices3
		response.PointOffDevices = data.Config.Test.PointOffDevices
		response.PointOffDevices2 = data.Config.Test.PointOffDevices2
		response.GroundDevices = data.Config.Test.GroundDevices
		response.PointOnDevices = data.Config.Test.PointOnDevices
	}

	// 返回JSON响应
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Failed to encode response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func getTopoFeederList(list []first.Topo) (feederList []string) {
	for _, item := range list {
		if first.Contain(feederList, item.FeederID) {
			continue
		}
		feederList = append(feederList, item.FeederID)
	}

	return feederList
}

func queryTopoData(feederId string) []first.Topo {
	var topoList []first.Topo
	result := data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
		Where("FEEDER_ID = ?", feederId).
		Find(&topoList)
	if result.Error != nil {
		log.Printf("❌ 查询分组数据失败: FeederID=%s, error=%v",
			feederId, result.Error)
	}
	return topoList
}

func getTopoList(idList []string) (topoList []first.Topo) {
	result := data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
		Where("ID in ?", idList).
		Find(&topoList)
	if result.Error != nil {
		log.Printf("Failed to get topo list: %v", result.Error)
		return
	}
	return topoList
}

func removeDeviceList(list, removeList []string) (resultList []string) {
	for _, item := range list {
		if utils.Contains(removeList, item) {
			continue
		}
		resultList = append(resultList, item)
	}

	return resultList
}

func removeRdfId(list []string) (resultList []string) {
	for _, item := range list {
		if strings.Contains(item, "_") {
			continue
		}
		resultList = append(resultList, item)
	}

	return resultList
}

func isAllAcLine(idList []string) bool {
	for _, item := range idList {
		if strings.HasPrefix(item, "1702") {
			continue
		}
		if strings.HasPrefix(item, "14000000_") {
			continue
		}
		if strings.HasPrefix(item, "20100000_") {
			continue
		}
		return false
	}

	return true
}

func main() {
	config, err := data.ReadAppConfig("application.yaml")
	if err != nil {
		log.Fatalf("Failed to read config: %v", err)
	}
	data.Config = *config
	// 初始化Redis连接
	initRedis()

	// 连接达梦数据库
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s", data.Config.DB.Username, data.Config.DB.Password, data.Config.DB.IP, data.Config.DB.Port)
	db, err := gorm.Open(dameng.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Print("连接数据库失败:", err)
		return
	}
	data.DB = db

	// 注册路由
	http.HandleFunc("/zy/start", zyStartHandler)

	// 启动服务器
	port := ":9201"
	fmt.Printf("Server starting on port %s\n", port)
	fmt.Println("Test with: curl -X POST http://localhost:9201/zy/start -H 'Content-Type: application/json' -d '{\"breakerIds\":[\"111\",\"222\"],\"busIds\":[\"333\"],\"deviceIds\":[\"444\",\"555\"],\"parentSvgName\":null,\"svgName\":\"PT.xxx_tu.svg\",\"type\":\"middle\"}'")

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}
