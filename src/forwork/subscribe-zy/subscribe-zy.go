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

// 遍历检查9个地市的缓存
func isItemInRedisHashRoundHasValue(setKey string) (bool, error) {
	organList := []string{"0021350100", "0021350200", "0021350300", "0021350400", "0021350500", "0021350600", "0021350700", "0021350800", "0021350900"}
	for _, organ := range organList {
		exist, err := isItemInRedisHashHasValue(organ + ":" + setKey)
		if exist {
			return true, nil
		}
		if err != nil {
			return true, err
		}
	}

	return false, nil
}

func isItemInRedisHashHasValue(setKey string) (bool, error) {
	if rdb == nil {
		return false, fmt.Errorf("redis client not initialized")
	}

	// 使用SIsMember命令检查成员是否存在
	exists, err := rdb.HGetAll(ctx, setKey).Result()
	log.Println(setKey, " hash:", exists)
	for k, v := range exists {
		if k == "updateTime" {
			continue
		}
		if v != "\"0.000\"" {
			log.Println("setKey:", setKey)
			return true, nil
		}
	}
	if err != nil {
		return false, fmt.Errorf("failed to check hash in %s: %v", setKey, err)
	}

	return false, nil
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

	// 查找拓扑, 找出相关馈线的所有拓扑
	list := getTopoList(onDevices)
	mainList := getMainTopoList(onDevices)
	log.Println("mainList:", mainList)
	feederList := getTopoFeederList(list) // 找有关的馈线
	var totalTopoList []first.Topo
	totalTopoList = append(totalTopoList, mainList...) // 添加主网部分的拓扑
	for _, feeder := range feederList {
		topoList := queryTopoData(feeder)
		totalTopoList = append(totalTopoList, topoList...)
	}
	var totalDevice []string
	for _, topo := range totalTopoList {
		totalDevice = append(totalDevice, topo.ID)
	}
	// 检查每个onDevice是否在POINTOFF set中
	for _, device := range totalDevice {
		exists, err := isItemInRedisSetRound(pointOffSetKey, device)
		log.Println("query device:" + device)
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

	response.OnDevices = totalDevice // 默认等于全部拓扑
	response.OnDevices = removeDeviceList(response.OnDevices, response.OffDevices)
	response.OnDevices = removeRdfIdAndDuplicate(response.OnDevices)
	{
		filteredTotalTopoList := make([]first.Topo, 0)
		// 过滤掉OffDevices的设备
		for _, topo := range totalTopoList {
			if utils.Contains(response.OffDevices, topo.ID) {
				continue
			}
			filteredTotalTopoList = append(filteredTotalTopoList, topo)
		}
		graph := first.BuildGraph(filteredTotalTopoList)
		for key, connected := range graph {
			fmt.Println(key, ": ", connected)
			if isAllPowerOff(connected) {
				fmt.Println("All PowerOff - Pass")
				response.OnDevices = removeDeviceList(response.OnDevices, connected)
				response.OffDevices = append(response.OffDevices, connected...)
				//response.PointOffDevices = response.OffDevices
			}
		}
		log.Printf("topoList list: %v", list)
	}

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

func getMainTopoList(idList []string) (topoList []first.Topo) {
	result := data.DB.Table(data.Config.DB.Database+".SG_CON_PWRGRID_R_TOPO").
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

func removeRdfIdAndDuplicate(list []string) (resultList []string) {
	for _, item := range list {
		if strings.Contains(item, "_") {
			continue
		}
		if utils.Contains(resultList, item) {
			continue
		}
		resultList = append(resultList, item)
	}

	return resultList
}

func isAllPowerOff(idList []string) bool {
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
		if strings.HasPrefix(item, "1711") {
			//var entityList []map[string]interface{}
			//data.DB.Table("DKY_DB_BIGDATA.SG_DEV_DBUS_H15_MEA_"+time.Now().Format("2006")).
			//	Where("CREATE_DATE = ? and ID = ?", time.Now().Format("2006-01-02"), item).
			//	Find(&entityList)
			//if len(entityList) == 0 {
			//	continue
			//}
			hasValue, err := isItemInRedisHashRoundHasValue("YC:BUS:" + item)
			if !hasValue {
				continue
			}
			if err != nil {
				log.Printf(item + " find error: " + err.Error())
			}
		}
		if strings.HasPrefix(item, "1706") {
			//var entityList []map[string]interface{}
			//data.DB.Table("DKY_DB_BIGDATA.SG_DEV_DBREAKER_H15_MEA_"+time.Now().Format("2006")).
			//	Where("CREATE_DATE = ? and ID = ?", time.Now().Format("2006-01-02"), item).
			//	Find(&entityList)
			//if len(entityList) == 0 {
			//	continue
			//}
			hasValue, err := isItemInRedisHashRoundHasValue("YC:BREAKER:" + item)
			if !hasValue {
				continue
			}
			if err != nil {
				log.Printf(item + " find error: " + err.Error())
			}
		}
		if strings.HasPrefix(item, "1703") {
			//var entityList []map[string]interface{}
			//data.DB.Table("DKY_DB_BIGDATA.SG_DEV_DPWRTRANSFM_H15_MEA_"+time.Now().Format("2006")).
			//	Where("CREATE_DATE = ? and ID = ?", time.Now().Format("2006-01-02"), item).
			//	Find(&entityList)
			//if len(entityList) == 0 {
			//	continue
			//}
			hasValue, err := isItemInRedisHashRoundHasValue("YC:POWERTRANSFORMER:" + item)
			if !hasValue {
				continue
			}
			if err != nil {
				log.Printf(item + " find error: " + err.Error())
			}
		}
		return false
	}

	return true
}

func isAll() {

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
