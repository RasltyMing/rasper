package main

import (
	"bufio"
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"raselper/src/forwork/read_model/data"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	dameng "github.com/godoes/gorm-dameng"
	"gorm.io/gorm"
)

var organMap = map[string]string{
	"35401": "350100",
	"35402": "350200",
	"35404": "350300",
	"35408": "350400",
	"35405": "350500",
	"35406": "350600",
	"35409": "350700",
	"35407": "350800",
	"35403": "350900",
}
var rdfDCloudMap = map[string]string{}

func main() {
	if config, err := data.ReadAppConfig("application.yaml"); err != nil {
		log.Fatal("read config fail!")
	} else {
		data.Config = *config
	}
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s", data.Config.DB.Username, data.Config.DB.Password, data.Config.DB.IP, data.Config.DB.Port)
	if db, err := gorm.Open(dameng.Open(dsn), &gorm.Config{}); err != nil {
		log.Fatal("连接数据库失败:", err)
		return
	} else {
		data.DB = db
	}

	var dataList []map[string]interface{}
	if result := data.DB.Raw("select BREAKER_RDF_ID, DCLOUD_ID from DKYPW.SG_DEV_BREAKER_C, DKYPW.BREAKERID_SMD where D5000_ID = BREAKER_EMS_ID;").
		Find(&dataList); result.Error != nil {
		log.Fatal("read db fail")
	} else {
		for _, entity := range dataList {
			rdfID := entity["BREAKER_RDF_ID"]
			cloudID := entity["DCLOUD_ID"]
			rdfDCloudMap[fmt.Sprintf("%v", rdfID)] = fmt.Sprintf("%v", cloudID)
		}
	}

	// 连接 Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     data.Config.Redis.Url,      // 根据你的 Redis 配置修改
		Password: data.Config.Redis.Password, // 如果没有密码则为空
		DB:       data.Config.Redis.DB,       // 使用默认数据库
	})

	ctx := context.Background()

	// 测试 Redis 连接
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Fatal("Failed to connect to Redis:", err)
	}
	defer rdb.Close()

	// 检查命令行参数
	if len(os.Args) < 3 {
		log.Fatal("Usage: program <file_path> <minutes_to_subtract>")
	}

	filePath := os.Args[1]
	minutesToSubtract := os.Args[2]

	// 计算时间戳
	timestamp, err := generateTimestamp(minutesToSubtract)
	if err != nil {
		log.Fatal("Error generating timestamp:", err)
	}

	fmt.Printf("Looking for files with timestamp: %s\n", timestamp)

	// 读取文件
	filepath.Walk(filePath, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() || !strings.Contains(path, timestamp) {
			return nil
		}

		ids, err := parseFile(path, timestamp)
		if err != nil {
			log.Fatal("Error parsing file:", err)
		}

		// 处理数据并添加到 Redis
		err = processAndStoreToRedis(ctx, rdb, ids)
		if err != nil {
			log.Fatal("Error storing to Redis:", err)
		}

		fmt.Println("Data successfully stored to Redis")

		return nil
	})
}

// 生成时间戳
func generateTimestamp(minutesToSubtract string) (string, error) {
	// 将分钟数转换为整数
	var minutes int
	_, err := fmt.Sscanf(minutesToSubtract, "%d", &minutes)
	if err != nil {
		return "", fmt.Errorf("invalid minutes format: %v", err)
	}

	// 计算当前时间减去指定分钟数
	t := time.Now().Add(time.Duration(minutes) * time.Minute)

	// 格式化为 YYYYMMDDHHM
	return t.Format("200601021504")[:11], nil // 取前11位: YYYYMMDDHHM
}

// 解析文件
func parseFile(filePath, timestamp string) (map[string][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	ids := map[string][]string{}

	scanner := bufio.NewScanner(file)
	insideTable := false
	code := ""

	// 提取 Code 值（假设在第一个包含 Code 的行中）
	scanner2 := bufio.NewScanner(strings.NewReader(""))
	// 重新读取文件来获取 Code
	file2, _ := os.Open(filePath)
	defer file2.Close()
	scanner2 = bufio.NewScanner(file2)
	for scanner2.Scan() {
		line := scanner2.Text()
		if strings.Contains(line, "Code=") {
			// 提取 Code 值
			start := strings.Index(line, "Code='") + 6
			end := strings.Index(line[start:], "'") + start
			if start > 5 && end > start {
				code = line[start:end]
				break
			}
		}
	}

	if code == "" {
		// 如果文件没有 Code，使用默认值
		code = "35408"
	}

	fmt.Printf("Extracted Code: %s\n", code)

	// 重新读取文件处理数据行
	file.Seek(0, 0)
	scanner = bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		// 检查是否在数据表格部分
		if strings.Contains(line, "<SHE_BEI_BIAN_WEI_INFO>") {
			insideTable = true
			continue
		}

		if insideTable && strings.Contains(line, "</SHE_BEI_BIAN_WEI_INFO>") {
			break
		}

		// 处理数据行
		if insideTable && strings.HasPrefix(line, "#") {
			fields := strings.Fields(line)
			if len(fields) >= 6 {
				id := fields[2]     // ID 在第3列
				status := fields[5] // STATUS 在第6列

				if rdfDCloudMap[id] == "" {
					continue
				}

				id = rdfDCloudMap[id]

				// 根据 status 分类
				for smd, organ := range organMap {
					if !strings.Contains(filePath, smd) {
						continue
					}
					if status == "536870913" {
						ids[organ+":POINTON"] = append(ids[organ+"POINTON"], id)
					}
					if status == "536870912" {
						ids[organ+":POINTOFF"] = append(ids[organ+":POINTOFF"], id)
					}
					if status == "805306368" {
						ids[organ+":POINTOFF"] = append(ids[organ+":POINTOFF"], id)
					}
					if status == "805306370" {
						ids[organ+":POINTON"] = append(ids[organ+":POINTON"], id)
					}
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

// 处理数据并存储到 Redis
func processAndStoreToRedis(ctx context.Context, rdb *redis.Client, idMap map[string][]string) error {
	for key, idList := range idMap {
		for _, id := range idList {
			id = "\"" + id + "\""
			err := rdb.SRem(ctx, strings.ReplaceAll(key, "POINTOFF", "POINTON"), id).Err()
			if err != nil {
				return fmt.Errorf("error remove to %s: %v", key, err)
			}
			err = rdb.SRem(ctx, strings.ReplaceAll(key, "POINTON", "POINTOFF"), id).Err()
			if err != nil {
				return fmt.Errorf("error remove to %s: %v", key, err)
			}
			err = rdb.SAdd(ctx, key, id).Err()
			if err != nil {
				return fmt.Errorf("error adding to %s: %v", key, err)
			}
			fmt.Printf("Added %s to %s\n", id, key)
		}
	}

	return nil
}
