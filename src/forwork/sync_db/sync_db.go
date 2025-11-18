package main

import (
	"fmt"
	dameng "github.com/godoes/gorm-dameng"
	"log"
	"reflect"
	"strings"

	_ "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 配置结构体
type SyncConfig struct {
	SourceDB         *gorm.DB
	TargetDB         *gorm.DB
	Tables           []TableConfig
	EnableSoftDelete bool // 是否启用软删除
	HardDelete       bool // 是否硬删除（如果启用软删除，则此配置无效）
	BatchSize        int  // 批量操作大小
}

// 表配置
type TableConfig struct {
	TableName       string   // 表名
	PrimaryKey      []string // 主键字段（支持复合主键）
	Schema1         string   // 数据库1的schema
	Schema2         string   // 数据库2的schema
	SoftDeleteField string   // 软删除字段，如 "deleted_at"
	SoftDeleteValue string   // 软删除值，如 "now()"
}

// 同步结果
type SyncResult struct {
	TableName   string
	Added       int
	Updated     int
	Deleted     int
	SoftDeleted int
	Errors      []error
}

// 主程序
type DBSynchronizer struct {
	config *SyncConfig
}

func NewDBSynchronizer(config *SyncConfig) *DBSynchronizer {
	// 设置默认批量大小
	if config.BatchSize <= 0 {
		config.BatchSize = 1000 // 默认1000条
	}
	return &DBSynchronizer{
		config: config,
	}
}

// 同步所有配置的表
func (s *DBSynchronizer) SyncAll() []SyncResult {
	results := make([]SyncResult, 0, len(s.config.Tables))

	for _, tableConfig := range s.config.Tables {
		result := s.SyncTable(tableConfig)
		results = append(results, result)
	}

	return results
}

// 同步单个表
func (s *DBSynchronizer) SyncTable(config TableConfig) SyncResult {
	result := SyncResult{
		TableName: config.TableName,
	}

	// 1. 从两个数据库读取数据
	sourceData, err := s.fetchData(s.config.SourceDB, config, config.Schema1)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("读取源数据库失败: %w", err))
		return result
	}

	targetData, err := s.fetchData(s.config.TargetDB, config, config.Schema2)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("读取目标数据库失败: %w", err))
		return result
	}

	// 2. 比较数据并执行操作
	added, updated, err := s.syncData(sourceData, targetData, config)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("同步数据失败: %w", err))
		return result
	}
	result.Added = added
	result.Updated = updated

	// 3. 处理删除
	if s.config.EnableSoftDelete && config.SoftDeleteField != "" {
		softDeleted, err := s.handleSoftDelete(sourceData, targetData, config)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("软删除失败: %w", err))
		}
		result.SoftDeleted = softDeleted
	} else if s.config.HardDelete {
		deleted, err := s.handleHardDelete(sourceData, targetData, config)
		if err != nil {
			result.Errors = append(result.Errors, fmt.Errorf("硬删除失败: %w", err))
		}
		result.Deleted = deleted
	}

	return result
}

// 获取数据并转换为Map
func (s *DBSynchronizer) fetchData(db *gorm.DB, config TableConfig, schema string) (map[string]map[string]interface{}, error) {
	var results []map[string]interface{}

	tableName := config.TableName
	if schema != "" {
		tableName = schema + "." + config.TableName
	}

	// 使用GORM的Table方法指定表名
	err := db.Table(tableName).Find(&results).Error
	if err != nil {
		return nil, err
	}

	// 转换为Map格式: Map<主键字符串, 记录Map>
	dataMap := make(map[string]map[string]interface{})
	for _, record := range results {
		key := s.generatePrimaryKey(record, config.PrimaryKey)
		dataMap[key] = record
	}

	return dataMap, nil
}

// 生成主键字符串
func (s *DBSynchronizer) generatePrimaryKey(record map[string]interface{}, primaryKeys []string) string {
	keyParts := make([]string, 0, len(primaryKeys))
	for _, pk := range primaryKeys {
		if value, exists := record[pk]; exists {
			keyParts = append(keyParts, fmt.Sprintf("%v", value))
		}
	}
	return strings.Join(keyParts, "::")
}

// 同步数据：新增和更新
func (s *DBSynchronizer) syncData(sourceData, targetData map[string]map[string]interface{}, config TableConfig) (int, int, error) {
	added := 0
	updated := 0

	tableName := config.TableName
	if config.Schema2 != "" {
		tableName = config.Schema2 + "." + config.TableName
	}

	// 批量操作
	var batchInsert []map[string]interface{}
	var batchUpdate []map[string]interface{}

	for key, sourceRecord := range sourceData {
		targetRecord, exists := targetData[key]

		if !exists {
			// 新增记录
			batchInsert = append(batchInsert, sourceRecord)
			added++
		} else if !s.recordsEqual(sourceRecord, targetRecord, config.PrimaryKey) {
			// 更新记录（确保包含主键）
			updateRecord := make(map[string]interface{})
			for k, v := range sourceRecord {
				updateRecord[k] = v
			}
			// 确保主键字段存在
			for _, pk := range config.PrimaryKey {
				if pkValue, exists := targetRecord[pk]; exists {
					updateRecord[pk] = pkValue
				}
			}
			batchUpdate = append(batchUpdate, updateRecord)
			updated++
		}
	}

	// 执行批量插入（分批次）
	if len(batchInsert) > 0 {
		err := s.batchInsert(tableName, batchInsert)
		if err != nil {
			return added, updated, fmt.Errorf("批量插入失败: %w", err)
		}
	}

	// 执行批量更新（分批次）
	if len(batchUpdate) > 0 {
		err := s.batchUpdate(tableName, batchUpdate, config.PrimaryKey)
		if err != nil {
			return added, updated, fmt.Errorf("批量更新失败: %w", err)
		}
	}

	return added, updated, nil
}

// 批量插入（分批次处理）
func (s *DBSynchronizer) batchInsert(tableName string, records []map[string]interface{}) error {
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000 // 默认批次大小
	}

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		err := s.config.TargetDB.Table(tableName).Create(batch).Error
		if err != nil {
			return fmt.Errorf("插入批次 %d-%d 失败: %w", i, end, err)
		}
	}

	return nil
}

// 批量更新（分批次处理）
func (s *DBSynchronizer) batchUpdate(tableName string, records []map[string]interface{}, primaryKeys []string) error {
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000 // 默认批次大小
	}

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		for _, record := range batch {
			// 构建WHERE条件
			whereClause := make(map[string]interface{})
			for _, pk := range primaryKeys {
				if value, exists := record[pk]; exists {
					whereClause[pk] = value
				}
			}

			// 构建更新数据（排除主键）
			updateData := make(map[string]interface{})
			for k, v := range record {
				isPrimaryKey := false
				for _, pk := range primaryKeys {
					if k == pk {
						isPrimaryKey = true
						break
					}
				}
				if !isPrimaryKey {
					updateData[k] = v
				}
			}

			err := s.config.TargetDB.Table(tableName).Where(whereClause).Updates(updateData).Error
			if err != nil {
				return fmt.Errorf("更新记录失败 (主键: %v): %w", whereClause, err)
			}
		}
	}

	return nil
}

// 批量软删除（分批次处理）
func (s *DBSynchronizer) batchSoftDelete(tableName string, records []map[string]interface{}, config TableConfig) error {
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		for _, record := range batch {
			whereClause := make(map[string]interface{})
			for _, pk := range config.PrimaryKey {
				if value, exists := record[pk]; exists {
					whereClause[pk] = value
				}
			}

			updateData := map[string]interface{}{
				config.SoftDeleteField: config.SoftDeleteValue,
			}

			err := s.config.TargetDB.Table(tableName).Where(whereClause).Updates(updateData).Error
			if err != nil {
				return fmt.Errorf("软删除失败 (主键: %v): %w", whereClause, err)
			}
		}
	}

	return nil
}

// 批量硬删除（分批次处理）
func (s *DBSynchronizer) batchHardDelete(tableName string, records []map[string]interface{}, config TableConfig) error {
	batchSize := s.config.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		for _, record := range batch {
			whereClause := make(map[string]interface{})
			for _, pk := range config.PrimaryKey {
				if value, exists := record[pk]; exists {
					whereClause[pk] = value
				}
			}

			err := s.config.TargetDB.Table(tableName).Where(whereClause).Delete(nil).Error
			if err != nil {
				return fmt.Errorf("硬删除失败 (主键: %v): %w", whereClause, err)
			}
		}
	}

	return nil
}

// 处理软删除
func (s *DBSynchronizer) handleSoftDelete(sourceData, targetData map[string]map[string]interface{}, config TableConfig) (int, error) {
	softDeleted := 0
	tableName := config.TableName
	if config.Schema2 != "" {
		tableName = config.Schema2 + "." + config.TableName
	}

	var recordsToDelete []map[string]interface{}

	for key, targetRecord := range targetData {
		if _, exists := sourceData[key]; !exists {
			// 源数据库不存在，目标数据库存在，执行软删除
			recordsToDelete = append(recordsToDelete, targetRecord)
			softDeleted++
		}
	}

	if len(recordsToDelete) > 0 {
		err := s.batchSoftDelete(tableName, recordsToDelete, config)
		if err != nil {
			return softDeleted, err
		}
	}

	return softDeleted, nil
}

// 处理硬删除
func (s *DBSynchronizer) handleHardDelete(sourceData, targetData map[string]map[string]interface{}, config TableConfig) (int, error) {
	deleted := 0
	tableName := config.TableName
	if config.Schema2 != "" {
		tableName = config.Schema2 + "." + config.TableName
	}

	var recordsToDelete []map[string]interface{}

	for key, targetRecord := range targetData {
		if _, exists := sourceData[key]; !exists {
			// 源数据库不存在，目标数据库存在，执行硬删除
			recordsToDelete = append(recordsToDelete, targetRecord)
			deleted++
		}
	}

	if len(recordsToDelete) > 0 {
		err := s.batchHardDelete(tableName, recordsToDelete, config)
		if err != nil {
			return deleted, err
		}
	}

	return deleted, nil
}

// 比较两个记录是否相等（忽略主键）
func (s *DBSynchronizer) recordsEqual(record1, record2 map[string]interface{}, primaryKeys []string) bool {
	if len(record1) != len(record2) {
		return false
	}

	for k, v1 := range record1 {
		// 跳过主键比较
		isPrimaryKey := false
		for _, pk := range primaryKeys {
			if k == pk {
				isPrimaryKey = true
				break
			}
		}
		if isPrimaryKey {
			continue
		}

		v2, exists := record2[k]
		if !exists {
			return false
		}

		if !reflect.DeepEqual(v1, v2) {
			return false
		}
	}

	return true
}

// 使用示例
func main() {
	options := map[string]string{
		"schema":         "DKYPW",
		"appName":        "GORM 连接达梦数据库示例",
		"connectTimeout": "30000",
	}

	// 初始化数据库连接
	//sourceDB, err := gorm.Open(mysql.Open("user:password@tcp(host:port)/database1?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{})
	sourceDB, err := gorm.Open(dameng.Open(dameng.BuildUrl("SYSDBA", "SYSDBA001", "127.0.0.1", 5238, options)), &gorm.Config{})
	if err != nil {
		log.Fatal("连接源数据库失败:", err)
	}

	options = map[string]string{
		"schema":         "DKYPW_TEST",
		"appName":        "GORM 连接达梦数据库示例",
		"connectTimeout": "30000",
	}
	//targetDB, err := gorm.Open(mysql.Open("user:password@tcp(host:port)/database2?charset=utf8mb4&parseTime=True&loc=Local"), &gorm.Config{})
	targetDB, err := gorm.Open(dameng.Open(dameng.BuildUrl("SYSDBA", "SYSDBA001", "127.0.0.1", 5238, options)), &gorm.Config{})
	if err != nil {
		log.Fatal("连接目标数据库失败:", err)
	}

	// 配置同步表
	config := &SyncConfig{
		SourceDB:         sourceDB,
		TargetDB:         targetDB,
		EnableSoftDelete: true,
		HardDelete:       false,
		BatchSize:        500, // 设置批次大小，避免超过65535限制
		Tables: []TableConfig{
			{
				TableName:  "SG_CON_FEEDERLINE_C",
				PrimaryKey: []string{"DCLOUD_ID"},
				Schema1:    "DKYPW",
				Schema2:    "DKYPW_TEST",
			},
			{
				TableName:       "SG_CON_FEEDERLINE_B",
				PrimaryKey:      []string{"ID"},
				Schema1:         "DKYPW",
				Schema2:         "DKYPW_TEST",
				SoftDeleteField: "RUNNING_STATE",
				SoftDeleteValue: "1006",
			},
		},
	}

	// 创建同步器并执行同步
	synchronizer := NewDBSynchronizer(config)
	results := synchronizer.SyncAll()

	// 输出结果
	for _, result := range results {
		fmt.Printf("表 %s 同步结果:\n", result.TableName)
		fmt.Printf("  新增: %d\n", result.Added)
		fmt.Printf("  更新: %d\n", result.Updated)
		fmt.Printf("  删除: %d\n", result.Deleted)
		fmt.Printf("  软删除: %d\n", result.SoftDeleted)
		if len(result.Errors) > 0 {
			fmt.Printf("  错误: %v\n", result.Errors)
		}
		fmt.Println()
	}
}
