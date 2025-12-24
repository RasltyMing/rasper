package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"reflect"
	"strings"

	dameng "github.com/godoes/gorm-dameng"
	"gopkg.in/yaml.v3"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// 配置文件结构体
type ConfigFile struct {
	Database struct {
		Source struct {
			Driver  string            `yaml:"driver"`
			DSN     string            `yaml:"dsn"`
			Options map[string]string `yaml:"options,omitempty"`
		} `yaml:"source"`
		Target struct {
			Driver  string            `yaml:"driver"`
			DSN     string            `yaml:"dsn"`
			Options map[string]string `yaml:"options,omitempty"`
		} `yaml:"target"`
	} `yaml:"database"`
	Sync struct {
		EnableSoftDelete bool `yaml:"enable_soft_delete"`
		HardDelete       bool `yaml:"hard_delete"`
		BatchSize        int  `yaml:"batch_size"`
	} `yaml:"sync"`
	Tables []TableConfig `yaml:"tables"`
}

// 从YAML文件读取配置
func LoadConfigFromFile(filename string) (*SyncConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var fileConfig ConfigFile
	err = yaml.Unmarshal(data, &fileConfig)
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 初始化数据库连接
	sourceDB, err := initDB(fileConfig.Database.Source)
	if err != nil {
		return nil, fmt.Errorf("初始化源数据库失败: %w", err)
	}

	targetDB, err := initDB(fileConfig.Database.Target)
	if err != nil {
		return nil, fmt.Errorf("初始化目标数据库失败: %w", err)
	}

	// 构建SyncConfig
	config := &SyncConfig{
		SourceDB:         sourceDB,
		TargetDB:         targetDB,
		Tables:           fileConfig.Tables,
		EnableSoftDelete: fileConfig.Sync.EnableSoftDelete,
		HardDelete:       fileConfig.Sync.HardDelete,
		BatchSize:        fileConfig.Sync.BatchSize,
	}

	// 设置默认值
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}

	return config, nil
}

// 初始化数据库连接
func initDB(dbConfig struct {
	Driver  string            `yaml:"driver"`
	DSN     string            `yaml:"dsn"`
	Options map[string]string `yaml:"options,omitempty"`
}) (*gorm.DB, error) {
	switch dbConfig.Driver {
	case "mysql":
		return gorm.Open(mysql.Open(dbConfig.DSN), &gorm.Config{})
	case "dameng":
		url := dameng.BuildUrl(
			getOption(dbConfig.Options, "user", "SYSDBA"),
			getOption(dbConfig.Options, "password", "SYSDBA"),
			getOption(dbConfig.Options, "host", "127.0.0.1"),
			getOptionInt(dbConfig.Options, "port", 5236),
			dbConfig.Options,
		)
		fmt.Printf("user %s password %s ", getOption(dbConfig.Options, "user", "SYSDBA"), getOption(dbConfig.Options, "password", "SYSDBA"))
		fmt.Printf("host %s port %d\n", getOption(dbConfig.Options, "host", "127.0.0.1"), getOptionInt(dbConfig.Options, "port", 5236))
		url = fmt.Sprintf("dm://%s:%s@%s:%s",
			getOption(dbConfig.Options, "user", "SYSDBA"),
			getOption(dbConfig.Options, "password", "SYSDBA"),
			getOption(dbConfig.Options, "host", "127.0.0.1"),
			getOption(dbConfig.Options, "port", "5236"),
		)
		// 为达梦数据库禁用自动引号
		return gorm.Open(dameng.Open(url), &gorm.Config{
			DisableAutomaticPing: true,
		})
	default:
		return nil, fmt.Errorf("不支持的数据库驱动: %s", dbConfig.Driver)
	}
}

// 获取字符串配置项，带默认值
func getOption(options map[string]string, key, defaultValue string) string {
	if options == nil {
		return defaultValue
	}
	if value, exists := options[key]; exists {
		return url.PathEscape(value)
	}
	return defaultValue
}

// 获取整数配置项，带默认值
func getOptionInt(options map[string]string, key string, defaultValue int) int {
	if options == nil {
		return defaultValue
	}
	if value, exists := options[key]; exists {
		var result int
		_, err := fmt.Sscanf(value, "%d", &result)
		if err != nil {
			return defaultValue
		}
		return result
	}
	return defaultValue
}

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
	TableName       string                 `yaml:"table_name"`        // 表名
	PrimaryKey      []string               `yaml:"primary_key"`       // 主键字段（支持复合主键）
	Schema1         string                 `yaml:"schema1"`           // 数据库1的schema
	Schema2         string                 `yaml:"schema2"`           // 数据库2的schema
	SoftDeleteField string                 `yaml:"soft_delete_field"` // 软删除字段，如 "deleted_at"
	SoftDeleteValue string                 `yaml:"soft_delete_value"` // 软删除值，如 "now()"
	BatchSize       int                    `yaml:"batch_size"`        // 批量操作大小
	WhereCondition  map[string]interface{} `yaml:"where_condition"`   // 查询条件
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
	fmt.Printf("源端表读取开始 %s\n", config.TableName)
	sourceData, err := s.fetchData(s.config.SourceDB, config, config.Schema1)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("读取源数据库失败: %w", err))
		return result
	}

	fmt.Printf("目的表读取开始 %s\n", config.TableName)
	targetData, err := s.fetchData(s.config.TargetDB, config, config.Schema2)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("读取目标数据库失败: %w", err))
		return result
	}

	// 2. 比较数据并执行操作
	fmt.Printf("开始比较数据 %s\n", config.TableName)
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

// 获取数据并转换为Map（分批次查询）
func (s *DBSynchronizer) fetchData(db *gorm.DB, config TableConfig, schema string) (map[string]map[string]interface{}, error) {
	dataMap := make(map[string]map[string]interface{})

	tableName := config.TableName
	if schema != "" {
		tableName = schema + "." + config.TableName
	}

	// 优先使用表级别的batch_size，如果没有则使用全局的
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = s.config.BatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000 // 最终默认值
	}

	offset := 0
	for {
		var batchResults []map[string]interface{}

		// 构建查询 - 使用原始SQL避免自动引号
		query := db.Table(tableName)

		// 添加WHERE条件 - 使用自定义方法避免自动引号
		if config.WhereCondition != nil && len(config.WhereCondition) > 0 {
			query = s.applyWhereCondition(query, config.WhereCondition)
		}

		// 使用Limit和Offset进行分批次查询
		err := query.Limit(batchSize).
			Offset(offset).
			Find(&batchResults).Error

		if err != nil {
			return nil, err
		}

		// 如果本批次没有数据，说明已经查询完毕
		if len(batchResults) == 0 {
			break
		}

		// 处理本批次数据
		for _, record := range batchResults {
			key := s.generatePrimaryKey(record, config.PrimaryKey)
			dataMap[key] = record
		}

		fmt.Printf("表 %s: 已读取 %d 条记录 (批次大小: %d)\n", tableName, offset+len(batchResults), batchSize)

		// 如果本批次数据量小于batchSize，说明已经是最后一批
		if len(batchResults) < batchSize {
			break
		}

		// 更新offset，准备查询下一批
		offset += batchSize
	}

	fmt.Printf("表 %s: 总共读取 %d 条记录\n", tableName, len(dataMap))
	return dataMap, nil
}

// 批量插入（分批次处理）
func (s *DBSynchronizer) batchInsert(tableName string, records []map[string]interface{}, config TableConfig) error {
	// 优先使用表级别的batch_size，如果没有则使用全局的
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = s.config.BatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000 // 最终默认值
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
func (s *DBSynchronizer) batchUpdate(tableName string, records []map[string]interface{}, primaryKeys []string, config TableConfig) error {
	// 优先使用表级别的batch_size，如果没有则使用全局的
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = s.config.BatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000 // 最终默认值
	}

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		for _, record := range batch {
			// 构建WHERE条件 - 使用原始SQL避免自动引号
			whereClause := make([]string, 0)
			whereValues := make([]interface{}, 0)
			for _, pk := range primaryKeys {
				if value, exists := record[pk]; exists {
					whereClause = append(whereClause, pk+" = ?")
					whereValues = append(whereValues, value)
				}
			}

			if len(whereClause) == 0 {
				continue
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

			// 使用原始SQL条件
			err := s.config.TargetDB.Table(tableName).
				Where(strings.Join(whereClause, " AND "), whereValues...).
				Updates(updateData).Error
			if err != nil {
				return fmt.Errorf("更新记录失败 (主键: %v): %w", whereValues, err)
			}
		}
	}

	return nil
}

// 批量软删除（分批次处理）
func (s *DBSynchronizer) batchSoftDelete(tableName string, records []map[string]interface{}, config TableConfig) error {
	// 优先使用表级别的batch_size，如果没有则使用全局的
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = s.config.BatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000 // 最终默认值
	}

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		for _, record := range batch {
			// 构建WHERE条件 - 使用原始SQL避免自动引号
			whereClause := make([]string, 0)
			whereValues := make([]interface{}, 0)
			for _, pk := range config.PrimaryKey {
				if value, exists := record[pk]; exists {
					whereClause = append(whereClause, pk+" = ?")
					whereValues = append(whereValues, value)
				}
			}

			if len(whereClause) == 0 {
				continue
			}

			updateData := map[string]interface{}{
				config.SoftDeleteField: config.SoftDeleteValue,
			}

			err := s.config.TargetDB.Table(tableName).
				Where(strings.Join(whereClause, " AND "), whereValues...).
				Updates(updateData).Error
			if err != nil {
				return fmt.Errorf("软删除失败 (主键: %v): %w", whereValues, err)
			}
		}
	}

	return nil
}

// 批量硬删除（分批次处理）
func (s *DBSynchronizer) batchHardDelete(tableName string, records []map[string]interface{}, config TableConfig) error {
	// 优先使用表级别的batch_size，如果没有则使用全局的
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = s.config.BatchSize
	}
	if batchSize <= 0 {
		batchSize = 1000 // 最终默认值
	}

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		for _, record := range batch {
			// 构建WHERE条件 - 使用原始SQL避免自动引号
			whereClause := make([]string, 0)
			whereValues := make([]interface{}, 0)
			for _, pk := range config.PrimaryKey {
				if value, exists := record[pk]; exists {
					whereClause = append(whereClause, pk+" = ?")
					whereValues = append(whereValues, value)
				}
			}

			if len(whereClause) == 0 {
				continue
			}

			err := s.config.TargetDB.Table(tableName).
				Where(strings.Join(whereClause, " AND "), whereValues...).
				Delete(nil).Error
			if err != nil {
				return fmt.Errorf("硬删除失败 (主键: %v): %w", whereValues, err)
			}
		}
	}

	return nil
}

// 应用WHERE条件，避免自动引号
func (s *DBSynchronizer) applyWhereCondition(query *gorm.DB, conditions map[string]interface{}) *gorm.DB {
	for field, value := range conditions {
		// 使用原始SQL来避免自动引号
		query = query.Where(field+" = ?", value)
	}
	return query
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

	// 在 syncData 方法中更新调用：
	// 执行批量插入（分批次）
	if len(batchInsert) > 0 {
		err := s.batchInsert(tableName, batchInsert, config)
		if err != nil {
			return added, updated, fmt.Errorf("批量插入失败: %w", err)
		}
	}

	// 执行批量更新（分批次）
	if len(batchUpdate) > 0 {
		err := s.batchUpdate(tableName, batchUpdate, config.PrimaryKey, config)
		if err != nil {
			return added, updated, fmt.Errorf("批量更新失败: %w", err)
		}
	}

	return added, updated, nil
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
	// 从配置文件读取配置
	config, err := LoadConfigFromFile("config.yaml")
	if err != nil {
		log.Print("加载配置文件失败:", err)
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
