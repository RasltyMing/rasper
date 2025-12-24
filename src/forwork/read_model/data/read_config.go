package data

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

// AppConfig 结构体用于映射 yaml 配置
type AppConfig struct {
	DB        DBConfig `yaml:"db"`
	DB2       DBConfig `yaml:"db2"`
	UpdateUrl string   `yaml:"update-url"`
	Delete    bool     `yaml:"delete"`
}
type DBConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Port     string `yaml:"port"`
	IP       string `yaml:"ip"`
	Database string `yaml:"database"` // 新增数据库名配置
}

// ReadAppConfig 读取 app.yaml 配置文件
func ReadAppConfig(filePath string) (*AppConfig, error) {
	config := &AppConfig{}

	// 读取文件内容
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("读取文件失败: %v", err)
	}

	// 解析 YAML
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("解析 YAML 失败: %v", err)
	}

	return config, nil
}
