package md5

import (
	"errors"
	"strconv"
	"strings"
)

type ConfigFileHelper struct {
	source   []string
	filePath string // -f
	command  string
	greater  int // -g 大于
}

func ReadConfig(configList []string) (*ConfigFileHelper, error) {
	config := &ConfigFileHelper{source: configList}
	configList = configList[2:]

	for i, subStr := range configList {
		if strings.HasPrefix(subStr, "-") { // 是参数
			switch subStr {
			case "-f":
				if len(configList) <= i { // 参数不存在?
					return nil, errors.New("param -f not exist")
				}
				config.filePath = configList[i+1]
			case "-g":
				if len(configList) <= i { // 参数不存在?
					return nil, errors.New("param -g not exist")
				}
				if greater, err := strconv.Atoi(configList[i+1]); err != nil {
					return nil, err
				} else {
					config.greater = greater
				}
			}
		} else { // 不是参数是命令
			if config.command == "" { // command为空则赋值为command
				config.command = subStr
				continue
			}
			if config.filePath == "" { // filePath为不空则赋值为filePath
				config.filePath = subStr
				continue
			}
		}
	}

	return config, nil
}
