package md5

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)

func RunLogicByConfig(config *ConfigFileHelper) (*LogicStruct, error) {
	switch config.command {
	case "list":
		logicStruct, err := ReadMd5InfoByConfig(config)
		ResultPrintLogicStruct(logicStruct, config)
		return logicStruct, err
	case "delete-repeat":
		return DeleteMd5RepeatByConfig(config)
	case "help":
		fmt.Println("list")
		fmt.Println("delete-repeat")
		return nil, nil
	}

	return nil, errors.New("command:" + config.command + " not found")
}

func ReadMd5InfoByConfig(config *ConfigFileHelper) (*LogicStruct, error) {
	logicStruct := &LogicStruct{
		itemMap: make(map[string]*LogicItem),
		md5Map:  make(map[string][]string),
	}

	if err := filepath.Walk(config.filePath, func(path string, info fs.FileInfo, err error) error {
		if info.IsDir() { // 跳过目录
			return nil
		}
		if md5, err := GetFileMD5(path); err != nil {
			return err
		} else {
			logicStruct.itemMap[path] = &LogicItem{
				path: path,
				md5:  md5,
			}
			logicStruct.md5Map[md5] = append(logicStruct.md5Map[md5], path)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return logicStruct, nil
}

func DeleteMd5RepeatByConfig(config *ConfigFileHelper) (*LogicStruct, error) {
	logicStruct, err := ReadMd5InfoByConfig(config)
	if err != nil {
		return nil, err
	}

	for _, item := range logicStruct.itemMap {
		if len(logicStruct.md5Map[item.md5]) == 1 { // md5没有重复的话
			continue
		}
		// md5重复 循环删除md5Map中的文件
		for i, filePath := range logicStruct.md5Map[item.md5] {
			if i == (len(logicStruct.md5Map[item.md5]) - 1) { // 最后一个元素不删
				continue
			}
			if err := os.Remove(filePath); err != nil {
				return nil, err
			} else {
				log.Println("delete fileu:", filePath, " md5:", item.md5)
			}
		}

		delete(logicStruct.itemMap, item.path)
		logicStruct.md5Map[item.md5] = logicStruct.md5Map[item.md5][len(logicStruct.md5Map[item.md5])-1:]
	}

	return logicStruct, nil
}
