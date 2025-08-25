package main

import (
	"log"
	"os"
	"path/filepath"
	"raselper/src/first/component"
	"raselper/src/first/component/impl"
	"strings"
)

var (
	InstanceList []component.Instance = []component.Instance{
		new(impl.InstanceUnZip),
		new(impl.InstanceRename),
		new(impl.InstanceDelete),
	}
)

func loadConfigArgs() [][]string {
	// 获取当前执行文件的目录
	dir, err := os.Getwd()
	if err != nil {
		return [][]string{os.Args}
	}

	// 查找 .raseper 文件
	configPath := filepath.Join(dir, ".raseper")
	content, err := os.ReadFile(configPath)
	if err != nil {
		return [][]string{os.Args}
	}

	// 读取内容并按空格分割
	args := strings.Split(string(content), "\n")
	if len(args) == 0 {
		return [][]string{os.Args}
	}

	// 确保第一个参数是程序名
	result := [][]string{}
	for _, arg := range args {
		result = append(result, append([]string{os.Args[0]}, strings.Fields(arg)...))
	}
	return result
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Print("err:", err)
		}
	}()

	// 获取参数，优先使用配置文件中的参数
	args := loadConfigArgs()

	for _, arg := range args {
		// 确保至少有一个参数
		if len(args) < 2 {
			log.Print("请提供命令参数")
			return
		}

		for _, instance := range InstanceList {
			if instance.SelectComponent(arg) {
				log.Print("args: ", arg[1:])
				instance.Run(arg)
			}
		}
	}
}
