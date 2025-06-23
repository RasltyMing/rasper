package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"raselper/app/component/filehelper"
	"raselper/app/component/md5"
	"strings"
)

func loadConfigArgs() []string {
	// 获取当前执行文件的目录
	dir, err := os.Getwd()
	if err != nil {
		return os.Args
	}

	// 查找 .raseper 文件
	configPath := filepath.Join(dir, ".raseper")
	content, err := os.ReadFile(configPath)
	if err != nil {
		return os.Args
	}

	// 读取内容并按空格分割
	args := strings.Fields(string(content))
	if len(args) == 0 {
		return os.Args
	}

	// 确保第一个参数是程序名
	return append([]string{os.Args[0]}, args...)
}

func main() {
	defer func() {
		if err := recover(); err != nil {
			log.Print(err)
		}
	}()

	// 获取参数，优先使用配置文件中的参数
	args := loadConfigArgs()

	// 确保至少有一个参数
	if len(args) < 2 {
		fmt.Println("请提供命令参数")
		return
	}

	switch args[1] {
	case "md5":
		if err := md5.Run(args); err != nil {
			fmt.Println(err)
		}
	case "filehelper":
		if err := filehelper.Run(args); err != nil {
			fmt.Println(err)
		}
	}
}
