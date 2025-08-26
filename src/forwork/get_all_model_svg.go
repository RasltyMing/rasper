package main

import (
	"fmt"
	"os"
	"path/filepath"
	"raselper/src/secondary/utils"
	"strings"
)

func loadConfigArgsSingle() []string {
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
	modelMap := map[string]string{
		"*35401*": "FZ",
		"*35402*": "XM",
		"*35403*": "PT",
		"*35404*": "SM",
		"*35405*": "QZ",
		"*35406*": "ZZ",
		"*35407*": "NP",
		"*35408*": "LY",
		"*35409*": "ND",
	}
	args := loadConfigArgsSingle()
	src := args[1]       // 源目录
	modelDest := args[2] // 目标目录
	svgDest := args[3]   // svg目标目录
	decode := ""
	if len(args) > 2 {
		decode = args[4] // 解码方式
	}

	for key, value := range modelMap {
		modelSrc := src + "/model/" + key
		svgSrc := src + "/svg/" + key
		modelSubDest := modelDest + "/" + value
		svgSubDest := svgDest + "/" + value
		fmt.Println("modelSrc", modelSrc, "svgSrc", svgSrc, decode)

		// 图形解压
		matches, err := filepath.Glob(svgSrc)
		if err != nil {
			fmt.Println("invalid path pattern:", err)
		}
		for _, match := range matches {
			if err := utils.UnzipSingle(match, svgSubDest, decode); err != nil {
				fmt.Println(err)
			}
			_ = utils.RenameFilesByRegex(svgSubDest+"/*", "(.+)_(.+)_(.+)_(.+)_(.+).svg", "$1.svg")
		}

		// 模型解压
		matches, err = filepath.Glob(modelSrc)
		if err != nil {
			fmt.Println("invalid path pattern:", err)
		}
		for _, match := range matches {
			if err := utils.UnzipSingle(match, modelSubDest, decode); err != nil {
				fmt.Println(err)
			}
			_ = utils.RenameFilesByRegex(modelSubDest+"/*", "(.+)_(.+)_(.+).xml", "$1.xml")
		}

	}
}
