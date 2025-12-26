package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"raselper/src/forwork/read_model/data"
	"strings"

	"golang.org/x/text/encoding/simplifiedchinese"
)

func main() {
	// 读取配置等初始化代码...

	args := os.Args
	fmt.Println("args:", args)

	if len(args) < 2 {
		log.Fatal("请提供路径参数")
	}
	path := args[1]

	var graphNameList []string
	if result := data.DB.Table(data.Config.DB.Database + ".SG_CON_FEEDERLINE_B").
		Select("GRAPH_NAME").
		Find(&graphNameList); result.Error != nil {
		fmt.Printf("Error: %v\n", result.Error)
	}
	for _, graphName := range graphNameList {
		graphNameSvg := strings.ReplaceAll(graphName, "dx.pic.g", "svg")
		gbkPath, _ := convertUTF8ToGBK(filepath.Join(path, graphNameSvg))

		if !fileExists(filepath.Join(path, graphNameSvg)) && !fileExists(gbkPath) {
			fmt.Println("file not exist: ", graphNameSvg)
		}
	}
}

// 将 UTF-8 字符串转换为 GBK 编码
func convertUTF8ToGBK(utf8Str string) (string, error) {
	// 使用 golang.org/x/text/encoding/simplifiedchinese 包
	// 注意：这需要先安装包：go get golang.org/x/text
	encoder := simplifiedchinese.GBK.NewEncoder()
	gbkBytes, err := encoder.Bytes([]byte(utf8Str))
	if err != nil {
		return "", err
	}
	return string(gbkBytes), nil
}

// 或者使用更简单的方法，如果只是文件名转换
func convertUTF8ToGBKSimple(utf8Str string) (string, error) {
	// 如果文件名包含中文字符，GBK 编码通常每个中文占2个字节
	// 这里使用一个简化版本
	return simplifiedchinese.GBK.NewEncoder().String(utf8Str)
}

// 检查文件是否存在
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}
