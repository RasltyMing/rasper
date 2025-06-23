package md5

import (
	"log"
	"testing"
)

func TestGetConfig(t *testing.T) {
	config, _ := ReadConfig([]string{"list", "-f", "D:\\Temporary"})
	log.Printf("%+v\n", config)
}

func TestGetFileMD5(t *testing.T) {
	md5, _ := GetFileMD5("D:\\Temporary\\record.txt")
	println(md5)
}

func TestList(t *testing.T) {
	config, _ := ReadConfig([]string{"list", "-f", "D:\\Temporary"})
	log.Printf("%+v\n", config)
	logicStruct, _ := ReadMd5InfoByConfig(config)
	ResultPrintLogicStruct(logicStruct, config)
}
