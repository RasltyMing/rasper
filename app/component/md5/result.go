package md5

import (
	"fmt"
)

func ResultPrintLogicStruct(logicStruct *LogicStruct, config *ConfigFileHelper) {
	for _, item := range logicStruct.itemMap {
		if len(logicStruct.md5Map[item.md5]) > config.greater {
			fmt.Printf("%+v, md5 repeat:%d\n", item, len(logicStruct.md5Map[item.md5]))
		}
	}
}
