package impl

import (
	"raselper/src/first/component"
	"raselper/src/secondary/utils"
)

type InstanceUnZip struct {
	component.Instance
}

func (r InstanceUnZip) SelectComponent(args []string) bool {
	return args[1] == "unzip"
}
func (r InstanceUnZip) Run(args []string) {
	decodeParam := ""
	if len(args) > 4 {
		decodeParam = args[4]
	}
	_ = utils.Unzip(args[2], args[3], decodeParam)
}
