package impl

import (
	"raselper/src/first/component"
	"raselper/src/secondary/utils"
)

type InstanceRename struct {
	component.Instance
}

func (r InstanceRename) Run(args []string) {
	_ = utils.RenameFilesByRegex(args[2], args[3], args[4])
}

func (r InstanceRename) SelectComponent(args []string) bool {
	return args[1] == "rename"
}
