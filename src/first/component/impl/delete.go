package impl

import (
	"raselper/src/first/component"
	"raselper/src/secondary/utils"
)

type InstanceDelete struct {
	component.Instance
}

func (r InstanceDelete) SelectComponent(args []string) bool {
	return args[1] == "delete"
}
func (r InstanceDelete) Run(args []string) {
	if err := utils.Delete(args[2]); err != nil {
		panic(err)
	}
}
