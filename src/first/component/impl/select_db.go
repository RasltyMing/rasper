package impl

import (
	"raselper/src/first/component"
)

type InstanceSelectDB struct {
	component.Instance
}

func (r InstanceSelectDB) Run(args []string) {
	//if err := utils.SelectDBFilesByRegex(args[2], args[3], args[4]); err != nil {
	//	panic(err)
	//}
}

func (r InstanceSelectDB) SelectComponent(args []string) bool {
	return args[1] == "select"
}
