package impl

import "raselper/src/first/component"

type InstanceCreateDB struct {
	component.Instance
}

func (r InstanceCreateDB) Run(args []string) {
	//if err := utils.CreateDBFilesByRegex(args[2], args[3], args[4]); err != nil {
	//	panic(err)
	//}
}

func (r InstanceCreateDB) SelectComponent(args []string) bool {
	return args[1] == "create"
}
