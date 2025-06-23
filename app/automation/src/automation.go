package src

type StatusNode struct {
	name string
}

func NewStatus(name string) StatusNode {
	return StatusNode{
		name: name,
	}
}

var (
	status      StatusNode
	statusTasks = map[StatusNode]func(){}
	statusTrans = map[StatusNode][]func() *StatusNode{}
)

func RegisterStatus(newStatus StatusNode, task func()) {
	status = newStatus
	statusTasks[status] = task
}

func RegisterTrans(status StatusNode, task func() *StatusNode) {
	statusTrans[status] = append(statusTrans[status], task)
}

func Start(startNode StatusNode) {
	status = startNode

	// 循环更新status
	go doStatusTrans()

	// 循环运行task
	doStatusTask()
}

func doStatusTrans() {
	for true {
		tranList := statusTrans[status]
		for _, trans := range tranList {
			statusNode := trans()
			if statusNode != nil {
				status = *statusNode
			}
		}
	}
}

func doStatusTask() {
	for true {
		task := statusTasks[status]
		task()
	}
}
