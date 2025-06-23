package md5

type LogicStruct struct {
	itemMap map[string]*LogicItem // 文件路径-详细信息
	md5Map  map[string][]string   // md5值-对应文件
}

type LogicItem struct {
	path string // 文件路径
	md5  string // md5值
}
