package main

import (
	"fmt"
	dameng "github.com/godoes/gorm-dameng"
	"gopkg.in/yaml.v3"
	"gorm.io/gorm"
	"io/ioutil"
	"log"
	"sort"
)

var config Config

// 拓扑连接关系表结构
type Topo struct {
	D             string `gorm:"column:D"`
	EffectiveTime string `gorm:"column:EFFECTIVE_TIME"`
	ExpiryTime    string `gorm:"column:EXPIRY_TIME"`
	FeederID      string `gorm:"column:FEEDER_ID"`
	FirstNodeID   string `gorm:"column:FIRST_NODE_ID"`
	ID            string `gorm:"column:ID"`
	Owner         string `gorm:"column:OWNER"`
	SecondNodeID  string `gorm:"column:SECOND_NODE_ID"`
	Stamp         string `gorm:"column:STAMP"`
}

// 分组信息
type GroupInfo struct {
	Owner    string `gorm:"column:OWNER"`
	FeederID string `gorm:"column:FEEDER_ID"`
}

// 不相连的拓扑组结果
type DisconnectedTopoGroup struct {
	Owner    string
	FeederID string
	Groups   [][]string // 每个子组包含可以相连的节点ID集合
}

// Config 结构体用于映射 yaml 配置
type Config struct {
	Owner  string   `yaml:"owner"`
	Feeder string   `yaml:"feeder"`
	DB     DBConfig `yaml:"db"`
}
type DBConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Port     string `yaml:"port"`
	IP       string `yaml:"ip"`
	Database string `yaml:"database"` // 新增数据库名配置
}

// ReadAppConfig 读取 app.yaml 配置文件
func ReadAppConfig(filePath string) (*Config, error) {
	// 读取文件内容
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("读取文件失败: %v", err)
	}

	// 解析 YAML
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("解析 YAML 失败: %v", err)
	}

	return &config, nil
}

func main() {
	config, err := ReadAppConfig("app.yaml")
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	fmt.Printf("Owner: %s\n", config.Owner)
	fmt.Printf("Feeder: %s\n", config.Feeder)

	// 连接达梦数据库
	dsn := fmt.Sprintf("dm://%s:%s@%s:%s", config.DB.Username, config.DB.Password, config.DB.IP, config.DB.Port)
	db, err := gorm.Open(dameng.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("连接数据库失败:", err)
	}

	// 计算不相连的拓扑组
	err = CalculateAndPrintTopoGroupsSequentially(db)
	if err != nil {
		log.Fatal("计算拓扑组失败:", err)
	}
}

// 逐个分组计算并打印结果
func CalculateAndPrintTopoGroupsSequentially(db *gorm.DB) error {
	// 首先获取所有唯一的分组（owner + feederID）
	var groups []GroupInfo
	result := db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
		Select("DISTINCT OWNER, FEEDER_ID").
		Order("OWNER, FEEDER_ID"). // 按顺序处理
		Where("OWNER = ? AND FEEDER_ID = ?", config.Owner, config.Feeder).
		Find(&groups)
	if result.Error != nil {
		return result.Error
	}

	fmt.Printf("=== 找到 %d 个分组需要处理 ===\n\n", len(groups))

	// 逐个分组处理
	for i, group := range groups {
		fmt.Printf("🚀 处理分组 %d/%d: Owner=%s, FeederID=%s\n",
			i+1, len(groups), group.Owner, group.FeederID)

		// 查询当前分组的所有拓扑数据
		var topoList []Topo
		result := db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
			Where("OWNER = ? AND FEEDER_ID = ?", group.Owner, group.FeederID).
			Find(&topoList)
		if result.Error != nil {
			log.Printf("❌ 查询分组数据失败: Owner=%s, FeederID=%s, error=%v",
				group.Owner, group.FeederID, result.Error)
			continue
		}

		fmt.Printf("   该分组有 %d 条拓扑记录\n", len(topoList))

		if len(topoList) == 0 {
			fmt.Printf("   ⚠️  该分组没有拓扑数据，跳过处理\n\n")
			continue
		}

		// 构建图并计算连通分量
		graph := buildGraph(topoList)
		for key, connected := range graph {
			fmt.Println(key, ": ", len(connected))
		}

		connnectTopo(topoList, graph, db)
		//connectedComponents := findConnectedComponentsWithUnionFind(graph)

		// 打印当前分组的结果
		//printGroupResult(group.Owner, group.FeederID, connectedComponents)

		fmt.Println() // 空行分隔不同分组
	}

	fmt.Printf("✅ 所有分组处理完成！\n")
	return nil
}

func connnectTopo(topoList []Topo, graph map[string][]string, db *gorm.DB) {
	if len(graph) == 1 {
		// 没有孤立岛
		return
	}

	topoMap := make(map[string]Topo)
	for _, topo := range topoList {
		topoMap[topo.ID] = topo
	}
	nodeMap := make(map[string][]string) // node - id
	for _, topo := range topoMap {
		nodeMap[topo.FirstNodeID] = append(nodeMap[topo.FirstNodeID], topo.ID)
		nodeMap[topo.SecondNodeID] = append(nodeMap[topo.SecondNodeID], topo.ID)
	}

	var lastTopo *Topo
	var lastTopoNode string
	for startTopo, strings := range graph {
		for _, topo := range strings {
			topoModel := topoMap[topo]
			if topoModel.FirstNodeID == "" && topoModel.SecondNodeID == "" {
				fmt.Printf("topo: %v\n", topoModel)
				if lastTopoNode == "" { // 都空就更新
					db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
						Where("ID = ?", topo).
						Updates(map[string]interface{}{"FIRST_NODE_ID": lastTopoNode})
					lastTopo.FirstNodeID = lastTopoNode
				}
			}
			if lastTopo == nil && (topoModel.FirstNodeID == "" || topoModel.SecondNodeID == "") {
				t := topoModel
				lastTopo = &t
				if t.FirstNodeID == "" {
					lastTopoNode = t.SecondNodeID
				}
				if t.SecondNodeID == "" {
					lastTopoNode = t.FirstNodeID
				}
				if lastTopoNode == "" { // 都空就更新
					db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
						Where("ID = ?", topo).
						Updates(map[string]interface{}{"FIRST_NODE_ID": topoMap[startTopo].FirstNodeID})
					lastTopo.FirstNodeID = topoMap[startTopo].FirstNodeID
				}
				continue
			}

			if topoModel.FirstNodeID == "" {
				// 末端节点
				fmt.Printf("FirstNodeID %v\n", topoModel)
				// 更新末端节点为startTopo的其中一个节点
				db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("ID = ?", topo).
					Updates(map[string]interface{}{"FIRST_NODE_ID": lastTopoNode})
			}
			if topoModel.SecondNodeID == "" {
				// 末端节点
				fmt.Printf("SecondNodeID %v\n", topoModel)
				// 更新末端节点为startTopo的其中一个节点
				db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("ID = ?", topo).
					Updates(map[string]interface{}{"SECOND_NODE_ID": lastTopoNode})
			}
		}
	}
}

// 构建图的邻接表
func buildGraph(topoList []Topo) map[string][]string {
	topoMap := make(map[string][]string)
	visited := make(map[string]bool)     // 已经击中的id
	idMap := make(map[string][]string)   // id - node
	nodeMap := make(map[string][]string) // node - id

	if len(topoList) == 0 {
		fmt.Printf("❌ 无拓扑数据，无法构建图\n")
	}

	for _, topo := range topoList {
		if topo.FirstNodeID != "" {
			nodeMap[topo.FirstNodeID] = append(nodeMap[topo.FirstNodeID], topo.ID)
		}
		if topo.SecondNodeID != "" {
			nodeMap[topo.SecondNodeID] = append(nodeMap[topo.SecondNodeID], topo.ID)
		}
		idMap[topo.ID] = append(idMap[topo.ID], []string{topo.FirstNodeID, topo.SecondNodeID}...)
	}

	for _, topo := range topoList {
		connected := RecusionGraph(visited, idMap, nodeMap, topoMap, topo.ID)
		if len(connected) > 0 {
			for id := range connected {
				topoMap[topo.ID] = append(topoMap[topo.ID], id)
			}
		}
	}

	return topoMap
}

func RecusionGraph(hitMap map[string]bool, idMap, nodeMap, topoMap map[string][]string, startTopo string) map[string]bool {
	connected := make(map[string]bool)

	// 是否已经查询到了
	if hitMap[startTopo] {
		return connected
	}
	hitMap[startTopo] = true // 标记查询状态

	for _, node := range idMap[startTopo] {
		for _, topo := range nodeMap[node] {
			connected[topo] = true
			subconnected := RecusionGraph(hitMap, idMap, nodeMap, topoMap, topo)
			for id := range subconnected {
				connected[id] = true
			}
		}
	}

	return connected
}

// 使用并查集算法计算连通分量
type UnionFind struct {
	parent map[string]string
	rank   map[string]int
}

func NewUnionFind() *UnionFind {
	return &UnionFind{
		parent: make(map[string]string),
		rank:   make(map[string]int),
	}
}

func (uf *UnionFind) Find(x string) string {
	if uf.parent[x] != x {
		uf.parent[x] = uf.Find(uf.parent[x]) // 路径压缩
	}
	return uf.parent[x]
}

func (uf *UnionFind) Union(x, y string) {
	rootX := uf.Find(x)
	rootY := uf.Find(y)

	if rootX != rootY {
		// 按秩合并
		if uf.rank[rootX] > uf.rank[rootY] {
			uf.parent[rootY] = rootX
		} else if uf.rank[rootX] < uf.rank[rootY] {
			uf.parent[rootX] = rootY
		} else {
			uf.parent[rootY] = rootX
			uf.rank[rootX]++
		}
	}
}

func (uf *UnionFind) AddNode(node string) {
	if _, exists := uf.parent[node]; !exists {
		uf.parent[node] = node
		uf.rank[node] = 0
	}
}

// 使用并查集查找连通分量
func findConnectedComponentsWithUnionFind(graph map[string][]string) [][]string {
	uf := NewUnionFind()

	// 初始化并查集
	for node := range graph {
		uf.AddNode(node)
	}

	// 合并相连的节点
	for node, neighbors := range graph {
		for _, neighbor := range neighbors {
			uf.Union(node, neighbor)
		}
	}

	// 收集连通分量
	components := make(map[string][]string)
	for node := range graph {
		root := uf.Find(node)
		components[root] = append(components[root], node)
	}

	// 转换为结果格式，并对每个组内的节点排序（便于查看）
	var result [][]string
	for _, component := range components {
		// 对节点ID进行排序
		sort.Strings(component)
		result = append(result, component)
	}

	// 按组的大小排序（大组在前）
	sort.Slice(result, func(i, j int) bool {
		return len(result[i]) > len(result[j])
	})

	return result
}

// 打印分组结果，包括每个拓扑组的首节点ID
func printGroupResult(owner, feederID string, groups [][]string) {
	fmt.Printf("📊 分组计算结果: Owner=%s, FeederID=%s\n", owner, feederID)
	fmt.Printf("   发现 %d 个不相连的拓扑组:\n", len(groups))

	for i, group := range groups {
		// 获取首节点ID（排序后的第一个节点）
		firstNodeID := ""
		if len(group) > 0 {
			firstNodeID = group[0]
		}

		fmt.Printf("   %d. 拓扑组 %d: 节点数=%d, 首节点ID=%s\n",
			i+1, i+1, len(group), firstNodeID)

		// 如果需要显示该组的所有节点，可以取消下面的注释
		// fmt.Printf("      所有节点: %v\n", group)
	}

	// 打印所有拓扑组的首节点ID列表
	fmt.Printf("   📍 所有拓扑组的首节点ID列表: ")
	firstNodeIDs := make([]string, 0, len(groups))
	for i, group := range groups {
		if len(group) > 0 {
			firstNodeIDs = append(firstNodeIDs, group[0])
		} else {
			firstNodeIDs = append(firstNodeIDs, fmt.Sprintf("空组%d", i+1))
		}
	}
	fmt.Printf("%v\n", firstNodeIDs)
}

// 如果需要保存结果到文件或数据库，可以添加以下函数
func saveGroupResult(db *gorm.DB, owner, feederID string, groups [][]string) error {
	// 这里可以添加保存到数据库或文件的逻辑
	// 例如保存到新的结果表中

	fmt.Printf("💾 保存结果: Owner=%s, FeederID=%s, 拓扑组数=%d\n",
		owner, feederID, len(groups))

	// 示例保存逻辑
	for i, group := range groups {
		if len(group) > 0 {
			firstNodeID := group[0]
			fmt.Printf("   保存拓扑组 %d: 首节点ID=%s, 节点数=%d\n",
				i+1, firstNodeID, len(group))
		}
	}

	return nil
}
