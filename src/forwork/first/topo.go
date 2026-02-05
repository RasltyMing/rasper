package first

import (
	"fmt"
)

func BuildGraph(topoList []Topo) map[string][]string {
	topoMap := make(map[string][]string)
	visited := make(map[string]bool) // 已经击中的id
	idMap, nodeMap, _, _, _ := GetNodeIDMap(topoList)

	if len(topoList) == 0 {
		fmt.Printf("❌ 无拓扑数据，无法构建图\n")
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

func GetNodeIDMap(topoList []Topo) (map[string][]string, map[string][]string, map[string][]string, map[string]Topo, map[string][]Topo) {
	idMap := make(map[string][]string)        // id - node
	nodeMap := make(map[string][]string)      // node - id
	idConnectMap := make(map[string][]string) // id - idList
	idEntityMap := make(map[string]Topo)
	duplicateTopoMap := make(map[string][]Topo)

	for _, topo := range topoList {
		idEntityMap[topo.ID] = topo
		if topo.FirstNodeID != "" {
			if !Contain(nodeMap[topo.FirstNodeID], topo.ID) {
				nodeMap[topo.FirstNodeID] = append(nodeMap[topo.FirstNodeID], topo.ID)
			}
		}
		if topo.SecondNodeID != "" {
			if !Contain(nodeMap[topo.SecondNodeID], topo.ID) {
				nodeMap[topo.SecondNodeID] = append(nodeMap[topo.SecondNodeID], topo.ID)
			}
		}
		if !Contain(idMap[topo.ID], topo.FirstNodeID) {
			idMap[topo.ID] = append(idMap[topo.ID], topo.FirstNodeID)
		}
		if !Contain(idMap[topo.ID], topo.SecondNodeID) {
			idMap[topo.ID] = append(idMap[topo.ID], topo.SecondNodeID)
		}
		{ // 查重复拓扑
			if topo.FirstNodeID == "" {
				duplicateTopoMap[topo.FirstNodeID] = append(duplicateTopoMap[topo.FirstNodeID], topo)
				continue
			}
			if topo.SecondNodeID == "" {
				duplicateTopoMap[topo.SecondNodeID] = append(duplicateTopoMap[topo.SecondNodeID], topo)
				continue
			}
			duplicateTopoMap[topo.FirstNodeID+topo.SecondNodeID] = append(duplicateTopoMap[topo.FirstNodeID+topo.SecondNodeID], topo)
			duplicateTopoMap[topo.SecondNodeID+topo.FirstNodeID] = append(duplicateTopoMap[topo.SecondNodeID+topo.FirstNodeID], topo)
			continue
		}
	}

	for id, nodeList := range idMap {
		for _, node := range nodeList {
			idList := nodeMap[node]
			for _, connect := range idList {
				if connect == id {
					continue
				}
				if Contain(idConnectMap[id], connect) {
					continue
				}
				idConnectMap[id] = append(idConnectMap[id], connect)
			}
		}
	}

	return idMap, nodeMap, idConnectMap, idEntityMap, duplicateTopoMap
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

func Contain(list []string, str string) bool {
	for _, s := range list {
		if s == str {
			return true
		}
	}
	return false
}
