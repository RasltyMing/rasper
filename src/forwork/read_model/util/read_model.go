package util

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"raselper/src/forwork/read_model/data"
	"strings"

	"gorm.io/gorm"
)

// 定义命名空间常量
const (
	RDFNS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	CIMNS = "http://www.sgcc.com.cn/SG-CIM/2010MAY#"
	IESNS = "http://www.ieslab.com.cn"
)

// 主结构体
type RDF struct {
	XMLName xml.Name `xml:"RDF"`

	BaseVoltages           []BaseVoltage           `xml:"BaseVoltage"`
	SubGeographicalRegions []SubGeographicalRegion `xml:"SubGeographicalRegion"`
	Circuits               []Circuit               `xml:"Circuit"`
	Substations            []Substation            `xml:"Substation"`
	Breakers               []Breaker               `xml:"Breaker"`
	Disconnectors          []Disconnector          `xml:"Disconnector"`
	Fuses                  []Fuse                  `xml:"Fuse"`
	PowerTransformers      []PowerTransformer      `xml:"PowerTransformer"`
	BusbarSections         []BusbarSection         `xml:"BusbarSection"`
	ACLineSegments         []ACLineSegment         `xml:"ACLineSegment"`
	Poles                  []Pole                  `xml:"Pole"`
	FaultIndicators        []FaultIndicator        `xml:"Faultindicator"`
	ConnectivityNodes      []ConnectivityNode      `xml:"ConnectivityNode"`
	Terminals              []Terminal              `xml:"Terminal"`
}

// 基础结构体
type IdentifiedObject struct {
	Name    string `xml:"IdentifiedObject.name"`
	Bianhao string `xml:"IdentifiedObject.bianhao"`
}

type PowerSystemResource struct {
	DeviceType   string `xml:"http://www.ieslab.com.cn DeviceType"`
	SubType      string `xml:"http://www.ieslab.com.cn SubType"`
	DeviceID     string `xml:"http://www.ieslab.com.cn DeviceID"`
	MaintainTeam string `xml:"http://www.ieslab.com.cn MaintainTeam"`
	PoleID       string `xml:"http://www.ieslab.com.cn PoleID"`
	IsYK         string `xml:"http://www.ieslab.com.cn IsYK"`
	UserType     string `xml:"http://www.ieslab.com.cn UserType"`
	TaiQuHao     string `xml:"http://www.ieslab.com.cn TaiQuHao"`
	Cxmc         string `xml:"http://www.ieslab.com.cn Cxmc"`
}

// 基础电压
type BaseVoltage struct {
	ID             string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	Name           string `xml:"IdentifiedObject.name"`
	IsDC           string `xml:"isDC"`
	NominalVoltage string `xml:"nominalVoltage"`
	DeviceID       string `xml:"http://www.ieslab.com.cn DeviceID"`
}

// 地理区域
type SubGeographicalRegion struct {
	ID     string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	Name   string `xml:"IdentifiedObject.name"`
	Region struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"Region"`
}

// 线路
type Circuit struct {
	ID                   string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	Name                 string `xml:"IdentifiedObject.name"`
	BelongtoHVSubstation struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"BelongtoHVSubstation"`
	SubGeographicalRegion struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"SubGeographicalRegion"`
	IsCurrentFeeder string `xml:"iscurrentfeeder"`
}

// 变电站
type Substation struct {
	ID   string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	Name string `xml:"IdentifiedObject.name"`
	PowerSystemResource
	PSRType struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PSRType"`
	SubGeographicalRegion struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"SubGeographicalRegion"`
}

// 断路器
type Breaker struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	IdentifiedObject
	PowerSystemResource
	EquipmentContainer struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"Equipment.EquipmentContainer"`
	Circuit struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PowerSystemResource.Circuit"`
	BaseVoltage struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"ConductingEquipment.BaseVoltage"`
}

// 隔离开关
type Disconnector struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	IdentifiedObject
	PowerSystemResource
	EquipmentContainer struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"Equipment.EquipmentContainer"`
	Circuit struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PowerSystemResource.Circuit"`
	BaseVoltage struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"ConductingEquipment.BaseVoltage"`
}

// 熔断器
type Fuse struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	IdentifiedObject
	PowerSystemResource
	EquipmentContainer struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"Equipment.EquipmentContainer"`
	Circuit struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PowerSystemResource.Circuit"`
	BaseVoltage struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"ConductingEquipment.BaseVoltage"`
}

// 电力变压器
type PowerTransformer struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	IdentifiedObject
	PowerSystemResource
	EquipmentContainer struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"Equipment.EquipmentContainer"`
	Circuit struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PowerSystemResource.Circuit"`
	BaseVoltage struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"ConductingEquipment.BaseVoltage"`
}

// 母线
type BusbarSection struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	IdentifiedObject
	PowerSystemResource
	EquipmentContainer struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"Equipment.EquipmentContainer"`
	Circuit struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PowerSystemResource.Circuit"`
	BaseVoltage struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"ConductingEquipment.BaseVoltage"`
}

// 线路段
type ACLineSegment struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	IdentifiedObject
	PowerSystemResource
	EquipmentContainer struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"Equipment.EquipmentContainer"`
	Circuit struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PowerSystemResource.Circuit"`
	BaseVoltage struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"ConductingEquipment.BaseVoltage"`
}

// 电杆
type Pole struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	IdentifiedObject
	PowerSystemResource
	Circuit struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PowerSystemResource.Circuit"`
	BaseVoltage struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"ConductingEquipment.BaseVoltage"`
}

// 故障指示器
type FaultIndicator struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	IdentifiedObject
	PowerSystemResource
	Circuit struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"PowerSystemResource.Circuit"`
	BaseVoltage struct {
		Resource string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# resource,attr"`
	} `xml:"ConductingEquipment.BaseVoltage"`
}

// 连接节点
type ConnectivityNode struct {
	ID string `xml:"http://www.w3.org/1999/02/22-rdf-syntax-ns# ID,attr"`
	PowerSystemResource
}

// 端子 - 简化版本
type Terminal struct {
	ID                  string `xml:"ID,attr"`
	ConductingEquipment struct {
		Resource string `xml:"resource,attr"`
	} `xml:"Terminal.ConductingEquipment"`
	ConnectivityNode struct {
		Resource string `xml:"resource,attr"`
	} `xml:"Terminal.ConnectivityNode"`
}

// 解析XML文件
func ParseCIMXML(filePath string) (*RDF, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var rdf RDF
	err = xml.Unmarshal(data, &rdf)
	if err != nil {
		return nil, err
	}

	return &rdf, nil
}

func GetTopoMap(rdf *RDF) (idNodeMap map[string][]string, nodeIdMap map[string][]string, deviceFeederMap map[string]string) {
	existDevice := make(map[string]bool)

	idNodeMap = make(map[string][]string)
	nodeIdMap = make(map[string][]string)
	deviceFeederMap = make(map[string]string)

	for _, breaker := range rdf.Breakers {
		existDevice[breaker.ID] = true
		deviceFeederMap[breaker.ID] = breaker.Circuit.Resource
	}
	for _, segment := range rdf.ACLineSegments {
		existDevice[segment.ID] = true
		deviceFeederMap[segment.ID] = segment.Circuit.Resource
	}
	for _, fus := range rdf.Fuses {
		existDevice[fus.ID] = true
		deviceFeederMap[fus.ID] = fus.Circuit.Resource
	}
	for _, disconnector := range rdf.Disconnectors {
		existDevice[disconnector.ID] = true
		deviceFeederMap[disconnector.ID] = disconnector.Circuit.Resource
	}
	for _, transformer := range rdf.PowerTransformers {
		existDevice[transformer.ID] = true
		deviceFeederMap[transformer.ID] = transformer.Circuit.Resource
	}
	for _, section := range rdf.BusbarSections {
		existDevice[section.ID] = true
		deviceFeederMap[section.ID] = section.Circuit.Resource
	}
	for _, pole := range rdf.Poles {
		existDevice[pole.ID] = true
		deviceFeederMap[pole.ID] = pole.Circuit.Resource
	}
	for _, faultIndicator := range rdf.FaultIndicators {
		existDevice[faultIndicator.ID] = true
		deviceFeederMap[faultIndicator.ID] = faultIndicator.Circuit.Resource
	}

	for _, terminal := range rdf.Terminals {
		id := strings.Replace(terminal.ConductingEquipment.Resource, "#", "", -1)
		nodeId := strings.Replace(terminal.ConnectivityNode.Resource, "#", "", -1)

		if !existDevice[id] {
			fmt.Println("pass id:", id)
			continue
		}

		idNodeMap[id] = append(
			idNodeMap[id],
			nodeId,
		)
		nodeIdMap[nodeId] = append(
			nodeIdMap[nodeId],
			id,
		)
	}

	return idNodeMap, nodeIdMap, deviceFeederMap
}

func GetDeviceTopoMap(rdf *RDF) (resultList []TopoBO) {
	topoCacheMap := make(map[string][]Terminal)
	mainFeederCacheMap := make(map[string]bool)

	for _, terminal := range rdf.Terminals {
		topoCacheMap[terminal.ConductingEquipment.Resource] = append(topoCacheMap[terminal.ConductingEquipment.Resource], terminal)
	}

	for _, circuit := range rdf.Circuits {
		if circuit.IsCurrentFeeder == "1" {
			mainFeederCacheMap["#"+circuit.ID] = true // 特殊处理, 设备里的带#号
		}
	}
	for _, entity := range rdf.Breakers {
		terminals := topoCacheMap["#"+entity.ID]
		if len(terminals) == 0 {
			fmt.Println("topo not found!:", entity.ID)
			continue
		}
		if mainFeederCacheMap[entity.Circuit.Resource] == false {
			fmt.Println("not in main feeder pass id:", entity.ID)
			continue
		}
		resultList = append(resultList, TopoBO{
			SourceID:     entity.ID,
			SourceNode:   terminals,
			SourceFeeder: strings.ReplaceAll(entity.Circuit.Resource, "#", ""),
		})
	}
	for _, entity := range rdf.Disconnectors {
		terminals := topoCacheMap["#"+entity.ID]
		if len(terminals) == 0 {
			fmt.Println("topo not found!:", entity.ID)
			continue
		}
		if mainFeederCacheMap[entity.Circuit.Resource] == false {
			fmt.Println("not in main feeder pass id:", entity.ID)
			continue
		}
		resultList = append(resultList, TopoBO{
			SourceID:     entity.ID,
			SourceNode:   terminals,
			SourceFeeder: strings.ReplaceAll(entity.Circuit.Resource, "#", ""),
		})
	}
	for _, entity := range rdf.Fuses {
		terminals := topoCacheMap["#"+entity.ID]
		if len(terminals) == 0 {
			fmt.Println("topo not found!:", entity.ID)
			continue
		}
		if mainFeederCacheMap[entity.Circuit.Resource] == false {
			fmt.Println("not in main feeder pass id:", entity.ID)
			continue
		}
		resultList = append(resultList, TopoBO{
			SourceID:     entity.ID,
			SourceNode:   terminals,
			SourceFeeder: strings.ReplaceAll(entity.Circuit.Resource, "#", ""),
		})
	}
	for _, entity := range rdf.PowerTransformers {
		terminals := topoCacheMap["#"+entity.ID]
		if len(terminals) == 0 {
			fmt.Println("topo not found!:", entity.ID)
			continue
		}
		if mainFeederCacheMap[entity.Circuit.Resource] == false {
			fmt.Println("not in main feeder pass id:", entity.ID)
			continue
		}
		resultList = append(resultList, TopoBO{
			SourceID:     entity.ID,
			SourceNode:   terminals,
			SourceFeeder: strings.ReplaceAll(entity.Circuit.Resource, "#", ""),
		})
	}
	for _, entity := range rdf.BusbarSections {
		terminals := topoCacheMap["#"+entity.ID]
		if len(terminals) == 0 {
			fmt.Println("topo not found!:", entity.ID)
			continue
		}
		if mainFeederCacheMap[entity.Circuit.Resource] == false {
			fmt.Println("not in main feeder pass id:", entity.ID)
			continue
		}
		resultList = append(resultList, TopoBO{
			SourceID:     entity.ID,
			SourceNode:   terminals,
			SourceFeeder: strings.ReplaceAll(entity.Circuit.Resource, "#", ""),
		})
	}
	for _, entity := range rdf.ACLineSegments {
		terminals := topoCacheMap["#"+entity.ID]
		if len(terminals) == 0 {
			fmt.Println("topo not found!:", entity.ID)
			continue
		}
		if mainFeederCacheMap[entity.Circuit.Resource] == false {
			fmt.Println("not in main feeder pass id:", entity.ID)
			continue
		}
		resultList = append(resultList, TopoBO{
			SourceID:     entity.ID,
			SourceNode:   terminals,
			SourceFeeder: strings.ReplaceAll(entity.Circuit.Resource, "#", ""),
		})
	}
	for _, entity := range rdf.Poles {
		terminals := topoCacheMap["#"+entity.ID]
		if len(terminals) == 0 {
			fmt.Println("topo not found!:", entity.ID)
			continue
		}
		if mainFeederCacheMap[entity.Circuit.Resource] == false {
			fmt.Println("not in main feeder pass id:", entity.ID)
			continue
		}
		resultList = append(resultList, TopoBO{
			SourceID:     entity.ID,
			SourceNode:   terminals,
			SourceFeeder: strings.ReplaceAll(entity.Circuit.Resource, "#", ""),
		})
	}
	for _, entity := range rdf.FaultIndicators {
		terminals := topoCacheMap["#"+entity.ID]
		if len(terminals) == 0 {
			fmt.Println("topo not found!:", entity.ID)
			continue
		}
		if mainFeederCacheMap[entity.Circuit.Resource] == false {
			fmt.Println("not in main feeder pass id:", entity.ID)
			continue
		}
		resultList = append(resultList, TopoBO{
			SourceID:     entity.ID,
			SourceNode:   terminals,
			SourceFeeder: strings.ReplaceAll(entity.Circuit.Resource, "#", ""),
		})
	}

	return resultList
}

// HandleTopo @deprecated
func HandleTopo(idNodeMap, nodeIDMap map[string][]string, topoList []Topo, rdfDCloudMap map[string]IdMap, nodeMap map[string]NodeMap, deviceFeederMap map[string]string, db *gorm.DB, config data.AppConfig, owner string, rdf *RDF, circuitDCloudMap map[string]string) {
	// DCloud的ID-topo Map
	topoMap := make(map[string]Topo)
	for _, topo := range topoList {
		topoMap[topo.ID] = topo
	}

	for id, nodeList := range idNodeMap {
		groupNodeMap := make(map[string]bool)
		feederDCloudID := circuitDCloudMap[strings.ReplaceAll(deviceFeederMap[id], "#", "")]
		deviceDCloudID := rdfDCloudMap[id].ID

		if feederDCloudID == "" {
			fmt.Println("Feeder ID Not Get:", id)
			continue
		}

		isMainFeeder := data.CircuitMainFeederMap[strings.ReplaceAll(deviceFeederMap[id], "#", "")]
		var groupNodeList []string
		for _, node := range nodeList {
			if nodeMap[node].NodeID == "" {
				continue
			}
			groupNodeMap[nodeMap[node].NodeID] = true
			groupNodeList = append(groupNodeList, nodeMap[node].NodeID)
		}

		fmt.Println("--------------------------------------------")
		// 源端设备没有录入
		if deviceDCloudID == "" {
			fmt.Println("ID:", id, " Device Lost!")
			if isMainFeeder {
				if newID, err := NewDevice(id, rdf, owner, feederDCloudID); err != nil {
					fmt.Println("NewDevice error:", err)
					continue
				} else {
					fmt.Println("NewDevice success!", " ID:", newID)
					rdfDCloudMap[id] = IdMap{
						ID:    newID,
						RdfID: id,
					}
				}
			} else {
				fmt.Println("ID:", id, " not in mainFeeder")
				continue
			}
		}

		fmt.Println("DCloud Feeder:" + feederDCloudID)
		fmt.Print("Source:")
		fmt.Printf("ID: %s", id)
		for _, node := range nodeList {
			fmt.Printf("\t%s", node)
		}
		fmt.Println()

		fmt.Print("Trans:")
		fmt.Printf("ID: %s", deviceDCloudID)
		for _, node := range nodeList {
			fmt.Printf("\t%s", nodeMap[node].NodeID)
		}
		fmt.Println()

		fmt.Print("DB:")
		fmt.Printf("ID: %s First: %s Second: %s\n", topoMap[deviceDCloudID].ID, topoMap[deviceDCloudID].FirstNodeID, topoMap[deviceDCloudID].SecondNodeID)

		// 源端和数据库FeederID不一致
		if deviceDCloudID != "" && feederDCloudID != topoMap[deviceDCloudID].FeederID {
			// 更新feeder_id
			fmt.Println("FEEDER_ID:", topoMap[deviceDCloudID].ID, " Not Compare!")
			updateMap := make(map[string]interface{})
			updateMap["FEEDER_ID"] = feederDCloudID
			if result := db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("ID = ?", topoMap[deviceDCloudID].ID).
				Updates(updateMap); result.Error != nil {
				log.Println(result.Error)
			} else {
				log.Println(result.RowsAffected)
			}
		}
		if topoMap[deviceDCloudID].ID == deviceDCloudID &&
			groupNodeMap[topoMap[deviceDCloudID].FirstNodeID] &&
			groupNodeMap[topoMap[deviceDCloudID].SecondNodeID] &&
			(topoMap[deviceDCloudID].SecondNodeID != topoMap[deviceDCloudID].FirstNodeID) &&
			len(nodeList) == 2 {
			fmt.Println("ID compare!")
			continue
		}

		if topoMap[deviceDCloudID].SecondNodeID == topoMap[deviceDCloudID].FirstNodeID { // 数据库里首位拓扑相同的刷新一次
			updateMap := map[string]interface{}{
				"FIRST_NODE_ID": groupNodeList[0],
			}
			if len(groupNodeList) > 1 {
				updateMap["SECOND_NODE_ID"] = groupNodeList[1]
			} else {
				updateMap["SECOND_NODE_ID"] = nil
			}
			if result := db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("ID = ?", topoMap[deviceDCloudID].ID).
				Updates(updateMap); result.Error != nil {
				log.Println(result.Error)
			} else {
				log.Println(result.RowsAffected)
			}
		}

		if len(nodeList) > 2 {
			fmt.Println("ID:", deviceDCloudID, " has multiply node")
			continue
		}
		// 源端没有映射到数据库节点或ID
		if len(groupNodeList) == 0 || listContainID(groupNodeList, "") {
			fmt.Println("ID:", topoMap[deviceDCloudID].ID, " Cannot Convert!")
			groupNodeList = []string{}
			for _, node := range nodeList {
				newNodeID := ""
				{ // 先查是否有
					var entity NodeMap
					if res := db.Table(config.DB.Database+".NODE_MAP").Where("ID = ?", owner+node).Find(&entity); res.Error != nil {
						log.Println(res.Error)
					}
					if entity.NodeID != "" { // 不为空用数据库的
						newNodeID = entity.NodeID
					} else { // 为空生成
						newNodeID = GetNoUseNodeInFeeder(deviceFeederMap[id], owner)
						if newNodeID == "" {
							newNodeID = node // 没生成成功先用源端ID
						}
						db.Table(config.DB.Database + ".NODE_MAP").Create(map[string]interface{}{
							"ID":      owner + node,
							"NODE_ID": newNodeID,
						})
					}
				}
				groupNodeList = append(groupNodeList, newNodeID)
			}
			if topoMap[deviceDCloudID].ID == "" { // 表里没数据就新增
				insertMap := map[string]interface{}{
					"ID":            deviceDCloudID,
					"FEEDER_ID":     feederDCloudID,
					"OWNER":         owner,
					"FIRST_NODE_ID": groupNodeList[0],
				}
				if len(groupNodeList) > 1 {
					insertMap["SECOND_NODE_ID"] = groupNodeList[1]
				}
				if result := db.Table(config.DB.Database + ".SG_CON_DPWRGRID_R_TOPO").Create(insertMap); result.Error != nil {
					log.Println(result.Error)
				} else {
					log.Println(result.RowsAffected)
				}
			} else { // 有数据更新
				updateMap := make(map[string]interface{})
				updateMap["FIRST_NODE_ID"] = groupNodeList[0]
				if len(groupNodeList) > 1 {
					updateMap["SECOND_NODE_ID"] = groupNodeList[1]
				}
				if result := db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("ID = ?", topoMap[deviceDCloudID].ID).
					Updates(updateMap); result.Error != nil {
					log.Println(result.Error)
				} else {
					log.Println(result.RowsAffected)
				}
			}
			continue
		}
		// 源端有数据库没有
		if topoMap[deviceDCloudID].ID == "" {
			fmt.Println("ID:", topoMap[deviceDCloudID].ID, " Topo Not Exist!")
			insertMap := map[string]interface{}{
				"ID":            deviceDCloudID,
				"FEEDER_ID":     feederDCloudID,
				"OWNER":         owner,
				"FIRST_NODE_ID": groupNodeList[0],
			}
			if len(groupNodeList) > 1 {
				insertMap["SECOND_NODE_ID"] = groupNodeList[1]
			}
			if result := db.Table(config.DB.Database + ".SG_CON_DPWRGRID_R_TOPO").Create(insertMap); result.Error != nil {
				log.Println(result.Error)
			} else {
				log.Println(result.RowsAffected)
			}
			continue
		}
		// 源端和数据库节点不一致, FirstNode或SecondNode不存在
		if (topoMap[deviceDCloudID].FirstNodeID != "" && !listContainID(groupNodeList, topoMap[deviceDCloudID].FirstNodeID)) ||
			topoMap[deviceDCloudID].SecondNodeID != "" && !listContainID(groupNodeList, topoMap[deviceDCloudID].SecondNodeID) {
			fmt.Println("ID:", topoMap[deviceDCloudID].ID, " Not Compare!")
			updateMap := make(map[string]interface{})
			updateMap["FIRST_NODE_ID"] = groupNodeList[0]
			if len(groupNodeList) > 1 {
				updateMap["SECOND_NODE_ID"] = groupNodeList[1]
			} else {
				updateMap["SECOND_NODE_ID"] = nil
			}
			if result := db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("ID = ?", topoMap[deviceDCloudID].ID).
				Updates(updateMap); result.Error != nil {
				log.Println(result.Error)
			} else {
				log.Println(result.RowsAffected)
			}
			continue
		}
		// 源端和数据库
	}
}

func listContainID(list []string, item string) bool {
	for _, s := range list {
		if s == item {
			return true
		}
	}
	return false
}

// 处理大于2个的节点
func ConnectMultiplyNode(rdf *RDF, owner string) {
	if owner == "" {
		fmt.Println("Owner Not Exist!")
		return
	}

	nodeMap, nodeIDMap, _ := GetTopoMap(rdf)
	fmt.Println("nodeIDMap:", len(nodeIDMap))
	for id, nodeList := range nodeMap {
		if len(nodeList) <= 2 {
			continue
		}

		// 出现大于2的
		fmt.Println("ID:", id, ", nodeList: ", len(nodeList))
		var idEntity IdMap
		data.DB.Table(data.Config.DB.Database+".ID_MAP").
			Where("RDF_ID = ?", id).
			Find(&idEntity)
		if idEntity.ID == "" {
			fmt.Println("ID:", idEntity.ID, " DCloud Not Exist!")
			continue
		}
		// 获取所有node对应的DCloudID
		var nodeListEntity []NodeMap
		sourceDCloudNodeMap := make(map[string]string)
		{
			var cloudNodeList []string
			for _, node := range nodeList {
				cloudNodeList = append(cloudNodeList, owner+node)
			}
			data.DB.Table(data.Config.DB.Database+".NODE_MAP").
				Where("ID in ?", cloudNodeList).
				Find(&nodeListEntity)
			for _, node := range nodeListEntity {
				sourceDCloudNodeMap[node.ID] = node.NodeID
			}
			fmt.Println("sourceDCloudNodeMap:", sourceDCloudNodeMap)
		}

		{ // 更新超2个连接点的设备的首节点和末节点为0和1号节点
			data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("ID = ?", idEntity.ID).
				Updates(map[string]interface{}{
					"FIRST_NODE_ID":  sourceDCloudNodeMap[owner+nodeList[0]],
					"SECOND_NODE_ID": sourceDCloudNodeMap[owner+nodeList[1]],
				})
			fmt.Print("DCloudID:", idEntity.ID, ", nodeList:", nodeList)
			fmt.Println(", sourceDCloudNodeMap:", sourceDCloudNodeMap)
		}

		for i, node := range nodeList[2:] { // 第二个以后的节点都连到0号节点
			updateID := sourceDCloudNodeMap[owner+nodeList[0]]
			if i > 6 {
				updateID = sourceDCloudNodeMap[owner+nodeList[1]]
			}

			nodeID := sourceDCloudNodeMap[owner+node]
			idList := nodeIDMap[node]
			fmt.Println("Node: ", node, "DCloud Node: ", nodeID)

			var idEntityList []IdMap
			data.DB.Table(data.Config.DB.Database+".ID_MAP").
				Where("RDF_ID in ?", idList).
				Find(&idEntityList)
			for _, idMap := range idEntityList { // 首节点或尾节点的node更新为0号节点的node
				fmt.Println("ID:", idMap.ID, " RDF : ", idMap.RdfID)
				data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("FIRST_NODE_ID = ?", nodeID).
					Updates(map[string]interface{}{
						"FIRST_NODE_ID": updateID,
					})
				data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("SECOND_NODE_ID = ?", nodeID).
					Updates(map[string]interface{}{
						"SECOND_NODE_ID": updateID,
					})

			}
		}
		//if len(nodeList) > 8 { // 超过8个以后的节点都连到1号节点
		//
		//}
	}
}
