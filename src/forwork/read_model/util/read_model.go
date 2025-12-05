package util

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
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

func GetTopoMap(rdf *RDF) (map[string][]string, map[string][]string, map[string]string) {
	existDevice := make(map[string]bool)

	idNodeMap := make(map[string][]string)
	nodeIdMap := make(map[string][]string)
	deviceFeederMap := make(map[string]string)

	for _, breaker := range rdf.Breakers {
		existDevice[breaker.ID] = true
		deviceFeederMap[breaker.ID] = breaker.Circuit.Resource
	}
	for _, segment := range rdf.ACLineSegments {
		existDevice[segment.ID] = true
		deviceFeederMap[segment.ID] = segment.ID
	}
	for _, fus := range rdf.Fuses {
		existDevice[fus.ID] = true
		deviceFeederMap[fus.ID] = fus.ID
	}
	for _, disconnector := range rdf.Disconnectors {
		existDevice[disconnector.ID] = true
		deviceFeederMap[disconnector.ID] = disconnector.ID
	}
	for _, transformer := range rdf.PowerTransformers {
		existDevice[transformer.ID] = true
		deviceFeederMap[transformer.ID] = transformer.Circuit.Resource
	}
	for _, section := range rdf.BusbarSections {
		existDevice[section.ID] = true
		deviceFeederMap[section.ID] = section.ID
	}
	for _, pole := range rdf.Poles {
		existDevice[pole.ID] = true
		deviceFeederMap[pole.ID] = pole.ID
	}
	for _, faultIndicator := range rdf.FaultIndicators {
		existDevice[faultIndicator.ID] = true
		deviceFeederMap[faultIndicator.ID] = faultIndicator.ID
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

func HandleTopo(idNodeMap, nodeIDMap map[string][]string, topoList []Topo, rdfDCloudMap map[string]IdMap, nodeMap map[string]NodeMap, deviceFeederMap map[string]string, db *gorm.DB, config Config, owner string) {
	// DCloud的ID-topo Map
	topoMap := make(map[string]Topo)
	for _, topo := range topoList {
		topoMap[topo.ID] = topo
	}

	for id, nodeList := range idNodeMap {
		infoDCloud := rdfDCloudMap[id]
		dbTopo := topoMap[infoDCloud.ID]
		groupNodeMap := make(map[string]bool)
		var groupNodeList []string
		for _, node := range nodeList {
			groupNodeMap[nodeMap[node].NodeID] = true
			groupNodeList = append(groupNodeList, nodeMap[node].NodeID)
		}
		if dbTopo.ID == infoDCloud.ID && groupNodeMap[dbTopo.FirstNodeID] && groupNodeMap[dbTopo.SecondNodeID] && len(nodeList) == 2 {
			continue
		}

		fmt.Println("--------------------------------------------")
		fmt.Print("Source:")
		fmt.Printf("ID: %s", id)
		for _, node := range nodeList {
			fmt.Printf("\t%s", node)
		}
		fmt.Println()

		fmt.Print("Trans:")
		fmt.Printf("ID: %s", infoDCloud.ID)
		for _, node := range nodeList {
			fmt.Printf("\t%s", nodeMap[node].NodeID)
		}
		fmt.Println()

		fmt.Print("DB:")
		fmt.Printf("ID: %s First: %s Second: %s\n", dbTopo.ID, dbTopo.FirstNodeID, dbTopo.SecondNodeID)

		if len(nodeList) > 2 {
			fmt.Println("ID:", infoDCloud.ID, " has multiply node")
			continue
		}
		// 源端设备没有录入
		if rdfDCloudMap[id].ID == "" {
			fmt.Println("ID:", dbTopo.ID, " Device Lost!")
			continue
		}
		// 源端没有映射到数据库节点或ID
		if len(groupNodeList) == 0 || listContainID(groupNodeList, "") {
			fmt.Println("ID:", dbTopo.ID, " Cannot Convert!")
			groupNodeList = []string{}
			for _, node := range nodeList {
				newNodeID := GetNoUseNodeInFeeder(deviceFeederMap[id], config, db)
				groupNodeList = append(groupNodeList, newNodeID)
				db.Table(config.DB.Database + ".NODE_MAP").Create(map[string]interface{}{
					"ID":      owner + node,
					"NODE_ID": newNodeID,
				})
			}
			if dbTopo.ID == "" { // 表里没数据就新增
				insertMap := map[string]interface{}{
					"ID":            infoDCloud.ID,
					"FEEDER_ID":     groupNodeList[0][0:18],
					"OWNER":         owner,
					"FIRST_NODE_ID": groupNodeList[0],
				}
				if len(groupNodeList) > 1 {
					insertMap["SECOND_NODE_ID"] = groupNodeList[1]
				}
				if result := db.Table(config.DB.Database + ".SG_CON_DPWRGRID_R_TOPO").Create(insertMap); result.Error != nil {
					log.Fatalln(result.Error)
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
					Where("ID = ?", dbTopo.ID).
					Updates(updateMap); result.Error != nil {
					log.Fatalln(result.Error)
				} else {
					log.Println(result.RowsAffected)
				}
			}
			continue
		}
		// 源端有数据库没有
		if dbTopo.ID == "" {
			fmt.Println("ID:", dbTopo.ID, " Not Exist!")
			insertMap := map[string]interface{}{
				"ID":            infoDCloud.ID,
				"FEEDER_ID":     groupNodeList[0][0:18],
				"OWNER":         owner,
				"FIRST_NODE_ID": groupNodeList[0],
			}
			if len(groupNodeList) > 1 {
				insertMap["SECOND_NODE_ID"] = groupNodeList[1]
			}
			if result := db.Table(config.DB.Database + ".SG_CON_DPWRGRID_R_TOPO").Create(insertMap); result.Error != nil {
				log.Fatalln(result.Error)
			} else {
				log.Println(result.RowsAffected)
			}
			continue
		}
		// 源端和数据库节点不一致, FirstNode或SecondNode不存在
		if (dbTopo.FirstNodeID != "" && !listContainID(groupNodeList, dbTopo.FirstNodeID)) ||
			dbTopo.SecondNodeID != "" && !listContainID(groupNodeList, dbTopo.SecondNodeID) {
			fmt.Println("ID:", dbTopo.ID, " Not Compare!")
			updateMap := make(map[string]interface{})
			updateMap["FIRST_NODE_ID"] = groupNodeList[0]
			if len(groupNodeList) > 1 {
				updateMap["SECOND_NODE_ID"] = groupNodeList[1]
			}
			if result := db.Table(config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("ID = ?", dbTopo.ID).
				Updates(updateMap); result.Error != nil {
				log.Fatalln(result.Error)
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
