package util

import (
	"errors"
	"fmt"
	"log"
	"raselper/src/forwork/read_model/data"
	"strconv"
	"strings"
	"time"

	"gorm.io/gorm"
)

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

type TopoWithPrt struct {
	D             *string `gorm:"column:D"`
	EffectiveTime *string `gorm:"column:EFFECTIVE_TIME"`
	ExpiryTime    *string `gorm:"column:EXPIRY_TIME"`
	FeederID      *string `gorm:"column:FEEDER_ID"`
	FirstNodeID   *string `gorm:"column:FIRST_NODE_ID"`
	ID            *string `gorm:"column:ID"`
	Owner         *string `gorm:"column:OWNER"`
	SecondNodeID  *string `gorm:"column:SECOND_NODE_ID"`
	Stamp         string  `gorm:"column:STAMP"`
}

type IdMap struct {
	ID           string `gorm:"column:ID"`
	RdfID        string `gorm:"column:RDF_ID"`
	Tbname       string `gorm:"column:TBNAME"`
	RegionID     string `gorm:"column:REGION_ID"`
	Type         string `gorm:"column:TYPE"`
	Mrid         string `gorm:"column:MRID"`
	Key          string `gorm:"column:KEY"`
	DevZone      string `gorm:"column:DEV_ZONE"`
	CheckBatchID string `gorm:"column:CHECK_BATCH_ID"`
	SendFlag     string `gorm:"column:SEND_FLAG"`
	PwType       string `gorm:"column:PW_TYPE"`
	PsrID        string `gorm:"column:PSR_ID"`
	IsZhuan      string `gorm:"column:IS_ZHUAN"`
}
type NodeMap struct {
	ID     string `gorm:"column:ID"`
	NodeID string `gorm:"column:NODE_ID"`
}
type NodeOwnerMap struct {
	ID     string `gorm:"column:ID"`
	NodeID string `gorm:"column:NODE_ID"`
	Owner  string `gorm:"column:OWNER"`
}
type FeederC struct {
	DCloudID string `gorm:"column:DCLOUD_ID"`
	PmsRdfID string `gorm:"column:PMS_RDF_ID"`
	Owner    string `gorm:"column:OWNER"`
}
type FeederIDMapTest struct {
	DCloudID string `gorm:"column:DCLOUD_ID"`
	PmsRdfID string `gorm:"column:PMS_RDF_ID"`
	Owner    string `gorm:"column:OWNER"`
}

type ModelFeederJoin struct {
	ID                string `gorm:"column:ID"`
	PmsRdfID          string `gorm:"column:PMS_RDF_ID"`
	MainNdValue       string `gorm:"column:MIAN_ND_VALUE"`
	FileNdValue       string `gorm:"column:FILE_ND_VALUE"`
	IsJoin            string `gorm:"column:IS_JOIN"`
	CIBDDCloudID      string `gorm:"column:CIBD_DCLOUD_ID"`       // 主网连接设备
	SubDeviceDCloudID string `gorm:"column:SUB_DEVICE_DCLOUD_ID"` // 配网连接设备
}

type FeederJoinDetail struct {
	ID         string `gorm:"column:ID"`
	Owner      string `gorm:"column:OWNER"`
	MainDevice string `gorm:"column:MAIN_DEVICE"`
	MainNode   string `gorm:"column:MAIN_NODE"`
	SubDevice  string `gorm:"column:SUB_DEVICE"`
	SubNode    string `gorm:"column:SUB_NODE"`
	InsertTime string `gorm:"column:INSERT_TIME"`
	UpdateTime string `gorm:"column:UPDATE_TIME"`
}
type SvgDevView struct {
	ID       string `gorm:"column:ID"`
	Owner    string `gorm:"column:OWNER"`
	PmsRdfID string `gorm:"column:PMS_RDF_ID"`
}

type SourceUnconnectTopo struct {
	ID        string `gorm:"column:ID"`
	Name      string `gorm:"column:NAME"`
	Ind       string `gorm:"column:IND"`
	Jnd       string `gorm:"column:JND"`
	RdfID     string `gorm:"column:RDF_ID"`
	StID      string `gorm:"column:ST_ID"`
	AreaID    string `gorm:"column:AREAID"`
	AreaRdfID string `gorm:"column:AREA_RDF_ID"`
	Owner     string `gorm:"column:OWNER"`
}

func GetFeederJoin(db *gorm.DB, config data.AppConfig, feederID string) ModelFeederJoin {
	var entity ModelFeederJoin
	result := db.Table(config.DB.Database+".MODEL_FEEDER_JOIN").
		Where("FEEDER_ID = ?", feederID).
		Find(&entity)
	if result.Error != nil {
		log.Printf("❌ 查询数据失败: FeederID=%s\n",
			feederID)
	}
	return entity
}

func GetDCloudIDList(config data.AppConfig, db *gorm.DB, idNodeMap map[string][]string) (map[string]IdMap, []string) {
	resultMap := make(map[string]IdMap)
	var rdfList []string
	var dcloudList []string
	for id, _ := range idNodeMap {
		rdfList = append(rdfList, id)
	}
	var idMap []IdMap
	result := db.Table(config.DB.Database+".ID_MAP").
		Where("RDF_ID in ?", rdfList).
		Find(&idMap)
	if result.Error != nil {
		log.Printf("查询IDMap失败: %v", result.Error)
	}
	for _, idMapEntity := range idMap {
		resultMap[idMapEntity.RdfID] = idMapEntity
		dcloudList = append(dcloudList, idMapEntity.ID)
	}
	if len(rdfList) != len(resultMap) {
		log.Printf("IdMap和RDFID查询数据不一致!")
	}

	return resultMap, dcloudList
}

func GetDCloudNodeIDList(config data.AppConfig, db *gorm.DB, nodeIDMap map[string][]string, owner string) map[string]NodeMap {
	resultMap := make(map[string]NodeMap)
	var rdfList []string
	for id, _ := range nodeIDMap {
		rdfList = append(rdfList, owner+id)
	}
	var idMap []NodeMap
	result := db.Table(config.DB.Database+".NODE_MAP").
		Where("ID in ?", rdfList).
		Find(&idMap)
	if result.Error != nil {
		log.Printf("查询NODE_MAP失败: %v", result.Error)
	}
	for _, idMapEntity := range idMap {
		sourceNode := strings.Replace(idMapEntity.ID, owner, "", -1)
		resultMap[sourceNode] = idMapEntity
	}
	if len(rdfList) != len(resultMap) {
		log.Printf("IdMap和RDFID查询数据不一致!")
	}

	return resultMap
}

func GetNoUseNodeInFeeder(feederID string, owner string) string {
	config := data.Config
	var nodeMap []NodeMap
	if owner == "" {
		return "170135" + fmt.Sprintf("%d", time.Now().UnixMicro())
	}
	if feederID == "" {
		return "170135" + fmt.Sprintf("%d", time.Now().UnixMicro())
	}
	if res := data.DB.Table(config.DB.Database+".NODE_MAP_OWNER").Where("NODE_ID like ?", feederID+"%").Find(&nodeMap); res.Error != nil {
		log.Println(res.Error)
	}
	nodeHit := make([]bool, 10000)
	for _, node := range nodeMap {
		if len(node.NodeID) < 18 {
			return "170135" + fmt.Sprintf("%d", time.Now().UnixMicro())
		}
		num, _ := strconv.Atoi(node.NodeID[18:])
		nodeHit[num] = true
	}

	for i, b := range nodeHit {
		if !b {
			n := fmt.Sprintf("%04d", i)
			return feederID + n
		}
	}

	return "170135" + fmt.Sprintf("%d", time.Now().UnixMicro())
}

func GetNoUseNodeInFeeder_NodeMapOwner(feederID string, id string, owner string) string {
	var nodeMapOwner NodeOwnerMap
	data.DB.Table(data.Config.DB.Database+".NODE_MAP_OWNER").
		Where("Owner = ? and ID = ?", owner, id).
		Find(&nodeMapOwner)
	if nodeMapOwner.NodeID != "" {
		return nodeMapOwner.NodeID
	}

	config := data.Config
	var nodeMap []NodeMap
	timeUniId := "170135" + fmt.Sprintf("%d", time.Now().UnixMicro())
	if owner == "" {
		if res := data.DB.Table(data.Config.DB.Database + ".NODE_MAP_OWNER").
			Create(&NodeOwnerMap{
				ID:     id,
				NodeID: timeUniId,
				Owner:  owner,
			}); res.Error != nil {
			fmt.Printf("Error: %v\n", res.Error)
		}
		return timeUniId
	}
	if feederID == "" {
		if res := data.DB.Table(data.Config.DB.Database + ".NODE_MAP_OWNER").
			Create(&NodeOwnerMap{
				ID:     id,
				NodeID: timeUniId,
				Owner:  owner,
			}); res.Error != nil {
			fmt.Printf("Error: %v\n", res.Error)
		}
		return timeUniId
	}
	if res := data.DB.Table(config.DB.Database+".NODE_MAP_OWNER").Where("NODE_ID like ?", feederID+"%").Find(&nodeMap); res.Error != nil {
		log.Println(res.Error)
	}
	nodeHit := make([]bool, 10000)
	for _, node := range nodeMap {
		if len(node.NodeID) < 18 {
			return timeUniId
		}
		num, _ := strconv.Atoi(node.NodeID[18:])
		nodeHit[num] = true
	}

	for i, b := range nodeHit {
		if !b {
			n := fmt.Sprintf("%04d", i)
			if res := data.DB.Table(data.Config.DB.Database + ".NODE_MAP_OWNER").
				Create(&NodeOwnerMap{
					ID:     id,
					NodeID: feederID + n,
					Owner:  owner,
				}); res.Error != nil {
				fmt.Printf("Error: %v\n", res.Error)
			}
			return feederID + n
		}
	}

	if res := data.DB.Table(data.Config.DB.Database + ".NODE_MAP_OWNER").
		Create(&NodeOwnerMap{
			ID:     id,
			NodeID: timeUniId,
			Owner:  owner,
		}); res.Error != nil {
		fmt.Printf("Error: %v\n", res.Error)
	}
	return timeUniId
}

func MainSubConnect(circuitDCloudMap map[string]string, circuitMainFeederMap map[string]bool) bool {
	db := data.DB
	success := true

	for key, cloudID := range circuitDCloudMap {
		log.Println("handle circuit connect: ", key, ":", cloudID)
		if !circuitMainFeederMap[key] {
			log.Println("not main circuit: ", key, ":", cloudID)
			continue
		}

		var modelModelJoin ModelFeederJoin
		result := db.Table(data.Config.DB.Database+".MODEL_FEEDER_JOIN").
			Where("ID = ?", cloudID).
			Find(&modelModelJoin)
		if result.Error != nil {
			log.Printf("查询MODEL_FEEDER_JOIN失败: %v", result.Error)
		}

		if modelModelJoin.IsJoin == "0" || modelModelJoin.CIBDDCloudID == "" {
			log.Println(cloudID, "未拼接!")
			continue
		}

		var topo Topo
		var mainNode string
		result = db.Table(data.Config.DB.Database+".SG_CON_PWRGRID_R_TOPO").
			Where("ID = ?", modelModelJoin.CIBDDCloudID).
			Find(&topo)
		if result.Error != nil {
			log.Println("查询SG_CON_PWRGRID_R_TOPO失败:", result.Error)
		}
		mainNode = topo.SecondNodeID
		if topo.SecondNodeID == "" { // 末端为空连首端
			mainNode = topo.FirstNodeID
		}

		// 查配网节点, 并连接到主网节点
		var subTopo Topo
		result = db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
			Where("ID = ?", modelModelJoin.SubDeviceDCloudID).
			Find(&subTopo)
		if result.Error != nil {
			log.Println("查询", modelModelJoin.SubDeviceDCloudID, "SG_CON_DPWRGRID_R_TOPO失败:", result.Error)
		}
		var topoList []Topo
		logPrefix := "[拓扑处理]"
		{ // 首节点判断
			log.Printf("%s 开始处理首节点，FirstNodeID: %s", logPrefix, subTopo.FirstNodeID)

			if subTopo.FirstNodeID != "" {
				result = db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("FIRST_NODE_ID = ? or SECOND_NODE_ID = ?", subTopo.FirstNodeID, subTopo.FirstNodeID).
					Find(&topoList)
				if result.Error != nil {
					log.Printf("%s 查询SG_CON_DPWRGRID_R_TOPO失败 - FirstNodeID: %s, 错误: %v",
						logPrefix, subTopo.FirstNodeID, result.Error)
				}
				log.Printf("%s 首节点查询完成 - FirstNodeID: %s, 查询到 %d 条记录",
					logPrefix, subTopo.FirstNodeID, len(topoList))

				if len(topoList) == 1 { // 未连接其他设备就连上主网设备
					log.Printf("%s 首节点为孤立节点，开始更新为主网节点 - FirstNodeID: %s → MainNode: %s",
						logPrefix, subTopo.FirstNodeID, mainNode)
					updateResult := db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
						Where("FIRST_NODE_ID = ?", subTopo.FirstNodeID).
						Updates(map[string]interface{}{
							"FIRST_NODE_ID": mainNode,
						})
					if updateResult.Error != nil {
						log.Printf("%s 首节点更新失败 - FirstNodeID: %s, 错误: %v",
							logPrefix, subTopo.FirstNodeID, updateResult.Error)
					} else {
						log.Printf("%s 首节点更新成功 - FirstNodeID: %s → MainNode: %s, 影响行数: %d",
							logPrefix, subTopo.FirstNodeID, mainNode, updateResult.RowsAffected)
						continue
					}
				}
			}
		}

		{ // 末端节点判断
			log.Printf("%s 开始处理末端节点，SecondNodeID: %s", logPrefix, subTopo.SecondNodeID)

			if subTopo.SecondNodeID != "" {
				result = db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("FIRST_NODE_ID = ? or SECOND_NODE_ID = ?", subTopo.SecondNodeID, subTopo.SecondNodeID).
					Find(&topoList)

				if result.Error != nil {
					log.Printf("%s 查询SG_CON_DPWRGRID_R_TOPO失败 - SecondNodeID: %s, 错误: %v",
						logPrefix, subTopo.SecondNodeID, result.Error)
				}

				log.Printf("%s 末端节点查询完成 - SecondNodeID: %s, 查询到 %d 条记录",
					logPrefix, subTopo.SecondNodeID, len(topoList))

				if len(topoList) == 1 { // 未连接其他设备就连上主网设备
					log.Printf("%s 末端节点为孤立节点，开始更新为主网节点 - SecondNodeID: %s → MainNode: %s",
						logPrefix, subTopo.SecondNodeID, mainNode)

					updateResult := db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
						Where("SECOND_NODE_ID = ?", subTopo.SecondNodeID).
						Updates(map[string]interface{}{
							"SECOND_NODE_ID": mainNode,
						})

					if updateResult.Error != nil {
						log.Printf("%s 末端节点更新失败 - SecondNodeID: %s, 错误: %v",
							logPrefix, subTopo.SecondNodeID, updateResult.Error)
					} else {
						log.Printf("%s 末端节点更新成功 - SecondNodeID: %s → MainNode: %s, 影响行数: %d",
							logPrefix, subTopo.SecondNodeID, mainNode, updateResult.RowsAffected)
						continue
					}
				}
			}
		}

		log.Printf("%s: %s 拓扑处理完成, but no node update... - FirstNodeID: %s, SecondNodeID: %s",
			key, logPrefix, subTopo.FirstNodeID, subTopo.SecondNodeID)
		success = false
	}
	return success
}

func NewDevice(id string, rdf *RDF, owner string, feederDCloudID string) (string, error) {
	config := data.Config
	db := data.DB
	if strings.HasPrefix(id, "10100000") {
		fmt.Println("Lost ACLine, fix...")
		var idFromSeq string
		if result := db.Raw("select " + config.DB.Database + ".SG_DEV_LOWVOLLINE_B_" + data.OwnerOrganMap[owner] + "_SEQ.NEXTVAL as ID;").Scan(&idFromSeq); result.Error != nil {
			return "", result.Error
		}
		var segmentFind ACLineSegment
		for _, segment := range rdf.ACLineSegments { // 找对应的设备信息
			if segment.ID == id {
				segmentFind = segment
				break
			}
		}
		// 新增设备
		if segmentFind.ID != id {
			return "", errors.New("Device Not Found?")
		}
		// 使用事务，并优化时间格式化和错误处理
		err := db.Transaction(func(tx *gorm.DB) error {
			// 在事务外部计算时间，确保两个记录使用相同的时间戳
			currentTime := time.Now()
			stamp := "350000_00613500000001_" + currentTime.Format("2006-01-02 15:04:05")
			updateTime := currentTime.Format("2006-01-02 15:04:05")

			// ID_MAP插入
			if err := tx.Table(config.DB.Database + ".ID_MAP").
				Create(map[string]interface{}{
					"ID":        idFromSeq,
					"RDF_ID":    id,
					"TBNAME":    "aclinesegment",
					"REGION_ID": owner,
				}).Error; err != nil {
				return fmt.Errorf("插入SG_DEV_LOWVOLLINE_B失败: %w", err)
			}
			// 第一个表插入
			if err := tx.Table(config.DB.Database + ".SG_DEV_LOWVOLLINE_B").
				Create(map[string]interface{}{
					"ID":            idFromSeq,
					"FEEDER_ID":     feederDCloudID,
					"NAME":          segmentFind.Name,
					"OWNER":         owner,
					"STAMP":         stamp,
					"ABBREVIATION":  segmentFind.Name,
					"DCC_ID":        "0021" + owner,
					"RUNNING_STATE": "1003",
					"VOLTAGE_TYPE":  "1010",
					"WIRE_TYPE":     "1001",
				}).Error; err != nil {
				return fmt.Errorf("插入SG_DEV_LOWVOLLINE_B失败: %w", err)
			}

			// 第二个表插入
			if err := tx.Table(config.DB.Database + ".SG_DEV_LOWVOLLINE_C").
				Create(map[string]interface{}{
					"DATASOURCE_ID": "0021" + owner,
					"DCLOUD_ID":     idFromSeq,
					"EMS_ID":        id,
					"OWNER":         owner,
					"PMS_RDF_ID":    id,
					"STAMP":         stamp,
					"UPDATE_TIME":   updateTime,
				}).Error; err != nil {
				return fmt.Errorf("插入SG_DEV_LOWVOLLINE_C失败: %w", err)
			}

			return nil
		})
		// 处理事务结果
		if err != nil {
			// 可以根据错误类型进行不同的处理
			log.Printf("数据插入失败: %v", err)
		}
		log.Println("数据插入成功")
		return idFromSeq, nil
	}

	return "", errors.New(id + ": Device Type Not Found")
}

type TopoBO struct {
	SourceID         string
	DCloudID         string
	SourceFeeder     string
	DCloudFeeder     string
	SourceFirstNode  string
	SourceSecondNode string
	TransFirstNode   string
	TransSecondNode  string
	DCloudFirstNode  string
	DCloudSecondNode string
	D                string  `gorm:"column:D"`
	EffectiveTime    string  `gorm:"column:EFFECTIVE_TIME"`
	ExpiryTime       string  `gorm:"column:EXPIRY_TIME"`
	FeederID         string  `gorm:"column:FEEDER_ID"`
	FirstNodeID      *string `gorm:"column:FIRST_NODE_ID"`
	ID               string  `gorm:"column:ID"`
	Owner            string  `gorm:"column:OWNER"`
	SecondNodeID     *string `gorm:"column:SECOND_NODE_ID"`
	Stamp            string  `gorm:"column:STAMP"`
	SourceNode       []Terminal
}

func GetTopoInfoInDCloud(topoInfo []TopoBO, cloudMap map[string]string, owner string) (resultList []TopoBO) {
	cloudTopoCacheMap := make(map[string]Topo)
	sourceIDCacheMap := make(map[string]string)
	sourceTopoCacheMap := make(map[string]string)

	{ // 查表
		var sourceIDList []string
		var sourceTopoList []string
		for _, topo := range topoInfo {
			sourceIDList = append(sourceIDList, topo.SourceID)
			for _, terminal := range topo.SourceNode {
				sourceTopoList = append(sourceTopoList, terminal.ConnectivityNode.Resource)
			}
		}
		// 查ID和Topo对应关系
		var cloudIDList []IdMap
		data.DB.Table(data.Config.DB.Database+".ID_MAP").
			Where("RDF_ID in ?", sourceIDList).
			Find(&cloudIDList)
		var cloudTopoList []NodeOwnerMap
		data.DB.Table(data.Config.DB.Database+".NODE_MAP_OWNER").
			Where("OWNER in ? and ID in ?", owner, sourceTopoList).
			Find(&cloudTopoList)
		for _, idMap := range cloudIDList {
			sourceIDCacheMap[idMap.RdfID] = idMap.ID
		}
		for _, node := range cloudTopoList {
			sourceTopoCacheMap[node.ID] = node.NodeID
		}

		{ // 用转换的ID查topo表
			var cloudIDForQuery []string
			for _, idMap := range cloudIDList {
				cloudIDForQuery = append(cloudIDForQuery, idMap.ID)
			}
			var topoList []Topo
			data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("ID in ?", cloudIDForQuery).
				Find(&topoList)
			for _, topo := range topoList {
				cloudTopoCacheMap[topo.ID] = topo
			}
		}
	}

	for _, topo := range topoInfo {
		cloudID := sourceIDCacheMap[topo.SourceID]
		cloudTopo := cloudTopoCacheMap[cloudID]
		topo.DCloudID = cloudID
		topo.DCloudFeeder = cloudMap[topo.SourceFeeder]
		topo.DCloudFirstNode = cloudTopo.FirstNodeID
		topo.DCloudSecondNode = cloudTopo.SecondNodeID
		{
			topo.D = cloudTopo.D
			topo.EffectiveTime = cloudTopo.EffectiveTime
			topo.ExpiryTime = cloudTopo.ExpiryTime
			topo.FeederID = cloudTopo.FeederID
			topo.FirstNodeID = StringReturnNil(&cloudTopo.FirstNodeID, "")
			topo.ID = cloudTopo.ID
			topo.Owner = cloudTopo.Owner
			topo.SecondNodeID = StringReturnNil(&cloudTopo.SecondNodeID, "")
			topo.Stamp = cloudTopo.Stamp
		}

		if len(topo.SourceNode) > 0 && topo.SourceNode[0].ConnectivityNode.Resource != "#0" {
			topo.TransFirstNode = sourceTopoCacheMap[topo.SourceNode[0].ConnectivityNode.Resource]
			topo.SourceFirstNode = topo.SourceNode[0].ConnectivityNode.Resource
		}
		if len(topo.SourceNode) > 1 && topo.SourceNode[1].ConnectivityNode.Resource != "#0" {
			topo.TransSecondNode = sourceTopoCacheMap[topo.SourceNode[1].ConnectivityNode.Resource]
			topo.SourceSecondNode = topo.SourceNode[1].ConnectivityNode.Resource
		}
		resultList = append(resultList, topo)
	}

	return resultList
}

func HandleDBTopo(topoList []TopoBO, cloudMap map[string]string, owner string, rdf *RDF) {
	for _, topoBO := range topoList {
		fmt.Printf("topo: %v\n", topoBO)                                 // 打印未处理时
		if topoBO.SourceFirstNode != "" && topoBO.TransFirstNode == "" { // 首节点空
			newNode := GetNoUseNodeInFeeder_NodeMapOwner(topoBO.DCloudFeeder, topoBO.SourceFirstNode, owner)
			topoBO.TransFirstNode = newNode
		}
		if topoBO.SourceSecondNode != "" && topoBO.TransSecondNode == "" { // 尾节点空
			newNode := GetNoUseNodeInFeeder_NodeMapOwner(topoBO.DCloudFeeder, topoBO.SourceSecondNode, owner)
			topoBO.TransSecondNode = newNode
		}
		// 处理后结果
		fmt.Printf("source: %s, %s, %s\n", topoBO.SourceID, topoBO.SourceFirstNode, topoBO.SourceSecondNode)
		fmt.Printf("trans: %s, %s, %s\n", topoBO.DCloudID, topoBO.TransFirstNode, topoBO.TransSecondNode)
		fmt.Printf("cloud: %s, %s, %s\n", topoBO.ID, SafeGetString(topoBO.FirstNodeID), SafeGetString(topoBO.SecondNodeID))

		// 开始录入
		if topoBO.DCloudID == "" {
			newID, _ := NewDevice(topoBO.SourceID, rdf, owner, topoBO.DCloudFeeder)
			if newID == "" { // 没生成跳过
				fmt.Println("newID is empty, pass:" + topoBO.DCloudID)
			}
			fmt.Println("newID:", newID)
		}

		if topoBO.ID == "" && topoBO.DCloudID != "" { // 没这个topo
			data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("ID = ?", topoBO.ID).
				Create(&TopoWithPrt{
					ID:           &topoBO.DCloudID,
					Owner:        &owner,
					FeederID:     &topoBO.DCloudFeeder,
					FirstNodeID:  StringReturnNil(&topoBO.TransFirstNode, ""),
					SecondNodeID: StringReturnNil(&topoBO.TransSecondNode, ""),
					Stamp:        "350000_00613500000001_" + time.Now().Format("2006-01-02 15:04:05"),
				})
			continue
		}

		if topoBO.TransFirstNode != topoBO.DCloudFirstNode || topoBO.TransSecondNode != topoBO.DCloudSecondNode {
			data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("ID = ?", topoBO.ID).
				Updates(map[string]interface{}{
					"FIRST_NODE_ID":  StringReturnNil(&topoBO.TransFirstNode, ""),
					"SECOND_NODE_ID": StringReturnNil(&topoBO.TransSecondNode, ""),
				})
			continue
		}

		fmt.Printf("else: %v\n", topoBO)
		fmt.Println("---------------------")
	}
}

func HandleMultiplyNode(topoList []TopoBO, cloudMap map[string]string, owner string, rdf *RDF) {
	for _, topoBO := range topoList {
		if len(topoBO.SourceNode) > 2 {
			fmt.Printf("topo more than 2: %s\n", topoBO.SourceID)
			fmt.Printf("topo: %v\n", topoBO)
			fmt.Printf("cloud: %s, %s, %s\n", topoBO.ID, SafeGetString(topoBO.FirstNodeID), SafeGetString(topoBO.SecondNodeID))
			for _, terminal := range topoBO.SourceNode[2:] {
				newNode := GetNoUseNodeInFeeder_NodeMapOwner(topoBO.DCloudFeeder, terminal.ConnectivityNode.Resource, owner)
				fmt.Println("multiply newNode:", newNode)
				data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("FEEDER_ID = ? and FIRST_NODE_ID = ?", topoBO.DCloudFeeder, newNode).
					Updates(map[string]interface{}{
						"FIRST_NODE_ID": topoBO.TransFirstNode,
					})
				data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("FEEDER_ID = ? and SECOND_NODE_ID = ?", topoBO.DCloudFeeder, newNode).
					Updates(map[string]interface{}{
						"SECOND_NODE_ID": topoBO.TransSecondNode,
					})
			}
		}
	}
}

func HandleEmptyTopo(cloud []TopoBO) {
	var firstEmptyList []string
	var secondEmptyList []string

	for _, topoBO := range cloud {
		if topoBO.TransFirstNode == "" {
			firstEmptyList = append(firstEmptyList, topoBO.DCloudID)
		}
		if topoBO.TransSecondNode == "" {
			secondEmptyList = append(secondEmptyList, topoBO.DCloudID)
		}
	}

	if res := data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
		Where("ID in ?", firstEmptyList).
		Updates(map[string]interface{}{
			"FIRST_NODE_ID": nil,
		}); res.Error != nil {
		fmt.Printf("ERROR: %v\n", res.Error)
	}
	if res := data.DB.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
		Where("ID in ?", secondEmptyList).
		Updates(map[string]interface{}{
			"SECOND_NODE_ID": nil,
		}); res.Error != nil {
		fmt.Printf("ERROR: %v\n", res.Error)
	}
}

func GetMainBus(busID string) string {
	var cloudBusID string
	data.DB.Raw("select DCLOUD_ID from DKYPW.SG_DEV_BUSBAR_C where DCLOUD_ID = (select BUSBAR_EMS_ID from DKYPW.BUSBARID_SMD where BUSBAR_RDF_ID = '" + busID + "')").
		Find(&cloudBusID)

	return cloudBusID
}
func GetMainBreaker(breakerID string) string {
	var cloudID string
	data.DB.Raw("select DCLOUD_ID from DKYPW.SG_DEV_BUSBAR_C where DCLOUD_ID = (select BUSBAR_EMS_ID from DKYPW.BUSBARID_SMD where BUSBAR_RDF_ID = '" + breakerID + "')").
		Find(&cloudID)

	return cloudID
}
