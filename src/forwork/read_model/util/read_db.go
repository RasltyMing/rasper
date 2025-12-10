package util

import (
	"fmt"
	"log"
	"raselper/src/forwork/read_model/data"
	"strconv"
	"strings"

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
type FeederC struct {
	DCloudID string `gorm:"column:DCLOUD_ID"`
	PmsRdfID string `gorm:"column:PMS_RDF_ID"`
	Owner    string `gorm:"column:OWNER"`
}

type ModelFeederJoin struct {
	ID                string `gorm:"column:ID"`
	PmsRdfID          string `gorm:"column:IS_JOIN"`
	MainNdValue       string `gorm:"column:MIAN_ND_VALUE"`
	FileNdValue       string `gorm:"column:FILE_ND_VALUE"`
	IsJoin            string `gorm:"column:IS_JOIN"`
	CIBDDCloudID      string `gorm:"column:CIBD_DCLOUD_ID"`       // 主网连接设备
	SubDeviceDCloudID string `gorm:"column:SUB_DEVICE_DCLOUD_ID"` // 配网连接设备
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

func GetNoUseNodeInFeeder(pmsFeederID string, db *gorm.DB) string {
	pmsFeederID = strings.Replace(pmsFeederID, "#", "", -1)
	config := data.Config
	var feederC FeederC
	if res := db.Table(config.DB.Database+".SG_CON_FEEDERLINE_C").Where("PMS_RDF_ID = ?", pmsFeederID).Find(&feederC); res.Error != nil {
		log.Fatalln(res.Error)
	} else {
		log.Println("查询feederC:", pmsFeederID)
	}
	var nodeMap []NodeMap
	log.Println("select feederNode:", feederC.DCloudID, " source feederID:", pmsFeederID)
	if res := db.Table(config.DB.Database+".NODE_MAP").Where("NODE_ID like ?", feederC.DCloudID+"%").Find(&nodeMap); res.Error != nil {
		log.Fatalln(res.Error)
	}
	nodeHit := make([]bool, 10000)
	for _, node := range nodeMap {
		num, _ := strconv.Atoi(node.NodeID[18:])
		nodeHit[num] = true
	}

	for i, b := range nodeHit {
		if !b {
			n := fmt.Sprintf("%04d", i)
			return feederC.DCloudID + n
		}
	}

	return ""
}

func MainSubConnect() {
	db := data.DB

	for _, cloudID := range data.CircuitFeederMap {
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
			log.Fatalln("查询SG_CON_PWRGRID_R_TOPO失败:", result.Error)
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
			log.Fatalln("查询", modelModelJoin.SubDeviceDCloudID, "SG_CON_DPWRGRID_R_TOPO失败:", result.Error)
		}
		var topoList []Topo
		logPrefix := "[拓扑处理]"
		{ // 首节点判断
			log.Printf("%s 开始处理首节点，FirstNodeID: %s", logPrefix, subTopo.FirstNodeID)

			result = db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("FIRST_NODE_ID = ?", subTopo.FirstNodeID).
				Find(&topoList)
			if result.Error != nil {
				log.Fatalf("%s 查询SG_CON_DPWRGRID_R_TOPO失败 - FirstNodeID: %s, 错误: %v",
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
				}
			}
		}

		{ // 末端节点判断
			log.Printf("%s 开始处理末端节点，SecondNodeID: %s", logPrefix, subTopo.SecondNodeID)

			result = db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
				Where("SECOND_NODE_ID = ?", subTopo.SecondNodeID).
				Find(&topoList)

			if result.Error != nil {
				log.Fatalf("%s 查询SG_CON_DPWRGRID_R_TOPO失败 - SecondNodeID: %s, 错误: %v",
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
				}
			}
		}

		log.Printf("%s 拓扑处理完成 - FirstNodeID: %s, SecondNodeID: %s",
			logPrefix, subTopo.FirstNodeID, subTopo.SecondNodeID)
	}
}
