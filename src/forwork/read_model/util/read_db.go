package util

import (
	"fmt"
	"log"
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
	ID          string `gorm:"column:ID"`
	PmsRdfID    string `gorm:"column:IS_JOIN"`
	MainNdValue string `gorm:"column:MIAN_ND_VALUE"`
	FileNdValue string `gorm:"column:FILE_ND_VALUE"`
}

func GetFeederJoin(db *gorm.DB, config *Config, feederID string) []ModelFeederJoin {
	var entity []ModelFeederJoin
	result := db.Table(config.DB.Database+".MODEL_FEEDER_JOIN").
		Where("FEEDER_ID = ?", feederID).
		Find(&entity)
	if result.Error != nil {
		log.Printf("❌ 查询数据失败: FeederID=%s\n",
			feederID)
	}
	return entity
}

func GetDCloudIDList(config Config, db *gorm.DB, idNodeMap map[string][]string) (map[string]IdMap, []string) {
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

func GetDCloudNodeIDList(config Config, db *gorm.DB, nodeIDMap map[string][]string, owner string) map[string]NodeMap {
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

func GetNoUseNodeInFeeder(pmsFeederID string, config Config, db *gorm.DB) string {
	var feederC FeederC
	if res := db.Table(config.DB.Database+".SG_CON_FEEDERLINE_C").Where("PMS_RDF_ID = ?", pmsFeederID).Find(&feederC); res.Error != nil {
		log.Fatalln(res.Error)
	}
	var nodeMap []NodeMap
	if res := db.Table(config.DB.Database+".NODE_MAP").Where("NODE_ID like ?", feederC.DCloudID).Find(&nodeMap); res.Error != nil {
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
