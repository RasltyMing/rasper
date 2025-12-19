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

func GetNoUseNodeInFeeder(pmsFeederID string, db *gorm.DB, owner string) string {
	pmsFeederID = strings.Replace(pmsFeederID, "#", "", -1)
	config := data.Config
	var feederC FeederC
	if res := db.Table(config.DB.Database+".SG_CON_FEEDERLINE_C").Where("PMS_RDF_ID = ?", pmsFeederID).Find(&feederC); res.Error != nil {
		log.Println(res.Error)
		return ""
	} else {
		log.Println("查询feederC:", pmsFeederID)
	}
	var nodeMap []NodeMap
	log.Println("select feederNode:", feederC.DCloudID, " source feederID:", pmsFeederID)
	if owner == "" {
		owner = "350000"
	}
	if feederC.DCloudID == "" {
		feederC.DCloudID = "17013" + fmt.Sprintf("%d", time.Now().UnixMilli())
	}
	if res := db.Table(config.DB.Database+".NODE_MAP").Where("NODE_ID like ?", feederC.DCloudID+"%").Find(&nodeMap); res.Error != nil {
		log.Fatalln(res.Error)
	}
	nodeHit := make([]bool, 10000)
	for _, node := range nodeMap {
		if len(node.NodeID) < 18 {
			return ""
		}
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

	for key, cloudID := range data.CircuitFeederMap {
		log.Println("handle circuit connect: ", key, ":", cloudID)

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

			if subTopo.FirstNodeID != "" {
				result = db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("FIRST_NODE_ID = ? or SECOND_NODE_ID = ?", subTopo.FirstNodeID, subTopo.FirstNodeID).
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
		}

		{ // 末端节点判断
			log.Printf("%s 开始处理末端节点，SecondNodeID: %s", logPrefix, subTopo.SecondNodeID)

			if subTopo.SecondNodeID != "" {
				result = db.Table(data.Config.DB.Database+".SG_CON_DPWRGRID_R_TOPO").
					Where("FIRST_NODE_ID = ? or SECOND_NODE_ID = ?", subTopo.SecondNodeID, subTopo.SecondNodeID).
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
		}

		log.Printf("%s: %s 拓扑处理完成 - FirstNodeID: %s, SecondNodeID: %s",
			key, logPrefix, subTopo.FirstNodeID, subTopo.SecondNodeID)
		delete(data.CircuitFeederMap, key)
	}
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
