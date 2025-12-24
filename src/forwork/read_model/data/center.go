package data

import "gorm.io/gorm"

var Config AppConfig
var DB *gorm.DB
var DB2 *gorm.DB
var CircuitFeederMap = make(map[string]string)   // 源端馈线ID - 云ID
var CircuitMainFeederMap = make(map[string]bool) // 源端馈线ID - 是否主馈线
var OwnerOrganMap = map[string]string{
	"350100": "FZ",
	"350200": "XM",
	"350300": "PT",
	"350400": "SM",
	"350500": "QZ",
	"350600": "ZZ",
	"350700": "NP",
	"350800": "LY",
	"350900": "ND",
}
