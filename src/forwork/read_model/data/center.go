package data

import "gorm.io/gorm"

var Config AppConfig
var DB *gorm.DB
var CircuitFeederMap = make(map[string]string)   // 源端馈线ID - 云ID
var CircuitMainFeederMap = make(map[string]bool) // 源端馈线ID - 是否主馈线
