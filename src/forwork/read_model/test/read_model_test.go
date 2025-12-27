package test

import (
	"raselper/src/forwork/read_model/util"
	"testing"
)

func TestReadFile(t *testing.T) {
	cimxml, _ := util.ParseCIMXML("../../data/西农线922线路_单线图_10000100@7606_20251106110425.xml")
	println(len(cimxml.Circuits))
	println(len(cimxml.Breakers))
}
