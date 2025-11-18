/*该例程实现插入数据，修改数据，删除数据，数据查询等基本操作。*/
package main

// 引入相关包
import (
	"database/sql"
	"dm"
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

var db *sql.DB
var err error
var ownerList = []string{"350100", "350200", "350300", "350400", "350500", "350600", "350700", "350800", "350900"}

// | 1702 | DEV  | 馈线段         | SG_DEV_LOWVOLLINE_B       | ACLine
// | 1703 | DEV  | 配电变压器     | SG_DEV_DPWRTRANSFM_B      | Pdtransformer
// | 1704 | CON  | 配电站房       | SG_CON_DDISTSUBSTATION_B  | zf
// | 1706 | DEV  | 断路器         | SG_DEV_DBREAKER_B         | CBreaker
// | 1707 | DEV  | 负荷开关       | SG_DEV_DLOADSWITCH_B      | Fhkg
// | 1708 | DEV  | 熔断器         | SG_DEV_DFUSE_B            | Dlsrdq
// | 1709 | DEV  | 刀闸           | SG_DEV_DDIS_B             | Disconnector
// | 1711 | DEV  | 母线段         | SG_DEV_DBUS_B             | Bus
var modelList = map[string]string{
	"ACLINE":        "SG_DEV_LOWVOLLINE_B",
	"Pdtransformer": "SG_DEV_DPWRTRANSFM_B",
	"zf":            "SG_CON_DDISTSUBSTATION_B",
	"CBreaker":      "SG_DEV_DBREAKER_B",
	"Fhkg":          "SG_DEV_DLOADSWITCH_B",
	"Dlsrdq":        "SG_DEV_DFUSE_B",
	"Disconnector":  "SG_DEV_DDIS_B",
	"Bus":           "SG_DEV_DBUS_B",
}
var templateMap = map[string]string{
	"ACLINE":        "        <ACLine id=\"%s\" ls=\"1\" lc=\"91,209,174\" lw=\"1\" keyid=\"%s\" rdfid=\"%s\" app=\"800000\" af=\"2147483647\" recordApp=\"128\" link=\"\" d=\"225.77969000000002,354.5 225.30002000000002,354.5\" voltype=\"112871465677750273\"/>\n",
	"Pdtransformer": "        <Pdtransformer id=\"%s\" x=\"211.0\" y=\"323.499996\" ls=\"1\" lc=\"91,209,174\" lw=\"1\" tfr=\"rotate(0,230.0,334.499996) translate(230.0,334.499996) scale(1.0,1.0) translate(-230.0,-334.499996)\" keyid=\"%s\" rdfid=\"%s\" devref=\"#QZPD_peibianbianyaqi_zb.pdtransformer.icn.g:QZPD_peibianbianyaqi_zb_nr\" w=\"38.0\" h=\"22.0\" voltype=\"112871465677750273\" keyidDesc=\"鍖哄浗绋�#1鍙�\"/>\n",
	"zf":            "        <zf id=\"%s\" x=\"694.25\" y=\"348.75\" ls=\"1\" lc=\"91,209,174\" keyid=\"%s\" rdfid=\"%s\" app=\"800000\" af=\"2147483647\" recordApp=\"128\" d=\"699.25,353.75 699.25,555.25 1497.75,555.25 1497.75,353.75\" w=\"798.5\" h=\"201.5\" lw=\"3.0\" type=\"1\" fill_bounds=\"false\" groupObjId=\"\" voltype=\"\" fm=\"0\" fc=\"255,255,255\" zoomScaleType=\"1\" keyidDesc=\"鍐滆仈绀剧幆缃戞煖\" switchapp=\"1\" effectId=\"\" idinmodel=\"\"/>\n",
	"CBreaker":      "        <CBreaker id=\"%s\" x=\"803.294396\" y=\"398.705604\" ls=\"1\" lc=\"91,209,174\" lw=\"1\" tfr=\"rotate(90,819.294396,405.205604) translate(819.294396,405.205604) scale(1.0,1.0) translate(-819.294396,-405.205604)\" keyid=\"%s\" rdfid=\"%s\" devref=\"#QZPD_duanluqi.dlq.icn.g:QZPD_duanluqi\" w=\"32.0\" h=\"13.0\" voltype=\"112871465677750273\" keyidDesc=\"鍐滆仈绀剧幆缃戞煖椹\uE102伓璺\uE21B薄姘村巶鈪犵嚎906寮€鍏�\"/>\n",
	"Fhkg":          "        <Fhkg id=\"%s\" x=\"719.35001\" y=\"393.64999\" ls=\"1\" lc=\"91,209,174\" lw=\"1\" tfr=\"rotate(90,741.35001,403.14999) translate(741.35001,403.14999) scale(1.0,1.0) translate(-741.35001,-403.14999)\" keyid=\"%s\" rdfid=\"%s\" devref=\"#QZPD_fuhekaiguan.fhkg.icn.g:QZPD_fuhekaiguan_nr\" w=\"44.0\" h=\"19.0\" voltype=\"112871465677750273\" keyidDesc=\"鍐滆仈绀剧幆缃戞煖鍐滄柊绠辩嚎902璐熻嵎寮€鍏�\"/>\n",
	"Dlsrdq":        "        <Dlsrdq id=\"%s\" x=\"190.5\" y=\"346.999989\" ls=\"1\" lc=\"91,209,174\" lw=\"1\" tfr=\"rotate(0,210.0,358.499989)\" keyid=\"%s\" rdfid=\"%s\" devref=\"#QZPD_dieluoshirongduanqi.dlsrdq.icn.g:QZPD_dieluoshirongduanqi\" w=\"39.0\" h=\"23.0\" voltype=\"112871465677750273\" keyidDesc=\"鎮︽腐鏂版潙鍖楁敮绾�#1璺岃惤寮忕啍鏂\uE15E櫒\"/>\n",
	"Disconnector":  "        <Disconnector id=\"%s\" x=\"800.850018\" y=\"436.149982\" ls=\"1\" lc=\"91,209,174\" lw=\"1\" tfr=\"rotate(90,820.850018,445.649982) translate(820.850018,445.649982) scale(1.0,1.0) translate(-820.850018,-445.649982)\" keyid=\"%s\" rdfid=\"%s\" devref=\"#QZPD_daozha.gld.icn.g:QZPD_daozha\" w=\"40.0\" h=\"19.0\" voltype=\"112871465677750273\" keyidDesc=\"鍐滆仈绀剧幆缃戞煖椹\uE102伓璺\uE21B薄姘村巶鈪犵嚎9061鍒€闂�\"/>\n",
	"Bus":           "        <Bus id=\"%s\" ls=\"1\" lc=\"91,209,174\" lw=\"3.0\" keyid=\"%s\" rdfid=\"%s\" app=\"800000\" af=\"2147483647\" recordApp=\"128\" d=\"719.25,375.25 1479.25,375.25\" voltype=\"112871465677750273\"/>\n",
}

type FeederData struct {
	id    string
	state string
	name  string
}

type RowData struct {
	id     string
	feeder string
	state  string
}

type DBConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Port     string `yaml:"port"`
	IP       string `yaml:"ip"`
}

type Config struct {
	DB DBConfig `yaml:"db"`
}

func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	// 环境变量覆盖配置
	if envUser := os.Getenv("DB_USERNAME"); envUser != "" {
		config.DB.Username = envUser
	}
	if envPass := os.Getenv("DB_PASSWORD"); envPass != "" {
		config.DB.Password = envPass
	}
	if envIP := os.Getenv("DB_IP"); envIP != "" {
		config.DB.IP = envIP
	}

	return &config, nil
}

func loadConfigArgs() [][]string {
	// 获取当前执行文件的目录
	dir, err := os.Getwd()
	if err != nil {
		return [][]string{os.Args}
	}

	// 查找 .raseper 文件
	configPath := filepath.Join(dir, "run.command")
	content, err := os.ReadFile(configPath)
	if err != nil {
		return [][]string{os.Args}
	}

	// 读取内容并按空格分割
	args := strings.Split(string(content), "\n")
	if len(args) == 0 {
		return [][]string{os.Args}
	}

	// 确保第一个参数是程序名
	result := [][]string{}
	for _, arg := range args {
		result = append(result, append([]string{os.Args[0]}, strings.Fields(arg)...))
	}
	return result
}

func main() {
	args := loadConfigArgs()

	maxLen := dm.COL_MAX_LEN
	println("maxLen:", maxLen)
	driverName := "dm"
	config, err := LoadConfig("app.yaml")
	if err != nil {
		fmt.Println("config", config)
	}
	dataSourceName := fmt.Sprintf("dm://%s:%s@%s:%s", config.DB.Username, config.DB.Password, config.DB.IP, config.DB.Port)
	if db, err = connect(driverName, dataSourceName); err != nil {
		fmt.Println(err)
		return
	}

	for _, arg := range args[0][1:] {
		CreateGFileForOwner(arg)
	}

	// 关闭连接
	if err = disconnect(); err != nil {
		fmt.Println(err)
		return
	}
}

func CreateGFileForOwner(owner string) {
	feederList, err := queryFeeder(owner)
	if err != nil {
		fmt.Println("err:", err)
	}
	for _, feeder := range feederList {
		fmt.Printf("feeder: %v", feeder)
		if feeder.state == "1006" {
			continue
		}
		if err := writeLineToFile(owner, feeder.name, "<?xml version=\"1.0\" encoding=\"GBK\"?>\n<G w=\"4159.51953125\" h=\"2091.788420832908\" bgc=\"0,0,0\" Substation=\"${substation}\">\n    <Layer name=\"绗�0骞抽潰\" show=\"1\" refreshcycle=\"1\">\n"); err != nil {
			fmt.Println("err:", err)
		}
		for k, v := range modelList {
			if dataList, err := queryTable("SELECT ID, FEEDER_ID, RUNNING_STATE from DKYPW." + v + " where owner = " + "'" + owner + "'" + "and FEEDER_ID = " + "'" + feeder.id + "'"); err != nil {
				fmt.Println("err", err)
				continue
			} else {
				if err := writeTableDataToFile(owner, feeder.name, dataList, k); err != nil {
					fmt.Println("err", err)
				}
			}
		}
		if err := writeLineToFileAppend(owner, feeder.name, "    </Layer>\n</G>"); err != nil {
			fmt.Println("err:", err)
		}
	}
}

/* 创建数据库连接 */
func connect(driverName string, dataSourceName string) (*sql.DB, error) {
	var db *sql.DB
	var err error
	if db, err = sql.Open(driverName, dataSourceName); err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	fmt.Printf("connect to \"%s\" succeed.\n", dataSourceName)
	return db, nil
}

/* 查询产品信息表 */
func queryTable(sql string) ([]RowData, error) {
	var dataList []RowData
	data := RowData{}
	rows, err := db.Query(sql)
	if err != nil {
		return dataList, err
	}
	defer rows.Close()
	fmt.Println("queryTable " + sql)
	for rows.Next() {
		if err = rows.Scan(&data.id, &data.feeder, &data.state); err != nil {
			return dataList, err
		}
		dataList = append(dataList, data)
	}
	return dataList, nil
}

func queryFeeder(owner string) ([]FeederData, error) {
	var dataList []FeederData
	data := FeederData{}
	rows, err := db.Query("select ID, \"NAME\", running_state from DKYPW.SG_CON_FEEDERLINE_B where owner =" + "'" + owner + "'")
	if err != nil {
		return dataList, err
	}
	defer rows.Close()
	for rows.Next() {
		if err = rows.Scan(&data.id, &data.name, &data.state); err != nil {
			return dataList, err
		}
		dataList = append(dataList, data)
	}
	return dataList, nil
}

/* 关闭数据库连接 */
func disconnect() error {
	if err := db.Close(); err != nil {
		fmt.Printf("db close failed: %s.\n", err)
		return err
	}
	fmt.Println("disconnect succeed")
	return nil
}

func writeLineToFile(owner string, feederName string, line string) error {
	outPath := filepath.Join(owner, feederName+".g")
	file, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	_, err = file.WriteString(line)
	if err != nil {
		return err
	}

	return nil
}

func writeLineToFileAppend(owner string, feederName string, line string) error {
	outPath := filepath.Join(owner, feederName+".g")
	file, err := os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	_, err = file.WriteString(line)
	if err != nil {
		return err
	}

	return nil
}

func writeTableDataToFile(owner string, feederName string, dataList []RowData, modelName string) error {
	outPath := filepath.Join(owner, feederName+".g")
	if err := os.MkdirAll(owner, os.ModePerm); err != nil {
		return err
	}
	file, err := os.OpenFile(outPath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	for _, data := range dataList {
		if data.state == "1006" {
			continue
		}

		_, err = file.WriteString(strings.ReplaceAll(templateMap[modelName], "%s", data.id))
		if err != nil {
			err = err
		}
	}

	return err
}
