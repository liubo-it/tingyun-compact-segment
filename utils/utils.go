package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/astaxie/beego"
	"github.com/astaxie/beego/logs"
	"gopkg.in/ini.v1"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"tingyun-compact-segment/tools"
)

/*
	@Title	utils.go
	@Description 	拼装compact task
	@Author LiuBo 2021/10/28 10:00
*/

//compact task模版
type Compaction struct {
	Type                   string `json:"type"`
	DataSource             string `json:"dataSource"`
//	SegmentGranularity	   string   `json:"segmentGranularity"`
//	taskPriority int	 	  `json:"taskPriority"`
	Interval     string       `json:"interval"`
	TuningConfig TuningConfig `json:"tuningConfig"`
	Context      Context      `json:"context"`
}
//tuningConfig
type TuningConfig struct {
	Type                string      `json:"type"`
	MaxRowsPerSegment 	int			`json:"maxRowsPerSegment"`
	MaxBytesInMemory    int 		`json:"maxBytesInMemory"`
	MaxTotalRows        int			`json:"maxTotalRows"`
}
//context
type Context struct {
	Opts          string `json:"druid.indexer.runner.javaOpts"`
	ProcessThread int64  `json:"druid.indexer.fork.property.druid.processing.numThreads"`
	MergeThread   int64  `json:"druid.indexer.fork.property.druid.processing.numMergeBuffers"`
	PoolBytes     int64  `json:"druid.indexer.fork.property.druid.processing.buffer.sizeBytes"`

}

//解析dataSource
type DSList struct {
	DataSource struct {
		Enable	bool	`yaml:"enable"`
		List   []string `yaml:"list"`
	}
}
//解析配置文件
func ParseConfig(section, key string) (string, error) {
	defer func(){
		if recover() != nil{logs.Error("Error obtaining the configuration file ! ")}
	}()
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	file, err := ini.LoadSources(ini.LoadOptions{IgnoreInlineComment: true}, cwd+"/config.ini")
	if err != nil {
		return "", err
	}

	sect, err := file.GetSection(section)
	if err != nil {
		return "", err
	}
	if sect.HasKey(key) {
		return sect.Key(key).String(), nil
	} else {
		return "", errors.New("no specified key in this section")
	}
}

func HttpClient(method, url string, body io.Reader) *http.Response {
	client := &http.Client{}
	request, err := http.NewRequest(method, url, body)
	if err != nil {
		beego.Error(err)
	}
	request.SetBasicAuth("admin", "nEtben@2_19")
	if method == "POST" {
		request.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(request)
	if err != nil || resp.StatusCode != 200 {
		beego.Error(resp)
		beego.Error("http请求异常，返回值错误", err, "或者返回状态码非200.")
	}
	return resp
}

//判断task是否完成
func IsTaskCompletedSuccessful(id string) string {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("判断task是否已经成功运行完成出现错误，错误信息：", err)
		}
	}()
	url, err := ParseConfig("overlord", "address")
	if err != nil {
		panic(err)
	}
	availableUrl, err := GetAvailableOverlordUrl(url)
	if err != nil {
		panic(err)
	}

	resp := HttpClient("GET", availableUrl+"/druid/indexer/v1/task/"+id+"/status", nil)
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 {
		return "UNKNOWN"
	}
	defer resp.Body.Close()
	var payload map[string]interface{}
	err = json.Unmarshal(respBytes, &payload)
	if err != nil {
		panic(err)
	}
	status := payload["status"].(map[string]interface{})
	if value, ok := status["statusCode"]; ok {
		return value.(string)
	} else {
		return "UNKNOWN"
	}
}
//获取overload leader节点
func GetAvailableOverlordUrl(address string) (string, error) {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("获取overlord可用节点出现异常，异常信息: ", err)
		}
	}()
	url := "http://" + address + "/druid/indexer/v1/leader"
	resp := HttpClient("GET", url, nil)
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if len(respData) > 0 {
		return string(respData), nil
	} else {
		return "", errors.New("empty available overlord url.")
	}
}

//获取本机ip
func GetLoaclIp()string{
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		logs.Error("获取本机ip失败: ",err)

	}
	var  addre string
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				addre = ipnet.IP.String()
			}
		}
	}
	return  addre
}

//向overload节点提交compact任务
func SubmitOverlordTask(content string) string {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("向overlord发送task时出现错误，错误信息：", err)
		}
	}()
	url, err := ParseConfig("overlord", "address")
	if err != nil {
		panic(err)
	}
	availableUrl, err := GetAvailableOverlordUrl(url)
	if err != nil {
		panic(err)
	}
	req := []byte(content)
	resp := HttpClient("POST", availableUrl+"/druid/indexer/v1/task", bytes.NewBuffer(req))
	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	taskID := map[string]string{}
	err = json.Unmarshal(result, &taskID)
	if err != nil {
		panic(err)
	}
	return taskID["task"]
}

func FormatSpecBeforePost(datasource string,interval string) string {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("解析结构体拼接模版：", err)
		}
	}()
	contexts := GetContext()
	tuningCongfig :=GetTuningConfig(datasource)
	compaction :=Compaction{
		Type:						"compact",
		DataSource:					datasource,
		Interval:					interval,
//		taskPriority:				100,
		TuningConfig:				tuningCongfig,
		Context:					contexts,
	}
	bytes, err := json.Marshal(compaction)
	if err != nil {
		panic(err)
	}
	result := string(bytes)
	return result
}
// 初次部署时获取时间
func GetTaskTnterval(dataSource string) string {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("获取interval时出现错误，错误信息：", err)
		}
	}()
	if strings.HasSuffix(dataSource, "_DAY") || dataSource == "APP_DEVICE_DATA_MIN"{
			startTime, err := ParseConfig(dataSource, "startTime")
			if err != nil {
				panic(err)
			}
			endTime, err := ParseConfig(dataSource, "endTime")
			if err != nil {
				panic(err)
			}
			timStr := startTime+"T16:00:00.000Z/" + endTime + "T16:00:00.000Z"

			return timStr
	}else {
			startTime, err := ParseConfig(dataSource, "startTime")
			if err != nil {
				panic(err)
			}
			endTime, err := ParseConfig(dataSource, "endTime")
			if err != nil {
				panic(err)
			}
			timStr := startTime+"T00:00:00.000Z/" + endTime + "T00:00:00.000Z"

			return timStr
	}
}

// 已经部署后时获取segments interval 时间
func GetTaskEndTnterval(dataSource,interval string) string {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("获取interval时出现错误，错误信息：", err)
		}
	}()
	startTime :=SegmentsMaxEendTime(dataSource,interval)

	endTimes,_:=time.ParseInLocation("2006-01-02",startTime[:10], time.Local)
	endTime:=endTimes.Add(time.Hour * 24).Format("2006-01-02")
	timStr :=startTime+"/" + endTime + "T00:00:00.000Z"

	return timStr
}

// 解析配置文件，组装TuningConfig
func GetTuningConfig(dataSource string) TuningConfig {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("获取tuningConfig时出现错误，错误信息：", err)
		}
	}()
	MaxRowsPerSegment, err := ParseConfig(dataSource, "MaxRowsPerSegment")
	MaxRowsPersegment :=tools.StringToInt(MaxRowsPerSegment)
	if err != nil {
		panic(err)
	}
	MaxTotalRows, err := ParseConfig(dataSource, "MaxTotalRows")
	MaxTotalRow :=tools.StringToInt(MaxTotalRows)
	if err != nil {
		panic(err)
	}
	tuningConfig := TuningConfig{
		Type: 							"index",
		MaxRowsPerSegment:				MaxRowsPersegment,
		MaxBytesInMemory: 				-1,
		MaxTotalRows:					MaxTotalRow,
	}
	return tuningConfig
}
// 解析配置文件，组装context
func GetContext() Context {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("获取context时出现错误，错误信息：", err)
		}
	}()
	xms, err := ParseConfig("jvm", "Xms")
	if err != nil {
		panic(err)
	}
	xmx, err := ParseConfig("jvm", "Xmx")
	if err != nil {
		panic(err)
	}
	direct, err := ParseConfig("jvm", "maxDirectMemorySize")
	if err != nil {
		panic(err)
	}
	processThread, err := ParseConfig("jvm", "processThread")
	if err != nil {
		panic(err)
	}
	processThreadInt, err := strconv.ParseInt(processThread, 10, 64)
	if err != nil {
		panic(err)
	}
	mergeThread, err := ParseConfig("jvm", "mergeThread")
	if err != nil {
		panic(err)
	}
	mergeThreadInt, err := strconv.ParseInt(mergeThread, 10, 64)
	if err != nil {
		panic(err)
	}
	poolBytes, err := ParseConfig("jvm", "poolBytes")
	if err != nil {
		panic(err)
	}
	poolBytesInt, err := strconv.ParseInt(poolBytes, 10, 64)
	if err != nil {
		panic(err)
	}
	context := Context{
		Opts:          "-server -Xms" + xms + " -Xmx" + xmx + " -XX:MaxDirectMemorySize=" + direct,
		ProcessThread: processThreadInt,
		MergeThread:   mergeThreadInt,
		PoolBytes:     poolBytesInt,
	}
	return context
}

//提交task
func SubmitTaskToDruidCompactSegments(){
	defer func() {
		if err := recover(); err != nil {
			logs.Error("生成compact task 失败:  ", err)
		}
	}()
	pwd, err := os.Getwd()
	if err != nil {
		logs.Error(err)
	}
	file := pwd + "/dataSource.yml"
	content, err := ioutil.ReadFile(file)
	if err != nil {
		logs.Error(err)
	}
	var  dsList DSList
	err = yaml.Unmarshal(content, &dsList)
	if err != nil {
		logs.Error(err)
	}
	delta, _ := time.ParseDuration("-360h")
	cktime,_ := time.ParseDuration("-48h")
	SqlStartTime :=time.Now().Add(delta).UTC().String()[:19]
	startSubmitCompactTaskTime :=time.Now().String()[:19]
	CheckTime :=time.Now().Add(cktime).String()[:19]
	if dsList.DataSource.Enable {
		for _,dataSources :=range dsList.DataSource.List{
				if IdAlreadyInQueue(dataSources,SqlStartTime) =="" {
					interval := GetTaskTnterval(dataSources)
					cont :=FormatSpecBeforePost(dataSources,interval)
					compactId:=SubmitOverlordTask(cont)
					intervalSplit:=strings.Split(interval,"/")
					InsertCompactId(startSubmitCompactTaskTime,dataSources,compactId,intervalSplit[0],intervalSplit[1],intervalSplit[1][:10])
					logs.Info(dataSources,"当前数据库查询为空,提交初始compact task : ",cont)
				}else if  IsTaskCompletedSuccessful(IdAlreadyInQueue(dataSources,SqlStartTime)) == "SUCCESS" && GetMaxTimeForSegmentsInQueue(dataSources,SqlStartTime,CheckTime) {
					timStr:=GetTaskEndTnterval(dataSources,SqlStartTime)
					cont :=FormatSpecBeforePost(dataSources,timStr)
					compactId:=SubmitOverlordTask(cont)
					intervalSplit:=strings.Split(timStr,"/")
					InsertCompactId(startSubmitCompactTaskTime,dataSources,compactId,intervalSplit[0],intervalSplit[1],intervalSplit[1][:10])
					logs.Info(dataSources,"提交compact task : ",cont)
				}
		}
	}

}