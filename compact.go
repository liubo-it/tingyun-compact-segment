package main

import (
	"github.com/astaxie/beego"
	"github.com/astaxie/beego/logs"
	"gopkg.in/alecthomas/kingpin.v2"
	"net/http"
	"time"
	"tingyun-compact-segment/utils"
)

//初始化log
func init ()  {
	beego.SetLogger(logs.AdapterFile, `{"filename":"logs/compact.log"}`)
	logs.SetLogger(logs.AdapterFile,`{"level":7,"maxlines":0,"maxsize":0,"daily":true,"maxdays":10,"color":true}`)
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", " ").Default(":10070").String()
	)
	kingpin.Parse()
	logs.Info("Listening on  ",utils.GetLoaclIp(),*listenAddress)

//	每分钟执行compact操作
	go func() {
		ticker := time.NewTicker(time.Minute * 1)
			for {
				<-ticker.C
				logs.Info("开始定时查询task是否完成,提交新compact task...")
				utils.SubmitTaskToDruidCompactSegments()
			}
	}()

	http.ListenAndServe(*listenAddress, nil)

}
