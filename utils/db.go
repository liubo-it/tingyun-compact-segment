package utils

import (
	"database/sql"
	"github.com/astaxie/beego/logs"
	_ "github.com/go-sql-driver/mysql"
)

/*
	@Title	db.go
	@Description  获取数据库连接
	@Author LiuBo 2021/11/01 10:50
*/


func GetMySQLConn() (string, error) {
	address, err := ParseConfig("mysql", "address")
	if err != nil {
		return "", err
	}
	schema, err := ParseConfig("mysql", "schema")
	if err != nil {
		return "", err
	}
	user, err := ParseConfig("mysql", "user")
	if err != nil {
		return "", err
	}
	password, err := ParseConfig("mysql", "password")
	if err != nil {
		return "", err
	}
	return user + ":" + password + "@tcp(" + address + ")/" + schema, nil
}

func OpenDB() *sql.DB {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("建立数据库连接出现错误，错误信息：", err)
		}
	}()
	conn, err := GetMySQLConn()
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("mysql", conn)
	if err != nil {
		panic(err)
	}
	return db
}
//插入taskID
func InsertCompactId(timestamp string,dataSource string,taskId string,startTime string,endTime string,checkTime string){
	defer func() {
		if err := recover(); err != nil {
			logs.Error("插入数据库出现错误，错误信息：", err)
		}
	}()
	db :=OpenDB()
	defer db.Close()
	insertStmt, err := db.Prepare("insert into DRUID_COMPACT_SEGMENTS_INFO(timestamp,dataSource,taskId,startTime,endTime,checkTime) values(?,?,?,?,?,?)")
	if err != nil {
		panic(err)
	}
	defer insertStmt.Close()
	_, err = insertStmt.Exec(timestamp,dataSource,taskId,startTime,endTime,checkTime)
	if err != nil {
		logs.Error("插入task Id 数据库出错: ",err)
	}
}

//查询taskId
func IdAlreadyInQueue(datasource string ,interval string) string {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("查询taskId出错: ", err)
		}
	}()
	db := OpenDB()
	defer db.Close()
	rows, err := db.Query("select taskId from DRUID_COMPACT_SEGMENTS_INFO where dataSource = ? and timestamp >?  order  by  timestamp desc limit 1 ", datasource, interval)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var result string
	if rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			panic(err)
		}
	}
	return result
}


//查询数据库里面最大的segment时间戳
func GetMaxTimeForSegmentsInQueue(datasource string ,interval string,checkTime string) bool {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("查询taskId出错: ", err)
		}
	}()
	db := OpenDB()
	defer db.Close()
	rows, err := db.Query("select checkTime  from DRUID_COMPACT_SEGMENTS_INFO where dataSource = ? and timestamp >? and  checkTime >= ? order by  checkTime desc limit 1 ", datasource, interval,checkTime)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var result string
	if rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			panic(err)
		}
	}
	var res bool
	if result !=""{
		res=false
	}else {
		res=true
	}
	return res
}

//查询segment 最大endTime
func SegmentsMaxEendTime(datasource string ,interval string) string {
	defer func() {
		if err := recover(); err != nil {
			logs.Error("查询taskId出错: ", err)
		}
	}()
	db := OpenDB()
	defer db.Close()
	rows, err := db.Query("select endTime from DRUID_COMPACT_SEGMENTS_INFO where dataSource = ? and timestamp >?  order  by  timestamp desc limit 1 ", datasource, interval)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var result string
	if rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			panic(err)
		}
	}
	return result
}
