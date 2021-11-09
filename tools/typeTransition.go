package tools

import "strconv"

/*
	@Title	typeTransition.go
	@Description 类型转换
	@Author LiuBo 2021/11/04 22:50
*/

func IntToString(key int)string{
	string := strconv.Itoa(key)
	return string
}

func Int64ToString(key int64)string{
	string := strconv.FormatInt(key,10)
	return string
}

func StringToInt(key string)int{
	result,_:= strconv.Atoi(key)
	return result

}

func StringToInt64(key string)int64{
	result,_:= strconv.ParseInt(key, 10, 64)
	return result

}
