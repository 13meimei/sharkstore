package controllers

import (
	"github.com/gin-gonic/gin"
	"strconv"
	"console/common"
	"util/log"
	"console/service"
	"strings"
)

const (
	REQURI_DB_CONSOLE_QUERY = "/db/console/query"
)


type StoreDataQuery struct {
}
func NewStoreDataQuery() *StoreDataQuery {
	return &StoreDataQuery {
	}
}
func (ctrl *StoreDataQuery)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	sql := c.PostForm("sql")
	if sql == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	dbName := c.PostForm("dbName")
	if dbName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	dbUserName := c.PostForm("dbUserName")
	if dbUserName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	dbPassWord := c.PostForm("dbPassWord")
	if dbPassWord == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	paramMap := make(map[string]string)
	paramMap["sql"] = sql
	paramMap["dbName"] = dbName
	paramMap["dbUserName"] = dbUserName
	paramMap["dbPassWord"] = dbPassWord
	log.Debug("query store data. cid:[%v]", cId)

	sqlLowerCase := strings.ToLower(sql)
	if strings.HasPrefix(sqlLowerCase, "delete") || strings.HasPrefix(sqlLowerCase, "update") || strings.HasPrefix(sqlLowerCase, "insert") {
		rowsAffected, err := service.NewService().OperateDb(cId, paramMap)
		if err != nil {
			return nil, err
		}
		data := make(map[string]interface{})
		var keys []string
		keys = append(keys, "res")
		data["keys"] = keys

		rows := make([]map[string]interface{}, 0)
		row := make(map[string]interface{}, 0)
		row["res"] = rowsAffected
		rows = append(rows, row)
		values := make(map[string]interface{},2)
		values["total"] = 1
		values["rows"] = rows
		data["values"] = values
		return data, nil
	}else {
		data, err := service.NewService().QueryDb(cId, paramMap)
		if err != nil {
			return nil, err
		}

		return data, nil

	}
}