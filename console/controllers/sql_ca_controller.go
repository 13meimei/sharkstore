package controllers

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"console/service"
	"console/common"
	"util/log"
	"encoding/json"
)

const (
	REQURI_SQL_CA_GETALL = "/sql/ca/queryList"
	REQURI_SQL_CA_GETBYID = "/sql/ca/getById"
	REQURI_SQL_CA_ADD = "/sql/ca/add"
	REQURI_SQL_CA_DEL = "/sql/ca/del"
)

/**
 * 查询sql凭证列表
 */
type SqlCAGetAllAction struct {
}

func NewSqlCAGetAllAction() *SqlCAGetAllAction {
	return &SqlCAGetAllAction{
	}
}
func (ca *SqlCAGetAllAction) Execute(c *gin.Context) (interface{}, error) {
	pageInfo, err := common.GetPagerInfo(c)
	if err != nil {
		return nil, err
	}
	log.Debug("get sql ca list")
	totalRecord, caList, err := service.NewService().GetSqlCaList(pageInfo)
	if err != nil {
		log.Warn("get sql ca list error, %v", err)
		return nil, err
	}
	pageData := new(PageData)
	pageData.Total = totalRecord
	pageData.Data = caList
	return pageData, nil
}

/**
 * 查询集群详情
 */
type SqlCAGetAction struct {
}

func NewSqlCAGetAction() *SqlCAGetAction {
	return &SqlCAGetAction{
	}
}
func (ca *SqlCAGetAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.ParseInt(cIdStr, 10, 64)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("query single sql ca: %v sql ca info", cId)
	return service.NewService().GetSqlCAById(cId)
}

/**
 * 新增sql凭证
 */
type SqlCAAddAction struct {
}

func NewSqlCAAddAction() *SqlCAAddAction {
	return &SqlCAAddAction{
	}
}
func (ca *SqlCAAddAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	uName := c.PostForm("userName")
	if uName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	password := c.PostForm("password")
	if password == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("create  cluster %v sql ca.", cId)

	err = service.NewService().AddSqlCA(cId, uName, password, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/**
 * 删除sql凭证
 */
type SqlCADelAction struct {
}

func NewSqlCADelAction() *SqlCADelAction {
	return &SqlCADelAction{
	}
}
func (ca *SqlCADelAction) Execute(c *gin.Context) (interface{}, error) {
	ids := c.PostForm("ids")
	if ids == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("delete sql ca record, ids:%v.", ids)
	var idArray []int
	if err := json.Unmarshal([]byte(ids), &idArray); err != nil {
		return nil, common.PARSE_PARAM_ERROR
	}
	return nil, service.NewService().DelSqlCA(idArray)
}