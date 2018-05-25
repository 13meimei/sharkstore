package controllers

import (
	"github.com/gin-gonic/gin"
	"strconv"
	"console/common"
	"console/service"
)

const(
	REQURI_TOPOLOGY_CHECK= "/topology/check"
	REQURI_TABLE_TOPOLOGY= "/table/topology/missing"
	REQURI_TABLE_TOPOLOGY_CREATE= "/table/topology/create"
	REQURI_CLUSTER_TOPOLOGY_GETALL= "/cluster/topology/getall"
)

type TopologyAction struct {
}
func NewTopologyAction() *TopologyAction {
	return &TopologyAction {
	}
}
func (ctrl *TopologyAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	if dbName == ""  || tableName == ""{
		return nil, common.PARSE_PARAM_ERROR
	}

	err = service.NewService().CheckTopology(cId, dbName, tableName)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

type TableTopologyAction struct {
}
func NewTableTopologyAction() *TableTopologyAction {
	return &TableTopologyAction {
	}
}
func (ctrl *TableTopologyAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	if dbName == ""  || tableName == ""{
		return nil, common.PARSE_PARAM_ERROR
	}

	return service.NewService().GetTableTopologyMissing(cId, dbName, tableName)
}

type TopologyRangeCreateAction struct {
}
func NewTopologyRangeCreateAction() *TopologyRangeCreateAction {
	return &TopologyRangeCreateAction {
	}
}
func (ctrl *TopologyRangeCreateAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	dbName := c.PostForm("dbName")
	tableName := c.PostForm("tableName")
	if dbName == ""  || tableName == "" {
	return nil, common.PARSE_PARAM_ERROR
	}

	bachFlag := c.PostForm("batchFlag")
	if "true" == bachFlag {
		return nil, service.NewService().BatchCreateTopologyRange(cId, dbName, tableName)
	}else{
		startKey := c.PostForm("startKey")
		endKey := c.PostForm("endKey")
		if startKey == "" || endKey == "" {
			return nil, common.PARSE_PARAM_ERROR
		}
		return nil, service.NewService().CreateTopologyRange(cId, dbName, tableName, startKey, endKey)

	}
}

type TopologyViewAction struct {
}
func NewTopologyViewAction() *TopologyViewAction {
	return &TopologyViewAction {
	}
}
func (ctrl *TopologyViewAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	return service.NewService().GetClusterTopology(cId)
}