package controllers

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"console/service"
	"console/common"
	"util/log"
	"console/right"
)

const (
	REQURI_CLUSTER_GETALL     = "/cluster/queryClusters"
	REQURI_CLUSTER_GETBYID    = "/cluster/getById"
	REQURI_CLUSTER_CREATE     = "/cluster/createCluster"
	REQURI_CLUSTER_INIT       = "/cluster/initCluster"
	REQURI_CLUSTER_TOGGLEAUTO = "/cluster/toggleAuto"
	REQURI_CLUSTER_DELETE     = "/cluster/deleteCluster"
)

/**
 * 查询集群列表
 */
type ClusterGetAllAction struct {
}

func NewClusterGetAllAction() *ClusterGetAllAction {
	return &ClusterGetAllAction{
	}
}
func (ctrl *ClusterGetAllAction) Execute(c *gin.Context) (interface{}, error) {
	userRight, ok := c.Get("userRight")
	if !ok {
		return nil, common.NO_RIGHT
	}
	userR := userRight.(*right.User)
	isSystemOwner, clusterIds := userR.IsSystemOwnerOrClusterIds()
	if isSystemOwner {
		log.Debug("admin [%v] get all cluster", userR.Name)
		return service.NewService().GetAllClusters()
	}
	log.Debug("user [%v] get cluster list: %v", userR.Name, clusterIds)
	return service.NewService().GetClusterById(clusterIds...)
}

/**
 * 查询集群详情
 */
type ClusterGetByIdAction struct {
}

func NewClusterGetByIdAction() *ClusterGetByIdAction {
	return &ClusterGetByIdAction{
	}
}
func (ctrl *ClusterGetByIdAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.ParseInt(cIdStr, 10, 64)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("query cluster: %v info", cId)
	clusters, err := service.NewService().GetClusterById(cId)
	if err != nil || len(clusters) == 0 {
		return nil, err
	}
	return clusters[0], nil
}

/**
 * 创建集群
 */
type ClusterCreateAction struct {
}

func NewClusterCreateAction() *ClusterCreateAction {
	return &ClusterCreateAction{
	}
}
func (ctrl *ClusterCreateAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	cName := c.PostForm("clusterName")
	if cName == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	masterUrl := c.PostForm("masterUrl")
	if masterUrl == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	gatewayHttpUrl := c.PostForm("gatewayHttpUrl")
	if gatewayHttpUrl == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	gatewaySqlUrl := c.PostForm("gatewaySqlUrl")
	if gatewaySqlUrl == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	token := c.PostForm("token")
	userName := c.PostForm("userName")
	password := c.PostForm("password")
	log.Debug("create new cluster. cid:[%v]", cId)
	cToken := common.BuildNewClusterToken(cId, token)
	err = service.NewService().CreateCluster(cId, cName, masterUrl, gatewayHttpUrl, gatewaySqlUrl, cToken, userName, password, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/**
 * 集群初始化
 */
type ClusterInitAction struct {
}

func NewClusterInitAction() *ClusterInitAction {
	return &ClusterInitAction{
	}
}
func (ctrl *ClusterInitAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	masterUrl := c.PostForm("masterUrl")
	if masterUrl == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("init cluster. cid:[%v]", cId)

	cToken := common.BuildNewClusterToken(cId, "")
	err = service.NewService().InitCluster(cId, masterUrl, cToken)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/**
 * 删除集群
 */
type ClusterDeleteAction struct {
}

func NewClusterDeleteAction() *ClusterDeleteAction {
	return &ClusterDeleteAction{
	}
}
func (ctrl *ClusterDeleteAction) Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.Query("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("delete cluster. cid:[%v]", cId)
	err = service.NewService().DeleteCluster(cId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

/**
 * 集群开关
 */
type ClusterToggleAction struct {
}

func NewClusterToggleAction() *ClusterToggleAction {
	return &ClusterToggleAction{
	}
}
func (ctrl *ClusterToggleAction) Execute(c *gin.Context) (interface{}, error) {
	log.Debug("set cluster toggle")
	cIdStr := c.Query("clusterId")
	if cIdStr == "" {
		log.Error("set cluster toggle error")
		return nil, common.PARSE_PARAM_ERROR
	}

	autoTransfer := c.PostForm("autoTransferUnable")
	autoFailover := c.PostForm("autoFailoverUnable")
	autoSplit := c.PostForm("autoSplitUnable")

	log.Debug("autoTransfer" + autoTransfer + ", autoFailover" + autoFailover + ", autoSplit" + autoSplit)

	if autoFailover == "" && autoTransfer == "" && autoSplit == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	clusterId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	err = service.NewService().SetClusterToggle(clusterId, autoTransfer, autoFailover, autoSplit)
	if err != nil {
		return nil, err
	}
	return nil, nil
}
