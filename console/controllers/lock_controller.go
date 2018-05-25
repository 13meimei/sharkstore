package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/sessions"

	"console/common"
	"console/right"
	"console/service"
	"console/models"
	"util/log"

	"fmt"
	"strconv"
	"time"
)

const (
	REQURI_LOCK_NAMESPACE_GETALL = "/lock/namespace/queryList"
	REQURI_LOCK_NAMESPACE_APPLY = "/lock/namespace/apply"
	REQURI_LOCK_CLUSTER_INFO = "/lock/cluster/get"
	REQURI_LOCK_NAMESPACE_UPDATE= "/lock/namespace/update"
)

/**
 * 查询lock namespace 列表
 */
type LockGetAllNspAction struct {
}
func NewLockGetAllNspAction() *LockGetAllNspAction {
	return &LockGetAllNspAction{
	}
}
func (ctrl *LockGetAllNspAction)Execute(c *gin.Context) (interface{}, error) {
	userName := sessions.Default(c).Get("user_name").(string)
	isAdmin, err := service.NewService().IsAdmin(userName)
	if err != nil {
		return nil, fmt.Errorf("query user right failed %v")
	}
	log.Debug("user [%v] get lock list, isAdmin: %v", userName, isAdmin)
	return service.NewService().GetAllNamespace(userName, isAdmin)
}

/**
 * 申请lock namespace
 */
type LockNspApplyAction struct {
}
func NewLockNspApplyAction() *LockNspApplyAction {
	return &LockNspApplyAction {
	}
}
func (ctrl *LockNspApplyAction)Execute(c *gin.Context) (interface{}, error) {
	userName := sessions.Default(c).Get("user_name").(string)
	user := right.GetCacheUser(userName)
	if user == nil {
		return nil, fmt.Errorf("no user cached %v", userName)
	}
	cIdStr := c.PostForm("clusterId")
	namespace := c.PostForm("namespace")
	if cIdStr == "" || namespace == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	log.Debug("apply lock namespace:%v, applyer:%v, cluserId:%v.", namespace, userName, cId)
	err = service.NewService().ApplyLockNamespace(cId, namespace, userName, time.Now().Unix())
	if err != nil {
		return nil, err
	}

	return nil, nil
}

/**
 * lock namespace修改
 */
type LockNspUpdateAction struct {
}
func NewLockNspUpdateAction() *LockNspUpdateAction {
	return &LockNspUpdateAction{
	}
}
func (ctrl *LockNspUpdateAction)Execute(c *gin.Context) (interface{}, error) {
	cIdStr := c.PostForm("clusterId")
	if cIdStr == "" {
		return nil, common.PARSE_PARAM_ERROR
	}
	cId, err := strconv.Atoi(cIdStr)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	namespace := c.PostForm("namespace")
	applyer := c.PostForm("applyer")
	if "" == namespace || "" == applyer {
		return nil, common.PARSE_PARAM_ERROR
	}
	log.Debug("update applyer %v of namespace %v and clusterId %v ", applyer, namespace, cId)
	err = service.NewService().UpdateLockNsp(cId, namespace, applyer, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	return nil, nil
}


/**
 * lock cluster 刚上线的时候，锁集群是通过配置的，后期改成获取对应权限的集群列表
 */
type LockClusterGetAction struct {
}
func NewLockClusterGetAction() *LockClusterGetAction {
	return &LockClusterGetAction{
	}
}
func (ctrl *LockClusterGetAction)Execute(c *gin.Context) (interface{}, error) {
	log.Debug("get lock cluster info")
	var clusters []*models.ClusterInfo
	cluster, err:= service.NewService().GetLockCluster()
	if err != nil {
		return nil, err
	}
	clusters = append(clusters, cluster)
	return clusters, nil
}