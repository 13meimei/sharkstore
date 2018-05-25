package controllers

import (
	"github.com/gin-gonic/gin"

	"console/service"
	"console/common"
	"strconv"
	"util/log"
	"encoding/json"
	"console/models"
)

const (
	REQURI_USER_ADMIN = "/userInfo/admin"
	REQURI_USER_GETUSERLIST = "/userInfo/getUserList"
	REQURI_USER_GETPRIVILEGELIST = "/userInfo/getPrivilegeList"
	REQURI_USER_UPDATEPRIVILEG = "/userInfo/updatePrivilege"
	REQURI_USER_DELRIVILEGS = "/userInfo/delPrivileges"
	REQURI_USER_GETROLELIST = "/userInfo/getRoleList"
	REQURI_USER_ADDROLE = "/userInfo/addRole"
	REQURI_USER_DELROLE = "/userInfo/delRole"
)

type UserAdminAction struct {
}

func NewUserAdminAction() *UserAdminAction {
	return &UserAdminAction {
	}
}

func (ctrl *UserAdminAction)Execute(c *gin.Context) (interface{}, error) {
	erp := c.Query("erp")
	if erp == "" {
		return nil, common.PARSE_PARAM_ERROR
	}

	return service.NewService().GetUserInfoByErp(erp)
}

type PrivilegeInfoAction struct {
}

func NewPrivilegeInfoAction() *PrivilegeInfoAction {
	return &PrivilegeInfoAction {
	}
}

func (ctrl *PrivilegeInfoAction)Execute(c *gin.Context) (interface{}, error) {
	order := c.Query("order")
	limit := c.Query("limit")
	offset := c.Query("offset")
	var err error
	var limitI, offsetI int
	if len(limit) > 0 {
		if limitI, err = strconv.Atoi(limit); err != nil {
			return nil, common.PARAM_FORMAT_ERROR
		}
	}
	if len(offset) > 0 {
		if offsetI, err = strconv.Atoi(offset); err != nil {
			return nil, common.PARAM_FORMAT_ERROR
		}
	}
	switch order {
		case "asc" :
		case "desc":
		case "":
		default:
			return nil, common.PARAM_FORMAT_ERROR
	}
	return service.NewService().GetPrivilegeInfo(offsetI, limitI, order)
}

type PrivilegeUpdateAction struct {
}

func NewPrivilegeUpdateAction() *PrivilegeUpdateAction {
	return &PrivilegeUpdateAction {
	}
}

func (ctrl *PrivilegeUpdateAction)Execute(c *gin.Context) (interface{}, error) {
	userName := c.PostForm("userName")
	roleId := c.PostForm("roleId")
	clusterId := c.PostForm("clusterId")

	if len(userName) == 0{
		return nil, common.PARAM_FORMAT_ERROR
	}
	rId, err := strconv.Atoi(roleId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}

	cId, err := strconv.Atoi(clusterId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	return nil, service.NewService().UpdatePrivilege(userName, cId, rId)
}

type PrivilegeDelAction struct {
}

func NewPrivilegeDelAction() *PrivilegeDelAction {
	return &PrivilegeDelAction {
	}
}

func (ctrl *PrivilegeDelAction)Execute(c *gin.Context) (interface{}, error) {
	param := c.PostForm("privileges")
	if len(param) == 0 {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("privileges %v",param)

	var privileges []models.UserPrivilege
	if err := json.Unmarshal([]byte(param), &privileges); err != nil {
		return nil, common.PARSE_PARAM_ERROR
	}
	return nil, service.NewService().DelPrivilege(privileges)
}

type RoleAddAction struct {
}

func NewRoleAddAction() *RoleAddAction {
	return &RoleAddAction {
	}
}

func (ctrl *RoleAddAction)Execute(c *gin.Context) (interface{}, error) {
	roleId := c.PostForm("roleId")
	roleName := c.PostForm("roleName")
	if len(roleId) == 0 || len(roleName) == 0{
		return nil, common.PARSE_PARAM_ERROR
	}
	rId, err := strconv.Atoi(roleId)
	if err != nil {
		return nil, common.PARAM_FORMAT_ERROR
	}
	log.Debug("roleName is [%v]", roleName)
	return nil, service.NewService().AddRole(rId, roleName)
}

type RoleDelAction struct {
}

func NewRoleDelAction() *RoleDelAction {
	return &RoleDelAction {
	}
}

func (ctrl *RoleDelAction)Execute(c *gin.Context) (interface{}, error) {
	param := c.PostForm("roleIds")
	if len(param) == 0 {
		return nil, common.PARSE_PARAM_ERROR
	}

	log.Debug("roleIds %s",param)

	var roleIds []int
	if err := json.Unmarshal([]byte(param), &roleIds); err != nil {
		return nil, common.PARSE_PARAM_ERROR
	}
	return nil, service.NewService().DelRole(roleIds)
}


type RoleInfoAction struct {
}

func NewRoleInfoAction() *RoleInfoAction {
	return &RoleInfoAction {
	}
}

func (ctrl *RoleInfoAction)Execute(c *gin.Context) (interface{}, error) {
	order := c.Query("order")
	limit := c.Query("limit")
	offset := c.Query("offset")
	var err error
	var limitI, offsetI int
	if len(limit) > 0 {
		if limitI, err = strconv.Atoi(limit); err != nil {
			return nil, common.PARAM_FORMAT_ERROR
		}
	}
	if len(offset) > 0 {
		if offsetI, err = strconv.Atoi(offset); err != nil {
			return nil, common.PARAM_FORMAT_ERROR
		}
	}
	switch order {
	case "asc" :
	case "desc":
	case "":
	default:
		return nil, common.PARAM_FORMAT_ERROR
	}
	return service.NewService().GetRoleInfo(offsetI, limitI, order)
}