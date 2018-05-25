/**
 * 用户数据结构
 */
package models

type UserInfo struct {
	Id 					string
	Erp 				string
	Mail				string
	Tel					string
	UserName			string
	RealName			string
	// SuperiorId			string
	SuperiorName		string
	Department1			string
	Department2			string
	OrganizationName	string
	CreateTime			string
	ModifyDate        	string
	// Count               int
}

func NewUserInfo() *UserInfo {
	return &UserInfo {
	}
}

type UserPrivilege struct {
	UserName string `json:"user_name"`
	ClusterId uint64 `json:"cluster_id"`
	Privilege uint64 `json:"privilege"`
}

func NewUserPrivilege() *UserPrivilege {
	return &UserPrivilege{
	}
}

type Role struct {
	Id uint64 `json:"role_id"`
	RoleName string `json:"role_name"`
}

func NewRole() *Role {
	return &Role{
	}
}