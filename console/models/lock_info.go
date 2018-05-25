package models


type NamespaceApply struct {
	NameSpace 	string        `json:"namespace"`
	ClusterId 	int64            `json:"cluster_id"`
	Applyer		string        `json:"applyer"`
	CreateTime  int64        `json:"create_time"`
}