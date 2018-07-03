package models

type NamespaceApply struct {
	NameSpace  string `json:"namespace"`
	ClusterId  int64  `json:"cluster_id"`
	Applyer    string `json:"applyer"`
	CreateTime int64  `json:"create_time"`
}

type SqlApply struct {
	Id         string `json:"id"`
	DbName     string `json:"db_name"`
	TableName  string `json:"table_name"`
	Sentence   string `json:"sentence"`
	Status     int8   `json:"status"`
	Applyer    string `json:"applyer"`
	Auditor    string `json:"auditor"`
	CreateTime int64  `json:"create_time"`
	Remark     string `json:"remark"`
}
