package models

type NamespaceApply struct {
	Id         string `json:"id"`
	DbName     string `json:"db_name"`
	TableName  string `json:"table_name"`
	ClusterId  int    `json:"cluster_id"`
	DbId       int    `json:"db_id"`
	TableId    int    `json:"table_id"`
	Status     int8   `json:"status"`
	Applyer    string `json:"applyer"`
	Auditor    string `json:"auditor"`
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

type LockInfo struct {
	LockKey     string `json:"lock_key"`
	LockValue   string `json:"lock_value"`
	LockId      string `json:"lock_id"`
	ExpiredTime int64  `json:"expired_time"`
	UpdTime     int64  `json:"upd_time"`
	DeleteFlag  int8   `json:"delete_flag"`
	Creator     string `json:"creator"`
}
