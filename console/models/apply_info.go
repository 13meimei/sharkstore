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
	K     string `json:"k"`
	V   string `json:"v"`
	LockId      string `json:"lock_id"`
	ExpiredTime int64  `json:"expired_time"`
	UpdTime     int64  `json:"upd_time"`
	DeleteFlag  int8   `json:"delete_flag"`
	Creator     string `json:"creator"`
}

type ConfigureInfo struct {
	K          string `json:"k"`
	V          string `json:"v"`
	Version    string `json:"version"`
	Extend     string `json:"extend"`
	CreateTime int64  `json:"create_time"`
	UpdTime    int64  `json:"upd_time"`
	DeleteFlag int8   `json:"delete_flag"`
	Creator    string `json:"creator"`
}
