package alarm

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)

type ClusterInfo struct {
	id uint64
	appName string
	appGwAddr string
	appMsAddr string
	remark string // struct clusterRemark json
}

type clusterRemark struct {
	MasterAddrsAlarmEnable bool 	`json:"master-addr-alarm-enable"`
	MasterAddrs []string 	`json:"master-addrs, omitempty"`

	GatewayAddrsAlarmEnable bool 	`json:"gateway-addr-alarm-enable"`
	GatewayAddrs []string 	`json:"gateway-addrs, omitempty"`
}

func (s *Server) getClusterInfo() (clusterInfos []ClusterInfo, err error) {
	db, err := sql.Open("mysql", s.sqlArgs)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var (
		clusterId uint64
		gwSqlAddr string
		remark string
	)
	rows, err := db.Query("select id, gateway_sql, remark from fbase_cluster")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&clusterId, &gwSqlAddr, &remark)
		if err != nil {
			return nil, err
		}
		clusterInfos = append(clusterInfos, ClusterInfo{
			id: clusterId,
			appName: APP_NAME_GATEWAY,
			appGwAddr: gwSqlAddr,
			remark: remark,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
}
