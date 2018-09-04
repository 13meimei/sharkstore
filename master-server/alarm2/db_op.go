package alarm2

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
)


/*
app
cluster_id      ip_addr |process_name

global_rule
rule_id | name threshold      durable count   interval    receiver_role    enable

cluster_rule
cluster_id rule_id |threshold durable count  interval receiver_role     enable

receiver
erp| role cluster_id mail tel
*/


func (s *Server) newMysqlClient() (*sql.DB, error) {
	return sql.Open("mysql", s.conf.MysqlArgs)
}

type TableApp struct {
	// pk
	clusterId 		int64
	ipAddr 			string
	//
	processName 	string
}
func (s *Server) getTableAppData() (ret []TableApp, err error) {
	var tmp TableApp

	rows, err := s.mysqlClient.Query("select " +
		"cluster_id, " +
		"ip_addr, " +
		"process_name " +
		"from sharkstore_app")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&tmp.clusterId,
			&tmp.ipAddr,
			&tmp.processName)
		if err != nil {
			return nil, err
		}
		ret = append(ret, TableApp{
			clusterId: 		tmp.clusterId,
			ipAddr: 		tmp.ipAddr,
			processName: 	tmp.processName,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
}

type TableGlobalRule struct {
	// pk
	name 			string
	//
	threshold   	float64
	durable 		int64
	count 			int64
	interval    	int64
	receiverRole 	string
	enable 			int64
}
func (s *Server) getTableGlobalRuleData() (ret []TableGlobalRule, err error) {
	var tmp TableGlobalRule

	rows, err := s.mysqlClient.Query( "select " +
		"name, " +
		"threshold, " +
		"durable, " +
		"count, " +
		"interval, " +
		"receiver_role, " +
		"enable " +
		"from sharkstore_global_rule")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&tmp.name,
			&tmp.threshold,
			&tmp.durable,
			&tmp.count,
			&tmp.interval,
			&tmp.receiverRole,
			&tmp.enable)
		if err != nil {
			return nil, err
		}
		ret = append(ret, TableGlobalRule{
			name: 			tmp.name,
			threshold: 		tmp.threshold,
			durable: 		tmp.durable,
			count: 			tmp.count,
			interval: 		tmp.interval,
			receiverRole: 	tmp.receiverRole,
			enable: 		tmp.enable,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
}

type TableClusterRule struct {
	// pk
	clusterId 		int64
	ruleName 		string
	//
	threshold   	float64
	durable 		int64
	count 			int64
	interval    	int64
	receiverRole 	string
	enable 			int64
}
func (s *Server) getTableClusterRuleData() (ret []TableClusterRule, err error) {
	var tmp TableClusterRule

	rows, err := s.mysqlClient.Query( "select " +
		"cluster_id, " +
		"rule_name, " +
		"threshold, " +
		"durable, " +
		"count, " +
		"interval, " +
		"receiver_role, " +
		"enable " +
		"from sharkstore_cluster_rule")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&tmp.clusterId,
			&tmp.ruleName,
			&tmp.threshold,
			&tmp.durable,
			&tmp.count,
			&tmp.interval,
			&tmp.receiverRole,
			&tmp.enable)
		if err != nil {
			return nil, err
		}
		ret = append(ret, TableClusterRule{
			clusterId: 		tmp.clusterId,
			ruleName: 		tmp.ruleName,
			threshold: 		tmp.threshold,
			durable: 		tmp.durable,
			count: 			tmp.count,
			interval: 		tmp.interval,
			receiverRole: 	tmp.receiverRole,
			enable: 		tmp.enable,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
}

type TableReceiver struct {
	//pk
	erp 		string
	clusterId 	int64
	//
	role 		string
	mail 		string
	tel			string
}
func (s *Server) getTableReceiveData() (ret []TableReceiver, err error) {
	var tmp TableReceiver

	rows, err := s.mysqlClient.Query("select " +
		"erp, " +
		"cluster_id, " +
		"role" +
		"mail" +
		"tel" +
		"from sharkstore_receiver")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&tmp.erp,
			&tmp.clusterId,
			&tmp.role,
			&tmp.mail,
			&tmp.tel)
		if err != nil {
			return nil, err
		}
		ret = append(ret, TableReceiver{
			erp: 			tmp.erp,
			clusterId: 		tmp.clusterId,
			role: 			tmp.role,
			mail: 			tmp.mail,
			tel: 			tmp.tel,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
}
