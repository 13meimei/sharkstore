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

type dbOp interface {
	getTableAppData() (ret []TableApp, err error)
	getTableGlobalRuleData() (ret []TableGlobalRule, err error)
	getTableClusterRuleData() (ret []TableClusterRule, err error)
	getTableReceiveData() (ret []TableReceiver, err error)
}

type dbOpImpl struct {
	db *sql.DB
}
func (s *Server) newDbOpImpl() *dbOpImpl {
	return &dbOpImpl{
		db: s.mysqlClient,
	}
}

func (s *Server) newMysqlClient() (*sql.DB, error) {
	return sql.Open("mysql", s.conf.MysqlArgs)
}

type TableApp struct {
	// pk
	ClusterId 		int64
	IpAddr 			string
	//
	ProcessName 	string
}
func (opImpl *dbOpImpl) getTableAppData() (ret []TableApp, err error) {
	var tmp TableApp

	rows, err := opImpl.db.Query("select " + TABLESCHEMA_APP + " from " + TABLENAME_APP)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&tmp.ClusterId,
			&tmp.IpAddr,
			&tmp.ProcessName)
		if err != nil {
			return nil, err
		}
		ret = append(ret, TableApp{
			ClusterId: 		tmp.ClusterId,
			IpAddr: 		tmp.IpAddr,
			ProcessName: 	tmp.ProcessName,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
}

type Rule struct {
	Name 			string
	Threshold   	float64
	Durable 		int64
	Count 			int64
	Interval    	int64
	ReceiverRole 	string
	Enable 			int64
}
type TableGlobalRule struct {
	Rule
}
func (opImpl *dbOpImpl) getTableGlobalRuleData() (ret []TableGlobalRule, err error) {
	var tmp TableGlobalRule

	rows, err := opImpl.db.Query( "select " + TABLESCHEMA_GLOBAL_RULE + " from " + TABLENAME_GLOBAL_RULE)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(
			&tmp.Name,
			&tmp.Threshold,
			&tmp.Durable,
			&tmp.Count,
			&tmp.Interval,
			&tmp.ReceiverRole,
			&tmp.Enable)
		if err != nil {
			return nil, err
		}
		ret = append(ret, TableGlobalRule{
			Rule{
				Name: 			tmp.Name,
				Threshold: 		tmp.Threshold,
				Durable: 		tmp.Durable,
				Count: 			tmp.Count,
				Interval: 		tmp.Interval,
				ReceiverRole: 	tmp.ReceiverRole,
				Enable: 		tmp.Enable,
			},
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
	ClusterId 		int64
	Rule
}
func (opImpl *dbOpImpl) getTableClusterRuleData() (ret []TableClusterRule, err error) {
	var tmp TableClusterRule

	rows, err := opImpl.db.Query( "select " + TABLESCHEMA_CLUSTER_RULE + " from " + TABLENAME_CLUSTER_RULE)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(
			&tmp.ClusterId,
			&tmp.Name,
			&tmp.Threshold,
			&tmp.Durable,
			&tmp.Count,
			&tmp.Interval,
			&tmp.ReceiverRole,
			&tmp.Enable)
		if err != nil {
			return nil, err
		}
		ret = append(ret, TableClusterRule{
			ClusterId: 		tmp.ClusterId,
			Rule: Rule{
				Name: 			tmp.Name,
				Threshold: 		tmp.Threshold,
				Durable: 		tmp.Durable,
				Count: 			tmp.Count,
				Interval: 		tmp.Interval,
				ReceiverRole: 	tmp.ReceiverRole,
				Enable: 		tmp.Enable,
			},
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
	Erp 		string
	ClusterId 	int64
	//
	Role 		string
	Mail 		string
	Tel			string
}
func (opImpl *dbOpImpl) getTableReceiveData() (ret []TableReceiver, err error) {
	var tmp TableReceiver

	rows, err := opImpl.db.Query("select " + TABLESCHEMA_RECEIVER + " from " + TABLENAME_RECEIVER)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(
			&tmp.Erp,
			&tmp.ClusterId,
			&tmp.Role,
			&tmp.Mail,
			&tmp.Tel)
		if err != nil {
			return nil, err
		}
		ret = append(ret, TableReceiver{
			Erp: 			tmp.Erp,
			ClusterId: 		tmp.ClusterId,
			Role: 			tmp.Role,
			Mail: 			tmp.Mail,
			Tel: 			tmp.Tel,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return
}
