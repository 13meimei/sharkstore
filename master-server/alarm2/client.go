package alarm2

import (
	"time"
	"context"

	"google.golang.org/grpc"

	"model/pkg/alarmpb2"
)

type Client struct {
	conn *grpc.ClientConn
}

func NewAlarmClient2(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

func (c *Client) Close() {
	if c == nil {
		return
	}
	c.conn.Close()
}

func (c *Client) AlarmAppHeartbeat(clusterId int64, ipAddr, appName string, intervalTime int64) error {
	if c == nil {
		return nil
	}

	cli := alarmpb2.NewAlarmClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := cli.Alarm(ctx, &alarmpb2.AlarmRequest{
		Header: &alarmpb2.RequestHeader{
			Type: alarmpb2.AlarmType_APP_HEARTBEAT,
			ClusterId: clusterId,
			IpAddr: ipAddr,
		},
		AppHeartbeat: &alarmpb2.AppHeartbeatRequest{
			AppName: appName,
			HbIntervalTime: intervalTime,
		},
	})
	return err
}

func (c *Client) RuleAlarm(clusterId int64, ipAddr string,
	ruleName string, alarmValue float64, cmpType alarmpb2.AlarmValueCompareType, remark []string) error {
	if c == nil {
		return nil
	}

	cli := alarmpb2.NewAlarmClient(c.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := cli.Alarm(ctx, &alarmpb2.AlarmRequest{
		Header: &alarmpb2.RequestHeader{
			Type: alarmpb2.AlarmType_RULE_ALARM,
			ClusterId: clusterId,
			IpAddr: ipAddr,
		},
		RuleAlarm: &alarmpb2.RuleAlarmRequest{
			RuleName: ruleName,
			AlarmValue: alarmValue,
			CmpType: cmpType,
			Remark: remark,
		},
	})
	return err
}

//func (c *Client) AppNotAlive(clusterId int64, ipAddr, appName, checkTime string) error {
//	if c == nil {
//		return nil
//	}
//
//	cli := alarmpb2.NewAlarmClient(c.conn)
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	_, err := cli.Alarm(ctx, &alarmpb2.AlarmRequest{
//		Header: &alarmpb2.RequestHeader{
//			Type: alarmpb2.AlarmType_APP_NOT_ALIVE,
//			ClusterId: clusterId,
//			IpAddr: ipAddr,
//		},
//		AppNotAlive: &alarmpb2.AppNotAliveRequest{
//			AppName: appName,
//			AliveCheckTime: checkTime,
//		},
//	})
//	return err
//}
//
//func (c *Client) GatewaySlowLog(clusterId int64, ipAddr string, slowLog []string) error {
//	if c == nil {
//		return nil
//	}
//
//	cli := alarmpb2.NewAlarmClient(c.conn)
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	_, err := cli.Alarm(ctx, &alarmpb2.AlarmRequest{
//		Header: &alarmpb2.RequestHeader{
//			Type: alarmpb2.AlarmType_GATEWAY_SLOWLOG,
//			ClusterId: clusterId,
//			IpAddr: ipAddr,
//		},
//		GwSlowLog: &alarmpb2.GatewaySlowLogRequest{
//			SlowLog: slowLog,
//		},
//	})
//	return err
//}
//
//func (c *Client) GatewayErrorLog(clusterId int64, ipAddr string, errorLog []string) error {
//	if c == nil {
//		return nil
//	}
//
//	cli := alarmpb2.NewAlarmClient(c.conn)
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	_, err := cli.Alarm(ctx, &alarmpb2.AlarmRequest{
//		Header: &alarmpb2.RequestHeader{
//			Type: alarmpb2.AlarmType_APP_NOT_ALIVE,
//			ClusterId: clusterId,
//			IpAddr: ipAddr,
//		},
//		GwErrorLog: &alarmpb2.GatewayErrorLogRequest{
//			ErrorLog: errorLog,
//		},
//	})
//	return err
//}
