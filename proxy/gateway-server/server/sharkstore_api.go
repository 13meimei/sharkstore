package server

import (
	"encoding/json"
	"fmt"
	"time"
	"util"
	"util/log"
)

type SharkStoreApi struct {
	proxy *Proxy
}

func NewSharkStoreAPI(msAddr []string, config *ProxyConfig) *SharkStoreApi {
	return &SharkStoreApi{
		proxy: NewProxy(msAddr, config),
	}
}

func (api *SharkStoreApi) RawSet(dbName, tableName string, key, value []byte) error {
	table := api.proxy.router.FindTable(dbName, tableName)
	if table == nil {
		err := fmt.Errorf("table(%s) of db(%s) not exist", tableName, dbName)
		return err
	}

	keyPrefix := util.EncodeStorePrefix(util.Store_Prefix_KV, table.ID())
	keyEncoded := append(keyPrefix, key...)

	return api.proxy.RawPut(dbName, tableName, keyEncoded, value)
}

func (api *SharkStoreApi) RawGet(dbName, tableName string, key []byte) ([]byte, error) {
	table := api.proxy.router.FindTable(dbName, tableName)
	if table == nil {
		err := fmt.Errorf("table(%s) of db(%s) not exist", tableName, dbName)
		return nil, err
	}

	keyPrefix := util.EncodeStorePrefix(util.Store_Prefix_KV, table.ID())
	keyEncoded := append(keyPrefix, key...)

	return api.proxy.RawGet(dbName, tableName, keyEncoded)
}

func (api *SharkStoreApi) Insert(dbName string, tableName string, fields []string, values [][]interface{}) *Reply {

	cmd := &Command{
		Type:   "set",
		Field:  fields,
		Values: values,
	}
	query := &Query{
		Command: cmd,
	}

	return api.execute(dbName, tableName, query)

}

func (api *SharkStoreApi) Select(dbName string, tableName string, fields []string, pks map[string]interface{}, limit_ *Limit_) *Reply {
	ands := make([]*And, 0)
	for k, v := range pks {
		and := &And{
			Field:  &Field_{Column: k, Value: v},
			Relate: "=",
		}
		ands = append(ands, and)

	}
	cmd := &Command{
		Type:  "get",
		Field: fields,
		Filter: &Filter_{
			And: ands,
		},
	}
	if limit_ != nil {
		cmd.Filter.Limit = limit_
	}
	query := &Query{
		Command: cmd,
	}

	return api.execute(dbName, tableName, query)
}

func (api *SharkStoreApi) MultSelect(dbName string, tableName string, fields []string, pkMults []map[string]interface{}, limit_ *Limit_) *Reply {
	andMult := make([][]*And, 0)
	for _, pks := range pkMults {
		ands := make([]*And, 0)
		for k, v := range pks {
			and := &And{
				Field:  &Field_{Column: k, Value: v},
				Relate: "=",
			}
			ands = append(ands, and)
		}
		andMult = append(andMult, ands)
	}

	cmd := &Command{
		Type:  "get",
		Field: fields,
		PKs:   andMult,
	}
	if limit_ != nil {
		cmd.Filter.Limit = limit_
	}
	query := &Query{
		Command: cmd,
	}

	return api.execute(dbName, tableName, query)
}

func (api *SharkStoreApi) Delete(dbName string, tableName string, fields []string, pks map[string]interface{}) *Reply {
	ands := make([]*And, 0)
	for k, v := range pks {
		and := &And{
			Field:  &Field_{Column: k, Value: v},
			Relate: "=",
		}
		ands = append(ands, and)

	}
	cmd := &Command{
		Type:  "del",
		Field: fields,
		Filter: &Filter_{
			And: ands,
		},
	}
	query := &Query{
		Command: cmd,
	}

	return api.execute(dbName, tableName, query)
}

func (api *SharkStoreApi) execute(dbName string, tableName string, query *Query) (reply *Reply) {
	var err error
	if len(dbName) == 0 {
		log.Error("args[dbName] wrong")
		reply = &Reply{Code: errCommandNoDb, Message: fmt.Errorf("dbName %v", ErrHttpCmdEmpty).Error()}
		return reply
	}
	if len(tableName) == 0 {
		log.Error("args[tableName] wrong")
		reply = &Reply{Code: errCommandNoTable, Message: fmt.Errorf("tablename %v", ErrHttpCmdEmpty).Error()}
		return reply
	}
	if query.Command == nil {
		log.Error("args[Command] wrong")
		reply = &Reply{Code: errCommandEmpty, Message: ErrHttpCmdEmpty.Error()}
		return reply
	}

	t := api.proxy.router.FindTable(dbName, tableName)
	if t == nil {
		log.Error("table %s.%s doesn't exist", dbName, tableName)
		reply = &Reply{Code: errCommandNoTable, Message: ErrNotExistTable.Error()}
		return reply
	}

	start := time.Now()
	var slowLogThreshold util.Duration
	query.commandFieldNameToLower()
	switch query.Command.Type {
	case "get":
		slowLogThreshold = api.proxy.config.Performance.SelectSlowLog
		reply, err = query.getCommand(api.proxy, t)
		if err != nil {
			log.Error("getcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: fmt.Errorf("%v: %v", ErrHttpCmdRun, err).Error()}
		}
	case "set":
		slowLogThreshold = api.proxy.config.Performance.InsertSlowLog
		reply, err = query.setCommand(api.proxy, t)
		if err != nil {
			log.Error("setcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: fmt.Errorf("%v: %v", ErrHttpCmdRun, err).Error()}
		}
	case "upd":
		slowLogThreshold = api.proxy.config.Performance.UpdateSlowLog
		reply, err = query.setCommand(api.proxy, t)
		if err != nil {
			log.Error("updcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: fmt.Errorf("%v: %v", ErrHttpCmdRun, err).Error()}
		}
	case "del":
		slowLogThreshold = api.proxy.config.Performance.SelectSlowLog
		reply, err = query.delCommand(api.proxy, t)
		if err != nil {
			log.Error("delcommand error: %v", err)
			reply = &Reply{Code: errCommandRun, Message: fmt.Errorf("%v: %v", ErrHttpCmdRun, err).Error()}
		}
	default:
		log.Error("unknown command")
		reply = &Reply{Code: errCommandUnknown, Message: ErrHttpCmdUnknown.Error()}
	}

	delay := time.Since(start)
	if delay > slowLogThreshold.Duration {
		cmd, _ := json.Marshal(query)

		log.Warn("[kvcommand slow log %v %v ", delay.String(), string(cmd))
	}

	return reply
}
