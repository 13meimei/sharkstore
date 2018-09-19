package server

import (
	"errors"

	"proxy/gateway-server/mysql"
	"proxy/gateway-server/sqlparser"
)

// HandleUpdate handle update
func (p *Proxy) HandleUpdate(db string, stmt *sqlparser.Update, args []interface{}) (*mysql.Result, error) {
	return nil, errors.New("not implement")
}
