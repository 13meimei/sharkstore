package alarm2

import "time"

type Alarm2ClientConfig struct {
	ServerAddress string 	`toml:"server-addr" json:"server-addr"`
}

type Alarm2ServerConfig struct {
	ServerPort int   		`toml:"server-port" json:"server-port"`

	MysqlArgs string		`toml:"mysql-args" json:"mysql-args"`
	MysqlPullingDurationSec time.Duration		`toml:"mysql-pulling-duration-sec" json:"mysql-pulling-duration-sec"`
	AppAliveCheckingDurationSec time.Duration	`toml:"app-alive-checking-duration-sec" json:"app-alive-checking-duration-sec"`

	JimUrl string 						`toml:"jim-url" json:"jim-url"`
	JimApAddr string					`toml:"jim-ap-addr" json:"jim-ap-addr"`
	JimConnTimeoutSec time.Duration 	`toml:"jim-conn-timeout-sec" json:"jim-conn-timeout-sec"`
	JimWriteTimeoutSec time.Duration	`toml:"jim-write-timeout-sec" json:"jim-write-timeout-sec"`
	JimReadTimeoutSec time.Duration		`toml:"jim-read-timeout-sec" json:"jim-read-timeout-sec"`


}