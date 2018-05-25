package auth

import (
	"time"
	"fmt"
	"console/config"
	"net/http"
	"testing"
)

func Test_Auth(t *testing.T){
	loginConfig := &config.LoginConfig{
		SsoCookieName: "sso.sharkstore.com",
		SsoVerifyUrl: "http://test.sso.sharkstore.com/api/verifyTicket",

		AppName: "source",
		AppToken: "123456789",
	}
	authConfig = &AuthConfig{
		loginConfig:loginConfig,
		cache: make(map[string]*UserInfo),
		client: &http.Client{
			Timeout: 3*time.Second,
		},
		cacheTime: 30*time.Minute,
	}
	userInfo, err:= getUserInfoFromRemote("123456", "/api/hello", "127.0.0.1" )
	if err != nil {
		fmt.Println(fmt.Sprintf("%v", err))
		return
	}
	fmt.Println(fmt.Sprintf("%v", userInfo))
}

func Test_Sign(t *testing.T)  {
	sing := generateSsoSignMock("123456", 1510298462628 )
	fmt.Println(sing)

}