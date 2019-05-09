package common

import (
	"github.com/gin-gonic/gin"
	"github.com/gin-contrib/sessions"
)

func GetUserName(c *gin.Context) (string, error) {
	userNameO := sessions.Default(c).Get("user_name")
	var userName string
	if userNameO != nil {
		userName = userNameO.(string)
	}
	if len(userName) == 0  {
		return "", NO_USER
	}
	return userName, nil
}