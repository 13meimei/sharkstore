package common

import "net/http"

func GetCookie(request *http.Request, name string) string {
	cookie, err := request.Cookie(name)
	if err == nil && cookie != nil{
		return cookie.Value
	}
	return ""
}