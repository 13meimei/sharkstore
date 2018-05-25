package controllers

import (
	"github.com/gin-gonic/gin"
)

type Response struct {
	Code 	int				`json:"code"`
	Msg		string			`json:"msg"`
	Data    interface{}		`json:"data"`
}

type Action interface {
	Execute(c *gin.Context) (interface{}, error)
}
