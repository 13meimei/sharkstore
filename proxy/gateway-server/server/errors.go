package server

import (
	"errors"
)

var (
	ErrInternalError      = errors.New("internal error")
	ErrNotExistDatabase   = errors.New("database not exist")
	ErrNotExistTable      = errors.New("table not exist")
	ErrNotExistNode       = errors.New("node not exist")
	ErrNotExistRange      = errors.New("range not exist")
	ErrNotExistPeer       = errors.New("range peer not exist")
	ErrInvalidColumn      = errors.New("invalid column")
	ErrNoRoute            = errors.New("no route")
	ErrExceedMaxLimit     = errors.New("exceeding the maximum limit")
	ErrEmptyRow           = errors.New("empty row")
	ErrHttpCmdUnknown 	= errors.New("invalid command")
	ErrHttpCmdParse 	= errors.New("parse error")
	ErrHttpCmdRun 		= errors.New("run error")
	ErrHttpCmdEmpty 	= errors.New("command empty")

	ErrAffectRows = errors.New("affect rows is not equal")
	ErrCreateDatabase   = errors.New(" create database err")
	ErrCreateTable      = errors.New("create table err")
)

const (
	errCommandUnknown 	= 1
	errCommandParse 	= 2
	errCommandRun 		= 3
	errCommandNoDb 		= 4
	errCommandNoTable 	= 5
	errCommandEmpty 	= 6
	errCreateDatabase   = 7
	errCreateTable      = 8
)

func CodeToErr(code int) error{
	switch code {
	case errCommandUnknown 	:
		return ErrInternalError
	case errCommandParse :
		return ErrHttpCmdParse
	case	errCommandRun :
		return ErrHttpCmdRun
	case	errCommandNoDb :
		return ErrNotExistDatabase
	case	errCommandNoTable :
		return ErrNotExistTable
	case	errCommandEmpty :
		return ErrHttpCmdEmpty
	case	errCreateDatabase  :
		return ErrCreateDatabase
	case	errCreateTable  :
		return ErrCreateTable
	default:
		return ErrInternalError
	}
}