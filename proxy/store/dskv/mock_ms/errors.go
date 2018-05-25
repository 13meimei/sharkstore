package mock_ms

import (
	"errors"
)

var (
	ErrNotExistDatabase   = errors.New("database not exist")
	ErrNotExistTable      = errors.New("table not exist")
	ErrNotExistRange      = errors.New("range not exist")
	ErrInvalidColumn      = errors.New("invalid column")
	ErrColumnNameTooLong        = errors.New("column name is too long")
	ErrDupColumnName            = errors.New("duplicate column name")
	ErrPkMustNotNull            = errors.New("primary key must be not nullable")
	ErrMissingPk                = errors.New("missing primary key")
	ErrPkMustNotSetDefaultValue = errors.New("primary key should not set defaultvalue")
	ErrNotFound           = errors.New("entity not found")
)
