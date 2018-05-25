package errors

import (
	"errors"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrReadOnly         = errors.New("fbase/engine: read-only mode")
	ErrSnapshotReleased = errors.New("fbase/engine: snapshot released")
	ErrIterReleased     = errors.New("fbase/engine: iterator released")
	ErrClosed           = errors.New("fbase/engine: closed")
	ErrSplit            = errors.New("fbase/engine: db is spliting, operate forbidden")
	ErrInvalidTimestamp = errors.New("fbase/engine: invalid timestamp")
)

func New(s string) error {
	return errors.New(s)
}
