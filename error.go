package gotickfile

import "errors"

var (
	ErrTickOutOfOder = errors.New("tick out of order not supported")
	ErrReadOnly      = errors.New("tickfile is in readonly")
)
