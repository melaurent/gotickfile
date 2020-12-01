package gotickfile

import "errors"

var (
	ErrTickOutOfOrder = errors.New("tick out of order not supported")
	ErrReadOnly       = errors.New("tickfile is in readonly")
	ErrReadTimeout    = errors.New("read timeout")
	ErrStreamClosed   = errors.New("stream closed")
	ErrTickFileV1     = errors.New("tickfile V1 not supported")
)
