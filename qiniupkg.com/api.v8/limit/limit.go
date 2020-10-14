package limit

import "errors"

type Limit interface {
	Running() int // return -1 if u donot want to implement this
	Acquire(key []byte) error
	Release(key []byte)
}

var (
	ErrLimit = errors.New("limit exceeded")
)
