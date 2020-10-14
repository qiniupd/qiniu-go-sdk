package limit

import "sync/atomic"

const minusOne = ^uint32(0)

type countLimit struct {
	limit   uint32
	current uint32
}

func New(n int) Limit {

	return &countLimit{limit: uint32(n)}
}

func (l *countLimit) Running() int {
	return int(atomic.LoadUint32(&l.current))
}

func (l *countLimit) Acquire(key []byte) error {

	if atomic.AddUint32(&l.current, 1) > l.limit {
		atomic.AddUint32(&l.current, minusOne)
		return ErrLimit
	}
	return nil
}

func (l *countLimit) Release(key []byte) {
	atomic.AddUint32(&l.current, minusOne)
}

// -------------------------------------------------------

type blockingCountLimit struct {
	ch chan struct{}
}

func NewBlockingCount(n int) Limit {

	ch := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		ch <- struct{}{}
	}
	return &blockingCountLimit{ch: ch}
}

func (l *blockingCountLimit) Running() int {
	return cap(l.ch) - len(l.ch)
}

func (l *blockingCountLimit) Acquire(key []byte) error {

	<-l.ch
	return nil
}

func (l *blockingCountLimit) Release(key []byte) {
	l.ch <- struct{}{}
}