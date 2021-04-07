package curl

import (
	"io"
	"os"
	"sync"
)

type ringBuffer struct {
	data        []byte
	readCursor  int
	writeCursor int
	unreadCount int
	lock        sync.Mutex
	cond        *sync.Cond
	writeClosed bool
	writeErr    error
}

func newRingBuffer(size int) *ringBuffer {
	rb := ringBuffer{data: make([]byte, size)}
	rb.cond = sync.NewCond(&rb.lock)
	return &rb
}

func (rb *ringBuffer) Write(p []byte) (int, error) {
	haveWritten := 0
	for haveWritten < len(p) {
		n, err := rb.write(p[haveWritten:])
		haveWritten += n
		if err != nil {
			return haveWritten, err
		}
	}
	return haveWritten, nil
}

func (rb *ringBuffer) write(p []byte) (int, error) {
	rb.lock.Lock()

	if rb.writeClosed {
		rb.lock.Unlock()
		return 0, os.ErrClosed
	}

	n := len(p)
	for rb.unreadCount == len(rb.data) { // 没有空间可写入
		rb.cond.Wait()
	}
	defer rb.lock.Unlock()

	if rb.unreadCount+n > len(rb.data) {
		n = len(rb.data) - rb.unreadCount
	}
	rb.unreadCount += n
	remaining := 0
	if rb.writeCursor+n > len(rb.data) {
		newN := len(rb.data) - rb.writeCursor
		remaining = n - newN
		n = newN
	}
	if n > 0 {
		copy(rb.data[rb.writeCursor:(rb.writeCursor+n)], p[:n])
		rb.writeCursor += n
	}
	if remaining > 0 {
		copy(rb.data[:remaining], p[n:(n+remaining)])
		rb.writeCursor = remaining
	}

	rb.cond.Broadcast()
	return n + remaining, nil
}

func (rb *ringBuffer) WriteClose(err error) {
	rb.lock.Lock()
	defer rb.lock.Unlock()

	rb.writeClosed = true
	rb.writeErr = err
	rb.cond.Broadcast()
}

func (rb *ringBuffer) IsClosed() bool {
	rb.lock.Lock()
	defer rb.lock.Unlock()

	return rb.writeClosed
}

func (rb *ringBuffer) Read(p []byte) (int, error) {
	haveRead := 0
	for haveRead < len(p) {
		n, err := rb.read(p[haveRead:])
		haveRead += n
		if err != nil {
			return haveRead, err
		}
	}
	return haveRead, nil
}

func (rb *ringBuffer) read(p []byte) (int, error) {
	rb.lock.Lock()

	for rb.unreadCount == 0 {
		if rb.writeClosed {
			err := io.EOF
			if rb.writeErr != nil {
				err = rb.writeErr
			}
			rb.lock.Unlock()
			return 0, err
		}
		rb.cond.Wait()
	}
	defer rb.lock.Unlock()

	n := len(p)
	if n > rb.unreadCount {
		n = rb.unreadCount
	}
	rb.unreadCount -= n
	remaining := 0
	if rb.readCursor+n > len(rb.data) {
		newN := len(rb.data) - rb.readCursor
		remaining = n - newN
		n = newN
	}
	if n > 0 {
		copy(p[:n], rb.data[rb.readCursor:(rb.readCursor+n)])
		rb.readCursor += n
	}
	if remaining > 0 {
		copy(p[n:(n+remaining)], rb.data[:remaining])
		rb.readCursor = remaining
	}

	rb.cond.Broadcast()
	return n + remaining, nil
}
