package curl

import (
	"bytes"
	"crypto/md5"
	"io"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRingBuffer(t *testing.T) {
	const size int64 = 1 << 24
	rb := newRingBuffer(10000)

	srcMD5 := md5.New()
	dstMD5 := md5.New()
	sourceData := io.LimitReader(rand.New(rand.NewSource(time.Now().UnixNano())), size)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer rb.WriteClose(nil)

		n, err := io.Copy(rb, io.TeeReader(sourceData, srcMD5))
		if err != nil {
			log.Fatalln(err)
		}
		assert.Equal(t, n, size)
	}()
	go func() {
		defer wg.Done()

		n, err := io.Copy(dstMD5, rb)
		if err != nil {
			log.Fatalln(err)
		}
		assert.Equal(t, n, size)
	}()
	wg.Wait()

	assert.True(t, bytes.Equal(srcMD5.Sum(nil), dstMD5.Sum(nil)))
}
