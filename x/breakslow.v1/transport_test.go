package breakslow_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	. "github.com/qiniupd/qiniu-go-sdk/x/breakslow.v1"
	"github.com/stretchr/testify/assert"
)

func TestBreakSlow(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		switch req.URL.Path {
		case "/long_wait":
			for i := 0; i < 3; i++ {
				time.Sleep(1 * time.Second)
				rw.Write(make([]byte, 16*1024))
				if flusher, ok := rw.(http.Flusher); ok {
					flusher.Flush()
				}
			}
		case "/slow_resp":
			for i := 0; i < 3*10; i++ {
				time.Sleep(1 * time.Second / 10)
				rw.Write(make([]byte, 32*1024/10))
				if flusher, ok := rw.(http.Flusher); ok {
					flusher.Flush()
				}
			}
		case "/fast_resp":
			for i := 0; i < 3*10; i++ {
				time.Sleep(1 * time.Second / 10)
				rw.Write(make([]byte, 64*1024/10))
				if flusher, ok := rw.(http.Flusher); ok {
					flusher.Flush()
				}
			}
		}
	}))
	defer testServer.Close()
	url := testServer.URL
	var buf bytes.Buffer

	timeoutClient := http.Client{Transport: &Transport{
		DefaultTransport:       http.DefaultTransport,
		ConnReadTimeout:        500 * time.Millisecond,
		LowSpeedDuration:       2 * time.Second,
		LowSpeedBytesPerSecond: 20 * 1024 * 1024,
	}}
	resp, err := timeoutClient.Get(url + "/long_wait")
	assert.NoError(t, err)
	_, err = io.Copy(&buf, resp.Body)
	assert.Equal(t, os.ErrDeadlineExceeded, err)

	slowClient := http.Client{Transport: &Transport{
		DefaultTransport:       http.DefaultTransport,
		ConnReadTimeout:        5 * time.Second,
		LowSpeedDuration:       2 * time.Second,
		LowSpeedBytesPerSecond: 40 * 1024,
	}}
	resp, err = slowClient.Get(url + "/slow_resp")
	assert.NoError(t, err)
	_, err = io.Copy(&buf, resp.Body)
	assert.Equal(t, ErrBreakSlowResponse, err)

	resp, err = slowClient.Get(url + "/fast_resp")
	assert.NoError(t, err)
	_, err = io.Copy(&buf, resp.Body)
	assert.NoError(t, err)
}
