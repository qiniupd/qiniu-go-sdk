package operation

import (
	"net/http"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/x/curl.v1"
	"github.com/qiniupd/qiniu-go-sdk/x/httputil.v1"
)

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	code := httputil.DetectCode(err)
	return code/100 == 5
}

func newTransport(connectTimeout, lowSpeedDuration time.Duration, lowSpeedBytesPerSecond int) http.RoundTripper {
	return &curl.Transport{
		ConnectTimeout:           connectTimeout,
		DisableExpect100Continue: true,
		FollowLocation:           true,
		LowSpeedDuration:         lowSpeedDuration,
		LowSpeedBytesPerSecond:   lowSpeedBytesPerSecond,
	}
}
