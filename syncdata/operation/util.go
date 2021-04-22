package operation

import (
	"net"
	"net/http"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/x/breakslow.v1"
	"github.com/qiniupd/qiniu-go-sdk/x/httputil.v1"
)

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	code := httputil.DetectCode(err)
	return code/100 == 5
}

func newTransport(connectTimeout time.Duration) http.RoundTripper {
	if connectTimeout == 0 {
		connectTimeout = 500 * time.Millisecond
	}
	return &breakslow.Transport{
		DefaultTransport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   connectTimeout,
				KeepAlive: 1 * time.Second,
				DualStack: true,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   100 * time.Millisecond,
			ResponseHeaderTimeout: 1 * time.Second,
			ExpectContinueTimeout: 100 * time.Millisecond,
		},
		ConnReadTimeout:        1 * time.Second,
		LowSpeedDuration:       5 * time.Second,
		LowSpeedBytesPerSecond: 1 << 20,
	}
}
