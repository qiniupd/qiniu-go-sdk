package operation

import (
	"net"
	"net/http"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/x/httputil.v1"
)

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	code := httputil.DetectCode(err)
	return code/100 == 5
}

func NewTransport(dialTimeoutMs int) http.RoundTripper {
	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   time.Duration(dialTimeoutMs) * time.Millisecond,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return t
}
