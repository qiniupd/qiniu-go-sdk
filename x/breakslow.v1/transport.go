package breakslow

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"sync/atomic"
	"time"
)

type Transport struct {
	DefaultTransport       http.RoundTripper
	ConnReadTimeout        time.Duration
	LowSpeedDuration       time.Duration
	LowSpeedBytesPerSecond int
}

var ErrBreakSlowResponse = errors.New("break slow response")
var _ http.RoundTripper = &Transport{}

func (t *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	reqCtx := request.Context()
	reqCtx, cancel := context.WithCancel(reqCtx)
	request = request.WithContext(reqCtx)

	resp, err := t.DefaultTransport.RoundTrip(request)
	if err != nil {
		cancel()
		return resp, err
	}

	respBody := breakslowBody{
		body:                   resp.Body,
		cancel:                 cancel,
		lowSpeedDuration:       t.getLowSpeedDuration(request),
		lowSpeedBytesPerSecond: t.getLowSpeedBytesPerSecond(request),
		readTimeout:            t.getConnReadTimeout(request),
	}
	if respBody.readTimeout > 0 {
		respBody.timer = time.AfterFunc(respBody.readTimeout, func() {
			atomic.StoreInt32(&respBody.timedOut, 1)
			cancel()
		})
	}
	resp.Body = &respBody
	return resp, nil
}

type breakslowBody struct {
	body                   io.ReadCloser
	cancel                 func()
	lowSpeedDuration       time.Duration
	lowSpeedBytesPerSecond int
	readTimeout            time.Duration
	timer                  *time.Timer
	readBytesInThisSecond  int
	thisSecondStarts       time.Time
	slowDuration           time.Duration
	timedOut               int32
}

func (body *breakslowBody) Read(p []byte) (int, error) {
	if body.timer != nil {
		body.timer.Reset(body.readTimeout)
	}
	n, err := body.body.Read(p)
	if err != nil {
		if atomic.LoadInt32(&body.timedOut) > 0 {
			err = os.ErrDeadlineExceeded
		}
		return n, err
	}
	if body.lowSpeedBytesPerSecond > 0 {
		if body.thisSecondStarts.IsZero() {
			body.thisSecondStarts = time.Now()
		} else {
			body.readBytesInThisSecond += n
			if time.Since(body.thisSecondStarts) >= time.Second {
				if body.readBytesInThisSecond < body.lowSpeedBytesPerSecond {
					body.slowDuration += time.Second
					if body.slowDuration >= body.lowSpeedDuration {
						return n, ErrBreakSlowResponse
					}
				} else {
					body.slowDuration = 0
				}
				body.thisSecondStarts = time.Now()
				body.readBytesInThisSecond = 0
			}
		}
	}
	return n, err
}

func (body *breakslowBody) Close() error {
	if body.timer != nil {
		body.timer.Stop()
	}

	err := body.body.Close()
	body.cancel()
	return err
}
