package breakslow

import (
	"context"
	"net/http"
	"time"
)

type transportOptionCtxKey int

const (
	contextKeyConnReadTimeout transportOptionCtxKey = iota
	contextKeyLowSpeedDuration
	contextKeyLowSpeedBytesPerSecond
)

func SetConnReadTimeout(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, contextKeyConnReadTimeout, timeout)
}

func (t *Transport) getConnReadTimeout(request *http.Request) time.Duration {
	if v := request.Context().Value(contextKeyConnReadTimeout); v != nil {
		return v.(time.Duration)
	} else {
		return t.ConnReadTimeout
	}
}

func SetLowSpeedDuration(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, contextKeyLowSpeedDuration, timeout)
}

func (t *Transport) getLowSpeedDuration(request *http.Request) time.Duration {
	if v := request.Context().Value(contextKeyLowSpeedDuration); v != nil {
		return v.(time.Duration)
	} else {
		return t.LowSpeedDuration
	}
}

func SetLowSpeedBytesPerSecond(ctx context.Context, bps int) context.Context {
	return context.WithValue(ctx, contextKeyLowSpeedBytesPerSecond, bps)
}

func (t *Transport) getLowSpeedBytesPerSecond(request *http.Request) int {
	if v := request.Context().Value(contextKeyLowSpeedBytesPerSecond); v != nil {
		return v.(int)
	} else {
		return t.LowSpeedBytesPerSecond
	}
}
