package curl

import (
	"context"
	"net/http"
	"time"
)

type curlOptionCtxKey int

const (
	contextKeyExpect100ContinueTimeout curlOptionCtxKey = iota
	contextKeyDisableExpect100Continue curlOptionCtxKey = iota
	contextKeyTimeout
	contextKeyConnectTimeout
	contextKeyFollowLocation
	contextKeyMaxRedirects
	contextKeyMaxConnections
	contextKeyLowSpeedDuration
	contextKeyLowSpeedBytesPerSecond
)

func SetExpect100ContinueTimeout(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, contextKeyExpect100ContinueTimeout, timeout)
}

func (t *Transport) getExpect100ContinueTimeout(request *http.Request) time.Duration {
	if v := request.Context().Value(contextKeyExpect100ContinueTimeout); v != nil {
		return v.(time.Duration)
	} else {
		return t.Expect100ContinueTimeout
	}
}

func SetDisableExpect100Continue(ctx context.Context, disabled bool) context.Context {
	return context.WithValue(ctx, contextKeyDisableExpect100Continue, disabled)
}

func (t *Transport) getDisableExpect100Continue(request *http.Request) bool {
	if v := request.Context().Value(contextKeyDisableExpect100Continue); v != nil {
		return v.(bool)
	} else {
		return t.DisableExpect100Continue
	}
}

func SetTimeout(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, contextKeyTimeout, timeout)
}

func (t *Transport) getTimeout(request *http.Request) time.Duration {
	if v := request.Context().Value(contextKeyTimeout); v != nil {
		return v.(time.Duration)
	} else {
		return t.Timeout
	}
}

func SetConnectTimeout(ctx context.Context, timeout time.Duration) context.Context {
	return context.WithValue(ctx, contextKeyConnectTimeout, timeout)
}

func (t *Transport) getConnectTimeout(request *http.Request) time.Duration {
	if v := request.Context().Value(contextKeyConnectTimeout); v != nil {
		return v.(time.Duration)
	} else {
		return t.ConnectTimeout
	}
}

func SetFollowLocation(ctx context.Context, followLocation bool) context.Context {
	return context.WithValue(ctx, contextKeyFollowLocation, followLocation)
}

func (t *Transport) getFollowLocation(request *http.Request) bool {
	if v := request.Context().Value(contextKeyFollowLocation); v != nil {
		return v.(bool)
	} else {
		return t.FollowLocation
	}
}

func SetMaxRedirects(ctx context.Context, maxRedirects int) context.Context {
	return context.WithValue(ctx, contextKeyMaxRedirects, maxRedirects)
}

func (t *Transport) getMaxRedirects(request *http.Request) int {
	if v := request.Context().Value(contextKeyMaxRedirects); v != nil {
		return v.(int)
	} else {
		return t.MaxRedirects
	}
}

func SetMaxConnections(ctx context.Context, maxConnections int) context.Context {
	return context.WithValue(ctx, contextKeyMaxConnections, maxConnections)
}

func (t *Transport) getMaxConnections(request *http.Request) int {
	if v := request.Context().Value(contextKeyMaxConnections); v != nil {
		return v.(int)
	} else {
		return t.MaxConnections
	}
}

func SetLowSpeedDuration(ctx context.Context, lowSpeedDuration time.Duration) context.Context {
	return context.WithValue(ctx, contextKeyLowSpeedDuration, lowSpeedDuration)
}

func (t *Transport) getLowSpeedDuration(request *http.Request) time.Duration {
	if v := request.Context().Value(contextKeyLowSpeedDuration); v != nil {
		return v.(time.Duration)
	} else {
		return t.LowSpeedDuration
	}
}

func SetLowSpeedBytesPerSecond(ctx context.Context, lowSpeedBytesPerSecond int) context.Context {
	return context.WithValue(ctx, contextKeyLowSpeedBytesPerSecond, lowSpeedBytesPerSecond)
}

func (t *Transport) getLowSpeedBytesPerSecond(request *http.Request) int {
	if v := request.Context().Value(contextKeyLowSpeedBytesPerSecond); v != nil {
		return v.(int)
	} else {
		return t.LowSpeedBytesPerSecond
	}
}
