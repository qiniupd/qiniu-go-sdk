package xlog

import (
	"context"
	"net/http"
	"os"
)

var UseReqCtx bool

func init() {
	if os.Getenv("qiniu_use_req_ctx") == "true" {
		UseReqCtx = true
	}
	if os.Getenv("qiniu_force_spawn_ctx") == "true" {
		ForceSpawnCtx = true
	}
}

func New(w http.ResponseWriter, req *http.Request) *Logger {

	reqId := req.Header.Get(reqidKey)
	if reqId == "" {
		reqId = genReqId()
		req.Header.Set(reqidKey, reqId)
	}
	h := w.Header()
	h.Set(reqidKey, reqId)

	var ctx context.Context
	if UseReqCtx {
		ctx = req.Context()
	}
	return &Logger{h, reqId, ctx}
}

func NewWithReq(req *http.Request) *Logger {

	reqId := req.Header.Get(reqidKey)
	if reqId == "" {
		reqId = genReqId()
		req.Header.Set(reqidKey, reqId)
	}
	h := http.Header{reqidKey: []string{reqId}}

	var ctx context.Context
	if UseReqCtx {
		ctx = req.Context()
	}
	return &Logger{h, reqId, ctx}
}

// Born a logger with:
// 	1. new random req id
//	2. provided ctx
//	3. **DUMMY** trace recorder (will not record anything)
//
func NewDummyWithCtx(ctx context.Context) *Logger {
	id := genReqId()
	return &Logger{
		h:     http.Header{reqidKey: []string{id}},
		reqId: id,
		ctx:   ctx,
	}
}
