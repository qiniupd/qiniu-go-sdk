package httputil

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"qiniupkg.com/x/xlog.v8"

	"qiniupkg.com/x/errors.v1"
	"qiniupkg.com/x/log.v7"
)

// ---------------------------------------------------------------------------
// func Reply

func Reply(w http.ResponseWriter, code int, data interface{}) {

	msg, err := json.Marshal(data)
	if err != nil {
		Error(w, err)
		return
	}
	h := w.Header()
	h.Set("Content-Length", strconv.Itoa(len(msg)))
	h.Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(msg)
}

func ReplyWith(w http.ResponseWriter, code int, bodyType string, msg []byte) {

	h := w.Header()
	h.Set("Content-Length", strconv.Itoa(len(msg)))
	h.Set("Content-Type", bodyType)
	w.WriteHeader(code)
	w.Write(msg)
}

func ReplyWithStream(w http.ResponseWriter, code int, bodyType string, body io.Reader, bytes int64) {

	h := w.Header()
	h.Set("Content-Length", strconv.FormatInt(bytes, 10))
	h.Set("Content-Type", bodyType)
	w.WriteHeader(code)
	io.Copy(w, body) // don't use io.CopyN: if you need, call io.LimitReader(body, bytes) by yourself
}

type Errno int

func (r Errno) Error() string {
	return "E" + strconv.Itoa(int(r))
}

func NewCodeError(code int) error {
	msg := http.StatusText(code)
	if msg == "" {
		return Errno(code)
	}
	return NewError(code, msg)
}

func ReplyWithCode(w http.ResponseWriter, code int) {
	// pandora 那边判断 416 逻辑有问题，会看body是否为空，
	// 这里临时修改把body去掉，后面这段逻辑会删掉。
	if code == 416 {
		h := w.Header()
		h.Set("Content-Length", "0")
		h.Set("Content-Type", "application/json")
		w.WriteHeader(code)
		return
	}
	if code < 400 {
		h := w.Header()
		h.Set("Content-Length", "2")
		h.Set("Content-Type", "application/json")
		w.WriteHeader(code)
		w.Write(emptyObj)
	} else {
		err := http.StatusText(code)
		if err == "" {
			err = "E" + strconv.Itoa(code)
		}
		ReplyErr(w, code, err)
	}
}

var emptyObj = []byte{'{', '}'}

// ---------------------------------------------------------------------------
// func Error

func Error(w http.ResponseWriter, err error) {

	ErrorWithCT(w, err, "")
}

func ErrorWithCT(w http.ResponseWriter, err error, contentType string) {

	detail := errors.Detail(err)
	code, errStr := DetectError(err)
	errCode := DetectErrorCode(err)
	replyErr(2, w, code, errCode, errStr, detail, contentType)
}

func ReplyErr(w http.ResponseWriter, code int, err string) {
	replyErr(2, w, code, "", err, err, "")
}

func ReplyError(w http.ResponseWriter, err string, code int) {

	ReplyErr(w, code, err)
}

func ReplyErrorCode(w http.ResponseWriter, code int, errCode, err string) {
	replyErr(2, w, code, errCode, err, err, "")
}

func DetectCode(err error) int {
	code, _ := errors.HttpCodeOf(err)
	return code
}

func DetectError(err error) (code int, desc string) {
	return errors.HttpCodeOf(err)
}

type errorCoder interface {
	ErrorCode() string
}

func DetectErrorCode(err error) string {
	var ec errorCoder
	if errors.As(err, &ec) {
		return ec.ErrorCode()
	}
	return ""
}

type ErrorRet struct {
	Error     string `json:"error"`
	ErrorCode string `json:"error_code,omitempty"`
}

func replyErr(lvl int, w http.ResponseWriter, code int, errCode, err, detail, contentType string) {
	if contentType == "" {
		contentType = "application/json"
	}
	logWithReqid(lvl+1, w.Header().Get("X-Reqid"), detail)
	msg, _ := json.Marshal(ErrorRet{err, errCode})
	h := w.Header()
	h.Set("Content-Length", strconv.Itoa(len(msg)))
	h.Set("Content-Type", contentType)
	w.WriteHeader(code)
	w.Write(msg)
}

func logWithReqid(lvl int, reqid string, str string) {
	str = strings.Replace(str, "\n", "\n["+reqid+"]", -1)
	log.Std.Output(reqid, log.Lwarn, lvl+1, str)
}

// ---------------------------------------------------------------------------
// type ErrorInfo

type ErrorInfo struct {
	Err     string
	Code    int
	ErrCode string
}

func (e ErrorInfo) ErrorCode() string {
	return e.ErrCode
}

func (e ErrorInfo) HttpCode() int {
	return e.Code
}

func NewError(code int, err string) *ErrorInfo {
	return &ErrorInfo{err, code, ""}
}

func NewErrorCode(code int, errCode, err string) *ErrorInfo {
	return &ErrorInfo{err, code, errCode}
}

func (e *ErrorInfo) Error() string {
	return e.Err
}

func (e *ErrorInfo) WithMsg(err string) *ErrorInfo {
	newErr := *e
	newErr.Err = err
	return &newErr
}

// ---------------------------------------------------------------------------

const (
	StatusGracefulQuit = 570 // 停止服务中（重启中）
	StatusOverload     = 571 // 过载保护阶段（处理部分请求，其余返回 571 重试码)
	StatusAbnormal     = 572 // 认为自身工作不正常时，比如长时间没法拿到数据
	StatusOutOfQuota   = 573 // 客户端请求超出限制，拒绝请求报573
)

var (
	ErrGracefulQuit         = NewError(StatusGracefulQuit, "graceful quit")
	ErrOverload             = NewError(StatusOverload, "overload")
	ErrOutOfConcurrentQuota = NewError(StatusOutOfQuota, "out of concurrent quota")
	ErrOutOfRequestQuota    = NewError(StatusOutOfQuota, "out of request quota")
	ErrOutOfFlowQuota       = NewError(StatusOutOfQuota, "out of flow quota")
)

// ---------------------------------------------------------------------------

// deprecated: use http.Request.Context()
func GetCloseNotifier(w http.ResponseWriter) (cn http.CloseNotifier, ok bool) {

	if cn, ok = w.(http.CloseNotifier); ok {
		return
	}

	v := reflect.ValueOf(w)
	v = reflect.Indirect(v)
	for v.Kind() == reflect.Struct {
		if fv := v.FieldByName("ResponseWriter"); fv.IsValid() {
			if cn, ok = fv.Interface().(http.CloseNotifier); ok {
				return
			}
			if fv.Kind() == reflect.Interface {
				fv = fv.Elem()
			}
			v = reflect.Indirect(fv)
		} else {
			break
		}
	}
	return
}

type fakeCloseNotifier struct{}

func (fcn fakeCloseNotifier) CloseNotify() <-chan bool {

	c := make(chan bool, 1)
	return c
}

// deprecated: use http.Request.Context()
func GetCloseNotifierSafe(w http.ResponseWriter) http.CloseNotifier {

	if cn, ok := GetCloseNotifier(w); ok {
		return cn
	}
	return fakeCloseNotifier{}
}

// ---------------------------------------------------------------------------

type RequestCanceler interface {
	CancelRequest(req *http.Request)
}

// deprecated: use http.Request.WithContext()
func GetRequestCanceler(tp http.RoundTripper) (rc RequestCanceler, ok bool) {

	v := reflect.ValueOf(tp)

subfield:
	// panic if the Field is unexported (but this can be detected in developing)
	if rc, ok = v.Interface().(RequestCanceler); ok {
		return
	}
	v = reflect.Indirect(v)
	if v.Kind() == reflect.Struct {
		for i := v.NumField() - 1; i >= 0; i-- {
			sv := v.Field(i)
			if sv.Kind() == reflect.Interface {
				sv = sv.Elem()
			}
			if sv.MethodByName("RoundTrip").IsValid() {
				v = sv
				goto subfield
			}
		}
	}
	return
}

// ---------------------------------------------------------------------------

func GetHijacker(w http.ResponseWriter) (hj http.Hijacker, ok bool) {

	if hj, ok = w.(http.Hijacker); ok {
		return
	}

	v := reflect.ValueOf(w)
	v = reflect.Indirect(v)
	for v.Kind() == reflect.Struct {
		if fv := v.FieldByName("ResponseWriter"); fv.IsValid() {
			if hj, ok = fv.Interface().(http.Hijacker); ok {
				return
			}
			if fv.Kind() == reflect.Interface {
				fv = fv.Elem()
			}
			v = reflect.Indirect(fv)
		} else {
			break
		}
	}
	return
}

//强制断开请求连接
func HijackClose(xl *xlog.Logger, w http.ResponseWriter) {
	if xl == nil {
		xl = xlog.NewDummy()
	}
	if wf, ok := Flusher(w); ok {
		xl.Info("response flushed")
		wf.Flush()
	} else {
		panic("http.ResponseWriter is not a http.Flusher")
	}
	if wh, ok := GetHijacker(w); ok {
		xl.Info("response can hijack")
		conn, _, err := wh.Hijack()
		if err != nil {
			xl.Errorf("hijack err %v", err)
		} else {
			conn.Close()
		}
	} else {
		panic("http.ResponseWriter is not a http.Hijacker")
	}
}

// ---------------------------------------------------------------------------

func Flusher(w http.ResponseWriter) (f http.Flusher, ok bool) {

	if f, ok = w.(http.Flusher); ok {
		return
	}

	v := reflect.ValueOf(w)
	v = reflect.Indirect(v)
	for v.Kind() == reflect.Struct {
		if fv := v.FieldByName("ResponseWriter"); fv.IsValid() {
			if f, ok = fv.Interface().(http.Flusher); ok {
				return
			}
			if fv.Kind() == reflect.Interface {
				fv = fv.Elem()
			}
			v = reflect.Indirect(fv)
		} else {
			break
		}
	}
	return
}

// ---------------------------------------------------------------------------
