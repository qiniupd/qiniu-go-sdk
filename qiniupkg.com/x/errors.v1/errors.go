package errors

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"qiniupkg.com/x/log.v7"
)

const prefix = " ==> "

// --------------------------------------------------------------------

func New(msg string) error {
	return errors.New(msg)
}

func As(err error, target interface{}) bool {
	return errors.As(err, target)
}

func Is(err, target error) bool {
	return errors.Is(err, target)
}

func Unwrap(err error) error {
	return errors.Unwrap(err)
}

// --------------------------------------------------------------------

type errorDetailer interface {
	ErrorDetail() string
}

func Detail(err error) string {
	if e, ok := err.(errorDetailer); ok {
		return e.ErrorDetail()
	}
	return prefix + err.Error()
}

type httpCoder interface {
	HttpCode() int
}

type errorCoder interface {
	ErrorCode() string
}

var HttpCodeOfUnEnum = func(err error) (code int, desc string) {
	return 599, err.Error()
}

func HttpCodeOf(err error) (code int, desc string) {
	if err == nil {
		return 200, ""
	}

	var hc httpCoder
	if errors.As(err, &hc) {
		return hc.HttpCode(), err.Error()
	}

	switch err {
	case syscall.EINVAL:
		return 400, "invalid arguments"
	case syscall.ENOENT:
		return 612, "no such entry"
	case syscall.EEXIST:
		return 614, "entry exists"
	case context.Canceled:
		return 499, context.Canceled.Error()
	}
	return HttpCodeOfUnEnum(err)
}

// --------------------------------------------------------------------

type ErrorInfo struct {
	Err  error
	Why  error
	Cmd  []interface{}
	File string
	Line int
}

func shortFile(file string) string {
	pos := strings.LastIndex(file, "/src/")
	if pos != -1 {
		return file[pos+5:]
	}
	return file
}

func Info(err error, cmd ...interface{}) *ErrorInfo {
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "???"
	}
	return &ErrorInfo{Cmd: cmd, Err: Err(err), File: file, Line: line, Why: err}
}

// file and line tracing may have problems with go1.9, see related issue: https://github.com/golang/go/issues/22916
func InfoEx(skip int, err error, cmd ...interface{}) *ErrorInfo {
	oldErr := err
	if e, ok := err.(*ErrorInfo); ok {
		err = e.Err
	}
	_, file, line, ok := runtime.Caller(skip)
	if !ok {
		file = "???"
	}
	return &ErrorInfo{Cmd: cmd, Err: err, File: file, Line: line, Why: oldErr}
}

func (r *ErrorInfo) Cause() error {
	return r.Err
}

func (r *ErrorInfo) Unwrap() error {
	return r.Why
}

func (r *ErrorInfo) Error() string {
	return r.Err.Error()
}

func (r *ErrorInfo) ErrorDetail() string {
	e := prefix + shortFile(r.File) + ":" + strconv.Itoa(r.Line) + ": " + r.Err.Error() + " ~ " + fmt.Sprintln(r.Cmd...)
	if r.Why != nil && r.Why != r.Err {
		e += Detail(r.Why)
	} else {
		e = e[:len(e)-1]
	}
	return e
}

func (r *ErrorInfo) ErrorCode() string {
	var ec errorCoder
	if errors.As(r.Err, &ec) {
		return ec.ErrorCode()
	}
	return ""
}

func (r *ErrorInfo) HttpCode() int {
	code, _ := HttpCodeOf(r.Err)
	return code
}

func (r *ErrorInfo) Detail(err error) *ErrorInfo {
	r.Why = err
	return r
}

func (r *ErrorInfo) Method() (cmd string, ok bool) {
	if len(r.Cmd) > 0 {
		if cmd, ok = r.Cmd[0].(string); ok {
			if pos := strings.Index(cmd, " "); pos > 1 {
				cmd = cmd[:pos]
			}
		}
	}
	return
}

func (r *ErrorInfo) LogMessage() string {
	detail := r.ErrorDetail()
	if cmd, ok := r.Method(); ok {
		detail = cmd + " failed:\n" + detail
	}
	return detail
}

// deprecated. please use (*ErrorInfo).LogWarn
//
func (r *ErrorInfo) Warn() *ErrorInfo {
	log.Std.Output("", log.Lwarn, 2, r.LogMessage())
	return r
}

func (r *ErrorInfo) LogWarn(reqId string) *ErrorInfo {
	log.Std.Output(reqId, log.Lwarn, 2, r.LogMessage())
	return r
}

func (r *ErrorInfo) LogError(reqId string) *ErrorInfo {
	log.Std.Output(reqId, log.Lerror, 2, r.LogMessage())
	return r
}

func (r *ErrorInfo) Log(level int, reqId string) *ErrorInfo {
	log.Std.Output(reqId, level, 2, r.LogMessage())
	return r
}

// --------------------------------------------------------------------

type Causer interface {
	Cause() error
}

func Err(err error) error {
	if e, ok := err.(Causer); ok {
		if diag := e.Cause(); diag != nil {
			return diag
		}
	}
	return err
}

// --------------------------------------------------------------------
