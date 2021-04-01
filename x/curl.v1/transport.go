package curl

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	libcurl "github.com/andelf/go-curl"
)

var (
	curlInit    sync.Once
	easyPool    *easyPoolImpl
	ErrCurlInit = errors.New("create easy handle error")
)

type Transport struct {
	Expect100ContinueTimeout time.Duration
	DisableExpect100Continue bool
	Timeout                  time.Duration
	ConnectTimeout           time.Duration
	FollowLocation           bool
	MaxRedirects             int
	MaxConnections           int
	LowSpeedDuration         time.Duration
	LowSpeedBytesPerSecond   int
}

var _ http.RoundTripper = &Transport{}

func (t *Transport) RoundTrip(request *http.Request) (*http.Response, error) {
	var (
		syncPerformed       = false
		err           error = nil
	)

	curlInit.Do(func() {
		err = libcurl.GlobalInit(libcurl.GLOBAL_ALL)
		easyPool = newEasyPoolImpl()
	})

	if err != nil {
		return nil, err
	}

	easy := easyPool.Get()
	if easy == nil {
		return nil, ErrCurlInit
	}
	defer func() {
		if !syncPerformed {
			easyPool.Put(easy)
		}
	}()

	if err = easy.Setopt(libcurl.OPT_URL, request.URL.String()); err != nil {
		return nil, err
	}
	if err = easy.Setopt(libcurl.OPT_NOSIGNAL, 1); err != nil {
		return nil, err
	}

	if request.Body != nil {
		err = easy.Setopt(libcurl.OPT_UPLOAD, 1)
	}

	switch request.Method {
	case http.MethodGet:
		err = easy.Setopt(libcurl.OPT_HTTPGET, 1)
	case http.MethodPost:
		err = easy.Setopt(libcurl.OPT_POST, 1)
	case http.MethodPut:
		err = easy.Setopt(libcurl.OPT_PUT, 1)
	case http.MethodHead:
		err = easy.Setopt(libcurl.OPT_NOBODY, 1)
	default:
		err = easy.Setopt(libcurl.OPT_CUSTOMREQUEST, request.Method)
	}
	if err != nil {
		return nil, err
	}

	reqHeaders := make([]string, 0, len(request.Header))
	for headerName, headerValues := range request.Header {
		for _, headerValue := range headerValues {
			reqHeaders = append(reqHeaders, headerName+": "+headerValue)
		}
	}
	if t.getDisableExpect100Continue(request) && request.Header.Get("Expect") == "" {
		reqHeaders = append(reqHeaders, "Expect:")
	}
	if err = easy.Setopt(libcurl.OPT_HTTPHEADER, reqHeaders); err != nil {
		return nil, err
	}

	if t.getExpect100ContinueTimeout(request) > 0 {
		if err = easy.Setopt(libcurl.OPT_EXPECT_100_TIMEOUT_MS, int(t.getExpect100ContinueTimeout(request)/time.Millisecond)); err != nil {
			return nil, err
		}
	}
	if t.getTimeout(request) > 0 {
		if err = easy.Setopt(libcurl.OPT_TIMEOUT_MS, int(t.getTimeout(request)/time.Millisecond)); err != nil {
			return nil, err
		}
	}
	if t.getConnectTimeout(request) > 0 {
		if err = easy.Setopt(libcurl.OPT_CONNECTTIMEOUT_MS, int(t.getConnectTimeout(request)/time.Millisecond)); err != nil {
			return nil, err
		}
	}
	if t.getFollowLocation(request) {
		if err = easy.Setopt(libcurl.OPT_FOLLOWLOCATION, 1); err != nil {
			return nil, err
		}
	}
	if t.getMaxRedirects(request) > 0 {
		if err = easy.Setopt(libcurl.OPT_MAXREDIRS, t.getMaxRedirects(request)); err != nil {
			return nil, err
		}
	}
	if t.getMaxConnections(request) > 0 {
		if err = easy.Setopt(libcurl.OPT_MAXCONNECTS, t.getMaxConnections(request)); err != nil {
			return nil, err
		}
	}
	if t.getLowSpeedDuration(request) > 0 {
		if err = easy.Setopt(libcurl.OPT_LOW_SPEED_TIME, int(t.getLowSpeedDuration(request)/time.Second)); err != nil {
			return nil, err
		}
	}
	if t.getLowSpeedBytesPerSecond(request) > 0 {
		if err = easy.Setopt(libcurl.OPT_LOW_SPEED_LIMIT, t.getLowSpeedBytesPerSecond(request)); err != nil {
			return nil, err
		}
	}

	var readErr error = nil
	if request.Body != nil {
		if err = easy.Setopt(libcurl.OPT_READFUNCTION, func(buf []byte, userData interface{}) int {
			if request.Body == nil {
				return 0
			}
			if haveRead, err := request.Body.Read(buf); err != nil {
				if err == io.EOF {
					return 0
				}
				readErr = err
				return 0
			} else {
				return haveRead
			}
		}); err != nil {
			return nil, err
		}
	}

	responseBody := newRingBuffer(1 << 22)

	response := http.Response{
		Header:  make(http.Header, 16),
		Body:    ioutil.NopCloser(responseBody),
		Request: request,
	}

	errorChan := make(chan error)
	errorChanDone := false

	if err = easy.Setopt(libcurl.OPT_HEADERFUNCTION, func(headerField []byte, userData interface{}) bool {
		if errorChanDone {
			return true
		} else if bytes.Equal(headerField, []byte("\r\n")) {
			if statusCode, err := easy.Getinfo(libcurl.INFO_HTTP_CODE); err == nil {
				response.StatusCode = statusCode.(int)
				if response.StatusCode < 200 {
					return true
				}
			}

			if httpVersion, err := easy.Getinfo(libcurl.INFO_HTTP_VERSION); err == nil {
				connectionHeader := response.Header.Get("Connection")

				switch httpVersion.(int) {
				case libcurl.HTTP_VERSION_1_0:
					response.ProtoMajor = 1
					response.ProtoMinor = 0
					response.Close = connectionHeader == "" || connectionHeader == "close"
				case libcurl.HTTP_VERSION_1_1:
					response.ProtoMajor = 1
					response.ProtoMinor = 1
					response.Close = connectionHeader == "close"
				}
			}

			if contentLength := response.Header.Get("Content-Length"); contentLength != "" {
				response.ContentLength, _ = strconv.ParseInt(contentLength, 10, 64)
			}
			errorChan <- nil
			errorChanDone = true
			return true
		}
		pair := strings.SplitN(string(headerField), ":", 2)
		if len(pair) != 2 {
			pair = strings.SplitN(string(headerField), " ", 2)
			if len(pair) == 2 {
				proto := strings.TrimSpace(pair[0])
				status := strings.TrimSpace(pair[1])
				if strings.HasPrefix(proto, "HTTP/") {
					response.Proto = proto
					response.Status = status
				}
			}
			return true
		}
		headerName := strings.TrimSpace(pair[0])
		headerValue := strings.TrimSpace(pair[1])
		response.Header.Add(headerName, headerValue)
		return true
	}); err != nil {
		return nil, err
	}

	if err = easy.Setopt(libcurl.OPT_WRITEFUNCTION, func(buf []byte, userData interface{}) bool {
		if responseBody.IsClosed() {
			return true
		}

		haveWritten := 0
		for haveWritten < len(buf) {
			if n, err := responseBody.Write(buf[haveWritten:]); err != nil {
				responseBody.WriteClose(err)
				break
			} else {
				haveWritten += n
			}
		}
		return true
	}); err != nil {
		return nil, err
	}

	syncPerformed = true
	go func() {
		defer close(errorChan)
		defer easyPool.Put(easy)

		runtime.LockOSThread()
		defer runtime.UnlockOSThread()

		if err := easy.Perform(); readErr != nil {
			if !errorChanDone {
				errorChan <- readErr
			}
			return
		} else if err != nil {
			if !errorChanDone {
				errorChan <- err
			}
			return
		}

		responseBody.WriteClose(nil)
	}()

	if err = <-errorChan; err != nil {
		return nil, err
	} else {
		return &response, nil
	}
}
