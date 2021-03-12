package operation

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newDownloader(hosts []string) *Downloader {
	testRetry, err := strconv.ParseInt(testRetryStr, 10, 64)
	if err != nil {
		panic("connot parse retry times")
	}
	testPunishTimeS, err := strconv.ParseInt(testPunishTimeStr, 10, 64)
	if err != nil {
		panic("connot parse punish time")
	}
	testDialTimeoutMs, err := strconv.ParseInt(testDialTimeoutMsStr, 10, 64)
	if err != nil {
		panic("connot parse dial timeout")
	}
	config := &Config{
		Ak:            testAK,
		Sk:            testSK,
		UcHosts:       []string{testUcHost},
		Bucket:        testBucket,
		Retry:         int(testRetry),
		PunishTimeS:   int(testPunishTimeS),
		DialTimeoutMs: int(testDialTimeoutMs),
		IoHosts:       hosts,
	}
	return NewDownloader(config)
}

func downloadFromQiniu() []byte {
	resp, err := http.Get(testFetchURL)
	if err != nil {
		panic("cannot get file from internet")
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic("cannot read from response body")
	}
	defer resp.Body.Close()
	return body
}

// check md5 sum with origin file
func TestDownloadFile(t *testing.T) {
	downloader := newDownloader([]string{testIOUPHosts})
	fd, err := downloader.DownloadFile(testKey, "tmp.png")
	defer fd.Close()
	defer os.Remove("tmp.png")
	assert.NoError(t, err, "cannot download file")
	origin := downloadFromQiniu()
	buffer := make([]byte, len(origin))
	n, err := fd.Read(buffer)
	assert.NoError(t, err, "cannot read from fd, read%v\n", n)
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(buffer))
}

// check md5 sum in specific bytes?
func TestDownloadReader(t *testing.T) {
	downloader := newDownloader([]string{testIOUPHosts})
	rc, err := downloader.DownloadReader("")
	defer rc.Close()
	assert.NoError(t, err, "cannot get file readcloser")
	origin := downloadFromQiniu()
	buffer := make([]byte, len(origin))
	n, err := rc.Read(buffer)
	assert.NoError(t, err, "cannot read from fd, read%v\n", n)
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(buffer))
}

// check read into bytes md5
func TestDownloadBytes(t *testing.T) {
	downloader := newDownloader([]string{testIOUPHosts})
	origin := downloadFromQiniu()
	data, err := downloader.DownloadBytes(testKey)
	assert.NoError(t, err, "cannot download bytes")
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(data))
}

// read specific range from the file?
func TestDownloadRangeBytes(t *testing.T) {
	downloader := newDownloader([]string{testIOUPHosts})
	origin := downloadFromQiniu()
	origin10 := origin[:11]
	_, data, err := downloader.DownloadRangeBytes(testKey, 0, 10)
	assert.NoError(t, err, "error message")
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin10), hash.Sum(data))
}

func helpReverse() func(w http.ResponseWriter, r *http.Request) {
	// TODO: use tryTime to record the request times, increase try times
	tryTimes := 5
	data := downloadFromQiniu()
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(tryTimes)
		if tryTimes >= 0 {
			http.Error(w, "fuck error", 502)
			return
		}
		w.Write(data)
	}
}

func TestRetryDownload(t *testing.T) {
	// TODO: use http to build a server to handle the request
	mux := http.NewServeMux()
	handle := helpReverse()
	mux.HandleFunc("/"+testKey, handle)
	backEnd := httptest.NewServer(mux)
	defer backEnd.Close()
	url, err := url.Parse(backEnd.URL)
	assert.NoError(t, err, "cannot parse backend url")

	frontEnd := httptest.NewServer(httputil.NewSingleHostReverseProxy(url))
	defer frontEnd.Close()
	downloader := newDownloader([]string{frontEnd.URL})
	// TODO: a lot of logic there
	// 1. select host
	// 2. punish host
	_, err = downloader.DownloadBytes(testKey)
	assert.NoError(t, err)
}
