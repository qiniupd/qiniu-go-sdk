package operation

import (
	"crypto/md5"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/kirsle/configdir"
	"github.com/stretchr/testify/assert"
)

func removeCacheFile() {
	configPath := configdir.LocalConfig("qiniu")
	configFile := filepath.Join(configPath, "query-cache.json")
	os.Remove(configFile)
}

func newDownloader() *Downloader {
	testRetry, err := strconv.ParseInt(testRetryStr, 10, 64)
	if err != nil {
		panic("cannot parse retry times")
	}
	testPunishTimeS, err := strconv.ParseInt(testPunishTimeStr, 10, 64)
	if err != nil {
		panic("cannot parse punish time")
	}
	testDialTimeoutMs, err := strconv.ParseInt(testDialTimeoutMsStr, 10, 64)
	if err != nil {
		panic("cannot parse dial timeout")
	}
	config := &Config{
		Ak:            testAK,
		Sk:            testSK,
		UcHosts:       []string{testUcHost},
		Bucket:        testBucket,
		Retry:         int(testRetry),
		PunishTimeS:   int(testPunishTimeS),
		DialTimeoutMs: int(testDialTimeoutMs),
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

func TestDownloadFile(t *testing.T) {
	removeCacheFile()
	downloader := newDownloader()
	downloader.ioSelector.setHosts([]string{testIOHosts})
	fd, err := downloader.DownloadFile(testKey, "qiniu.png")
	defer os.Remove("qiniu.png")
	assert.NoError(t, err, "cannot get fd")
	defer fd.Close()
	assert.NoError(t, err, "cannot download file")
	origin := downloadFromQiniu()
	buffer := make([]byte, len(origin))
	n, err := fd.Read(buffer)
	assert.NoError(t, err, "cannot read from fd, read%v\n", n)
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(buffer))
}

func TestDownloadReader(t *testing.T) {
	removeCacheFile()

	downloader := newDownloader()
	downloader.ioSelector.setHosts([]string{testIOHosts})
	rc, err := downloader.DownloadReader("qiniu.png")
	assert.NoError(t, err, "cannot get reader")
	defer rc.Close()

	origin := downloadFromQiniu()

	buffer, err := ioutil.ReadAll(rc)
	assert.NoError(t, err)
	n := len(buffer)
	assert.NoError(t, err, "cannot read from fd, read%v\n", n)

	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(buffer))
}

func TestDownloadBytes(t *testing.T) {
	removeCacheFile()
	downloader := newDownloader()
	downloader.ioSelector.setHosts([]string{testIOHosts})
	origin := downloadFromQiniu()
	data, err := downloader.DownloadBytes(testKey)
	assert.NoError(t, err, "cannot download bytes")
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(data))
}

func TestDownloadRangeBytes(t *testing.T) {
	removeCacheFile()
	downloader := newDownloader()
	downloader.ioSelector.setHosts([]string{testIOHosts})
	origin := downloadFromQiniu()
	origin10 := origin[:11]
	_, data, err := downloader.DownloadRangeBytes(testKey, 0, 10)
	assert.NoError(t, err, "error message")
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin10), hash.Sum(data))
}

func reverseDownload() func(w http.ResponseWriter, r *http.Request) {
	// use tryTime to record the request times, increase try times
	tryTimes := 2
	data := downloadFromQiniu()
	return func(w http.ResponseWriter, r *http.Request) {
		if tryTimes > 0 {
			http.Error(w, "human error", http.StatusBadGateway)
			tryTimes--
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
		w.Write(data)
	}
}

func reverseRangeDownload() func(w http.ResponseWriter, r *http.Request) {
	// use tryTime to record the request times, increase try times
	tryTimes := 2
	data := downloadFromQiniu()
	return func(w http.ResponseWriter, r *http.Request) {
		if tryTimes > 0 {
			http.Error(w, "human error", http.StatusBadGateway)
			tryTimes--
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(int64(len(data)), 10))
		w.Header().Set("Content-Range", "0/163469")
		w.WriteHeader(206)
		w.Write(data)
	}
}

func TestRetryDownload(t *testing.T) {
	removeCacheFile()
	mux := http.NewServeMux()
	handle := reverseDownload()
	mux.HandleFunc("/getfile/"+testAK+"/"+testBucket+"/"+testKey, handle)
	backEnd := httptest.NewServer(mux)
	defer backEnd.Close()
	url, err := url.Parse(backEnd.URL)
	assert.NoError(t, err, "cannot parse backend url")
	frontEnd := httptest.NewServer(httputil.NewSingleHostReverseProxy(url))
	defer frontEnd.Close()
	downloader := newDownloader()
	downloader.ioSelector.setHosts([]string{frontEnd.URL})
	file, err := downloader.DownloadFile(testKey, "/tmp/qiniu.png")
	assert.NoError(t, err, "cannot download file")
	data, err := ioutil.ReadAll(file)
	assert.NoError(t, err, "cannot read file")
	origin := downloadFromQiniu()
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(data))
	os.Remove("/tmp/qiniu.png")
}

func TestRetryDownloadReader(t *testing.T) {
	removeCacheFile()
	mux := http.NewServeMux()
	handle := reverseDownload()
	mux.HandleFunc("/getfile/"+testAK+"/"+testBucket+"/"+testKey, handle)
	backEnd := httptest.NewServer(mux)
	defer backEnd.Close()
	url, err := url.Parse(backEnd.URL)
	assert.NoError(t, err, "cannot parse backend url")
	frontEnd := httptest.NewServer(httputil.NewSingleHostReverseProxy(url))
	defer frontEnd.Close()
	downloader := newDownloader()
	downloader.ioSelector.setHosts([]string{frontEnd.URL})
	reader, err := downloader.DownloadReader(testKey)
	assert.NoError(t, err, "cannot get reader ")
	data, err := ioutil.ReadAll(reader)
	assert.NoError(t, err, "cannot read file")
	origin := downloadFromQiniu()
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(data))
}

func TestRetryDownloadBytes(t *testing.T) {
	removeCacheFile()
	mux := http.NewServeMux()
	handle := reverseDownload()
	mux.HandleFunc("/getfile/"+testAK+"/"+testBucket+"/"+testKey, handle)
	backEnd := httptest.NewServer(mux)
	defer backEnd.Close()
	url, err := url.Parse(backEnd.URL)
	assert.NoError(t, err, "cannot parse backend url")
	frontEnd := httptest.NewServer(httputil.NewSingleHostReverseProxy(url))
	defer frontEnd.Close()
	downloader := newDownloader()
	downloader.ioSelector.setHosts([]string{frontEnd.URL})
	data, err := downloader.DownloadBytes(testKey)
	assert.NoError(t, err, "cannot get reader ")
	origin := downloadFromQiniu()
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(data))
}

func TestRetryDownloadRangeBytes(t *testing.T) {
	removeCacheFile()
	mux := http.NewServeMux()
	handle := reverseRangeDownload()
	mux.HandleFunc("/getfile/"+testAK+"/"+testBucket+"/"+testKey, handle)
	backEnd := httptest.NewServer(mux)
	defer backEnd.Close()
	url, err := url.Parse(backEnd.URL)
	assert.NoError(t, err, "cannot parse backend url")
	frontEnd := httptest.NewServer(httputil.NewSingleHostReverseProxy(url))
	defer frontEnd.Close()
	downloader := newDownloader()
	downloader.ioSelector.setHosts([]string{frontEnd.URL})
	_, data, err := downloader.DownloadRangeBytes(testKey, 0, 163469)
	assert.NoError(t, err, "cannot get reader")
	origin := downloadFromQiniu()
	hash := md5.New()
	assert.Equal(t, hash.Sum(origin), hash.Sum(data))
}
