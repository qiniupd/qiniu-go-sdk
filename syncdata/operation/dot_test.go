package operation_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
	"github.com/stretchr/testify/assert"
)

func TestNewDotterWithoutMonitorHosts(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal("Failed to create temp dir", err)
	}
	defer os.RemoveAll(tmpDir)

	if err = SetCacheDirectoryAndLoad(tmpDir); err != nil {
		t.Fatal("Failed to set cache directory to temp dir", err)
	}
	if dotter, err := NewDotter(&Config{}); err != nil {
		t.Fatal("Failed to create dotter", err)
	} else {
		assert.Nil(t, dotter)
	}
}

func TestNewDotterWithMonitorHosts(t *testing.T) {
	const monitorHost = "localhost:9999"
	const badMonitorHost1 = "localhost:9998"
	const badMonitorHost2 = "localhost:9997"
	const badMonitorHost3 = "localhost:9996"
	const badMonitorHost4 = "localhost:9995"

	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal("Failed to create temp dir", err)
	}
	defer os.RemoveAll(tmpDir)

	if err = SetCacheDirectoryAndLoad(tmpDir); err != nil {
		t.Fatal("Failed to set cache directory to temp dir", err)
	}

	dotAPICalled := 0
	server := newMonitorServer(t, monitorHost, func(records remoteDotRecords) {
		dotAPICalled += 1
		recordsMatch := func(dotType DotType, apiName APIName) (successCount, failedCount int, successAverageElapsedDurationMs, failedAverageElapsedDurationMs int64) {
			for _, record := range records.Records {
				if record.Type == dotType && record.APIName == apiName {
					successCount = int(record.SuccessCount)
					failedCount = int(record.FailedCount)
					return
				}
			}
			return
		}
		successCount, failedCount, successAverageElapsedDurationMs, failedAverageElapsedDurationMs := recordsMatch(HTTPDotType, APIName("api_1"))
		assert.Equal(t, successCount, 2)
		assert.Equal(t, failedCount, 1)
		assert.Equal(t, successAverageElapsedDurationMs, 21)
		assert.Equal(t, failedAverageElapsedDurationMs, 24)
		successCount, failedCount, successAverageElapsedDurationMs, failedAverageElapsedDurationMs = recordsMatch(HTTPDotType, APIName("api_2"))
		assert.Equal(t, successCount, 3)
		assert.Equal(t, failedCount, 1)
		assert.Equal(t, successAverageElapsedDurationMs, 28)
		assert.Equal(t, failedAverageElapsedDurationMs, 30)
		successCount, failedCount, successAverageElapsedDurationMs, failedAverageElapsedDurationMs = recordsMatch(SDKDotType, APIName("api_1"))
		assert.Equal(t, successCount, 1)
		assert.Equal(t, failedCount, 1)
		assert.Equal(t, successAverageElapsedDurationMs, 10)
		assert.Equal(t, failedAverageElapsedDurationMs, 12)
		successCount, failedCount, successAverageElapsedDurationMs, failedAverageElapsedDurationMs = recordsMatch(SDKDotType, APIName("api_2"))
		assert.Equal(t, successCount, 2)
		assert.Equal(t, failedCount, 1)
		assert.Equal(t, successAverageElapsedDurationMs, 15)
		assert.Equal(t, failedAverageElapsedDurationMs, 18)
	})
	defer server.Close()

	badServer1 := newBadMonitorServer(t, badMonitorHost1)
	defer badServer1.Close()
	badServer2 := newBadMonitorServer(t, badMonitorHost2)
	defer badServer2.Close()
	badServer3 := newBadMonitorServer(t, badMonitorHost3)
	defer badServer3.Close()
	badServer4 := newBadMonitorServer(t, badMonitorHost4)
	defer badServer4.Close()

	urls := []string{"http://" + badMonitorHost1, "http://" + badMonitorHost2, "http://" + monitorHost, "http://" + badMonitorHost3, "http://" + badMonitorHost4}
	dotter, err := NewDotter(&Config{MonitorHosts: urls, MaxDotBufferSize: 300})
	if err != nil {
		t.Fatal("Failed to create dotter", err)
	} else {
		assert.NotNil(t, dotter)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err = dotter.Dot(SDKDotType, APIName("api_1"), true, time.Millisecond*10)
		assert.Nil(t, err)
		err = dotter.Dot(SDKDotType, APIName("api_1"), false, time.Millisecond*12)
		assert.Nil(t, err)
		err = dotter.Dot(SDKDotType, APIName("api_2"), true, time.Millisecond*14)
		assert.Nil(t, err)
		err = dotter.Dot(SDKDotType, APIName("api_2"), true, time.Millisecond*16)
		assert.Nil(t, err)
		err = dotter.Dot(SDKDotType, APIName("api_2"), false, time.Millisecond*18)
		assert.Nil(t, err)
	}()
	go func() {
		defer wg.Done()
		err = dotter.Dot(HTTPDotType, APIName("api_1"), true, time.Millisecond*20)
		assert.Nil(t, err)
		err = dotter.Dot(HTTPDotType, APIName("api_1"), true, time.Millisecond*22)
		assert.Nil(t, err)
		err = dotter.Dot(HTTPDotType, APIName("api_1"), false, time.Millisecond*24)
		assert.Nil(t, err)
		err = dotter.Dot(HTTPDotType, APIName("api_2"), true, time.Millisecond*26)
		assert.Nil(t, err)
		err = dotter.Dot(HTTPDotType, APIName("api_2"), true, time.Millisecond*28)
		assert.Nil(t, err)
		err = dotter.Dot(HTTPDotType, APIName("api_2"), false, time.Millisecond*30)
		assert.Nil(t, err)
		err = dotter.Dot(HTTPDotType, APIName("api_2"), true, time.Millisecond*32)
		assert.Nil(t, err)
	}()
	wg.Wait()
	time.Sleep(1 * time.Second)
	assert.Equal(t, dotAPICalled, 1)
}

type remoteDotRecord struct {
	Type         DotType `json:"type"`
	APIName      APIName `json:"api_name"`
	SuccessCount uint64  `json:"success_count"`
	FailedCount  uint64  `json:"failed_count"`
}

type remoteDotRecords struct {
	Records []*remoteDotRecord `json:"logs"`
}

func newMonitorServer(t *testing.T, bindAddr string, handle func(remoteDotRecords)) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stat", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "UpToken "))
		var records remoteDotRecords
		err := json.NewDecoder(r.Body).Decode(&records)
		if err != nil {
			t.Fatal("Failed to decode response body", err)
		}
		handle(records)
		w.WriteHeader(http.StatusOK)
	})
	server := http.Server{
		Addr:    bindAddr,
		Handler: mux,
	}
	go func() {
		err := server.ListenAndServe()
		assert.Equal(t, err, http.ErrServerClosed)
	}()
	return &server
}

func newBadMonitorServer(t *testing.T, bindAddr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/stat", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "UpToken "))
		w.WriteHeader(http.StatusInternalServerError)
	})
	server := http.Server{
		Addr:    bindAddr,
		Handler: mux,
	}
	go func() {
		err := server.ListenAndServe()
		assert.Equal(t, err, http.ErrServerClosed)
	}()
	return &server
}
