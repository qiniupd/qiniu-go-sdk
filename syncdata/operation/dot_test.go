package operation

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

	var dotAPICalled uint32 = 0
	server := newMonitorServer(t, monitorHost, func(records remoteDotRecords) {
		atomic.AddUint32(&dotAPICalled, 1)
		recordsMatch := func(dotType DotType, apiName APIName) (successCount, failedCount uint64, successAverageElapsedDurationMs, failedAverageElapsedDurationMs int64) {
			for _, record := range records.Records {
				if record.Type == dotType && record.APIName == apiName {
					successCount = record.SuccessCount
					failedCount = record.FailedCount
					successAverageElapsedDurationMs = record.SuccessAverageElapsedDurationMs
					failedAverageElapsedDurationMs = record.FailedAverageElapsedDurationMs
					return
				}
			}
			return
		}
		punishedRecordsMatch := func() (punishedCount uint64) {
			for _, record := range records.Records {
				if record.PunishedCount > 0 {
					punishedCount += record.PunishedCount
					return
				}
			}
			return
		}
		successCount, failedCount, successAverageElapsedDurationMs, failedAverageElapsedDurationMs := recordsMatch(HTTPDotType, APIName("api_1"))
		assert.Equal(t, successCount, uint64(2))
		assert.Equal(t, failedCount, uint64(1))
		assert.Equal(t, successAverageElapsedDurationMs, int64(21))
		assert.Equal(t, failedAverageElapsedDurationMs, int64(24))
		successCount, failedCount, successAverageElapsedDurationMs, failedAverageElapsedDurationMs = recordsMatch(HTTPDotType, APIName("api_2"))
		assert.Equal(t, successCount, uint64(3))
		assert.Equal(t, failedCount, uint64(1))
		assert.Equal(t, successAverageElapsedDurationMs, int64(28))
		assert.Equal(t, failedAverageElapsedDurationMs, int64(30))
		successCount, failedCount, successAverageElapsedDurationMs, failedAverageElapsedDurationMs = recordsMatch(SDKDotType, APIName("api_1"))
		assert.Equal(t, successCount, uint64(1))
		assert.Equal(t, failedCount, uint64(1))
		assert.Equal(t, successAverageElapsedDurationMs, int64(10))
		assert.Equal(t, failedAverageElapsedDurationMs, int64(12))
		successCount, failedCount, successAverageElapsedDurationMs, failedAverageElapsedDurationMs = recordsMatch(SDKDotType, APIName("api_2"))
		assert.Equal(t, successCount, uint64(2))
		assert.Equal(t, failedCount, uint64(1))
		assert.Equal(t, successAverageElapsedDurationMs, int64(15))
		assert.Equal(t, failedAverageElapsedDurationMs, int64(18))
		assert.Equal(t, punishedRecordsMatch(), uint64(2))
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
	dotter, err := NewDotter(&Config{MonitorHosts: urls, MaxDotBufferSize: 450})
	if err != nil {
		t.Fatal("Failed to create dotter", err)
	} else {
		assert.NotNil(t, dotter)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		err := dotter.Dot(SDKDotType, APIName("api_1"), true, time.Millisecond*10)
		assert.Nil(t, err)
		err = dotter.Dot(SDKDotType, APIName("api_1"), false, time.Millisecond*12)
		assert.Nil(t, err)
		err = dotter.Punish()
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
		err := dotter.Dot(HTTPDotType, APIName("api_1"), true, time.Millisecond*20)
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
		err = dotter.Punish()
		assert.Nil(t, err)
	}()
	wg.Wait()
	time.Sleep(1 * time.Second)
	assert.Equal(t, atomic.LoadUint32(&dotAPICalled), uint32(1))
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
