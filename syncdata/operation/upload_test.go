package operation

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newUploader() *Uploader {
	testRetry, err := strconv.ParseInt(testRetryStr, 10, 64)
	if err != nil {
		panic("cannot parse retry times")
	}
	testDialTimeoutMs, err := strconv.ParseInt(testDialTimeoutMsStr, 10, 64)
	if err != nil {
		panic("cannot parse dial timeout")
	}
	cfg := &Config{
		Ak:            testAK,
		Sk:            testSK,
		UpConcurrency: 3,
		PartSize:      10,
		UcHosts:       []string{testUcHost},
		Bucket:        testBucket,
		Retry:         int(testRetry),
		DialTimeoutMs: int(testDialTimeoutMs),
		UpHosts:       []string{testUPHosts},
		PunishTimeS:   1,
	}
	return NewUploader(cfg)
}

func TestUpload(t *testing.T) {
	removeCacheFile()
	uploader := newUploader()
	err := uploader.Upload("../../data/qiniu.png", "qiniu.png")
	assert.NoError(t, err, "upload successful")
}

func cleanFunc(t *testing.T) {
	cfg := Config{
		IoHosts:       []string{testIOHosts},
		UcHosts:       []string{testUcHost},
		UpHosts:       []string{testUPHosts},
		RsHosts:       []string{testRSHosts},
		RsfHosts:      []string{testRSFHosts},
		Bucket:        testBucket,
		Ak:            testAK,
		Sk:            testSK,
		PartSize:      10,
		UpConcurrency: 10,
	}
	list := NewLister(&cfg)
	err := list.Delete("part")
	assert.NoError(t, err, "delete successfully")
}

func TestUploadReader(t *testing.T) {
	removeCacheFile()
	uploader := newUploader()
	file, err := os.Open("../../data/qiniu.png")
	assert.NoError(t, err, "cannot open file")
	defer file.Close()

	err = uploader.UploadDataReader(file, 10, "part")
	assert.NoError(t, err, "upload part file successfully")
	defer cleanFunc(t)
}

func TestUploadData(t *testing.T) {
	removeCacheFile()
	uploader := newUploader()
	data := downloadFromQiniu()
	err := uploader.UploadData(data, testKey)
	assert.NoError(t, err, "cannot upload data")
}

func TestUploadDataReaderAt(t *testing.T) {
	removeCacheFile()
	uploader := newUploader()
	file, err := os.Open("../../data/qiniu.png")
	assert.NoError(t, err, "cannot open file")
	defer file.Close()
	err = uploader.UploadDataReaderAt(file, 10, "part")
	assert.NoError(t, err, "upload part successfully")
	file.Seek(0, 0)
	defer cleanFunc(t)
}

func TestUploadDataReader(t *testing.T) {
	removeCacheFile()
	uploader := newUploader()
	file, err := os.Open("../../data/qiniu.png")
	assert.NoError(t, err, "cannot open file")
	defer file.Close()
	err = uploader.UploadDataReader(file, 1000, "part")
	assert.NoError(t, err, "upload part successfully")

	defer cleanFunc(t)
}
