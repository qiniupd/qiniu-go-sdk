package operation

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testAK               = os.Getenv("accessKey")
	testSK               = os.Getenv("secretKey")
	testBucket           = os.Getenv("QINIU_TEST_BUCKET")
	testBucketPrivate    = os.Getenv("QINIU_TEST_BUCKET_PRIVATE")
	testUcHost           = os.Getenv("QINIU_TEST_UC_HOST")
	testRetryStr         = os.Getenv("QINIU_TEST_RETRY")
	testPunishTimeStr    = os.Getenv("QINIU_TEST_PUNISHTIME_S")
	testDialTimeoutMsStr = os.Getenv("QINIU_TEST_TIMEOUT_MS")
	testIOUPHosts        = os.Getenv("QINIU_TEST_IO_UP_HOSTS")

	testKey      = "qiniu.png"
	testFetchURL = "http://devtools.qiniu.com/qiniu.png"
	testSiteURL  = "http://devtools.qiniu.com"
)

func newUploader() *Uploader {
	testRetry, err := strconv.ParseInt(testRetryStr, 10, 64)
	if err != nil {
		panic("connot parse retry times")
	}
	testDialTimeoutMs, err := strconv.ParseInt(testDialTimeoutMsStr, 10, 64)
	if err != nil {
		panic("connot parse dial timeout")
	}
	cfg := &Config{
		Ak:            testAK,
		Sk:            testSK,
		UpConcurrency: 3,
		PartSize:      1,
		UcHosts:       []string{testUcHost},
		Bucket:        testBucket,
		Retry:         int(testRetry),
		DialTimeoutMs: int(testDialTimeoutMs),
		UpHosts:       []string{testIOUPHosts},
		PunishTimeS:   1,
	}
	return NewUploader(cfg)
}

func TestUploadData(t *testing.T) {
	upLoader := newUploader()
	data := downloadFromQiniu()
	err := upLoader.UploadData(data, testKey)
	assert.NoError(t, err, "cannot upload data")
}
