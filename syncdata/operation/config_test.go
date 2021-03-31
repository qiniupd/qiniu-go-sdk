package operation

import (
	"os"
	"strconv"
)

var (
	testAK               = os.Getenv("accessKey")
	testSK               = os.Getenv("secretKey")
	testBucket           = os.Getenv("QINIU_TEST_BUCKET")
	testBucketPrivate    = os.Getenv("QINIU_TEST_BUCKET_PRIVATE")
	testRetryStr         = os.Getenv("QINIU_TEST_RETRY")
	testPunishTimeStr    = os.Getenv("QINIU_TEST_PUNISHTIME_S")
	testDialTimeoutMsStr = os.Getenv("QINIU_TEST_TIMEOUT_MS")
	testUPHosts          = os.Getenv("QINIU_TEST_UP_HOSTS")
	testIOHosts          = os.Getenv("QINIU_TEST_IO_HOSTS")
	testUcHost           = os.Getenv("QINIU_TEST_UC_HOST")
	testRSHosts          = os.Getenv("QINIU_TEST_RS_HOSTS")
	testRSFHosts         = os.Getenv("QINIU_TEST_RSF_HOSTS")

	testKey      = "qiniu.png"
	testFetchURL = "http://devtools.qiniu.com/qiniu.png"
	testSiteURL  = "http://devtools.qiniu.com"
)

func newTestConfig() *Config {
	testRetry, err := strconv.ParseInt(testRetryStr, 10, 64)
	if err != nil {
		panic("connot parse retry times")
	}
	testDialTimeoutMs, err := strconv.ParseInt(testDialTimeoutMsStr, 10, 64)
	if err != nil {
		panic("connot parse dial timeout")
	}
	return &Config{
		Ak:            testAK,
		Sk:            testSK,
		UpConcurrency: 3,
		PartSize:      1,
		UcHosts:       []string{testUcHost, testUcHost, testUcHost, testUcHost, testUcHost},
		Bucket:        testBucket,
		Retry:         int(testRetry),
		DialTimeoutMs: int(testDialTimeoutMs),
		UpHosts:       []string{testUPHosts, testUPHosts, testUPHosts, testUPHosts, testUPHosts},
		PunishTimeS:   1,
	}
}
