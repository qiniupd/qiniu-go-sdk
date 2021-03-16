package kodocli

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"testing"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
)

var uploader Uploader

func init() {
	uploader = NewUploader(1, &UploadConfig{UploadPartSize: 1 << 24})
}

func TestBlockCount(t *testing.T) {
	partNumbers := map[int64]int{
		1 << 21:                     1,
		uploader.UploadPartSize:     1,
		uploader.UploadPartSize + 1: 2,
	}
	for fsize, num := range partNumbers {
		n1 := uploader.partNumber(int64(fsize))
		if n1 != num {
			t.Fatalf("partNumber failed, fsize: %d, expect part number: %d, but got: %d", fsize, num, n1)
		}
	}
}

func TestPartsUpload(t *testing.T) {
	ak := os.Getenv("QINIU_ACCESS_KEY")
	sk := os.Getenv("QINIU_SECRET_KEY")
	bucket := os.Getenv("QINIU_TEST_BUCKET")
	fpath := os.Getenv("FILE_PATH_UPLOAD")
	domain := os.Getenv("QINIU_TEST_BUCKET_DOMAIN")
	if ak == "" || sk == "" || bucket == "" || fpath == "" || domain == "" {
		return
	}

	policy := kodo.PutPolicy{
		Scope: bucket,
	}
	cli := kodo.New(1, &kodo.Config{AccessKey: ak, SecretKey: sk})
	uptoken := cli.MakeUptoken(&policy)

	f, err := os.Open(fpath)
	if err != nil {
		t.Fatal("open file failed: ", fpath, err)
	}

	fInfo, err := f.Stat()
	if err != nil {
		t.Fatal("get file stat failed: ", err)
	}

	fname1 := path.Base(fpath)
	err = uploader.Upload(context.Background(), nil, uptoken, fname1, f, fInfo.Size(), &CompleteMultipart{
		Metadata: map[string]string{"abc": "rain"},
	}, nil)
	if err != nil {
		t.Fatal("upload failed: ", err)
	}

	fname2 := fname1 + ".parts"
	uploadParts := uploader.makeUploadParts(fInfo.Size())
	err = uploader.UploadWithParts(context.Background(), nil, uptoken, fname2, f, fInfo.Size(), uploadParts, &CompleteMultipart{
		Metadata: map[string]string{"abc": "rain"},
	}, nil)
	if err != nil {
		t.Fatal("upload failed: ", err)
	}

	getUrl1 := domain + "/" + fname1
	req1, err := http.NewRequest("GET", getUrl1, nil)
	if err != nil {
		t.Fatal("make request failed:", getUrl1, err)
	}
	getUrl2 := domain + "/" + fname2
	req2, err := http.NewRequest("GET", getUrl2, nil)
	if err != nil {
		t.Fatal("make request failed:", getUrl2, err)
	}

	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal("make http call failed:", getUrl1, err)
	}

	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal("make http call failed:", getUrl2, err)
	}

	h1 := md5.New()
	h2 := md5.New()

	w1, err := io.Copy(h1, resp1.Body)
	if err != nil {
		t.Fatal("copy failed:", err)
	}
	w2, err := io.Copy(h2, resp2.Body)
	if err != nil {
		t.Fatal("copy failed:", err)
	}

	s1 := h1.Sum(nil)
	s2 := h2.Sum(nil)
	if w1 != w2 || string(s1) != string(s2) {
		t.Fatal("different file", w1, w2, s1, s2)
	}
}

func TestStreamUpload(t *testing.T) {
	ak := os.Getenv("QINIU_ACCESS_KEY")
	sk := os.Getenv("QINIU_SECRET_KEY")
	bucket := os.Getenv("QINIU_TEST_BUCKET")
	filePath := os.Getenv("FILE_PATH_UPLOAD")
	upHost := os.Getenv("UP_HOST")

	if ak == "" || sk == "" || bucket == "" || filePath == "" || upHost == "" {
		return
	}

	go func() {
		_ = http.ListenAndServe(":35782", http.FileServer(http.Dir(path.Dir(filePath))))
	}()

	key := path.Base(filePath)
	policy := &AuthPolicy{
		Scope:    fmt.Sprintf("%s:%s", bucket, key),
		Deadline: 3600*24 + time.Now().Unix(),
	}
	upToken := MakeAuthTokenString(ak, sk, policy)
	upCli := NewUploader(0, &UploadConfig{
		UpHosts:        []string{upHost},
		Transport:      http.DefaultTransport,
		UploadPartSize: 1 << 24,
	})

	resp, err := http.Get("http://localhost:35782/" + key)
	if err != nil {
		t.Fatalf("get file err: %v", err)
	}

	defer resp.Body.Close()
	var ret PutRet
	err = upCli.StreamUpload(context.TODO(), &ret, upToken, key, resp.Body, nil)
	if err != nil {
		t.Fatalf("up file err: %v", err)
	}
	t.Log(ret)
}
