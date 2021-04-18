package kodocli

import (
	"bytes"
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
		n1 := uploader.partNumber(int64(fsize), 0)
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
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		t.Fatal("get file stat failed: ", err)
	}

	fname := path.Base(fpath)
	err = uploader.Upload(context.Background(), nil, uptoken, fname, f, fInfo.Size(), &CompleteMultipart{
		Metadata: map[string]string{"abc": "rain"},
	}, nil)
	if err != nil {
		t.Fatal("upload failed: ", err)
	}

	getUrl := domain + "/" + fname
	req, err := http.NewRequest("GET", getUrl, nil)
	if err != nil {
		t.Fatal("make request failed:", getUrl, err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("make http call failed:", getUrl, err)
	}

	_, err = f.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal("seek file failed:", err)
	}

	h := md5.New()
	hsrc := md5.New()

	w, err := io.Copy(h, resp.Body)
	if err != nil {
		t.Fatal("copy failed:", err)
	}
	wsrc, err := io.Copy(hsrc, f)
	if err != nil {
		t.Fatal("copy failed:", err)
	}

	s := h.Sum(nil)
	ssrc := hsrc.Sum(nil)
	if w != wsrc || bytes.Equal(s, ssrc) {
		t.Fatal("different file", w, wsrc, s, ssrc)
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
	err = upCli.StreamUpload(context.TODO(), &ret, upToken, key, resp.Body, nil, nil)
	if err != nil {
		t.Fatalf("up file err: %v", err)
	}
	t.Log(ret)
}
