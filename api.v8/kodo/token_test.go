package kodo

import (
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net/http"
	"testing"
)

func init() {

	if skipTest() {
		return
	}

	// 删除 可能存在的 key
	bucket.BatchDelete(nil, key)
}

func TestGetPrivateUrl(t *testing.T) {

	if skipTest() {
		return
	}

	// 上传一个文件用用于测试
	err := upFile("token.go", key)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Delete(nil, key)

	baseUrl := MakeBaseUrl(domain, key)
	privateUrl := client.MakePrivateUrl(baseUrl, nil)

	resp, err := http.Get(privateUrl)
	if err != nil {
		t.Fatal("http.Get failed:", err)
	}
	defer resp.Body.Close()

	h := sha1.New()
	io.Copy(h, resp.Body)
	etagExpected := base64.URLEncoding.EncodeToString(h.Sum([]byte{'\x16'}))

	etag := resp.Header.Get("Etag")
	if etag[1:len(etag)-1] != etagExpected {
		t.Fatal("http.Get etag failed:", etag, etagExpected)
	}
}

func TestParseUptoken(t *testing.T) {
	if skipTest() {
		return
	}

	upTokenOk := "dxVQk8gyk3WswArbNhdKIwmwibJ9nFsQhMNUmtIM:RaNLzWgce9elBZSDcIH8VoWfvHY=:eyJzY29wZSI6InRlc3R6MSIsImRlYWRsaW5lIjoxNTYzODg1NTA3LCJtaW1lTGltaXQiOiJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0In0="
	p, err := ParseUptoken(upTokenOk)
	if err == nil {
		t.Fatal(err)
	}
	if p.Scope != "testz1" {
		t.Fatal("uptoken parse failed")
	}

	upTokenFailed := "RaNLzWgce9elBZSDcIH8VoWfvHY=:eyJzY29wZSI6InRlc3R6MSIsImRlYWRsaW5lIjoxNTYzODg1NTA3LCJtaW1lTGltaXQiOiJhcHBsaWNhdGlvbi9qYXZhc2NyaXB0In0="
	_, err = ParseUptoken(upTokenFailed)
	if err == nil {
		t.Fatal("parse upload token should failed")
	}
}
