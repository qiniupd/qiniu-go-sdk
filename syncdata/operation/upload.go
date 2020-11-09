package operation

import (
	"context"
	"encoding/json"
	"github.com/qiniupd/qiniu-go-sdk/x/bytes.v7"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
	q "github.com/qiniupd/qiniu-go-sdk/api.v8/kodocli"
)

type Uploader struct {
	bucket        string
	upHosts       []string
	credentials   *qbox.Mac
	partSize      int64
	upConcurrency int
}

func (p *Uploader) makeUptoken(policy *kodo.PutPolicy) string {
	var rr = *policy
	if rr.Expires == 0 {
		rr.Expires = 3600 + uint32(time.Now().Unix())
	}
	b, _ := json.Marshal(&rr)
	return qbox.SignWithData(p.credentials, b)
}

func (p *Uploader) UploadData(data []byte, key string) (err error) {
	t := time.Now()
	defer func() {
		log.Println("up time ", key, time.Now().Sub(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}

	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UpHosts:        p.upHosts,
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
	})
	for i := 0; i < 3; i++ {
		err = uploader.Put2(context.Background(), nil, upToken, key, bytes.NewReader(data), int64(len(data)), nil)
		if err == nil {
			break
		}
		log.Println("small upload retry", i, err)
	}
	return
}

func (p *Uploader) UploadDataReader(data io.Reader, size int, key string) (err error) {
	t := time.Now()
	defer func() {
		log.Println("up time ", key, time.Now().Sub(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}

	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UpHosts:        p.upHosts,
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
	})

	for i := 0; i < 3; i++ {
		err = uploader.Put2(context.Background(), nil, upToken, key, data, int64(size), nil)
		if err == nil {
			break
		}
		log.Println("small upload retry", i, err)
	}
	return
}

func (p *Uploader) Upload(file string, key string) (err error) {
	t := time.Now()
	defer func() {
		log.Println("up time ", key, time.Now().Sub(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}
	upToken := p.makeUptoken(&policy)

	f, err := os.Open(file)
	if err != nil {
		log.Println("open file failed: ", file, err)
		return err
	}

	fInfo, err := f.Stat()
	if err != nil {
		log.Println("get file stat failed: ", err)
		return err
	}
	var uploader = q.NewUploader(1, &q.UploadConfig{
		UpHosts:        p.upHosts,
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
	})

	if fInfo.Size() <= p.partSize*1024*1024 {
		for i := 0; i < 3; i++ {
			err = uploader.Put2(context.Background(), nil, upToken, key, f, fInfo.Size(), nil)
			if err == nil {
				break
			}
			log.Println("small upload retry", i, err)
		}
		return
	}

	for i := 0; i < 3; i++ {
		err = uploader.Upload(context.Background(), nil, upToken, key, f, fInfo.Size(), nil,
			func(partIdx int, etag string) {
				log.Println("callback", partIdx, etag)
			})
		if err == nil {
			break
		}
		log.Println("part upload retry", i, err)
	}
	return
}

func NewUploader(c *Config) *Uploader {
	mac := qbox.NewMac(c.Ak, c.Sk)
	part := c.PartSize * 1024 * 1024
	if part < 4*1024*1024 {
		part = 4 * 1024 * 1024
	}
	return &Uploader{
		bucket:        c.Bucket,
		upHosts:       dupStrings(c.UpHosts),
		credentials:   mac,
		partSize:      part,
		upConcurrency: c.UpConcurrency,
	}
}

func NewUploaderV2() *Uploader {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewUploader(c)
}
