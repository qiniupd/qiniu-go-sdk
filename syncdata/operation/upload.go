package operation

import (
	"context"
	"encoding/json"
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

func (p *Uploader) Upload(file string, key string) error {
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

	if fInfo.Size() <= 16*1024*1024 {
		err = uploader.PutFile(context.Background(), nil, upToken, key, file, nil)
		if err == nil {
			return nil
		}
		log.Println("small upload retry0", err)
		err = uploader.PutFile(context.Background(), nil, upToken, key, file, nil)
		log.Println("small upload retry1", err)
		return err
	}

	err = uploader.Upload(context.Background(), nil, upToken, key, f, fInfo.Size(), nil,
		func(partIdx int, etag string) {
			log.Println("callback", partIdx, etag)
		})
	if err == nil {
		return nil
	}
	log.Println("part upload retry0", err)
	err = uploader.Upload(context.Background(), nil, upToken, key, f, fInfo.Size(), nil,
		func(partIdx int, etag string) {
			log.Println("callback", partIdx, etag)
		})

	log.Println("part upload retry1", err)
	return err
}

func NewUploader(c *Config) *Uploader {
	mac := qbox.NewMac(c.Ak, c.Sk)
	part := c.PartSize * 1024 * 1024
	if part < 4*1024*1024 {
		part = 4 * 1024 * 1024
	}
	return &Uploader{
		bucket:        c.Bucket,
		upHosts:       c.UpHosts,
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
