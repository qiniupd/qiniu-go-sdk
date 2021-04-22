package operation

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
	q "github.com/qiniupd/qiniu-go-sdk/api.v8/kodocli"
)

type Uploader struct {
	bucket        string
	upSelector    *HostSelector
	credentials   *qbox.Mac
	partSize      int64
	upConcurrency int
	queryer       *Queryer
	tries         int
	transport     http.RoundTripper
}

func (p *Uploader) makeUptoken(policy *kodo.PutPolicy) string {
	var rr = *policy
	if rr.Expires == 0 {
		rr.Expires = 3600 + uint32(time.Now().Unix())
	}
	b, _ := json.Marshal(&rr)
	return qbox.SignWithData(p.credentials, b)
}

func (p *Uploader) retry(uploader *q.Uploader, f func() error) (err error) {
	for i := 0; i < p.tries; i++ {
		err = f()
		if shouldRetry(err) {
			elog.Warn("upload try failed. punish host", i, err)
			continue
		}
		break
	}
	return
}

func (p *Uploader) UploadData(data []byte, key string) error {
	return p.UploadDataWithContext(context.Background(), data, key, nil)
}

func (p *Uploader) UploadDataWithContext(ctx context.Context, data []byte, key string, ret interface{}) error {
	t := time.Now()
	defer func() {
		elog.Info("upload file:", key, "time:", time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}

	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})
	return p.retry(&uploader, func() error {
		return uploader.Put2(ctx, ret, upToken, key, bytes.NewReader(data), int64(len(data)), nil)
	})
}

func (p *Uploader) UploadDataReaderAt(data io.ReaderAt, size int64, key string) error {
	return p.UploadDataReader(data, size, key)
}

func (p *Uploader) UploadDataReaderAtWithContext(ctx context.Context, data io.ReaderAt, size int64, key string, ret interface{}) error {
	return p.UploadDataReaderWithContext(ctx, data, size, key, ret)
}

func (p *Uploader) UploadDataReader(data io.ReaderAt, size int64, key string) error {
	return p.UploadDataReaderWithContext(context.Background(), data, size, key, nil)
}

func (p *Uploader) UploadDataReaderWithContext(ctx context.Context, data io.ReaderAt, size int64, key string, ret interface{}) error {
	t := time.Now()
	defer func() {
		elog.Info("upload file:", key, "time:", time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}

	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})

	return p.retry(&uploader, func() error {
		return uploader.Put2(ctx, ret, upToken, key, newReaderAtNopCloser(data), size, nil)
	})
}

func (p *Uploader) Upload(file string, key string) error {
	return p.UploadWithContext(context.Background(), file, key, nil)
}

func (p *Uploader) UploadWithContext(ctx context.Context, file string, key string, ret interface{}) error {
	t := time.Now()
	defer func() {
		elog.Info("upload file:", key, "time:", time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}
	upToken := p.makeUptoken(&policy)

	f, err := os.Open(file)
	if err != nil {
		elog.Error("open file failed: ", file, err)
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		elog.Error("get file stat failed: ", file, err)
		return err
	}

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})

	if fInfo.Size() <= p.partSize {
		return p.retry(&uploader, func() error {
			return uploader.Put2(ctx, ret, upToken, key, newReaderAtNopCloser(f), fInfo.Size(), nil)
		})
	}

	return p.retry(&uploader, func() error {
		return uploader.Upload(ctx, ret, upToken, key, newReaderAtNopCloser(f), fInfo.Size(), nil,
			func(partIdx int, etag string) {
				elog.Info("upload", key, "callback", "part:", partIdx, "etag:", etag)
			})
	})
}

func (p *Uploader) UploadReader(reader io.Reader, key string) error {
	return p.UploadReaderWithContext(context.Background(), reader, key, nil)
}

func (p *Uploader) UploadReaderWithContext(ctx context.Context, reader io.Reader, key string, ret interface{}) error {
	t := time.Now()
	defer func() {
		elog.Info("upload file:", key, "time:", time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}
	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})

	bufReader := bufio.NewReader(reader)
	firstPart, err := ioutil.ReadAll(io.LimitReader(bufReader, p.partSize))
	if err != nil {
		return err
	}

	smallUpload := false
	if len(firstPart) < int(p.partSize) {
		smallUpload = true
	} else if _, err = bufReader.Peek(1); err != nil {
		if err == io.EOF {
			smallUpload = true
		} else {
			return err
		}
	}

	if smallUpload {
		return p.retry(&uploader, func() error {
			return uploader.Put2(ctx, ret, upToken, key, bytes.NewReader(firstPart), int64(len(firstPart)), nil)
		})
	}

	return uploader.StreamUpload(ctx, ret, upToken, key, io.MultiReader(bytes.NewReader(firstPart), bufReader),
		func(partIdx int, etag string) {
			elog.Info("upload", key, "callback", "part:", partIdx, "etag:", etag)
		})
}

func (p *Uploader) UploadWithDataChan(key string, dataCh chan q.PartData, ret interface{}, initNotify func(suggestedPartSize int64)) error {
	return p.UploadWithDataChanWithContext(context.Background(), key, dataCh, ret, initNotify)
}

func (p *Uploader) UploadWithDataChanWithContext(ctx context.Context, key string, dataCh chan q.PartData, ret interface{}, initNotify func(suggestedPartSize int64)) error {
	t := time.Now()
	defer func() {
		elog.Info("upload file:", key, "time:", time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope:   p.bucket + ":" + key,
		Expires: 3600*24 + uint32(time.Now().Unix()),
	}
	upToken := p.makeUptoken(&policy)

	var uploader = q.NewUploader(1, &q.UploadConfig{
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.transport,
		HostSelector:   p.upSelector,
	})

	return uploader.UploadWithDataChan(ctx, ret, upToken, key, dataCh, nil, initNotify,
		func(partIdx int, etag string) {
			elog.Info("upload", key, "callback", "part:", partIdx, "etag:", etag)
		})
}

func NewUploader(c *Config) *Uploader {
	mac := qbox.NewMac(c.Ak, c.Sk)
	partSize := c.PartSize * 1024 * 1024
	if partSize < 4*1024*1024 {
		partSize = 4 * 1024 * 1024
	}
	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	uploader := &Uploader{
		bucket:        c.Bucket,
		credentials:   mac,
		partSize:      partSize,
		upConcurrency: c.UpConcurrency,
		queryer:       queryer,
		tries:         c.Retry,
		transport:     newTransport(time.Duration(c.DialTimeoutMs) * time.Millisecond),
	}
	update := func() []string {
		if uploader.queryer != nil {
			return uploader.queryer.QueryUpHosts(false)
		}
		return nil
	}
	uploader.upSelector = NewHostSelector(dupStrings(c.UpHosts), update, 0, time.Duration(c.PunishTimeS)*time.Second, 0, -1, shouldRetry)

	if uploader.tries <= 0 {
		uploader.tries = 5
	}

	return uploader
}

func NewUploaderV2() *Uploader {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewUploader(c)
}

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

type readerAtNopCloser struct {
	io.ReaderAt
}

func (readerAtNopCloser) Close() error { return nil }

// newReaderAtNopCloser returns a readerAtCloser with a no-op Close method wrapping
// the provided ReaderAt r.
func newReaderAtNopCloser(r io.ReaderAt) readerAtCloser {
	return readerAtNopCloser{r}
}
