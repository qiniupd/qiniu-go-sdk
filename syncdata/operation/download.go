package operation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
)

type Downloader struct {
	bucket         string
	ioSelector     *HostSelector
	credentials    *qbox.Mac
	queryer        *Queryer
	tries          int
	downloadClient *http.Client
}

func NewDownloader(c *Config) *Downloader {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	downloadClient := &http.Client{
		Transport: NewTransport(c.DialTimeoutMs),
		Timeout:   10 * time.Minute,
	}

	downloader := Downloader{
		bucket:         c.Bucket,
		credentials:    mac,
		queryer:        queryer,
		tries:          c.Retry,
		downloadClient: downloadClient,
	}

	update := func() []string {
		if downloader.queryer != nil {
			return downloader.queryer.QueryIoHosts(false)
		}
		return nil
	}
	downloader.ioSelector = NewHostSelector(dupStrings(c.IoHosts), update, 0, time.Duration(c.PunishTimeS)*time.Second, 0, -1, shouldRetry)

	if downloader.tries <= 0 {
		downloader.tries = 5
	}

	return &downloader
}

func NewDownloaderV2() *Downloader {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewDownloader(c)
}

func (d *Downloader) retry(f func(host string) error) {
	for i := 0; i < d.tries; i++ {
		host := d.ioSelector.SelectHost()
		err := f(host)
		if err != nil {
			d.ioSelector.PunishIfNeeded(host, err)
			elog.Warn("download try failed. punish host", host, i, err)
			if shouldRetry(err) {
				continue
			}
		} else {
			d.ioSelector.Reward(host)
		}
		break
	}
}

func (d *Downloader) DownloadFile(key, path string) (*os.File, error) {
	return d.DownloadFileWithContext(context.Background(), key, path)
}

func (d *Downloader) DownloadFileWithContext(ctx context.Context, key, path string) (f *os.File, err error) {
	d.retry(func(host string) error {
		f, err = d.downloadFileInner(ctx, host, key, path)
		return err
	})
	return
}

func (d *Downloader) DownloadReader(key string) (io.ReadCloser, error) {
	return d.DownloadReaderWithContext(context.Background(), key)
}

func (d *Downloader) DownloadReaderWithContext(ctx context.Context, key string) (r io.ReadCloser, err error) {
	d.retry(func(host string) error {
		r, err = d.downloadReaderInner(ctx, host, key)
		return err
	})
	return
}

func (d *Downloader) DownloadBytes(key string) ([]byte, error) {
	return d.DownloadBytesWithContext(context.Background(), key)
}

func (d *Downloader) DownloadBytesWithContext(ctx context.Context, key string) (data []byte, err error) {
	d.retry(func(host string) error {
		data, err = d.downloadBytesInner(ctx, host, key)
		return err
	})
	return
}

func (d *Downloader) DownloadRangeBytes(key string, offset, size int64) (int64, []byte, error) {
	return d.DownloadRangeBytesWithContext(context.Background(), key, offset, size)
}

func (d *Downloader) DownloadRangeBytesWithContext(ctx context.Context, key string, offset, size int64) (l int64, data []byte, err error) {
	d.retry(func(host string) error {
		l, data, err = d.downloadRangeBytesInner(ctx, host, key, offset, size)
		return err
	})
	return
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func (d *Downloader) downloadFileInner(ctx context.Context, host, key, path string) (*os.File, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	length, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	elog.Debug("downloadFileInner with remote path", key)
	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "")
	if length != 0 {
		r := fmt.Sprintf("bytes=%d-", length)
		req.Header.Set("Range", r)
		elog.Info("continue download", key, "Range", r)
	}

	response, err := d.downloadClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return f, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		if response.Body != nil {
			response.Body.Close()
		}
		return nil, errors.New(response.Status)
	}
	ctLength := response.ContentLength
	n, err := io.Copy(f, response.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		elog.Warn("download", key, "length not equal with ctlength:", ctLength, "actual:", n)
	}
	f.Seek(0, io.SeekStart)
	return f, nil
}

func (d *Downloader) downloadReaderInner(ctx context.Context, host, key string) (io.ReadCloser, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}

	elog.Debug("downloadReaderInner with remote path", key)
	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, url.PathEscape(key))
	reader := urlReader{
		url:    url,
		ctx:    ctx,
		client: d.downloadClient,
		tries:  d.tries,
	}
	if err := reader.sendRequest(); err != nil {
		return nil, err
	} else {
		return &reader, nil
	}
}

type urlReader struct {
	url      string
	ctx      context.Context
	client   *http.Client
	response *http.Response
	closed   bool
	offset   int
	tries    int
}

func (r *urlReader) Read(p []byte) (n int, err error) {
	if r.closed {
		n, err = 0, io.EOF
		return
	}
	for i := 0; i < r.tries; i++ {
		if r.response == nil {
			if err = r.sendRequest(); err != nil {
				return
			}
		}
		if r.response.Body == nil {
			n, err = 0, io.EOF
			return
		}
		n, err = r.response.Body.Read(p)
		if i == r.tries-1 { // Last Retry
			r.offset += n
		}
		if err == nil || err == io.EOF {
			return
		}
		r.response.Body.Close()
		r.response = nil
	}
	return
}

func (r *urlReader) sendRequest() (err error) {
	req, err := http.NewRequestWithContext(r.ctx, "GET", r.url, http.NoBody)
	if err != nil {
		return
	}
	req.Header.Set("Accept-Encoding", "")
	if r.offset != 0 {
		rangeHeader := fmt.Sprintf("bytes=%d-", r.offset)
		req.Header.Set("Range", rangeHeader)
		elog.Info("continue download:", r.url, "from:", r.offset)
	}

	r.response, err = r.client.Do(req)
	if err != nil {
		return
	}
	if r.response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return
	}
	if r.response.StatusCode != http.StatusOK && r.response.StatusCode != http.StatusPartialContent {
		if r.response.Body != nil {
			r.response.Body.Close()
		}
		err = errors.New(r.response.Status)
		return
	}
	return
}

func (r *urlReader) Close() (err error) {
	if r.response != nil {
		err = r.response.Body.Close()
		r.response = nil
	}
	r.closed = true
	return
}

func (d *Downloader) downloadBytesInner(ctx context.Context, host, key string) ([]byte, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	response, err := d.downloadClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, errors.New(response.Status)
	}
	return ioutil.ReadAll(response.Body)
}

func generateRange(offset, size int64) string {
	if offset == -1 {
		return fmt.Sprintf("bytes=-%d", size)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+size)
}

func (d *Downloader) downloadRangeBytesInner(ctx context.Context, host, key string, offset, size int64) (int64, []byte, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.AccessKey, d.bucket, url.PathEscape(key))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return -1, nil, err
	}

	req.Header.Set("Range", generateRange(offset, size))
	response, err := d.downloadClient.Do(req)
	if err != nil {
		return -1, nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusPartialContent {
		return -1, nil, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		return -1, nil, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		return -1, nil, err
	}
	b, err := ioutil.ReadAll(response.Body)
	return l, b, err
}

func getTotalLength(crange string) (int64, error) {
	cr := strings.Split(crange, "/")
	if len(cr) != 2 {
		return -1, errors.New("wrong range " + crange)
	}

	return strconv.ParseInt(cr[1], 10, 64)
}
