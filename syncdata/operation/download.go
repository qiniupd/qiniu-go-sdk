package operation

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
)

type Downloader struct {
	bucket      string
	ioHosts     []string
	credentials *qbox.Mac
	uid         uint64
}

func NewDownloader(c *Config) *Downloader {
	mac := qbox.NewMac(c.Ak, c.Sk)
	return &Downloader{
		bucket:      c.Bucket,
		ioHosts:     dupStrings(c.IoHosts),
		credentials: mac,
		uid:         c.Uid,
	}
}
func NewDownloaderV2() *Downloader {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewDownloader(c)
}

func (d *Downloader) DownloadFile(key, path string) (f *os.File, err error) {
	for i := 0; i < 3; i++ {
		f, err = d.downloadFileInner(key, path)
		if err == nil {
			return
		}
	}
	return
}

func (d *Downloader) DownloadBytes(key string) (data []byte, err error) {
	for i := 0; i < 3; i++ {
		data, err = d.downloadBytesInner(key)
		if err == nil {
			break
		}
	}
	return
}

func (d *Downloader) LastBytes(key, path string) (data []byte, err error) {
	for i := 0; i < 3; i++ {
		data, err = d.downloadBytesInner(key)
		if err == nil {
			break
		}
	}
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

func (d *Downloader) nextHost() string {
	return d.ioHosts[randomNext()%uint32(len(d.ioHosts))]
}

func (d *Downloader) downloadFileInner(key, path string) (*os.File, error) {
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
	host := d.nextHost()

	fmt.Println("remote path", key)
	url := fmt.Sprintf("%s/getfile/%d/%s/%s", host, d.uid, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "")
	if length != 0 {
		r := fmt.Sprintf("bytes=%d-", length)
		req.Header.Set("Range", r)
		fmt.Println("continue download")
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return f, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		return nil, errors.New(response.Status)
	}
	ctLength := response.ContentLength
	n, err := io.Copy(f, response.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		log.Println("download length not equal", ctLength, n)
	}
	f.Seek(0, io.SeekStart)
	return f, nil
}

func (d *Downloader) downloadBytesInner(key string) ([]byte, error) {
	if strings.HasPrefix(key, "/") {
		key = strings.TrimPrefix(key, "/")
	}
	host := d.nextHost()

	url := fmt.Sprintf("%s/getfile/%d/%s/%s", host, d.uid, d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, errors.New(response.Status)
	}
	return ioutil.ReadAll(response.Body)
}