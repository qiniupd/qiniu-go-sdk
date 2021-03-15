package kodocli

import (
	"net/http"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/conf"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/dot"
	"github.com/qiniupd/qiniu-go-sdk/x/rpc.v7"
	"github.com/qiniupd/qiniu-go-sdk/x/url.v7"
)

// ----------------------------------------------------------

type zoneConfig struct {
	UpHosts []string
}

var zones = []zoneConfig{
	// z0:
	{
		UpHosts: []string{
			"http://upload.qiniu.com",
			"http://up.qiniu.com",
			"-H up.qiniu.com http://183.136.139.16",
		},
	},
	// z1:
	{
		UpHosts: []string{
			"http://upload-z1.qiniu.com",
			"http://up-z1.qiniu.com",
			"-H up-z1.qiniu.com http://106.38.227.27",
		},
	},
}

// ----------------------------------------------------------

type UploadConfig struct {
	UpHosts        []string
	Transport      http.RoundTripper
	UploadPartSize int64
	Concurrency    int
	UseBuffer      bool
	HostSelector   IHostSelector
	Dotter         dot.IDotter
}

type Uploader struct {
	Conn           rpc.Client
	UploadPartSize int64
	Concurrency    int
	UseBuffer      bool
	HostSelector   IHostSelector
	Dotter         dot.IDotter
}

func NewUploader(zone int, cfg *UploadConfig) (p Uploader) {

	var uc UploadConfig
	if cfg != nil {
		uc = *cfg
	}
	if len(uc.UpHosts) == 0 {
		if zone < 0 || zone >= len(zones) {
			panic("invalid upload config: invalid zone")
		}
		uc.UpHosts = zones[zone].UpHosts
	}

	if uc.UploadPartSize != 0 {
		p.UploadPartSize = uc.UploadPartSize
	} else {
		p.UploadPartSize = minUploadPartSize * 2
	}

	if uc.Concurrency != 0 {
		p.Concurrency = uc.Concurrency
	} else {
		p.Concurrency = 4
	}

	if uc.HostSelector != nil {
		p.HostSelector = uc.HostSelector
	} else {
		p.HostSelector = &DefaultSelector{Hosts: uc.UpHosts}
	}
	p.Dotter = uc.Dotter
	p.UseBuffer = uc.UseBuffer
	p.Conn.Client = &http.Client{Transport: uc.Transport, Timeout: 10 * time.Minute}

	return
}

// ----------------------------------------------------------

// 根据空间(Bucket)的域名，以及文件的 key，获得 baseUrl。
// 如果空间是 public 的，那么通过 baseUrl 可以直接下载文件内容。
// 如果空间是 private 的，那么需要对 baseUrl 进行私有签名得到一个临时有效的 privateUrl 进行下载。
//
func MakeBaseUrl(domain, key string) (baseUrl string) {
	return "http://" + domain + "/" + url.Escape(key)
}

// ----------------------------------------------------------

// 设置使用这个SDK的应用程序名。userApp 必须满足 [A-Za-z0-9_\ \-\.]*
//
func SetAppName(userApp string) error {

	return conf.SetAppName(userApp)
}

// ----------------------------------------------------------
