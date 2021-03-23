package config

import (
	"net/http"

	"github.com/qiniupd/qiniu-go-sdk/gas/logger"
)

// DefaultAPIPrefix 默认的 API 前缀
const DefaultAPIPrefix = "https://ipfs-api.qiniu.com/gas-api"

// Config 是 SDK 的配置，控制 SDK 的行为
type Config struct {
	MinerID   string            // 矿工号
	AccessKey string            // 用户 AccessKey
	SecretKey string            // 用户 SecretKey
	APIPrefix string            // API 前缀，通过指定 API 前缀控制请求的 server，一般无需配置
	Transport http.RoundTripper // 自定义 HTTP RoundTripper，一般无需配置，默认会使用 http.DefaultTransport
	Logger    logger.Logger     // 自定义的 logger，不提供则不打印信息
}
