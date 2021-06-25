package operation

import (
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodocli"
)

// elog is embedded logger
var elog kodocli.Ilog

// 设置全局 Logger
func SetLogger(logger kodocli.Ilog) {
	elog = logger
	kodocli.SetLogger(logger)
}

func init() {
	if elog == nil {
		elog = kodocli.NewLogger()
	}
}
