package operation

import (
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodocli"
	"log"
	"os"
)

// elog is embedded logger
var elog *log.Logger

func SetLogger(logger *log.Logger) {
	elog = logger
	kodocli.SetLogger(logger)
}

func init() {
	if elog == nil {
		elog = log.New(os.Stderr, "", log.Lshortfile|log.LstdFlags)
	}
}
