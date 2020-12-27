package kodocli

import (
	"log"
	"os"
)

// elog is embedded logger
var elog *log.Logger

func SetLogger(logger *log.Logger) {
	elog = logger
}

func init() {
	if elog == nil {
		elog = log.New(os.Stderr, "", log.Lshortfile|log.LstdFlags)
	}
}
