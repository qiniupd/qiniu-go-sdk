package kodocli

import (
	"log"
	"os"
)

type Ilog interface {
	Println(v ...interface{})
}

// elog is embedded logger
var elog Ilog

func SetLogger(logger Ilog) {
	elog = logger
}

func init() {
	if elog == nil {
		elog = log.New(os.Stderr, "", log.Lshortfile|log.LstdFlags)
	}
}
