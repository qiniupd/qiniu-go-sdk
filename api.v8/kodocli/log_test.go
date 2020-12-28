package kodocli

import (
	"log"
	"testing"
)

func TestElog(t *testing.T) {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	log.Println("aaaaa")
	elog.Info("aaa")
}
