package main

import (
	"flag"
	"fmt"
	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
)

func main() {
	cf := flag.String("c", "cfg.toml", "config")
	f := flag.String("f", "file", "upload file")
	flag.Parse()

	x, err := operation.Load(*cf)
	if err != nil {
		fmt.Println(err)
		return
	}

	up := operation.NewUploader(x)
	err = up.Upload(*f, *f)
	if err != nil {
		fmt.Println(err)
	}
}


