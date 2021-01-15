package main

import (
	"flag"
	"log"
	"os"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
)

func main() {
	cf := flag.String("c", "cfg.toml", "config")
	f := flag.String("f", "file", "upload file")
	flag.Parse()

	x, err := operation.Load(*cf)
	if err != nil {
		log.Fatalln(err)
	}

	up := operation.NewUploader(x)

	file, err := os.Open(*f)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	err = up.UploadReader(file, *f)
	if err != nil {
		log.Fatalln(err)
	}
}
