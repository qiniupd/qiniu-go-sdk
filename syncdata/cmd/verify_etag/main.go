package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/qiniupd/qiniu-go-sdk/syncdata/operation"
)

func main() {
	if len(os.Args) < 2 {
		log.Printf("Usage: %s KEY ...\n", os.Args[0])
		os.Exit(1)
	}

	lister := operation.NewListerV2()
	if lister == nil {
		log.Println("load config file", os.Getenv("QINIU"), "failed")
	}
	downloader := operation.NewDownloaderV2()
	if lister == nil {
		log.Println("load config file", os.Getenv("QINIU"), "failed")
	}

	for _, key := range os.Args[1:] {
		verifyEtagOfKey(key, lister, downloader)
	}
}

func verifyEtagOfKey(key string, lister *operation.Lister, downloader *operation.Downloader) {
	fileInfo, err := lister.Stat(key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "key: %s, error: %s", key, err)
		return
	}

	tempFile, err := ioutil.TempFile(os.Getenv("TMPDIR"), "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "key: %s, error: %s", key, err)
		return
	}
	defer os.Remove(tempFile.Name())
	if err = tempFile.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "key: %s, error: %s", key, err)
		return
	}

	tempFile, err = downloader.DownloadFile(key, tempFile.Name())
	if err != nil {
		fmt.Fprintf(os.Stderr, "key: %s, error: %s", key, err)
		return
	}
	defer tempFile.Close()

	if len(fileInfo.Parts) == 0 {
		etagResult, err := operation.EtagV1(tempFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "key: %s, error: %s", key, err)
			return
		}
		fmt.Printf("key: %s, version: v1, etag from rs: %s, etag from io: %s, match: %v\n", key, fileInfo.Hash, etagResult, fileInfo.Hash == etagResult)
	} else {
		etagResult, err := operation.EtagV2(tempFile, fileInfo.Parts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "key: %s, error: %s", key, err)
			return
		}
		fmt.Printf("key: %s, version: v2, etag from rs: %s, etag from io: %s, match: %v\n", key, fileInfo.Hash, etagResult, fileInfo.Hash == etagResult)
	}
}
