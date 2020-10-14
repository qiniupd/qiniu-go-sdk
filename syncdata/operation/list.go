package operation

import (
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"os"
	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v7/kodo"
	"time"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano() | int64(os.Getpid())))

func randomNext() uint32 {
	return random.Uint32()
}

type Lister struct {
	bucket      string
	rsHosts     []string
	upHosts     []string
	credentials *qbox.Mac
}

type FileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (l *Lister) list(r io.Reader) []*FileStat {
	j := json.NewDecoder(r)
	var fl []string
	err := j.Decode(&fl)
	if err != nil {
		log.Println(err)
		return nil
	}
	return l.ListStat(fl)
}

func (l *Lister) nextHost() string {
	return l.rsHosts[randomNext()%uint32(len(l.rsHosts))]
}

func (l *Lister) ListStat(paths []string) []*FileStat {
	host := l.nextHost()
	bucket := l.newBucket(host)
	var stats []*FileStat
	for i := 0; i < len(paths); i += 1000 {
		size := 1000
		if size > len(paths)-i {
			size = len(paths) - i
		}
		array := paths[i : i+size]
		r, err := bucket.BatchStat(nil, array...)
		if err != nil {
			log.Println("list retry 0", host, err)
			host = l.nextHost()
			bucket = l.newBucket(host)
			r, err = bucket.BatchStat(nil, paths[i:i+size]...)
			if err != nil {
				log.Println("list retry 1", host, err)
				return []*FileStat{}
			}
		}
		for j, v := range r {
			if v.Code != 200 {
				stats = append(stats, &FileStat{
					Name: array[j],
					Size: -1,
				})
				log.Println("bad file", array[j])
			} else {
				stats = append(stats, &FileStat{
					Name: array[j],
					Size: v.Data.Fsize,
				})
			}
		}
	}
	return stats
}

func NewLister(c *Config) *Lister {
	mac := qbox.NewMac(c.Ak, c.Sk)
	return &Lister{
		bucket:      c.Bucket,
		rsHosts:     c.RsHosts,
		upHosts:     c.UpHosts,
		credentials: mac,
	}
}

func (l *Lister) newBucket(host string) kodo.Bucket {
	cfg := kodo.Config{
		AccessKey: l.credentials.AccessKey,
		SecretKey: string(l.credentials.SecretKey),
		RSHost:    host,
		UpHosts:   l.upHosts,
	}
	client := kodo.NewWithoutZone(&cfg)
	b := client.Bucket(l.bucket)
	return b
}
