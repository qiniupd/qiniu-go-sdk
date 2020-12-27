package operation

import (
	"encoding/json"
	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v7/kodo"
	"io"
	"math/rand"
	"os"
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
	rsfHosts    []string
	credentials *qbox.Mac
	queryer     *Queryer
}

type FileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (l *Lister) batchStat(r io.Reader) []*FileStat {
	j := json.NewDecoder(r)
	var fl []string
	err := j.Decode(&fl)
	if err != nil {
		elog.Println("ERROR", err)
		return nil
	}
	return l.ListStat(fl)
}

func (l *Lister) nextRsHost() string {
	rsHosts := l.rsHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryRsHosts(false); len(hosts) > 0 {
			rsHosts = hosts
		}
	}
	return rsHosts[randomNext()%uint32(len(rsHosts))]
}

func (l *Lister) nextRsfHost() string {
	rsfHosts := l.rsfHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryRsHosts(false); len(hosts) > 0 {
			rsfHosts = hosts
		}
	}
	return rsfHosts[randomNext()%uint32(len(rsfHosts))]
}

func (l *Lister) Delete(key string) error {
	host := l.nextRsHost()
	bucket := l.newBucket(host, "")
	err := bucket.Delete(nil, key)
	if err != nil {
		elog.Println("INFO", "delete retry 0", host, err)
		host = l.nextRsHost()
		bucket = l.newBucket(host, "")
		err = bucket.Delete(nil, key)
		if err != nil {
			elog.Println("INFO", "batchStat retry 1", host, err)
			return err
		}
	}
	return nil
}

func (l *Lister) ListStat(paths []string) []*FileStat {
	host := l.nextRsHost()
	bucket := l.newBucket(host, "")
	var stats []*FileStat
	for i := 0; i < len(paths); i += 1000 {
		size := 1000
		if size > len(paths)-i {
			size = len(paths) - i
		}
		array := paths[i : i+size]
		r, err := bucket.BatchStat(nil, array...)
		if err != nil {
			elog.Println("INFO", "batchStat retry 0", host, err)
			host = l.nextRsHost()
			bucket = l.newBucket(host, "")
			r, err = bucket.BatchStat(nil, paths[i:i+size]...)
			if err != nil {
				elog.Println("INFO", "batchStat retry 1", host, err)
				return []*FileStat{}
			}
		}
		for j, v := range r {
			if v.Code != 200 {
				stats = append(stats, &FileStat{
					Name: array[j],
					Size: -1,
				})
				elog.Println("WARN", "bad file", array[j])
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

func (l *Lister) ListPrefix(prefix string) []string {
	rsHost := l.nextRsHost()
	rsfHost := l.nextRsfHost()
	bucket := l.newBucket(rsHost, rsfHost)
	var files []string
	marker := ""
	for {
		r, _, out, err := bucket.List(nil, prefix, "", marker, 1000)
		if err != nil && err != io.EOF {
			elog.Println("INFO", "ListPrefix retry 0", rsfHost, err)
			rsfHost = l.nextRsfHost()
			bucket = l.newBucket(rsHost, rsfHost)
			r, _, out, err = bucket.List(nil, prefix, "", "", 1000)
			if err != nil {
				elog.Println("INFO", "ListPrefix retry 1", rsfHost, err)
				return []string{}
			}
		}
		elog.Println("INFO", "list len", marker, len(r))
		for _, v := range r {
			files = append(files, v.Key)
		}

		if out == "" {
			break
		}
		marker = out
	}
	return files
}

func NewLister(c *Config) *Lister {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	return &Lister{
		bucket:      c.Bucket,
		rsHosts:     dupStrings(c.RsHosts),
		upHosts:     dupStrings(c.UpHosts),
		rsfHosts:    dupStrings(c.RsfHosts),
		credentials: mac,
		queryer:     queryer,
	}
}

func NewListerV2() *Lister {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewLister(c)
}

func (l *Lister) newBucket(host, rsfHost string) kodo.Bucket {
	cfg := kodo.Config{
		AccessKey: l.credentials.AccessKey,
		SecretKey: string(l.credentials.SecretKey),
		RSHost:    host,
		RSFHost:   rsfHost,
		UpHosts:   l.upHosts,
	}
	client := kodo.NewWithoutZone(&cfg)
	b, err := client.BucketWithSafe(l.bucket)
	if err != nil {
		elog.Println("ERROR", "Get Bucket(%s) failed: %+v", l.bucket, err)
	}
	return b
}
