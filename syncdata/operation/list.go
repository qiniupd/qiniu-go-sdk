package operation

import (
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"

	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v7/kodo"
)

// 列举器
type Lister struct {
	bucket           string
	rsHosts          []string
	upHosts          []string
	rsfHosts         []string
	credentials      *qbox.Mac
	queryer          *Queryer
	batchSize        int
	batchConcurrency int
}

// 文件元信息
type FileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (l *Lister) batchStat(r io.Reader) []*FileStat {
	j := json.NewDecoder(r)
	var fl []string
	err := j.Decode(&fl)
	if err != nil {
		elog.Error(err)
		return nil
	}
	return l.ListStat(fl)
}

var curRsHostIndex uint32 = 0

func (l *Lister) nextRsHost() string {
	rsHosts := l.rsHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryRsHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			rsHosts = hosts
		}
	}
	switch len(rsHosts) {
	case 0:
		panic("No Rs hosts is configured")
	case 1:
		return rsHosts[0]
	default:
		var rsHost string
		for i := 0; i <= len(rsHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curRsHostIndex, 1) - 1)
			rsHost = rsHosts[index%len(rsHosts)]
			if isHostNameValid(rsHost) {
				break
			}
		}
		return rsHost
	}
}

var curRsfHostIndex uint32 = 0

func (l *Lister) nextRsfHost() string {
	rsfHosts := l.rsfHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryRsfHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			rsfHosts = hosts
		}
	}
	switch len(rsfHosts) {
	case 0:
		panic("No Rsf hosts is configured")
	case 1:
		return rsfHosts[0]
	default:
		var rsfHost string
		for i := 0; i <= len(rsfHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curRsfHostIndex, 1) - 1)
			rsfHost = rsfHosts[index%len(rsfHosts)]
			if isHostNameValid(rsfHost) {
				break
			}
		}
		return rsfHost
	}
}

// 重命名对象
func (l *Lister) Rename(fromKey, toKey string) error {
	host := l.nextRsHost()
	bucket := l.newBucket(host, "")
	err := bucket.Move(nil, fromKey, toKey)
	if err != nil {
		failHostName(host)
		elog.Info("rename retry 0", host, err)
		host = l.nextRsHost()
		bucket = l.newBucket(host, "")
		err = bucket.Move(nil, fromKey, toKey)
		if err != nil {
			failHostName(host)
			elog.Info("rename retry 1", host, err)
			return err
		} else {
			succeedHostName(host)
		}
	} else {
		succeedHostName(host)
	}
	return nil
}

// 移动对象到指定存储空间的指定对象中
func (l *Lister) MoveTo(fromKey, toBucket, toKey string) error {
	host := l.nextRsHost()
	bucket := l.newBucket(host, "")
	err := bucket.MoveEx(nil, fromKey, toBucket, toKey)
	if err != nil {
		failHostName(host)
		elog.Info("move retry 0", host, err)
		host = l.nextRsHost()
		bucket = l.newBucket(host, "")
		err = bucket.MoveEx(nil, fromKey, toBucket, toKey)
		if err != nil {
			failHostName(host)
			elog.Info("move retry 1", host, err)
			return err
		} else {
			succeedHostName(host)
		}
	} else {
		succeedHostName(host)
	}
	return nil
}

// 复制对象到当前存储空间的指定对象中
func (l *Lister) Copy(fromKey, toKey string) error {
	host := l.nextRsHost()
	bucket := l.newBucket(host, "")
	err := bucket.Copy(nil, fromKey, toKey)
	if err != nil {
		failHostName(host)
		elog.Info("copy retry 0", host, err)
		host = l.nextRsHost()
		bucket = l.newBucket(host, "")
		err = bucket.Copy(nil, fromKey, toKey)
		if err != nil {
			failHostName(host)
			elog.Info("copy retry 1", host, err)
			return err
		} else {
			succeedHostName(host)
		}
	} else {
		succeedHostName(host)
	}
	return nil
}

// 删除指定对象
func (l *Lister) Delete(key string) error {
	host := l.nextRsHost()
	bucket := l.newBucket(host, "")
	err := bucket.Delete(nil, key)
	if err != nil {
		failHostName(host)
		elog.Info("delete retry 0", host, err)
		host = l.nextRsHost()
		bucket = l.newBucket(host, "")
		err = bucket.Delete(nil, key)
		if err != nil {
			failHostName(host)
			elog.Info("delete retry 1", host, err)
			return err
		} else {
			succeedHostName(host)
		}
	} else {
		succeedHostName(host)
	}
	return nil
}

// 获取指定对象列表的元信息
func (l *Lister) ListStat(paths []string) []*FileStat {
	type PathWithIdx struct {
		paths []string
		index int
	}

	concurrency := (len(paths) + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}

	var (
		wg       sync.WaitGroup
		c        = make(chan PathWithIdx)
		stats    = make([]*FileStat, len(paths))
		finalErr error
		lock     sync.Mutex
	)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for pi := range c {
				host := l.nextRsHost()
				bucket := l.newBucket(host, "")
				r, err := bucket.BatchStat(nil, pi.paths...)
				if err != nil {
					failHostName(host)
					elog.Info("batchStat retry 0", host, err)
					host = l.nextRsHost()
					bucket = l.newBucket(host, "")
					r, err = bucket.BatchStat(nil, pi.paths...)
					if err != nil {
						failHostName(host)
						elog.Info("batchStat retry 1", host, err)
						lock.Lock()
						finalErr = err
						lock.Unlock()
						return
					} else {
						succeedHostName(host)
					}
				} else {
					succeedHostName(host)
				}
				lock.Lock()
				for j, v := range r {
					if v.Code != 200 {
						stats[pi.index+j] = &FileStat{Name: pi.paths[j], Size: -1}
						elog.Warn("stat bad file:", pi.paths[j], "with code:", v.Code)
					} else {
						stats[pi.index+j] = &FileStat{Name: pi.paths[j], Size: v.Data.Fsize}
					}
				}
				lock.Unlock()
			}
		}()
	}

	for i := 0; i < len(paths); i += l.batchSize {
		size := l.batchSize
		if size > len(paths)-i {
			size = len(paths) - i
		}
		c <- PathWithIdx{paths: paths[i : i+size], index: i}
	}
	close(c)
	wg.Wait()
	if finalErr != nil {
		return []*FileStat{}
	} else {
		return stats
	}
}

// 根据前缀列举存储空间
func (l *Lister) ListPrefix(prefix string) []string {
	rsHost := l.nextRsHost()
	rsfHost := l.nextRsfHost()
	bucket := l.newBucket(rsHost, rsfHost)
	var files []string
	marker := ""
	for {
		r, _, out, err := bucket.List(nil, prefix, "", marker, 1000)
		if err != nil && err != io.EOF {
			failHostName(rsfHost)
			elog.Info("ListPrefix retry 0", rsfHost, err)
			rsfHost = l.nextRsfHost()
			bucket = l.newBucket(rsHost, rsfHost)
			r, _, out, err = bucket.List(nil, prefix, "", "", 1000)
			if err != nil {
				failHostName(rsfHost)
				elog.Info("ListPrefix retry 1", rsfHost, err)
				return []string{}
			} else {
				succeedHostName(rsfHost)
			}
		} else {
			succeedHostName(rsfHost)
		}
		elog.Info("list len", marker, len(r))
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

// 根据配置创建列举器
func NewLister(c *Config) *Lister {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	lister := Lister{
		bucket:           c.Bucket,
		rsHosts:          dupStrings(c.RsHosts),
		upHosts:          dupStrings(c.UpHosts),
		rsfHosts:         dupStrings(c.RsfHosts),
		credentials:      mac,
		queryer:          queryer,
		batchConcurrency: c.BatchConcurrency,
		batchSize:        c.BatchSize,
	}
	if lister.batchConcurrency <= 0 {
		lister.batchConcurrency = 20
	}
	if lister.batchSize <= 0 {
		lister.batchSize = 100
	}
	shuffleHosts(lister.rsHosts)
	shuffleHosts(lister.rsfHosts)
	shuffleHosts(lister.upHosts)
	return &lister
}

// 根据环境变量创建列举器
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
	return client.Bucket(l.bucket)
}
