package operation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/dot"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
)

const (
	APINameStatFile       APIName = "rs_stat"
	APINameMoveFile       APIName = "rs_move"
	APINameCopyFile       APIName = "rs_copy"
	APINameDeleteFile     APIName = "rs_delete"
	APINameBatchStatFiles APIName = "rs_batch_stat"
	APINameListFiles      APIName = "rsf_list"
	APINameStat           APIName = "stat"
	APINameRename         APIName = "rename"
	APINameMoveTo         APIName = "move_to"
	APINameCopy           APIName = "copy"
	APINameDelete         APIName = "delete"
	APINameListStat       APIName = "list_stat"
	APINameListPrefix     APIName = "list_prefix"
)

type Lister struct {
	bucket      string
	upHosts     []string
	rsSelector  *HostSelector
	rsfSelector *HostSelector
	dotter      *Dotter
	credentials *qbox.Mac
	queryer     *Queryer
	tries       int
	transport   http.RoundTripper
}

type FileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func (l *Lister) withDot(apiName dot.APIName, f func() error) (err error) {
	beginAt := time.Now()
	err = f()
	l.dotter.Dot(dot.SDKDotType, apiName, err == nil, time.Since(beginAt))
	return
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

func (l *Lister) retryRs(apiName dot.APIName, f func(host string) error) (err error) {
	for i := 0; i < l.tries; i++ {
		host := l.rsSelector.SelectHost()
		beginAt := time.Now()
		err = f(host)
		elapsedDuration := time.Since(beginAt)
		if err != nil {
			if l.rsSelector.PunishIfNeeded(host, err) {
				elog.Warn("rs try failed. punish host", host, i, err)
				l.dotter.Dot(dot.HTTPDotType, apiName, false, elapsedDuration)
			} else {
				elog.Warn("rs try failed but not punish host", host, i, err)
				l.dotter.Dot(dot.HTTPDotType, apiName, true, elapsedDuration)
			}
			if shouldRetry(err) {
				continue
			}
		} else {
			l.rsSelector.Reward(host)
			l.dotter.Dot(dot.HTTPDotType, apiName, true, elapsedDuration)
		}
		break
	}
	return err
}

func (l *Lister) retryRsf(apiName dot.APIName, f func(host string) error) (err error) {
	for i := 0; i < l.tries; i++ {
		host := l.rsfSelector.SelectHost()
		beginAt := time.Now()
		err = f(host)
		elapsedDuration := time.Since(beginAt)
		if err != nil {
			if l.rsfSelector.PunishIfNeeded(host, err) {
				elog.Warn("rsf try failed. punish host", host, i, err)
				l.dotter.Dot(dot.HTTPDotType, apiName, false, elapsedDuration)
			} else {
				elog.Warn("rsf try failed but not punish host", host, i, err)
				l.dotter.Dot(dot.HTTPDotType, apiName, true, elapsedDuration)
			}
			if shouldRetry(err) {
				continue
			}
		} else {
			l.rsfSelector.Reward(host)
			l.dotter.Dot(dot.HTTPDotType, apiName, true, elapsedDuration)
		}
		break
	}
	return err
}

func (l *Lister) Stat(key string) (kodo.Entry, error) {
	return l.StatWithContext(context.Background(), key)
}

func (l *Lister) StatWithContext(ctx context.Context, key string) (kodo.Entry, error) {
	var entry kodo.Entry
	err := l.withDot(APINameStat, func() error {
		return l.retryRs(APINameStatFile, func(host string) error {
			var err error
			bucket := l.newBucket(host, "")
			entry, err = bucket.Stat(ctx, key)
			return err
		})
	})
	return entry, err
}

func (l *Lister) Rename(fromKey, toKey string) error {
	return l.RenameWithContext(context.Background(), fromKey, toKey)
}

func (l *Lister) RenameWithContext(ctx context.Context, fromKey, toKey string) error {
	return l.withDot(APINameRename, func() error {
		return l.retryRs(APINameMoveFile, func(host string) error {
			bucket := l.newBucket(host, "")
			return bucket.Move(ctx, fromKey, toKey)
		})
	})
}

func (l *Lister) MoveTo(fromKey, toBucket, toKey string) error {
	return l.MoveToWithContext(context.Background(), fromKey, toBucket, toKey)
}

func (l *Lister) MoveToWithContext(ctx context.Context, fromKey, toBucket, toKey string) error {
	return l.withDot(APINameMoveTo, func() error {
		return l.retryRs(APINameMoveFile, func(host string) error {
			bucket := l.newBucket(host, "")
			return bucket.MoveEx(ctx, fromKey, toBucket, toKey)
		})
	})
}

func (l *Lister) Copy(fromKey, toKey string) error {
	return l.CopyWithContext(context.Background(), fromKey, toKey)
}

func (l *Lister) CopyWithContext(ctx context.Context, fromKey, toKey string) error {
	return l.withDot(APINameCopy, func() error {
		return l.retryRs(APINameCopyFile, func(host string) error {
			bucket := l.newBucket(host, "")
			return bucket.Copy(ctx, fromKey, toKey)
		})
	})
}

func (l *Lister) Delete(key string) error {
	return l.DeleteWithContext(context.Background(), key)
}

func (l *Lister) DeleteWithContext(ctx context.Context, key string) error {
	return l.withDot(APINameDelete, func() error {
		return l.retryRs(APINameDeleteFile, func(host string) error {
			bucket := l.newBucket(host, "")
			return bucket.Delete(ctx, key)
		})
	})
}

func (l *Lister) ListStat(paths []string) []*FileStat {
	stats, _ := l.ListStatWithContext(context.Background(), paths)
	return stats
}

func (l *Lister) ListStatWithContext(ctx context.Context, paths []string) (stats []*FileStat, anyError error) {
	beginAt := time.Now()
	stats = make([]*FileStat, 0, len(paths))
	for i := 0; i < len(paths); i += 1000 {
		size := 1000
		if size > len(paths)-i {
			size = len(paths) - i
		}
		array := paths[i : i+size]
		err := l.retryRs(APINameBatchStatFiles, func(host string) error {
			bucket := l.newBucket(host, "")
			r, err := bucket.BatchStat(ctx, array...)
			if err != nil {
				return err
			}
			for j, v := range r {
				if v.Code != 200 {
					stats = append(stats, &FileStat{
						Name: array[j],
						Size: -1,
					})
					elog.Warn("stat bad file:", array[j], "with code:", v.Code)
				} else {
					stats = append(stats, &FileStat{
						Name: array[j],
						Size: v.Data.Fsize,
					})
				}
			}
			return nil
		})
		if err != nil {
			for range array {
				stats = append(stats, nil)
			}
			if anyError == nil {
				anyError = err
			}
		}
	}
	l.dotter.Dot(dot.SDKDotType, APINameListStat, anyError == nil, time.Since(beginAt))
	return
}

func (l *Lister) ListPrefix(prefix string) []string {
	return l.ListPrefixWithContext(context.Background(), prefix)
}

func (l *Lister) ListPrefixWithContext(ctx context.Context, prefix string) (keys []string) {
	var marker string
	for {
		entries, nextContinuousToken, err := l.ListPrefixWithMarkerAndContext(ctx, prefix, marker, 1000)
		if err != nil && err != io.EOF {
			return nil
		}
		elog.Info("list prefix:", prefix, "marker:", marker, "len:", len(entries))
		for _, entry := range entries {
			keys = append(keys, entry.Key)
		}
		if nextContinuousToken == "" {
			break
		}
		marker = nextContinuousToken
	}
	return
}

func (l *Lister) ListPrefixWithMarker(prefix, marker string, limit int) ([]kodo.ListItem, string, error) {
	return l.ListPrefixWithMarkerAndContext(context.Background(), prefix, marker, limit)
}

func (l *Lister) ListPrefixWithMarkerAndContext(ctx context.Context, prefix, marker string, limit int) ([]kodo.ListItem, string, error) {
	var (
		entries             []kodo.ListItem
		nextContinuousToken string
	)
	err := l.withDot(APINameListPrefix, func() error {
		return l.retryRsf(APINameListFiles, func(host string) error {
			var err error
			bucket := l.newBucket("", host)
			entries, nextContinuousToken, err = bucket.List(ctx, prefix, marker, limit)
			if err == io.EOF {
				err = nil
			}
			return err
		})
	})
	return entries, nextContinuousToken, err
}

func NewLister(c *Config) *Lister {
	mac := qbox.NewMac(c.Ak, c.Sk)
	dotter, _ := NewDotter(c)
	lister := Lister{
		bucket:      c.Bucket,
		upHosts:     dupStrings(c.UpHosts),
		credentials: mac,
		queryer:     NewQueryer(c),
		tries:       c.Retry,
		transport:   newTransport(time.Duration(c.DialTimeoutMs)*time.Millisecond, 5*time.Second),
		dotter:      dotter,
	}
	updateRs := func() []string {
		if lister.queryer != nil {
			return lister.queryer.QueryRsHosts(false)
		}
		return nil
	}
	lister.rsSelector = NewHostSelector(dupStrings(c.RsHosts), updateRs, 0, time.Duration(c.PunishTimeS)*time.Second, 0, -1, shouldRetry)
	updateRsf := func() []string {
		if lister.queryer != nil {
			return lister.queryer.QueryRsfHosts(false)
		}
		return nil
	}
	lister.rsfSelector = NewHostSelector(dupStrings(c.RsfHosts), updateRsf, 0, time.Duration(c.PunishTimeS)*time.Second, 0, -1, shouldRetry)

	if lister.tries <= 0 {
		lister.tries = 5
	}
	return &lister
}

func NewListerV2() *Lister {
	c := getConf()
	if c == nil {
		return nil
	}
	return NewLister(c)
}

func (l *Lister) newBucket(rsHost, rsfHost string) kodo.Bucket {
	cfg := kodo.Config{
		AccessKey: l.credentials.AccessKey,
		SecretKey: string(l.credentials.SecretKey),
		RSHost:    rsHost,
		RSFHost:   rsfHost,
		UpHosts:   l.upHosts,
		Transport: l.transport,
	}
	client := kodo.NewWithoutZone(&cfg)
	return client.Bucket(l.bucket)
}
