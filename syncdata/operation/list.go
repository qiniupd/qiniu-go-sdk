package operation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/qiniupd/qiniu-go-sdk/api.v8/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v8/kodo"
)

type Lister struct {
	bucket      string
	upHosts     []string
	rsSelector  *HostSelector
	rsfSelector *HostSelector
	credentials *qbox.Mac
	queryer     *Queryer
	tries       int
	transport   http.RoundTripper
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
		elog.Error(err)
		return nil
	}
	return l.ListStat(fl)
}

func (l *Lister) retryRs(f func(host string) error) (err error) {
	for i := 0; i < l.tries; i++ {
		host := l.rsSelector.SelectHost()
		err = f(host)
		if err != nil {
			l.rsSelector.PunishIfNeeded(host, err)
			elog.Warn("rs try failed. punish host", host, i, err)
			if shouldRetry(err) {
				continue
			}
		} else {
			l.rsSelector.Reward(host)
		}
		break
	}
	return err
}

func (l *Lister) retryRsf(f func(host string) error) (err error) {
	for i := 0; i < l.tries; i++ {
		host := l.rsfSelector.SelectHost()
		err = f(host)
		if err != nil {
			l.rsfSelector.PunishIfNeeded(host, err)
			elog.Warn("rsf try failed. punish host", host, i, err)
			if shouldRetry(err) {
				continue
			}
		} else {
			l.rsfSelector.Reward(host)
		}
		break
	}
	return err
}

func (l *Lister) Stat(key string) (kodo.Entry, error) {
	return l.StatWithContext(context.Background(), key)
}

func (l *Lister) StatWithContext(ctx context.Context, key string) (entry kodo.Entry, err error) {
	l.retryRs(func(host string) error {
		bucket := l.newBucket(host, "")
		entry, err = bucket.Stat(ctx, key)
		return err
	})
	return
}

func (l *Lister) Rename(fromKey, toKey string) error {
	return l.RenameWithContext(context.Background(), fromKey, toKey)
}

func (l *Lister) RenameWithContext(ctx context.Context, fromKey, toKey string) (err error) {
	l.retryRs(func(host string) error {
		bucket := l.newBucket(host, "")
		err = bucket.Move(ctx, fromKey, toKey)
		return err
	})
	return
}

func (l *Lister) MoveTo(fromKey, toBucket, toKey string) error {
	return l.MoveToWithContext(context.Background(), fromKey, toBucket, toKey)
}

func (l *Lister) MoveToWithContext(ctx context.Context, fromKey, toBucket, toKey string) (err error) {
	l.retryRs(func(host string) error {
		bucket := l.newBucket(host, "")
		err = bucket.MoveEx(ctx, fromKey, toBucket, toKey)
		return err
	})
	return
}

func (l *Lister) Copy(fromKey, toKey string) error {
	return l.CopyWithContext(context.Background(), fromKey, toKey)
}

func (l *Lister) CopyWithContext(ctx context.Context, fromKey, toKey string) (err error) {
	l.retryRs(func(host string) error {
		bucket := l.newBucket(host, "")
		err = bucket.Copy(ctx, fromKey, toKey)
		return err
	})
	return
}

func (l *Lister) Delete(key string) error {
	return l.DeleteWithContext(context.Background(), key)
}

func (l *Lister) DeleteWithContext(ctx context.Context, key string) (err error) {
	l.retryRs(func(host string) error {
		bucket := l.newBucket(host, "")
		err = bucket.Delete(ctx, key)
		return err
	})
	return
}

func (l *Lister) ListStat(paths []string) []*FileStat {
	stats, _ := l.ListStatWithContext(context.Background(), paths)
	return stats
}

func (l *Lister) ListStatWithContext(ctx context.Context, paths []string) (stats []*FileStat, anyError error) {
	stats = make([]*FileStat, 0, len(paths))
	for i := 0; i < len(paths); i += 1000 {
		size := 1000
		if size > len(paths)-i {
			size = len(paths) - i
		}
		array := paths[i : i+size]
		err := l.retryRs(func(host string) error {
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

func (l *Lister) ListPrefixWithMarkerAndContext(ctx context.Context, prefix, marker string, limit int) (entries []kodo.ListItem, nextContinuousToken string, err error) {
	l.retryRsf(func(host string) error {
		bucket := l.newBucket("", host)
		entries, nextContinuousToken, err = bucket.List(ctx, prefix, marker, limit)
		if err == io.EOF {
			return nil
		}
		return err
	})
	return
}

func NewLister(c *Config) *Lister {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	lister := Lister{
		bucket:      c.Bucket,
		upHosts:     dupStrings(c.UpHosts),
		credentials: mac,
		queryer:     queryer,
		tries:       c.Retry,
		transport: newTransport(
			time.Duration(c.DialTimeoutMs)*time.Millisecond,
			time.Duration(c.LowSpeedTimeS)*time.Second,
			c.BaseLowSpeedLimit),
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
