package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kirsle/configdir"
)

var queryClient = &http.Client{
	Transport: newTransport(500*time.Millisecond, 1*time.Second),
	Timeout:   1 * time.Second,
}

var (
	cacheMap         sync.Map
	cacheUpdaterLock sync.Mutex
	cachePersisting  uint32 = 0
	cacheDirectory          = configdir.LocalCache("qiniu", "go-sdk")
)

type (
	Queryer struct {
		ak         string
		bucket     string
		ucSelector *HostSelector
		tries      int
	}

	cache struct {
		CachedHosts    cachedHosts `json:"hosts"`
		CacheExpiredAt time.Time   `json:"expired_at"`
	}

	cachedHosts struct {
		Hosts []cachedHost `json:"hosts"`
	}

	cachedHost struct {
		Ttl int64                `json:"ttl"`
		Io  cachedServiceDomains `json:"io"`
		Up  cachedServiceDomains `json:"up"`
		Rs  cachedServiceDomains `json:"rs"`
		Rsf cachedServiceDomains `json:"rsf"`
	}

	cachedServiceDomains struct {
		Domains []string `json:"domains"`
	}
)

func init() {
	loadQueryersCache()
}

func NewQueryer(c *Config) *Queryer {
	queryer := Queryer{
		ak:         c.Ak,
		bucket:     c.Bucket,
		tries:      c.Retry,
		ucSelector: NewHostSelector(dupStrings(c.UcHosts), nil, 0, time.Duration(c.PunishTimeS)*time.Second, 0, -1, shouldRetry),
	}

	if queryer.tries <= 0 {
		queryer.tries = 10
	}

	return &queryer
}

func (queryer *Queryer) retry(f func(host string) (bool, error)) (err error) {
	var dontRetry bool
	for i := 0; i < queryer.tries; i++ {
		host := queryer.ucSelector.SelectHost()
		dontRetry, err = f(host)
		if err != nil {
			queryer.ucSelector.PunishIfNeeded(host, err)
			elog.Warn("uc try failed. punish host", host, i, err)
			if !dontRetry && shouldRetry(err) {
				continue
			}
		} else {
			queryer.ucSelector.Reward(host)
		}
		break
	}
	return err
}

func (queryer *Queryer) QueryUpHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Up.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) QueryIoHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Io.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) QueryRsHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Rs.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) QueryRsfHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Rsf.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) fromDomainsToUrls(https bool, domains []string) (urls []string) {
	urls = make([]string, len(domains))
	for i, domain := range domains {
		if strings.Contains(domain, "://") {
			urls[i] = domain
		} else if https {
			urls[i] = fmt.Sprintf("https://%s", domain)
		} else {
			urls[i] = fmt.Sprintf("http://%s", domain)
		}
	}
	return urls
}

func (queryer *Queryer) query() (*cache, error) {
	var err error
	c := queryer.getCache()
	if c == nil {
		return func() (*cache, error) {
			var err error
			cacheUpdaterLock.Lock()
			defer cacheUpdaterLock.Unlock()
			c := queryer.getCache()
			if c == nil {
				if c, err = queryer.mustQuery(); err != nil {
					return nil, err
				} else {
					queryer.setCache(c)
					saveQueryersCache()
					return c, nil
				}
			} else {
				return c, nil
			}
		}()
	} else {
		if c.CacheExpiredAt.Before(time.Now()) {
			queryer.asyncRefresh()
		}
		return c, err
	}
}

func (queryer *Queryer) mustQuery() (c *cache, err error) {
	var resp *http.Response
	c = new(cache)

	query := make(url.Values, 2)
	query.Set("ak", queryer.ak)
	query.Set("bucket", queryer.bucket)

	queryer.retry(func(host string) (bool, error) {
		url := fmt.Sprintf("%s/v4/query?%s", host, query.Encode())
		resp, err = queryClient.Get(url)
		if err != nil {
			return false, err
		}
		defer resp.Body.Close()

		if resp.StatusCode/100 != 2 {
			err = fmt.Errorf("uc queryV4 status code error: %d", resp.StatusCode)
			return false, err
		}

		if err = json.NewDecoder(resp.Body).Decode(&c.CachedHosts); err != nil {
			return false, err
		}
		if len(c.CachedHosts.Hosts) == 0 {
			err = errors.New("uc queryV4 returns empty hosts")
			return true, err
		}
		minTTL := c.CachedHosts.Hosts[0].Ttl
		for _, host := range c.CachedHosts.Hosts[1:] { // 取出 Hosts 内最小的 TTL
			if minTTL > host.Ttl {
				minTTL = host.Ttl
			}
		}
		c.CacheExpiredAt = time.Now().Add(time.Duration(minTTL) * time.Second)
		return false, nil
	})

	if err != nil {
		c = nil
	}
	return
}

func (queryer *Queryer) asyncRefresh() {
	go func() {
		var err error

		cacheUpdaterLock.Lock()
		defer cacheUpdaterLock.Unlock()

		c := queryer.getCache()
		if c == nil || c.CacheExpiredAt.Before(time.Now()) {
			if c, err = queryer.mustQuery(); err == nil {
				queryer.setCache(c)
				saveQueryersCache()
			}
		}
	}()
}

func (queryer *Queryer) getCache() *cache {
	value, ok := cacheMap.Load(queryer.cacheKey())
	if !ok {
		return nil
	}
	return value.(*cache)
}

func (queryer *Queryer) setCache(c *cache) {
	cacheMap.Store(queryer.cacheKey(), c)
}

func (queryer *Queryer) cacheKey() string {
	return fmt.Sprintf("%s:%s", queryer.bucket, queryer.ak)
}

func SetCacheDirectoryAndLoad(path string) error {
	cacheDirectory = path
	cacheMap.Range(func(key, _ interface{}) bool {
		cacheMap.Delete(key)
		return true
	})
	return loadQueryersCache()
}

func loadQueryersCache() error {
	cacheFile, err := os.Open(filepath.Join(cacheDirectory, "query-cache.json"))

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer cacheFile.Close()

	m := make(map[string]*cache)
	err = json.NewDecoder(cacheFile).Decode(&m)
	if err != nil {
		return err
	}

	for key, value := range m {
		cacheMap.Store(key, value)
	}
	return nil
}

func saveQueryersCache() error {
	cacheDirInfo, err := os.Stat(cacheDirectory)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err = os.MkdirAll(cacheDirectory, 0700); err != nil {
				return err
			}
		} else {
			return err
		}
	} else if !cacheDirInfo.IsDir() {
		return errors.New("cache directory path is occupied and not directory")
	}

	if !atomic.CompareAndSwapUint32(&cachePersisting, 0, 1) {
		return nil
	}
	defer atomic.StoreUint32(&cachePersisting, 1)

	cacheFile, err := os.OpenFile(filepath.Join(cacheDirectory, "query-cache.json"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer cacheFile.Close()

	m := make(map[string]*cache)
	cacheMap.Range(func(key, value interface{}) bool {
		m[key.(string)] = value.(*cache)
		return true
	})

	bytes, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = cacheFile.Write(bytes)
	return err
}
