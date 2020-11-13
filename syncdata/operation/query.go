package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var queryClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   500 * time.Millisecond,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	},
	Timeout: 1 * time.Second,
}

var (
	cacheMap         sync.Map
	cacheUpdaterLock sync.Mutex
)

type (
	Queryer struct {
		ak      string
		bucket  string
		ucHosts []string
	}

	cache struct {
		cachedHosts    cachedHosts
		cacheExpiredAt time.Time
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

func NewQueryer(c *Config) *Queryer {
	return &Queryer{
		ak:      c.Ak,
		bucket:  c.Bucket,
		ucHosts: dupStrings(c.UcHosts),
	}
}

func (queryer *Queryer) QueryUpHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.cachedHosts.Hosts[0].Up.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) QueryIoHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.cachedHosts.Hosts[0].Io.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) QueryRsHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.cachedHosts.Hosts[0].Rs.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) QueryRsfHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.cachedHosts.Hosts[0].Rsf.Domains
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
					return c, nil
				}
			} else {
				return c, nil
			}
		}()
	} else {
		if c.cacheExpiredAt.Before(time.Now()) {
			queryer.asyncRefresh()
		}
		return c, err
	}
}

func (queryer *Queryer) mustQuery() (c *cache, err error) {
	var resp *http.Response

	query := make(url.Values, 2)
	query.Set("ak", queryer.ak)
	query.Set("bucket", queryer.bucket)

	for i := 0; i < 10; i++ {
		url := fmt.Sprintf("%s/v4/query?%s", queryer.nextUcHost(), query.Encode())
		resp, err = queryClient.Get(url)
		if err != nil {
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode/100 != 2 {
			err = fmt.Errorf("uc queryV4 status code error: %d", resp.StatusCode)
			continue
		}

		c = new(cache)
		if err = json.NewDecoder(resp.Body).Decode(&c.cachedHosts); err != nil {
			continue
		}
		if len(c.cachedHosts.Hosts) == 0 {
			return nil, errors.New("uc queryV4 returns empty hosts")
		}
		minTTL := c.cachedHosts.Hosts[0].Ttl
		for _, host := range c.cachedHosts.Hosts[1:] { // 取出 Hosts 内最小的 TTL
			if minTTL > host.Ttl {
				minTTL = host.Ttl
			}
		}
		c.cacheExpiredAt = time.Now().Add(time.Duration(minTTL) * time.Second)
		break
	}
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
		if c == nil || c.cacheExpiredAt.Before(time.Now()) {
			if c, err = queryer.mustQuery(); err == nil {
				queryer.setCache(c)
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

func (queryer *Queryer) nextUcHost() string {
	return queryer.ucHosts[randomNext()%uint32(len(queryer.ucHosts))]
}
