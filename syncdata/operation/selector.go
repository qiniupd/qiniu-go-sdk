package operation

import (
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	random     *rand.Rand
	randomLock sync.Mutex
)

func init() {
	random = rand.New(rand.NewSource(time.Now().UnixNano() | int64(os.Getpid())))
}

type (
	punishedInfo struct {
		m                       sync.Mutex
		lastPunishededAt        time.Time
		continuousPunishedTimes int
	}

	HostSelector struct {
		hostsValue              atomic.Value
		hostsMap                sync.Map
		update                  func() []string
		updateDuration          time.Duration
		punishDuration          time.Duration
		maxPunishedTimes        int
		maxPunishedHostsPrecent int
		shouldPunish            func(error) bool
		index                   uint32
	}
)

func NewHostSelector(hosts []string, update func() []string, updateDuration, punishDuration time.Duration,
	maxPunishedTimes, maxPunishedHostsPrecent int, shouldPunish func(error) bool) *HostSelector {

	if updateDuration <= time.Duration(0) {
		updateDuration = 5 * time.Minute
	}
	if punishDuration <= time.Duration(0) {
		punishDuration = 30 * time.Second
	}
	if maxPunishedTimes <= 0 {
		maxPunishedTimes = 5
	}
	if maxPunishedHostsPrecent < 0 {
		maxPunishedHostsPrecent = 50
	}

	hostSelector := &HostSelector{
		update:                  update,
		updateDuration:          updateDuration,
		punishDuration:          punishDuration,
		maxPunishedTimes:        maxPunishedTimes,
		maxPunishedHostsPrecent: maxPunishedHostsPrecent,
		shouldPunish:            shouldPunish,
	}
	hostSelector.setHosts(hosts)
	hostSelector.updateHosts()
	go hostSelector.updateWorker()
	return hostSelector
}

func (hostSelector *HostSelector) updateWorker() {
	for {
		time.Sleep(hostSelector.updateDuration)
		hostSelector.updateHosts()
	}
}

func (hostSelector *HostSelector) setHosts(hosts []string) {
	newHostsMap := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		newHostsMap[host] = struct{}{}
		hostSelector.hostsMap.LoadOrStore(host, &punishedInfo{})
	}
	hostSelector.hostsMap.Range(func(hostVal, _ interface{}) bool {
		host := hostVal.(string)
		if _, ok := newHostsMap[host]; !ok {
			hostSelector.hostsMap.Delete(host)
		}
		return true
	})

	randomLock.Lock()
	random.Shuffle(len(hosts), func(i, j int) {
		hosts[i], hosts[j] = hosts[j], hosts[i]
	})
	randomLock.Unlock()

	hostSelector.hostsValue.Store(hosts)
}

func (hostSelector *HostSelector) updateHosts() {
	var newHosts []string
	if hostSelector.update != nil {
		newHosts = hostSelector.update()
	}
	if len(newHosts) > 0 {
		hostSelector.setHosts(newHosts)
	}
}

func (hostSelector *HostSelector) SelectHost() string {
	var (
		currentHost = ""
		hosts       = hostSelector.hostsValue.Load().([]string)
	)

	for i := 0; i <= len(hosts)*hostSelector.maxPunishedHostsPrecent/100; i++ {
		hostIndex := int(atomic.AddUint32(&hostSelector.index, 1) - 1)
		currentHost = hosts[hostIndex%len(hosts)]
		if punishedInfoVal, exists := hostSelector.hostsMap.Load(currentHost); exists {
			info := punishedInfoVal.(*punishedInfo)
			info.m.Lock()
			defer info.m.Unlock()

			if info.continuousPunishedTimes <= hostSelector.maxPunishedTimes ||
				info.lastPunishededAt.Add(hostSelector.punishDuration).Before(time.Now()) {
				break
			}
		}
	}

	if currentHost == "" {
		panic("Cannot select host")
	}

	return currentHost
}

func (hostSelector *HostSelector) Reward(host string) {
	if punishedInfoVal, ok := hostSelector.hostsMap.Load(host); ok {
		info := punishedInfoVal.(*punishedInfo)
		info.m.Lock()
		defer info.m.Unlock()

		info.continuousPunishedTimes = 0
		info.lastPunishededAt = time.Time{}
	}
}

func (hostSelector *HostSelector) Punish(host string) {
	if punishedInfoVal, ok := hostSelector.hostsMap.Load(host); ok {
		info := punishedInfoVal.(*punishedInfo)
		info.m.Lock()
		defer info.m.Unlock()

		info.continuousPunishedTimes += 1
		info.lastPunishededAt = time.Now()
	}
}

func (hostSelector *HostSelector) PunishIfNeeded(host string, err error) bool {
	needed := true
	if hostSelector.shouldPunish != nil {
		needed = hostSelector.shouldPunish(err)
	}
	if needed {
		hostSelector.Punish(host)
	}
	return needed
}
