package operation

import (
	"math/rand"
	"os"
	"sync"
	"time"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano() | int64(os.Getpid())))

func shuffleHosts(hosts []string) {
	if len(hosts) >= 2 {
		random.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})
	}
}

type hostFailureInfo struct {
	m                      sync.Mutex
	lastFailedAt           time.Time
	continuousFailureTimes int
}

var (
	MaxContinuousFailureTimes    = 5
	MaxContinuousFailureDuration = 1 * time.Minute
	MaxFindHostsPrecent          = 50
	hostsFailures                sync.Map
)

func isHostNameValid(hostName string) bool {
	if val, ok := hostsFailures.Load(hostName); ok {
		failure := val.(*hostFailureInfo)
		failure.m.Lock()
		defer failure.m.Unlock()

		return failure.continuousFailureTimes <= MaxContinuousFailureTimes ||
			failure.lastFailedAt.Add(MaxContinuousFailureDuration).Before(time.Now())
	}
	return true
}

func failHostName(hostName string) {
	val, _ := hostsFailures.LoadOrStore(hostName, &hostFailureInfo{})
	failure := val.(*hostFailureInfo)
	failure.m.Lock()
	defer failure.m.Unlock()

	failure.lastFailedAt = time.Now()
	failure.continuousFailureTimes += 1
}

func succeedHostName(hostName string) {
	hostsFailures.Delete(hostName)
}
