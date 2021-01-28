package kodocli

import (
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var curUpHostIndex uint32 = 0

func (p Uploader) chooseUpHost() string {
	switch len(p.UpHosts) {
	case 0:
		panic("No Up hosts is configured")
	case 1:
		return p.UpHosts[0]
	default:
		var upHost string
		for i := 0; i <= len(p.UpHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curUpHostIndex, 1) - 1)
			upHost = p.UpHosts[index%len(p.UpHosts)]
			if isHostNameValid(upHost) {
				break
			}
		}
		return upHost
	}
}

func (p Uploader) shuffleUpHosts() {
	if len(p.UpHosts) >= 2 {
		rander := rand.New(rand.NewSource(time.Now().UnixNano() | int64(os.Getpid())))
		rander.Shuffle(len(p.UpHosts), func(i, j int) {
			p.UpHosts[i], p.UpHosts[j] = p.UpHosts[j], p.UpHosts[i]
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
