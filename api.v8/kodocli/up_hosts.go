package kodocli

import (
	"container/ring"
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

type hostsScore struct {
	m      sync.Mutex
	scores *ring.Ring
}

var (
	MaxContinuousFailureTimes    = 5
	MaxContinuousFailureDuration = 1 * time.Minute
	MaxFindHostsPrecent          = 50
	hostsScores                  sync.Map
)

func isHostNameValid(hostName string) bool {
	if hs, ok := hostsScores.Load(hostName); ok {
		return hs.(*hostsScore).isValid()
	}
	return true
}

func failHostName(hostName string) {
	hs, _ := hostsScores.LoadOrStore(hostName, newHostsScore())
	hs.(*hostsScore).fail()
}

func succeedHostName(hostName string) {
	hostsScores.Delete(hostName)
}

func newHostsScore() *hostsScore {
	return &hostsScore{
		scores: ring.New(MaxContinuousFailureTimes),
	}
}

func (hs *hostsScore) isValid() bool {
	hs.m.Lock()
	defer hs.m.Unlock()

	for i := 0; i < hs.scores.Len(); i++ {
		if t, ok := hs.scores.Value.(time.Time); ok {
			if t.Add(MaxContinuousFailureDuration).Before(time.Now()) {
				return true
			}
		} else {
			return true
		}
		hs.scores = hs.scores.Next()
	}
	return false
}

func (hs *hostsScore) fail() {
	hs.m.Lock()
	defer hs.m.Unlock()

	hs.scores.Value = time.Now()
	hs.scores = hs.scores.Next()
}
