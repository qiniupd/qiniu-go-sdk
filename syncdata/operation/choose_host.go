package operation

import (
	"container/ring"
	"math/rand"
	"os"
	"sync"
	"time"
)

var (
	random     = rand.New(rand.NewSource(time.Now().UnixNano() | int64(os.Getpid())))
	randomLock sync.Mutex
)

func shuffleHosts(hosts []string) {
	if len(hosts) >= 2 {
		randomLock.Lock()
		random.Shuffle(len(hosts), func(i, j int) {
			hosts[i], hosts[j] = hosts[j], hosts[i]
		})
		randomLock.Unlock()
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
