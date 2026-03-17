package persistence

import (
	"sync"
	"time"
)

type HeatTracker struct {
	mu            sync.Mutex
	counts        map[uint64]uint64
	lastDecayAt   time.Time
	decayInterval time.Duration
	decayFactor   float64
}

func NewHeatTracker(decayInterval time.Duration, decayFactor float64) *HeatTracker {
	if decayInterval <= 0 {
		decayInterval = 30 * time.Second
	}
	if decayFactor <= 0 || decayFactor >= 1 {
		decayFactor = 0.5
	}

	return &HeatTracker{
		counts:        make(map[uint64]uint64),
		lastDecayAt:   time.Now(),
		decayInterval: decayInterval,
		decayFactor:   decayFactor,
	}
}

func (t *HeatTracker) Add(fileNum uint64, n uint64) {
	if fileNum == 0 || n == 0 {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.maybeDecayLocked(time.Now())
	t.counts[fileNum] += n
}

func (t *HeatTracker) Get(fileNum uint64) uint64 {
	if fileNum == 0 {
		return 0
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.maybeDecayLocked(time.Now())
	return t.counts[fileNum]
}

func (t *HeatTracker) maybeDecayLocked(now time.Time) {
	if now.Sub(t.lastDecayAt) < t.decayInterval {
		return
	}
	t.lastDecayAt = now

	for k, v := range t.counts {
		nv := uint64(float64(v) * t.decayFactor)
		if nv == 0 {
			delete(t.counts, k)
			continue
		}
		t.counts[k] = nv
	}
}
