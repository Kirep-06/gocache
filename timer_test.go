package gocache

import (
	"sync"
	"testing"
	"time"
)

func TestTimerCurrentTime(t *testing.T) {
	timer := NewCachedTimer()
	defer timer.Stop()
	for _, now := range []uint32{defaultTimer{}.Now(), timer.Now()} {
		if diff := int64(now) - time.Now().Unix(); diff < -1 || diff > 1 {
			t.Fatalf("timer differs from wall clock by %d seconds", diff)
		}
	}
}

func TestCachedTimerConcurrentReads(t *testing.T) {
	timer := NewCachedTimer()
	defer timer.Stop()
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for j := 0; j < 100; j++ {
				if timer.Now() == 0 {
					t.Error("cached timer returned zero time")
				}
			}
		}()
	}
	readers.Wait()
}
