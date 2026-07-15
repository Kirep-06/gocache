package gocache

import (
	"sync/atomic"
	"time"
)

// Timer supplies the current Unix time in seconds.
type Timer interface {
	Now() uint32
}

// StoppableTimer releases background resources when no longer needed.
type StoppableTimer interface {
	Timer
	Stop()
}

func getUnixTime() uint32 {
	return uint32(time.Now().Unix())
}

type defaultTimer struct{}

func (defaultTimer) Now() uint32 {
	return getUnixTime()
}

type cachedTimer struct {
	now    uint32
	ticker *time.Ticker
	done   chan bool
}

// NewCachedTimer caches the current second in a background goroutine.
func NewCachedTimer() StoppableTimer {
	timer := &cachedTimer{
		now:    getUnixTime(),
		ticker: time.NewTicker(time.Second),
		done:   make(chan bool),
	}
	go timer.update()
	return timer
}

func (timer *cachedTimer) Now() uint32 {
	return atomic.LoadUint32(&timer.now)
}

// Stop must be called once when the cached timer is no longer needed.
func (timer *cachedTimer) Stop() {
	timer.ticker.Stop()
	timer.done <- true
	close(timer.done)
	timer.done = nil
	timer.ticker = nil
}

func (timer *cachedTimer) update() {
	for {
		select {
		case <-timer.done:
			return
		case <-timer.ticker.C:
			atomic.StoreUint32(&timer.now, getUnixTime())
		}
	}
}
