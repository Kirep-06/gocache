package gocache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCacheBasicOperations(t *testing.T) {
	clock := &testTimer{now: 100}
	cache := NewCacheCustomTimer(1, clock)
	if cache.segments[0].rb.Size() != minBufSize/segmentCount {
		t.Fatal("minimum cache size not applied")
	}
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		if err := cache.Set(key, key, 10); err != nil {
			t.Fatal(err)
		}
		value, err := cache.Get(key)
		if err != nil || string(value) != string(key) {
			t.Fatalf("key=%q value=%q err=%v", key, value, err)
		}
	}
	key := []byte("key-1")
	if ttl, err := cache.TTL(key); ttl != 10 || err != nil {
		t.Fatalf("TTL=%d err=%v", ttl, err)
	}
	if !cache.Del(key) || cache.Del(key) {
		t.Fatal("delete should succeed once")
	}
}

func TestCacheConcurrentSegments(t *testing.T) {
	cache := NewCache(512 * 1024)
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			key := []byte(fmt.Sprintf("worker-%d", i))
			if err := cache.Set(key, key, 0); err != nil {
				t.Error(err)
				return
			}
			if value, err := cache.Get(key); err != nil || string(value) != string(key) {
				t.Errorf("key=%q value=%q err=%v", key, value, err)
			}
		}(i)
	}
	workers.Wait()
}

func TestCacheReadModes(t *testing.T) {
	clock := &testTimer{now: 100}
	cache := NewCacheCustomTimer(512*1024, clock)
	key := []byte("key")
	cache.Set(key, []byte("value"), 5)
	buf := make([]byte, 0, 16)
	value, expireAt, err := cache.GetWithExpirationAndBuf(key, buf)
	if err != nil || expireAt != 105 || string(value) != "value" || &value[0] != &buf[:cap(buf)][0] {
		t.Fatalf("buffered read: value=%q expiry=%d err=%v", value, expireAt, err)
	}
	if err := cache.GetFn(key, func(v []byte) error {
		if string(v) != "value" {
			t.Fatalf("view=%q", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	clock.now = 106
	if value, err := cache.Peek(key); err != nil || string(value) != "value" {
		t.Fatalf("peek expired value: value=%q err=%v", value, err)
	}
	if _, err := cache.Get(key); err != ErrNotFound {
		t.Fatalf("expired get: err=%v", err)
	}
}

func TestGetFnPanicReleasesSegmentLock(t *testing.T) {
	cache := NewCache(512 * 1024)
	key := []byte("key")
	cache.Set(key, []byte("value"), 0)
	func() {
		defer func() { recover() }()
		cache.GetFn(key, func([]byte) error { panic("callback failed") })
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		cache.Set(key, []byte("next"), 0)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("callback panic left the segment locked")
	}
}
