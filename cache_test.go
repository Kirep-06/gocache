package gocache

import (
	"fmt"
	"sync"
	"testing"
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
