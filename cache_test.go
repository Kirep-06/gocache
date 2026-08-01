package gocache

import (
	"fmt"
	"strconv"
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

func TestCacheAtomicUpdates(t *testing.T) {
	cache := NewCache(512 * 1024)
	key := []byte("counter")
	if old, err := cache.GetOrSet(key, []byte("0"), 0); old != nil || err != nil {
		t.Fatalf("new GetOrSet: old=%q err=%v", old, err)
	}
	if old, err := cache.GetOrSet(key, []byte("ignored"), 0); string(old) != "0" || err != nil {
		t.Fatalf("existing GetOrSet: old=%q err=%v", old, err)
	}
	if old, found, err := cache.SetAndGet(key, []byte("1"), 0); string(old) != "0" || !found || err != nil {
		t.Fatalf("SetAndGet: old=%q found=%v err=%v", old, found, err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 20; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, _, err := cache.Update(key, func(value []byte, found bool) ([]byte, bool, int) {
				if !found {
					t.Error("counter disappeared")
				}
				n, _ := strconv.Atoi(string(value))
				return []byte(strconv.Itoa(n + 1)), true, 0
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if value, err := cache.Get(key); string(value) != "21" || err != nil {
		t.Fatalf("atomic update result: value=%q err=%v", value, err)
	}
}

func TestCacheMultiGetPreservesInputOrder(t *testing.T) {
	cache := NewCache(512 * 1024)
	cache.Set([]byte("a"), []byte("A"), 0)
	cache.Set([]byte("b"), []byte("B"), 0)
	keys := [][]byte{[]byte("b"), []byte("missing"), []byte("a"), []byte("b")}
	values, errs := cache.MultiGet(keys)
	if len(values) != len(keys) || len(errs) != len(keys) {
		t.Fatalf("result lengths: values=%d errs=%d", len(values), len(errs))
	}
	if string(values[0]) != "B" || values[1] != nil || string(values[2]) != "A" || string(values[3]) != "B" {
		t.Fatalf("values=%q", values)
	}
	if errs[0] != nil || errs[1] != ErrNotFound || errs[2] != nil || errs[3] != nil {
		t.Fatalf("errors=%v", errs)
	}
	if values, errs := cache.MultiGet(nil); values != nil || errs != nil {
		t.Fatalf("empty batch: values=%v errs=%v", values, errs)
	}
}

func TestCacheIntegerKeys(t *testing.T) {
	clock := &testTimer{now: 100}
	cache := NewCacheCustomTimer(512*1024, clock)
	if err := cache.SetInt(-42, []byte("value"), 5); err != nil {
		t.Fatal(err)
	}
	value, expiry, err := cache.GetIntWithExpiration(-42)
	if err != nil || string(value) != "value" || expiry != 105 {
		t.Fatalf("integer read: value=%q expiry=%d err=%v", value, expiry, err)
	}
	if !cache.DelInt(-42) {
		t.Fatal("integer key should delete")
	}
	if _, err := cache.GetInt(-42); err != ErrNotFound {
		t.Fatalf("deleted integer key: err=%v", err)
	}
}

func TestCacheStatisticsAndClear(t *testing.T) {
	cache := NewCache(512 * 1024)
	if cache.HitRate() != 0 || cache.AverageAccessTime() != 0 {
		t.Fatal("empty cache statistics should be zero")
	}
	cache.Set([]byte("key"), []byte("value"), 0)
	cache.Get([]byte("key"))
	cache.Get([]byte("missing"))
	cache.Set([]byte("key"), []byte("next"), 0)
	cache.Touch([]byte("key"), 0)
	if cache.EntryCount() != 1 || cache.HitCount() != 1 || cache.MissCount() != 1 || cache.LookupCount() != 2 || cache.HitRate() != 0.5 {
		t.Fatal("lookup statistics differ from operations")
	}
	if cache.OverwriteCount() != 1 || cache.TouchedCount() != 1 || cache.AverageAccessTime() == 0 {
		t.Fatal("mutation statistics differ from operations")
	}
	cache.ResetStatistics()
	if cache.EntryCount() != 1 || cache.LookupCount() != 0 || cache.OverwriteCount() != 0 {
		t.Fatal("reset statistics should preserve entries")
	}
	cache.Clear()
	if cache.EntryCount() != 0 {
		t.Fatal("clear should remove entries")
	}
}

func TestCacheIteratorSkipsExpiredAndCopiesValues(t *testing.T) {
	clock := &testTimer{now: 100}
	cache := NewCacheCustomTimer(512*1024, clock)
	cache.Set([]byte("live"), []byte("value"), 0)
	cache.Set([]byte("expired"), []byte("old"), 1)
	clock.now = 101
	entries := map[string]string{}
	for it := cache.NewIterator(); ; {
		entry := it.Next()
		if entry == nil {
			break
		}
		entries[string(entry.Key)] = string(entry.Value)
		entry.Value[0] = 'X'
	}
	if len(entries) != 1 || entries["live"] != "value" {
		t.Fatalf("iterated entries=%v", entries)
	}
	if value, err := cache.Get([]byte("live")); err != nil || string(value) != "value" {
		t.Fatalf("iterator mutated cache: value=%q err=%v", value, err)
	}
}
