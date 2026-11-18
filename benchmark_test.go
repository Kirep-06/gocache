package gocache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type mapCache struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newMapCache() *mapCache {
	return &mapCache{data: make(map[string][]byte)}
}

func (m *mapCache) Set(key, val []byte) {
	v := make([]byte, len(val))
	copy(v, val)
	m.mu.Lock()
	m.data[string(key)] = v
	m.mu.Unlock()
}

func (m *mapCache) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	v, ok := m.data[string(key)]
	m.mu.RUnlock()
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func prepareKeys(n int) [][]byte {
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
	}
	return keys
}

func BenchmarkGoCacheSetParallel(b *testing.B) {
	const keySpace = 65536
	keys := prepareKeys(keySpace)
	val := make([]byte, 128)
	cache := NewCache(64 * 1024 * 1024)
	var seq atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := seq.Add(1) % keySpace
			if err := cache.Set(keys[i], val, 60); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMapSetParallel(b *testing.B) {
	const keySpace = 65536
	keys := prepareKeys(keySpace)
	val := make([]byte, 128)
	cache := newMapCache()
	var seq atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := seq.Add(1) % keySpace
			cache.Set(keys[i], val)
		}
	})
}

func BenchmarkGoCacheGetParallel(b *testing.B) {
	const keySpace = 65536
	keys := prepareKeys(keySpace)
	val := make([]byte, 128)
	cache := NewCache(64 * 1024 * 1024)
	for _, k := range keys {
		if err := cache.Set(k, val, 0); err != nil {
			b.Fatal(err)
		}
	}
	var seq atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := seq.Add(1) % keySpace
			if _, err := cache.Get(keys[i]); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkGoCacheGetWithBufParallel(b *testing.B) {
	const keySpace = 65536
	keys := prepareKeys(keySpace)
	val := make([]byte, 128)
	cache := NewCache(64 * 1024 * 1024)
	for _, k := range keys {
		if err := cache.Set(k, val, 0); err != nil {
			b.Fatal(err)
		}
	}
	var seq atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 128)
		for pb.Next() {
			i := seq.Add(1) % keySpace
			if _, err := cache.GetWithBuf(keys[i], buf); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMapGetParallel(b *testing.B) {
	const keySpace = 65536
	keys := prepareKeys(keySpace)
	val := make([]byte, 128)
	cache := newMapCache()
	for _, k := range keys {
		cache.Set(k, val)
	}
	var seq atomic.Uint64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := seq.Add(1) % keySpace
			if _, ok := cache.Get(keys[i]); !ok {
				b.Fatal("missing key")
			}
		}
	})
}
