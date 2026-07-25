package gocache

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

const (
	segmentCount    = 256
	segmentAndOpVal = 255
	minBufSize      = 512 * 1024
)

// Cache stores entries across independently locked segments.
type Cache struct {
	locks    [segmentCount]sync.Mutex
	segments [segmentCount]segment
}

func hashFunc(data []byte) uint64 {
	return xxhash.Sum64(data)
}

// NewCache preallocates at least 512 KiB for cache entries.
func NewCache(size int) *Cache {
	return NewCacheCustomTimer(size, defaultTimer{})
}

func NewCacheCustomTimer(size int, timer Timer) *Cache {
	if size < minBufSize {
		size = minBufSize
	}
	if timer == nil {
		timer = defaultTimer{}
	}
	cache := new(Cache)
	for i := range cache.segments {
		cache.segments[i] = newSegment(size/segmentCount, i, timer)
	}
	return cache
}

func (cache *Cache) Set(key, value []byte, expireSeconds int) error {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	return cache.segments[segID].set(key, value, hashVal, expireSeconds)
}

func (cache *Cache) Get(key []byte) ([]byte, error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	value, _, err := cache.segments[segID].get(key, nil, hashVal, false)
	return value, err
}

func (cache *Cache) Touch(key []byte, expireSeconds int) error {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	return cache.segments[segID].touch(key, hashVal, expireSeconds)
}

func (cache *Cache) TTL(key []byte) (uint32, error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	return cache.segments[segID].ttl(key, hashVal)
}

func (cache *Cache) Del(key []byte) bool {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	return cache.segments[segID].del(key, hashVal)
}

func (cache *Cache) GetWithBuf(key, buf []byte) ([]byte, error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	value, _, err := cache.segments[segID].get(key, buf, hashVal, false)
	return value, err
}

func (cache *Cache) GetWithExpiration(key []byte) ([]byte, uint32, error) {
	return cache.GetWithExpirationAndBuf(key, nil)
}

func (cache *Cache) GetWithExpirationAndBuf(key, buf []byte) ([]byte, uint32, error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	return cache.segments[segID].get(key, buf, hashVal, false)
}

// Peek reads an entry without checking expiration or updating statistics.
func (cache *Cache) Peek(key []byte) ([]byte, error) {
	value, _, err := cache.PeekWithExpiration(key)
	return value, err
}

func (cache *Cache) PeekWithExpiration(key []byte) ([]byte, uint32, error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	return cache.segments[segID].get(key, nil, hashVal, true)
}

// GetFn calls fn while the segment lock protects a view of the value.
func (cache *Cache) GetFn(key []byte, fn func([]byte) error) error {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	return cache.segments[segID].view(key, fn, hashVal, false)
}

func (cache *Cache) PeekFn(key []byte, fn func([]byte) error) error {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	return cache.segments[segID].view(key, fn, hashVal, true)
}
