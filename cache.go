package gocache

import (
	"encoding/binary"
	"sync"
	"sync/atomic"

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

type Updater func(value []byte, found bool) (newValue []byte, replace bool, expireSeconds int)

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

func (cache *Cache) GetOrSet(key, value []byte, expireSeconds int) ([]byte, error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	current, _, err := cache.segments[segID].get(key, nil, hashVal, false)
	if err == nil {
		return current, nil
	}
	return nil, cache.segments[segID].set(key, value, hashVal, expireSeconds)
}

func (cache *Cache) SetAndGet(key, value []byte, expireSeconds int) ([]byte, bool, error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	current, _, err := cache.segments[segID].get(key, nil, hashVal, false)
	found := err == nil
	err = cache.segments[segID].set(key, value, hashVal, expireSeconds)
	return current, found, err
}

func (cache *Cache) Update(key []byte, updater Updater) (found, replaced bool, err error) {
	hashVal := hashFunc(key)
	segID := hashVal & segmentAndOpVal
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()
	current, _, getErr := cache.segments[segID].get(key, nil, hashVal, false)
	found = getErr == nil
	value, replaced, expireSeconds := updater(current, found)
	if !replaced {
		return found, false, nil
	}
	return found, true, cache.segments[segID].set(key, value, hashVal, expireSeconds)
}

// MultiGet returns one value and error per key, locking each segment once.
func (cache *Cache) MultiGet(keys [][]byte) (values [][]byte, errs []error) {
	if len(keys) == 0 {
		return nil, nil
	}
	values = make([][]byte, len(keys))
	errs = make([]error, len(keys))
	type keyLoc struct {
		idx     int
		hashVal uint64
	}
	var groups [segmentCount][]keyLoc
	for i, key := range keys {
		hashVal := hashFunc(key)
		segID := hashVal & segmentAndOpVal
		groups[segID] = append(groups[segID], keyLoc{idx: i, hashVal: hashVal})
	}
	for segID, batch := range groups {
		if len(batch) == 0 {
			continue
		}
		cache.locks[segID].Lock()
		for _, loc := range batch {
			value, _, err := cache.segments[segID].get(keys[loc.idx], nil, loc.hashVal, false)
			values[loc.idx], errs[loc.idx] = value, err
		}
		cache.locks[segID].Unlock()
	}
	return values, errs
}

func (cache *Cache) SetInt(key int64, value []byte, expireSeconds int) error {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(key))
	return cache.Set(encoded[:], value, expireSeconds)
}

func (cache *Cache) GetInt(key int64) ([]byte, error) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(key))
	return cache.Get(encoded[:])
}

func (cache *Cache) GetIntWithExpiration(key int64) ([]byte, uint32, error) {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(key))
	return cache.GetWithExpiration(encoded[:])
}

func (cache *Cache) DelInt(key int64) bool {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], uint64(key))
	return cache.Del(encoded[:])
}

func (cache *Cache) EntryCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].entryCount)
	}
	return count
}

func (cache *Cache) HitCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].hitCount)
	}
	return count
}

func (cache *Cache) MissCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].missCount)
	}
	return count
}

func (cache *Cache) LookupCount() int64 {
	return cache.HitCount() + cache.MissCount()
}

func (cache *Cache) HitRate() float64 {
	hits, misses := cache.HitCount(), cache.MissCount()
	if hits+misses == 0 {
		return 0
	}
	return float64(hits) / float64(hits+misses)
}

func (cache *Cache) EvacuateCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalEvacuate)
	}
	return count
}

func (cache *Cache) ExpiredCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalExpired)
	}
	return count
}

func (cache *Cache) OverwriteCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].overwrites)
	}
	return count
}

func (cache *Cache) TouchedCount() (count int64) {
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].touched)
	}
	return count
}

func (cache *Cache) AverageAccessTime() int64 {
	var count, total int64
	for i := range cache.segments {
		count += atomic.LoadInt64(&cache.segments[i].totalCount)
		total += atomic.LoadInt64(&cache.segments[i].totalTime)
	}
	if count == 0 {
		return 0
	}
	return total / count
}

func (cache *Cache) Clear() {
	for i := range cache.segments {
		cache.locks[i].Lock()
		cache.segments[i].clear()
		cache.locks[i].Unlock()
	}
}

func (cache *Cache) ResetStatistics() {
	for i := range cache.segments {
		cache.locks[i].Lock()
		cache.segments[i].resetStatistics()
		cache.locks[i].Unlock()
	}
}
