package gocache

import "unsafe"

// Iterator scans cache entries without guaranteeing an order.
type Iterator struct {
	cache      *Cache
	segmentIdx int
	slotIdx    int
	entryIdx   int
}

// Entry is a copied key and value with its absolute expiration time.
type Entry struct {
	Key      []byte
	Value    []byte
	ExpireAt uint32
}

func (cache *Cache) NewIterator() *Iterator {
	return &Iterator{cache: cache}
}

// Next returns the next unexpired entry, or nil after the last entry.
func (it *Iterator) Next() *Entry {
	for it.segmentIdx < segmentCount {
		entry := it.nextForSegment(it.segmentIdx)
		if entry != nil {
			return entry
		}
		it.segmentIdx++
		it.slotIdx, it.entryIdx = 0, 0
	}
	return nil
}

func (it *Iterator) nextForSegment(segIdx int) *Entry {
	it.cache.locks[segIdx].Lock()
	defer it.cache.locks[segIdx].Unlock()
	seg := &it.cache.segments[segIdx]
	for it.slotIdx < 256 {
		if entry := it.nextForSlot(seg); entry != nil {
			return entry
		}
		it.slotIdx++
		it.entryIdx = 0
	}
	return nil
}

func (it *Iterator) nextForSlot(seg *segment) *Entry {
	slot := seg.getSlot(uint8(it.slotIdx))
	for it.entryIdx < len(slot) {
		ptr := slot[it.entryIdx]
		it.entryIdx++
		var hdrBuf [ENTRY_HDR_SIZE]byte
		seg.rb.ReadAt(hdrBuf[:], ptr.offset)
		hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
		if isExpired(hdr.expireAt, seg.timer.Now()) {
			continue
		}
		entry := &Entry{
			Key:      make([]byte, hdr.keyLen),
			Value:    make([]byte, hdr.valLen),
			ExpireAt: hdr.expireAt,
		}
		seg.rb.ReadAt(entry.Key, ptr.offset+ENTRY_HDR_SIZE)
		seg.rb.ReadAt(entry.Value, ptr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
		return entry
	}
	return nil
}
