package gocache

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

const HASH_ENTRY_SIZE = 16
const ENTRY_HDR_SIZE = 24

var ErrLargeKey = errors.New("The key is larger than 65535")
var ErrLargeEntry = errors.New("The entry size is larger than 1/1024 of cache size")
var ErrNotFound = errors.New("Entry not found")

// entryPtr points to an entry stored in the ring buffer.
type entryPtr struct {
	offset   int64
	hash16   uint16
	keyLen   uint16
	reserved uint32
}

// entryHdr precedes each key and value in the ring buffer.
type entryHdr struct {
	accessTime uint32
	expireAt   uint32
	keyLen     uint16
	hash16     uint16
	valLen     uint32
	valCap     uint32
	deleted    bool
	slotId     uint8
	reserved   uint16
}

// A segment holds 256 sorted hash slots and their data in one ring buffer.
type segment struct {
	rb            RingBuf
	segId         int
	_             uint32
	missCount     int64
	hitCount      int64
	entryCount    int64
	totalCount    int64
	totalTime     int64
	timer         Timer
	totalEvacuate int64
	totalExpired  int64
	overwrites    int64
	touched       int64
	vacuumLen     int64
	slotLens      [256]int32
	slotCap       int32
	slotsData     []entryPtr
}

func newSegment(bufSize int, segId int, timer Timer) (seg segment) {
	seg.rb = NewRingBuf(bufSize, 0)
	seg.segId = segId
	seg.timer = timer
	seg.vacuumLen = int64(bufSize)
	seg.slotCap = 1
	seg.slotsData = make([]entryPtr, 256*seg.slotCap)
	return seg
}

func (seg *segment) getSlot(slotId uint8) []entryPtr {
	slotOff := int32(slotId) * seg.slotCap
	return seg.slotsData[slotOff : slotOff+seg.slotLens[slotId] : slotOff+seg.slotCap]
}

func entryPtrIdx(slot []entryPtr, hash16 uint16) (idx int) {
	high := len(slot)
	for idx < high {
		mid := (idx + high) >> 1
		if slot[mid].hash16 < hash16 {
			idx = mid + 1
		} else {
			high = mid
		}
	}
	return idx
}

func (seg *segment) lookupByOff(slot []entryPtr, hash16 uint16, offset int64) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) && slot[idx].hash16 == hash16 {
		if slot[idx].offset == offset {
			return idx, true
		}
		idx++
	}
	return idx, false
}

func isExpired(keyExpireAt, now uint32) bool {
	return keyExpireAt != 0 && keyExpireAt <= now
}

func (seg *segment) expand() {
	newSlotData := make([]entryPtr, seg.slotCap*2*256)
	for i := 0; i < 256; i++ {
		off := int32(i) * seg.slotCap
		copy(newSlotData[off*2:], seg.slotsData[off:off+seg.slotLens[i]])
	}
	seg.slotCap *= 2
	seg.slotsData = newSlotData
}

func (seg *segment) insertEntryPtr(slotId uint8, hash16 uint16, offset int64, idx int, keyLen uint16) {
	if seg.slotLens[slotId] == seg.slotCap {
		seg.expand()
	}
	seg.slotLens[slotId]++
	atomic.AddInt64(&seg.entryCount, 1)
	slot := seg.getSlot(slotId)
	copy(slot[idx+1:], slot[idx:])
	slot[idx] = entryPtr{offset: offset, hash16: hash16, keyLen: keyLen}
}

func (seg *segment) lookup(slot []entryPtr, hash16 uint16, key []byte) (idx int, match bool) {
	idx = entryPtrIdx(slot, hash16)
	for idx < len(slot) && slot[idx].hash16 == hash16 {
		ptr := &slot[idx]
		if int(ptr.keyLen) == len(key) && seg.rb.EqualAt(key, ptr.offset+ENTRY_HDR_SIZE) {
			return idx, true
		}
		idx++
	}
	return idx, false
}

func (seg *segment) updateEntryPtr(slotId uint8, hash16 uint16, oldOff, newOff int64) {
	slot := seg.getSlot(slotId)
	if idx, found := seg.lookupByOff(slot, hash16, oldOff); found {
		slot[idx].offset = newOff
	}
}

func (seg *segment) delEntryPtrByOffset(slotId uint8, hash16 uint16, offset int64) {
	slot := seg.getSlot(slotId)
	if idx, found := seg.lookupByOff(slot, hash16, offset); found {
		seg.delEntryPtr(slotId, slot, idx)
	}
}

func (seg *segment) delEntryPtr(slotId uint8, slot []entryPtr, idx int) {
	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], slot[idx].offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	hdr.deleted = true
	seg.rb.WriteAt(hdrBuf[:], slot[idx].offset)
	copy(slot[idx:], slot[idx+1:])
	seg.slotLens[slotId]--
	atomic.AddInt64(&seg.entryCount, -1)
}
