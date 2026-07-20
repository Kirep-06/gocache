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

// evacuate frees enough ring space for the next entry while retaining hot data.
func (seg *segment) evacuate(entryLen int64, slotId uint8, now uint32) (slotModified bool) {
	var oldHdrBuf [ENTRY_HDR_SIZE]byte
	consecutiveEvacuate := 0
	for seg.vacuumLen < entryLen {
		oldOff := seg.rb.End() + seg.vacuumLen - seg.rb.Size()
		seg.rb.ReadAt(oldHdrBuf[:], oldOff)
		oldHdr := (*entryHdr)(unsafe.Pointer(&oldHdrBuf[0]))
		oldEntryLen := ENTRY_HDR_SIZE + int64(oldHdr.keyLen) + int64(oldHdr.valCap)
		if oldHdr.deleted {
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			continue
		}
		expired := isExpired(oldHdr.expireAt, now)
		leastRecentUsed := int64(oldHdr.accessTime)*atomic.LoadInt64(&seg.totalCount) <= atomic.LoadInt64(&seg.totalTime)
		if expired || leastRecentUsed || consecutiveEvacuate > 5 {
			seg.delEntryPtrByOffset(oldHdr.slotId, oldHdr.hash16, oldOff)
			if oldHdr.slotId == slotId {
				slotModified = true
			}
			consecutiveEvacuate = 0
			atomic.AddInt64(&seg.totalTime, -int64(oldHdr.accessTime))
			atomic.AddInt64(&seg.totalCount, -1)
			seg.vacuumLen += oldEntryLen
			if expired {
				atomic.AddInt64(&seg.totalExpired, 1)
			} else {
				atomic.AddInt64(&seg.totalEvacuate, 1)
			}
		} else {
			newOff := seg.rb.Evacuate(oldOff, int(oldEntryLen))
			seg.updateEntryPtr(oldHdr.slotId, oldHdr.hash16, oldOff, newOff)
			consecutiveEvacuate++
			atomic.AddInt64(&seg.totalEvacuate, 1)
		}
	}
	return slotModified
}

func (seg *segment) set(key, value []byte, hashVal uint64, expireSeconds int) error {
	if len(key) > 65535 {
		return ErrLargeKey
	}
	maxKeyValLen := len(seg.rb.data)/4 - ENTRY_HDR_SIZE
	if len(key)+len(value) > maxKeyValLen {
		return ErrLargeEntry
	}
	now := seg.timer.Now()
	expireAt := uint32(0)
	if expireSeconds > 0 {
		expireAt = now + uint32(expireSeconds)
	}
	slotId, hash16 := uint8(hashVal>>8), uint16(hashVal>>16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	var hdrBuf [ENTRY_HDR_SIZE]byte
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if match {
		ptr := &slot[idx]
		seg.rb.ReadAt(hdrBuf[:], ptr.offset)
		hdr.slotId, hdr.hash16, hdr.keyLen = slotId, hash16, uint16(len(key))
		oldAccessTime := hdr.accessTime
		hdr.accessTime, hdr.expireAt, hdr.valLen = now, expireAt, uint32(len(value))
		if hdr.valCap >= hdr.valLen {
			atomic.AddInt64(&seg.totalTime, int64(now)-int64(oldAccessTime))
			seg.rb.WriteAt(hdrBuf[:], ptr.offset)
			seg.rb.WriteAt(value, ptr.offset+ENTRY_HDR_SIZE+int64(hdr.keyLen))
			atomic.AddInt64(&seg.overwrites, 1)
			return nil
		}
		seg.delEntryPtr(slotId, slot, idx)
		for hdr.valCap < hdr.valLen {
			hdr.valCap *= 2
		}
		if hdr.valCap > uint32(maxKeyValLen-len(key)) {
			hdr.valCap = uint32(maxKeyValLen - len(key))
		}
	} else {
		hdr.slotId, hdr.hash16, hdr.keyLen = slotId, hash16, uint16(len(key))
		hdr.accessTime, hdr.expireAt = now, expireAt
		hdr.valLen, hdr.valCap = uint32(len(value)), uint32(len(value))
		if hdr.valCap == 0 {
			hdr.valCap = 1
		}
	}
	entryLen := ENTRY_HDR_SIZE + int64(len(key)) + int64(hdr.valCap)
	if seg.evacuate(entryLen, slotId, now) {
		slot = seg.getSlot(slotId)
		idx, _ = seg.lookup(slot, hash16, key)
	}
	newOff := seg.rb.End()
	seg.insertEntryPtr(slotId, hash16, newOff, idx, hdr.keyLen)
	seg.rb.Write(hdrBuf[:])
	seg.rb.Write(key)
	seg.rb.Write(value)
	seg.rb.Skip(int64(hdr.valCap - hdr.valLen))
	atomic.AddInt64(&seg.totalTime, int64(now))
	atomic.AddInt64(&seg.totalCount, 1)
	seg.vacuumLen -= entryLen
	return nil
}

func (seg *segment) locate(key []byte, hashVal uint64, peek bool) (entryHdr, int64, error) {
	slotId, hash16 := uint8(hashVal>>8), uint16(hashVal>>16)
	slot := seg.getSlot(slotId)
	idx, match := seg.lookup(slot, hash16, key)
	if !match {
		if !peek {
			atomic.AddInt64(&seg.missCount, 1)
		}
		return entryHdr{}, 0, ErrNotFound
	}
	ptr := &slot[idx]
	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], ptr.offset)
	hdr := (*entryHdr)(unsafe.Pointer(&hdrBuf[0]))
	if !peek {
		now := seg.timer.Now()
		if isExpired(hdr.expireAt, now) {
			seg.delEntryPtr(slotId, slot, idx)
			atomic.AddInt64(&seg.totalExpired, 1)
			atomic.AddInt64(&seg.missCount, 1)
			return entryHdr{}, 0, ErrNotFound
		}
		atomic.AddInt64(&seg.totalTime, int64(now)-int64(hdr.accessTime))
		hdr.accessTime = now
		seg.rb.WriteAt(hdrBuf[:], ptr.offset)
	}
	return *hdr, ptr.offset, nil
}

func (seg *segment) get(key, buf []byte, hashVal uint64, peek bool) ([]byte, uint32, error) {
	hdr, off, err := seg.locate(key, hashVal, peek)
	if err != nil {
		return nil, 0, err
	}
	if cap(buf) < int(hdr.valLen) {
		buf = make([]byte, hdr.valLen)
	} else {
		buf = buf[:hdr.valLen]
	}
	seg.rb.ReadAt(buf, off+ENTRY_HDR_SIZE+int64(hdr.keyLen))
	if !peek {
		atomic.AddInt64(&seg.hitCount, 1)
	}
	return buf, hdr.expireAt, nil
}
