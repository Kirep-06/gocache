package gocache

import (
	"testing"
	"unsafe"
)

func TestSegmentLayoutAndSlots(t *testing.T) {
	if unsafe.Sizeof(entryPtr{}) != HASH_ENTRY_SIZE || unsafe.Sizeof(entryHdr{}) != ENTRY_HDR_SIZE {
		t.Fatal("entry layout does not match serialized sizes")
	}
	seg := newSegment(1024, 7, defaultTimer{})
	if seg.rb.Size() != 1024 || seg.segId != 7 || seg.slotCap != 1 || seg.vacuumLen != 1024 {
		t.Fatal("unexpected segment initialization")
	}
	if len(seg.getSlot(3)) != 0 || cap(seg.getSlot(3)) != 1 {
		t.Fatal("new slot should be empty with capacity one")
	}
}

func TestSegmentHashSlotSearch(t *testing.T) {
	slot := []entryPtr{{offset: 11, hash16: 2}, {offset: 20, hash16: 5}, {offset: 30, hash16: 5}, {offset: 40, hash16: 9}}
	if entryPtrIdx(slot, 5) != 1 || entryPtrIdx(slot, 6) != 3 {
		t.Fatal("hash search should find the first matching or insertion position")
	}
	seg := newSegment(1024, 0, defaultTimer{})
	if idx, ok := seg.lookupByOff(slot, 5, 30); idx != 2 || !ok {
		t.Fatalf("offset lookup: idx=%d match=%v", idx, ok)
	}
	if _, ok := seg.lookupByOff(slot, 5, 40); ok {
		t.Fatal("offset with a different hash should not match")
	}
}
