package gocache

import (
	"testing"
	"unsafe"
)

type testTimer struct{ now uint32 }

func (timer *testTimer) Now() uint32 { return timer.now }

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

func TestSegmentInsertAndFindEntries(t *testing.T) {
	seg := newSegment(256, 0, defaultTimer{})
	const slotID = 3
	for _, entry := range []struct {
		key  string
		hash uint16
	}{{"z", 9}, {"a", 2}, {"b", 9}} {
		off := seg.rb.End()
		seg.rb.Write(make([]byte, ENTRY_HDR_SIZE))
		seg.rb.Write([]byte(entry.key))
		idx, found := seg.lookup(seg.getSlot(slotID), entry.hash, []byte(entry.key))
		if found {
			t.Fatalf("key %q found before insertion", entry.key)
		}
		seg.insertEntryPtr(slotID, entry.hash, off, idx, uint16(len(entry.key)))
	}
	if seg.slotCap != 4 || seg.entryCount != 3 {
		t.Fatalf("slot growth: capacity=%d entries=%d", seg.slotCap, seg.entryCount)
	}
	slot := seg.getSlot(slotID)
	if slot[0].hash16 != 2 || slot[1].hash16 != 9 || slot[2].hash16 != 9 {
		t.Fatalf("slot not ordered: %+v", slot)
	}
	for _, key := range []string{"a", "b", "z"} {
		if _, found := seg.lookup(slot, map[string]uint16{"a": 2, "b": 9, "z": 9}[key], []byte(key)); !found {
			t.Fatalf("key %q not found", key)
		}
	}
	if _, found := seg.lookup(slot, 9, []byte("c")); found {
		t.Fatal("different key with same hash should not match")
	}
	oldOff := slot[0].offset
	seg.delEntryPtrByOffset(slotID, 2, oldOff)
	if seg.entryCount != 2 || len(seg.getSlot(slotID)) != 2 {
		t.Fatalf("after deletion: entries=%d slot length=%d", seg.entryCount, len(seg.getSlot(slotID)))
	}
	var hdrBuf [ENTRY_HDR_SIZE]byte
	seg.rb.ReadAt(hdrBuf[:], oldOff)
	if !(*entryHdr)(unsafe.Pointer(&hdrBuf[0])).deleted {
		t.Fatal("deleted entry header should be marked")
	}
	seg.updateEntryPtr(slotID, 9, seg.getSlot(slotID)[0].offset, 100)
	if seg.getSlot(slotID)[0].offset != 100 {
		t.Fatal("entry pointer offset should update")
	}
}

func TestSegmentReclaimsExpiredEntry(t *testing.T) {
	seg := newSegment(64, 0, defaultTimer{})
	hdr := entryHdr{accessTime: 10, expireAt: 20, keyLen: 1, hash16: 4, valLen: 1, valCap: 1, slotId: 3}
	hdrBytes := unsafe.Slice((*byte)(unsafe.Pointer(&hdr)), ENTRY_HDR_SIZE)
	seg.rb.Write(hdrBytes)
	seg.rb.Write([]byte("k"))
	seg.rb.Write([]byte("v"))
	seg.insertEntryPtr(3, 4, 0, 0, 1)
	seg.totalCount = 1
	seg.totalTime = 10
	seg.vacuumLen -= ENTRY_HDR_SIZE + 2
	if !seg.evacuate(40, 3, 30) {
		t.Fatal("target slot should be marked modified")
	}
	if seg.entryCount != 0 || seg.totalExpired != 1 || seg.vacuumLen != 64 {
		t.Fatalf("reclaim state: entries=%d expired=%d vacuum=%d", seg.entryCount, seg.totalExpired, seg.vacuumLen)
	}
}

func TestSegmentKeepsHotEntryWhenReclaiming(t *testing.T) {
	seg := newSegment(80, 0, defaultTimer{})
	for _, item := range []struct {
		key    byte
		access uint32
		slot   uint8
	}{{'a', 100, 3}, {'b', 1, 4}} {
		hdr := entryHdr{accessTime: item.access, keyLen: 1, hash16: uint16(item.slot), valLen: 1, valCap: 1, slotId: item.slot}
		off := seg.rb.End()
		seg.rb.Write(unsafe.Slice((*byte)(unsafe.Pointer(&hdr)), ENTRY_HDR_SIZE))
		seg.rb.Write([]byte{item.key, 'v'})
		seg.insertEntryPtr(item.slot, hdr.hash16, off, 0, 1)
		seg.vacuumLen -= ENTRY_HDR_SIZE + 2
	}
	seg.totalCount, seg.totalTime = 2, 101
	if !seg.evacuate(30, 4, 100) {
		t.Fatal("eviction should modify the second slot")
	}
	if seg.entryCount != 1 || seg.getSlot(3)[0].offset != 52 || len(seg.getSlot(4)) != 0 {
		t.Fatalf("hot entry should move, cold entry should leave: entries=%d hot=%+v", seg.entryCount, seg.getSlot(3))
	}
}

func TestSegmentStoreAndRead(t *testing.T) {
	seg := newSegment(1024, 0, defaultTimer{})
	const hash = uint64(3<<8 | 5<<16)
	if err := seg.set([]byte("k"), []byte("one"), hash, 0); err != nil {
		t.Fatal(err)
	}
	value, _, err := seg.get([]byte("k"), nil, hash, false)
	if err != nil || string(value) != "one" {
		t.Fatalf("first read: value=%q err=%v", value, err)
	}
	if err := seg.set([]byte("k"), []byte("two"), hash, 0); err != nil {
		t.Fatal(err)
	}
	value, _, err = seg.get([]byte("k"), nil, hash, false)
	if err != nil || string(value) != "two" || seg.entryCount != 1 || seg.overwrites != 1 {
		t.Fatalf("overwrite: value=%q err=%v count=%d overwrites=%d", value, err, seg.entryCount, seg.overwrites)
	}
	if _, _, err := seg.get([]byte("missing"), nil, hash, false); err != ErrNotFound {
		t.Fatalf("missing key: err=%v", err)
	}
	if err := seg.set([]byte("k"), []byte("longer-value"), hash, 0); err != nil {
		t.Fatal(err)
	}
	value, _, err = seg.get([]byte("k"), nil, hash, false)
	if err != nil || string(value) != "longer-value" || seg.entryCount != 1 {
		t.Fatalf("larger overwrite: value=%q err=%v count=%d", value, err, seg.entryCount)
	}
	if err := seg.set(make([]byte, 65536), nil, hash, 0); err != ErrLargeKey {
		t.Fatalf("large key: err=%v", err)
	}
	if err := seg.set([]byte("k"), make([]byte, 300), hash, 0); err != ErrLargeEntry {
		t.Fatalf("large entry: err=%v", err)
	}
}

func TestSegmentExpirationAndDelete(t *testing.T) {
	clock := &testTimer{now: 100}
	seg := newSegment(1024, 0, clock)
	const hash = uint64(3<<8 | 5<<16)
	if err := seg.set([]byte("k"), []byte("v"), hash, 5); err != nil {
		t.Fatal(err)
	}
	if ttl, err := seg.ttl([]byte("k"), hash); ttl != 5 || err != nil {
		t.Fatalf("initial TTL: ttl=%d err=%v", ttl, err)
	}
	clock.now = 103
	if err := seg.touch([]byte("k"), hash, 10); err != nil {
		t.Fatal(err)
	}
	if ttl, err := seg.ttl([]byte("k"), hash); ttl != 10 || err != nil {
		t.Fatalf("renewed TTL: ttl=%d err=%v", ttl, err)
	}
	clock.now = 113
	if _, _, err := seg.get([]byte("k"), nil, hash, false); err != ErrNotFound || seg.entryCount != 0 {
		t.Fatalf("expired entry: err=%v count=%d", err, seg.entryCount)
	}
	seg.set([]byte("other"), []byte("v"), hash, 0)
	if !seg.del([]byte("other"), hash) || seg.del([]byte("other"), hash) {
		t.Fatal("delete should affect an existing key once")
	}
}

func TestSegmentViewAndClear(t *testing.T) {
	seg := newSegment(1024, 0, defaultTimer{})
	const hash = uint64(3<<8 | 5<<16)
	seg.set([]byte("k"), []byte("value"), hash, 0)
	if err := seg.view([]byte("k"), func(value []byte) error {
		if string(value) != "value" {
			t.Fatalf("viewed value=%q", value)
		}
		return nil
	}, hash, false); err != nil {
		t.Fatal(err)
	}
	if seg.hitCount != 1 {
		t.Fatalf("view hit count=%d", seg.hitCount)
	}
	seg.resetStatistics()
	if seg.hitCount != 0 || seg.entryCount != 1 {
		t.Fatalf("statistics reset should keep entries: hits=%d entries=%d", seg.hitCount, seg.entryCount)
	}
	seg.clear()
	if seg.entryCount != 0 || seg.rb.End() != 0 || seg.vacuumLen != 1024 {
		t.Fatalf("clear state: entries=%d end=%d vacuum=%d", seg.entryCount, seg.rb.End(), seg.vacuumLen)
	}
}
