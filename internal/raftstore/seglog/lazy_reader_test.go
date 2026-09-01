package seglog

import (
	"bytes"
	"testing"
)

type countingReaderAt struct {
	reader *bytes.Reader
	calls  int
	bytes  int
}

func (r *countingReaderAt) ReadAt(dst []byte, offset int64) (int, error) {
	r.calls++
	n, err := r.reader.ReadAt(dst, offset)
	r.bytes += n
	return n, err
}

func buildLazyRouteFile(t *testing.T, key [32]byte, logID [16]byte, segmentID, groupID uint64, base uint32, entries []routeEntry) ([]byte, sealedRunRef) {
	t.Helper()
	payload, err := appendRoutePayload(nil, entries)
	if err != nil {
		t.Fatal(err)
	}
	payloadOffset := base + sealedRouteDescriptorBytes
	file := make([]byte, int(payloadOffset)+len(payload))
	descriptor := routeDescriptor{PayloadOffset: payloadOffset, PayloadBytes: uint32(len(payload)), Entries: uint32(len(entries)), ExtentOffset: uint32(entries[0].ExtentOffset), ExtentBytes: uint32(entries[len(entries)-1].ExtentOffset + entries[len(entries)-1].ExtentBytes - entries[0].ExtentOffset)}
	if _, err = marshalRouteDescriptor(file[base:base+sealedRouteDescriptorBytes], descriptor, key, logID, segmentID, groupID, 0, payload); err != nil {
		t.Fatal(err)
	}
	copy(file[payloadOffset:], payload)
	return file, sealedRunRef{SegmentID: segmentID, GroupID: groupID, First: 1, Last: uint64(len(entries)), DescriptorBase: uint64(base), DescriptorCount: 1, BlockEntries: sealedDefaultBlockEntries, ExtentOffset: entries[0].ExtentOffset, ExtentBytes: uint64(descriptor.ExtentBytes)}
}

func TestLazyPointReadsOnlyOneRouteBlockAndCaches(t *testing.T) {
	key, logID := [32]byte{1}, [16]byte{2}
	entries := []routeEntry{{Term: 4, ExtentOffset: 4096, ExtentBytes: 128, DataBytes: 8}, {Term: 4, ExtentOffset: 4096, ExtentBytes: 128, DataOffset: 8, DataBytes: 12}}
	file, run := buildLazyRouteFile(t, key, logID, 7, 9, 128, entries)
	counted := &countingReaderAt{reader: bytes.NewReader(file)}
	reader, err := NewLazyRouteReader(counted, key, logID, 7, 2)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.Point(run, 2)
	if err != nil || got != entries[1] {
		t.Fatalf("point = %#v, %v", got, err)
	}
	payloadBytes := len(file) - 128 - sealedRouteDescriptorBytes
	if counted.calls != 2 || counted.bytes != sealedRouteDescriptorBytes+payloadBytes {
		t.Fatalf("cold route I/O calls=%d bytes=%d", counted.calls, counted.bytes)
	}
	before := reader.Metrics()
	if got, err = reader.Point(run, 1); err != nil || got != entries[0] {
		t.Fatalf("cached point = %#v, %v", got, err)
	}
	after := reader.Metrics()
	if counted.calls != 2 || after.CacheHits != before.CacheHits+1 {
		t.Fatalf("cache performed I/O: calls=%d metrics=%#v", counted.calls, after)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, pointErr := reader.Point(run, 1); pointErr != nil {
			panic(pointErr)
		}
	}); allocs != 0 {
		t.Fatalf("cached point allocs/run=%v", allocs)
	}
	coldCounted := &countingReaderAt{reader: bytes.NewReader(file)}
	cold, err := NewLazyRouteReader(coldCounted, key, logID, 7, 0)
	if err != nil {
		t.Fatal(err)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		if _, pointErr := cold.Point(run, 2); pointErr != nil {
			panic(pointErr)
		}
	}); allocs != 0 {
		t.Fatalf("cold authenticated point allocs/run=%v", allocs)
	}
}

func TestLazyRouteCacheEvictsWithinFixedCharge(t *testing.T) {
	key, logID := [32]byte{1}, [16]byte{2}
	entries := []routeEntry{{Term: 1, ExtentOffset: 1024, ExtentBytes: 64, DataBytes: 1}}
	firstFile, first := buildLazyRouteFile(t, key, logID, 1, 1, 64, entries)
	secondFile, second := buildLazyRouteFile(t, key, logID, 1, 2, 256, entries)
	file := make([]byte, len(secondFile))
	copy(file, firstFile)
	copy(file[256:], secondFile[256:])
	counted := &countingReaderAt{reader: bytes.NewReader(file)}
	reader, err := NewLazyRouteReader(counted, key, logID, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Point(first, 1); err != nil {
		t.Fatal(err)
	}
	if len(reader.cache.slots) != 1 || cap(reader.cache.slots[0].entries) != sealedDefaultBlockEntries {
		t.Fatalf("cache charge changed: %#v", reader.cache)
	}
	if _, err = reader.Point(second, 1); err != nil {
		t.Fatal(err)
	}
	if reader.cache.slots[0].groupID != 2 || reader.Metrics().CacheMisses != 2 {
		t.Fatalf("cache did not evict: %#v metrics=%#v", reader.cache.slots[0], reader.Metrics())
	}
}
