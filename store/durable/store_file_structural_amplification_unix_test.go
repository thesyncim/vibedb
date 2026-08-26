//go:build linux || darwin

package durable

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"golang.org/x/sys/unix"
)

// TestFilePrimaryStructuralChurnAmplification is the compact, always-on
// counterpart to the 10M qualification. It repeatedly removes complete routed
// leaves, reinserts their exact rows, checkpoints, and reopens. The gate binds
// all three different space observables: logical EOF, allocated filesystem
// blocks, and bytes actually handed to the durability device.
func TestFilePrimaryStructuralChurnAmplification(t *testing.T) {
	const (
		rows   = 8_192
		cycles = 8
	)
	builder, err := store.NewBuilder(store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, rows)
	values := make(map[string][]byte, rows)
	logicalBytes := uint64(0)
	for index := range rows {
		key := fmt.Sprintf("row-%08d", index)
		value := structuralAmplificationValue(index)
		keys[index] = key
		values[key] = value
		logicalBytes += uint64(len(key) + len(value))
		if err = builder.Append(key, value); err != nil {
			t.Fatal(err)
		}
	}
	built, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "structural-churn.vibe")
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{
		Backend: BackendPortable, Durability: DurabilityBufferedVisible,
		ResidentBytes: 64 << 20,
	}
	if _, err = CreateFromPrimary(built, file, options); err != nil {
		t.Fatal(err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if err = collection.Flush(); err != nil {
		t.Fatal(err)
	}
	before := collection.Stats()
	mutatedBytes := uint64(0)
	for cycle := range cycles {
		byBucket := make(map[uint32][]string)
		for _, key := range keys {
			route, ok := collection.primaryRouter.Load().Route([]byte(key))
			if !ok {
				t.Fatalf("cycle %d route %q", cycle, key)
			}
			byBucket[uint32(route.Bucket)] = append(byBucket[uint32(route.Bucket)], key)
		}
		buckets := make([]uint32, 0, len(byBucket))
		for bucket := range byBucket {
			buckets = append(buckets, bucket)
		}
		sort.Slice(buckets, func(left, right int) bool { return buckets[left] < buckets[right] })
		if len(buckets) < 3 {
			t.Fatalf("cycle %d has only %d routed leaves", cycle, len(buckets))
		}
		selected := byBucket[buckets[1+cycle%(len(buckets)-2)]]
		for _, key := range selected {
			value := values[key]
			deleted, deleteErr := collection.Delete([]byte(key))
			if deleteErr != nil || !deleted {
				t.Fatalf("cycle %d delete %q = %v,%v", cycle, key, deleted, deleteErr)
			}
			mutatedBytes += uint64(len(key) + len(value))
		}
		for _, key := range selected {
			value := values[key]
			created, putErr := collection.Put([]byte(key), value)
			if putErr != nil || !created {
				t.Fatalf("cycle %d reinsert %q = %v,%v", cycle, key, created, putErr)
			}
			mutatedBytes += uint64(len(key) + len(value))
		}
		if err = collection.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	after := collection.Stats()
	reclaims := after.PrimaryEmptyReclaims - before.PrimaryEmptyReclaims
	splits := after.PrimaryLeafSplits - before.PrimaryLeafSplits
	if reclaims == 0 {
		t.Fatalf("structural churn reclaimed no empty leaf: %+v -> %+v", before, after)
	}
	// Each reclaim in this fixture removes one row from a non-singleton routing
	// anchor, so it must take the localized COW path. Splits have the same base
	// rewrite set and may add one anchor only when the selected anchor is full.
	const routingBase = uint64(storeio.SegmentedTabletRouterAnchorPageBytes +
		storeio.GlobalTabletCatalogLocatorBytes +
		storeio.GlobalTabletCatalogTabletBytes)
	staged := after.PrimaryStructuralRoutingStagedBytes -
		before.PrimaryStructuralRoutingStagedBytes
	retired := after.PrimaryStructuralRoutingRetiredBytes -
		before.PrimaryStructuralRoutingRetiredBytes
	minStaged := (reclaims + splits) * routingBase
	maxStaged := minStaged + splits*storeio.SegmentedTabletRouterAnchorPageBytes
	if staged < minStaged || staged > maxStaged {
		t.Fatalf("localized churn routing staged bytes = %d, want [%d,%d] for %d reclaims and %d splits",
			staged, minStaged, maxStaged, reclaims, splits)
	}
	if want := (reclaims + splits) * routingBase; retired != want {
		t.Fatalf("localized churn routing retired bytes = %d, want %d for %d reclaims and %d splits",
			retired, want, reclaims, splits)
	}
	if after.DocumentCount != rows {
		t.Fatalf("live rows = %d, want %d", after.DocumentCount, rows)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err = unix.Fstat(int(file.Fd()), &stat); err != nil {
		t.Fatal(err)
	}
	allocated := uint64(stat.Blocks) * 512
	deviceBytes := after.DeviceBytes - before.DeviceBytes
	// These bounds are intentionally normalized, not fixture byte ceilings.
	// Structural churn must not retain more than 2x live bytes in either the
	// namespace or filesystem allocator, nor write more than 8x the exact
	// logical mutations it acknowledged.
	if uint64(info.Size()) > 2*logicalBytes {
		t.Fatalf("apparent amplification = %d/%d (>2x)", info.Size(), logicalBytes)
	}
	if allocated > 2*logicalBytes {
		t.Fatalf("allocated amplification = %d/%d (>2x)", allocated, logicalBytes)
	}
	if mutatedBytes == 0 || deviceBytes > 8*mutatedBytes {
		t.Fatalf("device write amplification = %d/%d (>8x)", deviceBytes, mutatedBytes)
	}
	t.Logf("structural churn ratios: apparent=%.3fx allocated=%.3fx device-write=%.3fx reclaims=%d splits=%d",
		float64(info.Size())/float64(logicalBytes), float64(allocated)/float64(logicalBytes),
		float64(deviceBytes)/float64(mutatedBytes),
		reclaims, splits)
	if err = collection.Close(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	file, err = os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.Len() != rows {
		t.Fatalf("reopened rows = %d, want %d", reopened.Len(), rows)
	}
	for _, index := range []int{0, rows / 3, rows / 2, rows - 1} {
		key := keys[index]
		got, found, readErr := reopened.AppendRaw(nil, []byte(key))
		if readErr != nil || !found || !bytes.Equal(got, values[key]) {
			t.Fatalf("reopened row %d = %q,%v,%v", index, got, found, readErr)
		}
	}
}

func structuralAmplificationValue(index int) []byte {
	const alphabet = "0123456789abcdef"
	value := make([]byte, 130)
	value[0], value[len(value)-1] = '"', '"'
	state := uint64(index+1)*0x9e3779b97f4a7c15 + 0x6a09e667f3bcc909
	for at := 1; at < len(value)-1; at++ {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		value[at] = alphabet[state&15]
	}
	return value
}
