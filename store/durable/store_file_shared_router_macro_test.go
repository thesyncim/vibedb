package durable

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// TestFilePrimarySharedRouterMacroSplitSnapshotAndReopen exercises the
// non-localized structural path at the bounded anchor geometry limit. The old
// router image and snapshot must remain complete while the published router
// replaces one tablet range with routes spanning the original and spill
// tablets; the same complete image must survive reopen.
func TestFilePrimarySharedRouterMacroSplitSnapshotAndReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("anchor-boundary durable regression is omitted from short tests")
	}
	const (
		groups       = 144
		rowsPerGroup = 3
		seeded       = groups * rowsPerGroup
		rowPayload   = 50_000
		batchRows    = 16
	)
	options := primaryLargeTopologyOptions(batchRows)
	options.ResidentBytes = 128 << 20
	options.MaxKeyBytes = 256
	options.InlineValueBytes = 60 << 10
	options.MaxDocumentBytes = 60 << 10
	options.MaxBatchBytes = 8 << 20

	records := make([]PrimaryBulkBytesRecord, 0, seeded)
	want := make(map[string][]byte, seeded+batchRows)
	for group := range groups {
		for row := range rowsPerGroup {
			key := bytes.Repeat([]byte{'p'}, 128)
			key[0] = byte(group)
			key[len(key)-1] = byte(row + 1)
			value := fmt.Appendf(nil, `{"kind":"seed","n":%d,"payload":"`, group*rowsPerGroup+row)
			value = appendWideJSONSafePattern(value, rowPayload, group*rowsPerGroup+row)
			value = append(value, `"}`...)
			records = append(records, PrimaryBulkBytesRecord{Key: key, Value: value})
			want[string(key)] = bytes.Clone(value)
		}
	}

	file, err := os.CreateTemp(t.TempDir(), "shared-router-macro-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFromByteRecords(records, file, options); err != nil {
		t.Fatalf("CreateFromByteRecords(%d): %v", seeded, err)
	}
	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	oldSnapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	oldRouter := oldSnapshot.primaryRouter
	if oldRouter == nil || oldRouter.Len() != seeded {
		t.Fatalf("old router leaves = %v, want %d", oldRouter, seeded)
	}
	type capturedRoute struct {
		key   []byte
		route storeio.ResidentPrimaryRoute
	}
	captured := make([]capturedRoute, len(records))
	for at := range records {
		route, ok := oldRouter.Route(records[at].Key)
		if !ok {
			t.Fatalf("capture route %d", at)
		}
		captured[at] = capturedRoute{key: records[at].Key, route: route}
	}

	base := bytes.Repeat([]byte{'p'}, 128)
	base[0] = 40
	base[len(base)-1] = 1
	batchKeys := make([][]byte, batchRows)
	batchValues := make([][]byte, batchRows)
	for at := range batchRows {
		batchKeys[at] = append(bytes.Clone(base), fmt.Appendf(nil, "-batch-%02d", at)...)
		value := fmt.Appendf(nil, `{"kind":"batch","n":%d,"payload":"`, at)
		value = appendWideJSONSafePattern(value, rowPayload, at*17+3)
		batchValues[at] = append(value, `"}`...)
		want[string(batchKeys[at])] = bytes.Clone(batchValues[at])
	}
	before := collection.Stats()
	if err := collection.Update(func(batch *WriteBatch) error {
		for at := range batchRows {
			if err := batch.Put(batchKeys[at], batchValues[at]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("macro Update: %v", err)
	}
	after := collection.Stats()
	if after.PrimaryMacroSplits <= before.PrimaryMacroSplits {
		t.Fatalf("Update did not take macro path: before=%d after=%d required=%d",
			before.PrimaryMacroSplits, after.PrimaryMacroSplits, after.PrimaryMacroSplitRequired)
	}
	if got := after.PrimaryTabletRoutingRebuilds - before.PrimaryTabletRoutingRebuilds; got == 0 {
		t.Fatal("macro Update did not exercise the non-localized tablet rewrite")
	}
	for at, old := range captured {
		got, ok := oldRouter.Route(old.key)
		if !ok || got.Ref != old.route.Ref || got.Bucket != old.route.Bucket {
			t.Fatalf("old router route %d mutated: got=%+v,%v want=%+v", at, got, ok, old.route)
		}
	}
	if oldRouter.Generation() != oldSnapshot.Generation() {
		t.Fatalf("old router generation=%d snapshot=%d", oldRouter.Generation(), oldSnapshot.Generation())
	}
	postRouter := collection.primaryRouter.Load()
	if postRouter == nil || postRouter == oldRouter {
		t.Fatalf("published macro router = %p, old = %p", postRouter, oldRouter)
	}
	tablets := make(map[uint32]struct{})
	for rank := range postRouter.Len() {
		route, ok := postRouter.RouteAtRank(rank)
		if !ok {
			t.Fatalf("published route rank %d", rank)
		}
		byBucket, bucketOK := postRouter.ResolveBucketID(route.Bucket)
		if !bucketOK || byBucket.Ref != route.Ref || byBucket.Bucket != route.Bucket {
			t.Fatalf("published bucket %d = %+v,%v, want rank route %+v",
				route.Bucket, byBucket, bucketOK, route)
		}
		tablet, _, ok := storeio.SplitTabletLocalIdentityBucket(uint32(route.Bucket))
		if !ok {
			t.Fatalf("published invalid bucket %d", route.Bucket)
		}
		tablets[tablet] = struct{}{}
	}
	if len(tablets) < 2 {
		t.Fatalf("macro router spans %d tablets, want at least two", len(tablets))
	}

	assertRows := func(label string, snapshot *Snapshot, expected map[string][]byte) {
		t.Helper()
		seen := 0
		if err := snapshot.RangeRaw(func(key, value []byte) error {
			wantValue, ok := expected[string(key)]
			if !ok || !bytes.Equal(value, wantValue) {
				t.Fatalf("%s row %q = %q, expected=%v", label, key, value, ok)
			}
			seen++
			return nil
		}); err != nil {
			t.Fatalf("%s RangeRaw: %v", label, err)
		}
		if seen != len(expected) {
			t.Fatalf("%s rows = %d, want %d", label, seen, len(expected))
		}
	}
	seedWant := make(map[string][]byte, seeded)
	for _, record := range records {
		seedWant[string(record.Key)] = record.Value
	}
	assertRows("old snapshot", oldSnapshot, seedWant)
	for _, key := range batchKeys {
		if _, found, readErr := oldSnapshot.AppendRaw(nil, key); readErr != nil || found {
			t.Fatalf("old snapshot batch %q = found %v, err %v", key, found, readErr)
		}
	}
	current, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertRows("current snapshot", current, want)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	if err := oldSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen macro image: %v", err)
	}
	defer reopened.Close()
	reopenedSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSnapshot.Close()
	assertRows("reopened snapshot", reopenedSnapshot, want)
	if report, verifyErr := Verify(file); verifyErr != nil || !report.OK() {
		t.Fatalf("Verify after macro reopen = %+v, %v", report, verifyErr)
	}
}
