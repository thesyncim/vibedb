package durable

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

func TestFileStoreBulkFingerprintCreateReadMutateAndReopen(t *testing.T) {
	const rows = 500
	builder, err := store.NewBuilder(store.Options{
		ChunkDocuments: 16, ShapeTapes: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, rows)
	for row := range rows {
		keys[row] = fmt.Sprintf(
			"tenant/%04d/%s", row, strings.Repeat(string(rune('a'+row%26)), 120),
		)
		if err := builder.Append(
			keys[row], fmt.Appendf(nil, `{"row":%d,"value":"bulk"}`, row),
		); err != nil {
			t.Fatal(err)
		}
	}
	source, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	options := testFileStoreOptions()
	options.Collection.ChunkDocuments = 16
	options.MaxKeyBytes = 256
	file, err := os.CreateTemp(t.TempDir(), "bulk-fingerprint-reopen-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := CreateFrom(source, file, options); err != nil {
		t.Fatal(err)
	}

	collection, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	if got := collection.state.Load().root.KeyDirectory.Kind; got != storeio.PageFingerprintDirectory {
		t.Fatalf("bulk key root kind = %v", got)
	}
	for _, row := range []int{0, 1, 42, 43, 249, rows - 1} {
		raw, ok, err := collection.AppendRaw(nil, keys[row])
		if err != nil || !ok || !strings.Contains(string(raw), fmt.Sprintf(`"row":%d`, row)) {
			t.Fatalf("row %d = (%q,%v,%v)", row, raw, ok, err)
		}
	}
	if created, err := collection.Put(keys[249], []byte(`{"row":249,"value":"updated"}`)); err != nil || created {
		t.Fatalf("update = (%v,%v)", created, err)
	}
	if deleted, err := collection.Delete(keys[42]); err != nil || !deleted {
		t.Fatalf("delete = (%v,%v)", deleted, err)
	}
	if err := collection.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, ok, err := reopened.AppendRaw(nil, keys[42]); err != nil || ok {
		t.Fatalf("deleted key after reopen = (%v,%v)", ok, err)
	}
	raw, ok, err := reopened.AppendRaw(nil, keys[249])
	if err != nil || !ok || !strings.Contains(string(raw), `"updated"`) {
		t.Fatalf("updated key after reopen = (%q,%v,%v)", raw, ok, err)
	}
}

func TestFileStoreBulkFingerprintPlannerBalancesFixedWidthLeavesAndBranches(t *testing.T) {
	const (
		pageSize = 4096
		rows     = 25_000
	)
	build := fileStoreBulkBuild{
		options: normalizedFileStoreOptions{
			Options: Options{PageSize: pageSize},
		},
		allocator: fileStoreBulkAllocator{
			offset: uint64(testMutableDataStart(pageSize)), nextLogical: storeio.StateRootLogicalID + 1,
			generation: 1, pageSize: pageSize,
		},
		keyRows: make([]storeio.PageKeyLocation, rows),
	}
	for row := range build.keyRows {
		build.keyRows[row] = storeio.PageKeyLocation{
			// Runs longer than a leaf prove equal-hash routing across pages.
			Hash: uint64(row / 400), Chunk: uint32(row / 64), Slot: uint8(row % 64),
		}
	}
	if err := build.planFingerprintKeys(); err != nil {
		t.Fatal(err)
	}
	if build.keyRoot.Kind != storeio.PageFingerprintDirectory {
		t.Fatalf("key root kind = %v, want fingerprint directory", build.keyRoot.Kind)
	}

	leafCount := 0
	maxLevel := uint8(0)
	minimumLeafBody := fileStoreBulkFingerprintMinimumBody(pageSize)
	branchCapacity := fileStoreBulkFingerprintBranchCapacity(pageSize)
	minimumBranch := (branchCapacity + 1) / 2
	for _, plan := range build.keys {
		if plan.ref.Kind != storeio.PageFingerprintDirectory {
			t.Fatalf("planned page kind = %v", plan.ref.Kind)
		}
		maxLevel = max(maxLevel, plan.level)
		if plan.level == 0 {
			leafCount++
			part := build.keyRows[plan.first:plan.last]
			if size := storeio.PageKeyLeafEncodedSize(part); size > pageSize {
				t.Fatalf("leaf encoded size = %d", size)
			}
			body := storeio.PageKeyLeafEncodedSize(part) -
				storeio.PageHeaderSize - storeio.PageTrailerSize -
				storeio.PageKeyDirectoryPayloadHeaderSize
			if plan.ref != build.keyRoot && body < minimumLeafBody {
				t.Fatalf("nonroot leaf body = %d, want >= %d", body, minimumLeafBody)
			}
			continue
		}
		if len(plan.children) > branchCapacity {
			t.Fatalf("branch children = %d, capacity %d", len(plan.children), branchCapacity)
		}
		if plan.ref != build.keyRoot && len(plan.children) < minimumBranch {
			t.Fatalf("nonroot branch children = %d, want >= %d", len(plan.children), minimumBranch)
		}
	}
	if leafCount < branchCapacity || maxLevel < 2 {
		t.Fatalf("tree shape leaves=%d maxLevel=%d, want multi-level", leafCount, maxLevel)
	}
}

func TestFileStoreBulkFingerprintWritesCollisionRunInExactLocationOrder(t *testing.T) {
	const (
		pageSize = 4096
		rows     = 700
	)
	build := fileStoreBulkBuild{
		storeID: [16]byte{1, 3, 3, 7},
		options: normalizedFileStoreOptions{
			Options: Options{
				PageSize: pageSize, Collection: store.Options{ChunkDocuments: 64},
			},
		},
		allocator: fileStoreBulkAllocator{
			offset: uint64(testMutableDataStart(pageSize)), nextLogical: storeio.StateRootLogicalID + 1,
			generation: 1, pageSize: pageSize,
		},
		documents: make([]fileStoreBulkDocumentPlan, (rows+63)/64),
		keyRows:   make([]storeio.PageKeyLocation, rows),
	}
	for row := range build.keyRows {
		build.keyRows[row] = storeio.PageKeyLocation{
			Hash: 77, Chunk: uint32(row / 64), Slot: uint8(row % 64),
		}
	}
	if err := build.planFingerprintKeys(); err != nil {
		t.Fatal(err)
	}
	build.fileEnd = build.allocator.offset

	file, err := os.CreateTemp(t.TempDir(), "bulk-fingerprint-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(int64(build.fileEnd)); err != nil {
		t.Fatal(err)
	}
	if err := build.writeKeyPages(file, make([]byte, pageSize)); err != nil {
		t.Fatal(err)
	}

	got := make([]storeio.PageKeyLocation, 0, rows)
	leafPlans := make([]fileStoreBulkKeyPlan, 0, 4)
	for _, plan := range build.keys {
		page := make([]byte, plan.ref.Length)
		if _, err := file.ReadAt(page, int64(plan.ref.Offset)); err != nil {
			t.Fatal(err)
		}
		view, err := storeio.OpenPageFingerprintDirectory(
			page, build.fileEnd, build.allocator.nextLogical,
			uint32(len(build.documents)), 64,
		)
		if err != nil {
			t.Fatalf("open typed plan level %d: %v", plan.level, err)
		}
		if _, err := storeio.OpenPageKeyDirectory(
			page, build.fileEnd, build.allocator.nextLogical,
			uint32(len(build.documents)), 64,
		); err == nil {
			t.Fatal("fingerprint page admitted as legacy key page")
		}
		if plan.level != 0 {
			continue
		}
		leafPlans = append(leafPlans, plan)
		for rank := range view.Len() {
			location, ok := view.LocationAt(rank)
			if !ok {
				t.Fatalf("missing leaf rank %d", rank)
			}
			got = append(got, location)
		}
	}
	if len(leafPlans) < 2 {
		t.Fatalf("collision leaves = %d, want multiple", len(leafPlans))
	}
	for i := range leafPlans {
		wantNext := storeio.PageRef{}
		if i+1 < len(leafPlans) {
			wantNext = leafPlans[i+1].ref
		}
		if leafPlans[i].next != wantNext {
			t.Fatalf("leaf %d next = %+v, want %+v", i, leafPlans[i].next, wantNext)
		}
	}
	if len(got) != len(build.keyRows) {
		t.Fatalf("decoded rows = %d, want %d", len(got), len(build.keyRows))
	}
	for i := range got {
		if got[i] != build.keyRows[i] {
			t.Fatalf("decoded row %d = %+v, want %+v", i, got[i], build.keyRows[i])
		}
	}
}

func fileStoreBulkFingerprintMinimumBody(pageSize int) int {
	usable := pageSize - storeio.PageHeaderSize -
		storeio.PageTrailerSize - storeio.PageKeyDirectoryPayloadHeaderSize
	capacity := usable / storeio.PageKeyLeafEntrySize
	return ((capacity + 1) / 2) * storeio.PageKeyLeafEntrySize
}

func fileStoreBulkFingerprintBranchCapacity(pageSize int) int {
	return (pageSize - storeio.PageHeaderSize -
		storeio.PageTrailerSize - storeio.PageKeyDirectoryPayloadHeaderSize) /
		storeio.PageKeyBranchEntrySize
}
