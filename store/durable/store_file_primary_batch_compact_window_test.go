package durable

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// TestPrimaryBatchCompactWindowKeepsUnindexedWideStripe is the deterministic
// regression for the runtime's historical 256-row topology cap. These small
// shared documents fit comfortably in one compact 64 KiB leaf, so a split is
// caused only by the old count limit.
func TestPrimaryBatchCompactWindowKeepsUnindexedWideStripe(t *testing.T) {
	const rows = 300
	options := primaryLargeTopologyOptions(rows)
	collection, _ := openBatchCollection(t, options)
	start := collection.Stats()

	if err := collection.Update(func(batch *WriteBatch) error {
		for row := range rows {
			key := fmt.Appendf(nil, "wide-%04d", row)
			if err := batch.Put(key, []byte(`{"group":"shared","value":1}`)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("wide unindexed Update: %v", err)
	}
	if got := collection.Len(); got != rows {
		t.Fatalf("live rows = %d, want %d", got, rows)
	}
	if got := collection.Stats().PrimaryLeafSplits - start.PrimaryLeafSplits; got != 0 {
		t.Fatalf("compact unindexed rows needed %d structural splits, want 0", got)
	}
}

func TestPrimaryBatchCompactWindowMixedWideMutations(t *testing.T) {
	const rows = 300
	options := primaryLargeTopologyOptions(rows + 1)
	options.MaxBatchBytes = 1 << 20
	options.InlineValueBytes = 4 << 10
	options.MaxDocumentBytes = 4 << 10
	collection, file := openBatchCollection(t, options)
	want := make(map[string][]byte, rows+1)
	for row := range rows {
		key := fmt.Sprintf("wide-%04d", row)
		value := []byte(`{"group":"shared","value":1}`)
		want[key] = value
	}
	if err := collection.Update(func(batch *WriteBatch) error {
		for row := range rows {
			key := fmt.Appendf(nil, "wide-%04d", row)
			if err := batch.Put(key, want[string(key)]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed wide unindexed Update: %v", err)
	}
	if got := collection.primaryRouter.Load().Len(); got != 1 {
		t.Fatalf("seed router leaves = %d, want one compact wide leaf", got)
	}
	start := collection.Stats()
	before, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer before.Close()

	last := []byte(`{"group":"last","value":2}`)
	reinserted := []byte(`{"group":"reinserted","value":4}`)
	shrunk := []byte(`{"group":"shrunk"}`)
	inserted := []byte(`{"group":"inserted","value":5}`)
	if err := collection.Update(func(batch *WriteBatch) error {
		key := []byte("wide-0001")
		if err := batch.Put(key, []byte(fmt.Sprintf(
			`{"group":"grown","payload":%q}`, strings.Repeat("g", 1024),
		))); err != nil {
			return err
		}
		// Duplicate mutations publish only the final value.
		if err := batch.Put(key, last); err != nil {
			return err
		}
		if err := batch.Delete([]byte("wide-0002")); err != nil {
			return err
		}
		if err := batch.Put([]byte("wide-0002"), reinserted); err != nil {
			return err
		}
		if err := batch.Put([]byte("wide-0003"), shrunk); err != nil {
			return err
		}
		if err := batch.Delete([]byte("wide-0004")); err != nil {
			return err
		}
		return batch.Put([]byte("wide-0300"), inserted)
	}); err != nil {
		t.Fatalf("mixed wide Update: %v", err)
	}
	delete(want, "wide-0004")
	want["wide-0001"] = last
	want["wide-0002"] = reinserted
	want["wide-0003"] = shrunk
	want["wide-0300"] = inserted
	if got := collection.Stats().PrimaryLeafSplits - start.PrimaryLeafSplits; got != 0 {
		t.Fatalf("small mixed batch split a compact wide leaf %d times, want 0", got)
	}
	assertPrimaryBatchCompactSnapshot(t, before, seedPrimaryBatchCompactOracle(rows))
	after, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	assertPrimaryBatchCompactSnapshot(t, after, want)
	if err := after.Close(); err != nil {
		t.Fatal(err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	if err := collection.Flush(); err != nil {
		t.Fatalf("flush mixed wide rows: %v", err)
	}
	if err := collection.Close(); err != nil {
		t.Fatalf("close mixed wide collection: %v", err)
	}
	reopened, err := Open(file, options)
	if err != nil {
		t.Fatalf("reopen mixed wide collection: %v", err)
	}
	defer reopened.Close()
	reopenedSnapshot, err := reopened.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer reopenedSnapshot.Close()
	assertPrimaryBatchCompactSnapshot(t, reopenedSnapshot, want)
	if report, err := Verify(file); err != nil || !report.OK() {
		t.Fatalf("Verify after mixed wide reopen = %+v, %v", report, err)
	}
}

func TestPrimaryBatchCompactWindowSplitsAtFormatRowLimit(t *testing.T) {
	const rows = storeio.CompactPrimaryStripeMaxRows + 1
	options := primaryLargeTopologyOptions(rows)
	options.MaxBatchBytes = 2 << 20
	options.ResidentBytes = 512 << 20
	options.InlineValueBytes = 256
	options.MaxDocumentBytes = 256
	collection, _ := openBatchCollection(t, options)
	start := collection.Stats()
	if err := collection.Update(func(batch *WriteBatch) error {
		for row := range rows {
			key := fmt.Appendf(nil, "limit-%05d", row)
			if err := batch.Put(key, []byte(`{"v":1}`)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update beyond compact format row limit: %v", err)
	}
	if got := collection.Len(); got != rows {
		t.Fatalf("live rows = %d, want %d", got, rows)
	}
	if got := collection.Stats().PrimaryLeafSplits - start.PrimaryLeafSplits; got == 0 {
		t.Fatal("Update above CompactPrimaryStripeMaxRows did not split")
	}
	router := collection.primaryRouter.Load()
	for rank := 0; rank < router.Len(); rank++ {
		route, ok := router.RouteAtRank(rank)
		if !ok {
			t.Fatalf("route rank %d", rank)
		}
		lease, err := collection.cache.Acquire(route.Ref)
		if err != nil {
			t.Fatal(err)
		}
		stripe, valid := storeio.AdmittedCompactPrimaryStripe(
			lease.Page(), collection.storeID, route.Bucket,
		)
		lease.Release()
		if !valid || stripe.Len() > storeio.CompactPrimaryStripeMaxRows {
			t.Fatalf("route %d compact rows=%d valid=%v", rank, stripe.Len(), valid)
		}
	}
}

func TestPrimaryBatchCompactWindowKeepsDeclaredSlotGeometry(t *testing.T) {
	for _, tc := range []struct {
		name       string
		definition store.IndexDefinition
		wantExact  bool
	}{
		{name: "exact", definition: store.IndexDefinition{
			Name: "by_group", Paths: []string{"/group"},
		}, wantExact: true},
		{name: "tin-declared", definition: store.IndexDefinition{
			Name: "group_tin", Paths: []string{"/group"}, Kind: store.IndexTin,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const rows = 300
			options := primaryLargeTopologyOptions(rows + storeio.CommonPrimaryLeafWideSlots)
			collection, _ := openBatchCollection(t, options)
			if err := collection.Update(func(batch *WriteBatch) error {
				for row := range rows {
					key := fmt.Appendf(nil, "wide-%04d", row)
					if err := batch.Put(key, []byte(`{"group":"shared"}`)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("seed wide rows: %v", err)
			}
			if got := collection.primaryRouter.Load().Len(); got != 1 {
				t.Fatalf("unindexed seed leaves = %d, want one wide leaf", got)
			}
			initialRoute, _ := collection.primaryRouter.Load().RouteAtRank(0)
			initialLease, err := collection.cache.Acquire(initialRoute.Ref)
			if err != nil {
				t.Fatal(err)
			}
			initialStripe, valid := storeio.AdmittedCompactPrimaryStripe(
				initialLease.Page(), collection.storeID, initialRoute.Bucket,
			)
			initialRows := initialStripe.Len()
			initialLease.Release()
			if !valid || initialRows <= storeio.CommonPrimaryLeafWideSlots {
				t.Fatalf("unindexed seed rows=%d valid=%v, want >256", initialRows, valid)
			}
			if _, err := collection.CreateIndex(tc.definition); err != nil {
				t.Fatalf("declare %s index: %v", tc.name, err)
			}
			state := collection.state.Load()
			if !primarySlotGeometryMaintained(state.root) {
				t.Fatalf("%s declaration did not enable slot geometry", tc.name)
			}
			if tc.wantExact && state.root.IndexCount == 0 {
				t.Fatal("exact declaration has no physical index")
			}
			if !tc.wantExact && (state.root.IndexCount != 0 ||
				state.root.Options&storeio.StateOptionTinIndexes == 0) {
				t.Fatalf("tin-only root index count/options = %d/%#x", state.root.IndexCount, state.root.Options)
			}
			router := collection.primaryRouter.Load()
			var insertKeys [][]byte
			for rank := 0; rank < router.Len() && len(insertKeys) == 0; rank++ {
				route, ok := router.RouteAtRank(rank)
				if !ok {
					t.Fatalf("route rank %d", rank)
				}
				lease, err := collection.cache.Acquire(route.Ref)
				if err != nil {
					t.Fatal(err)
				}
				stripe, valid := storeio.AdmittedCompactPrimaryStripe(
					lease.Page(), collection.storeID, route.Bucket,
				)
				if !valid {
					lease.Release()
					t.Fatalf("route rank %d has invalid stripe", rank)
				}
				leafRows := stripe.Len()
				if leafRows > 0 && leafRows <= storeio.CommonPrimaryLeafWideSlots {
					baseKey, found := stripe.AppendKey(nil, 0)
					lease.Release()
					if !found {
						t.Fatalf("route rank %d missing first key", rank)
					}
					needed := storeio.CommonPrimaryLeafWideSlots + 1 - leafRows
					candidates := make([][]byte, 0, needed)
					for at := range needed {
						candidate := fmt.Appendf(nil, "%s-insert-%04d", baseKey, at)
						resident, routeErr := collection.currentPrimaryResidentRoute(
							state, candidate,
						)
						if routeErr != nil || resident.Bucket != route.Bucket {
							candidates = nil
							break
						}
						candidates = append(candidates, candidate)
					}
					if len(candidates) == needed {
						insertKeys = candidates
					}
					continue
				}
				lease.Release()
			}
			if len(insertKeys) == 0 {
				t.Fatal("declared geometry has no leaf interval for crossing 256 rows")
			}
			if err := collection.Update(func(batch *WriteBatch) error {
				for _, key := range insertKeys {
					if err := batch.Put(key, []byte(`{"group":"shared"}`)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				t.Fatalf("insert past 256 in a maintained leaf: %v", err)
			}
			for rank := 0; rank < collection.primaryRouter.Load().Len(); rank++ {
				route, ok := collection.primaryRouter.Load().RouteAtRank(rank)
				if !ok {
					t.Fatalf("post-update route rank %d", rank)
				}
				lease, err := collection.cache.Acquire(route.Ref)
				if err != nil {
					t.Fatal(err)
				}
				stripe, valid := storeio.AdmittedCompactPrimaryStripe(
					lease.Page(), collection.storeID, route.Bucket,
				)
				leafRows := stripe.Len()
				lease.Release()
				if !valid || leafRows > storeio.CommonPrimaryLeafWideSlots {
					t.Fatalf("post-update route %d rows=%d valid=%v; slot geometry must cap at 256", rank, leafRows, valid)
				}
			}
			if tc.wantExact {
				got := primaryExactTestKeys(
					t, collection, "by_group", primaryExactTestNeedle(t, `"shared"`),
				)
				if len(got) != rows+len(insertKeys) {
					t.Fatalf("exact keys=%d, want %d", len(got), rows+len(insertKeys))
				}
			} else {
				snapshot, err := collection.Snapshot()
				if err != nil {
					t.Fatalf("snapshot tin-maintained rows: %v", err)
				}
				hits, searchErr := collection.TinSearch(
					snapshot, "/group", "shared", rows+len(insertKeys),
				)
				closeErr := snapshot.Close()
				if searchErr != nil {
					t.Fatalf("TinSearch after wide-leaf update: %v", searchErr)
				}
				if closeErr != nil {
					t.Fatalf("close tin-search snapshot: %v", closeErr)
				}
				if len(hits) != rows+len(insertKeys) {
					t.Fatalf("tin hits=%d, want %d", len(hits), rows+len(insertKeys))
				}
			}
		})
	}
}

func seedPrimaryBatchCompactOracle(rows int) map[string][]byte {
	want := make(map[string][]byte, rows)
	for row := range rows {
		want[fmt.Sprintf("wide-%04d", row)] = []byte(`{"group":"shared","value":1}`)
	}
	return want
}

func assertPrimaryBatchCompactSnapshot(
	t testing.TB, snapshot *Snapshot, want map[string][]byte,
) {
	t.Helper()
	got := make(map[string][]byte, len(want))
	if err := snapshot.RangeRaw(func(key, value []byte) error {
		got[string(key)] = bytes.Clone(value)
		return nil
	}); err != nil {
		t.Fatalf("scan compact-window snapshot: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("snapshot rows=%d, want %d", len(got), len(want))
	}
	for key, value := range want {
		if actual, ok := got[key]; !ok || !bytes.Equal(actual, value) {
			t.Fatalf("snapshot %q=%q, found=%v; want %q", key, actual, ok, value)
		}
	}
}
