package gateway

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestReplicatedCatalogPreparedReadPublicationConverges(t *testing.T) {
	for _, state := range []string{"empty", "older", "same"} {
		t.Run(state, func(t *testing.T) {
			next := testCatalogAuthoritySnapshot(t, 2)
			var current *Snapshot
			switch state {
			case "older":
				current = testCatalogAuthoritySnapshot(t, 1)
			case "same":
				current = next
			}
			authority := &ReplicatedCatalogAuthority{holder: NewCatalogHolder(current)}
			raw, err := appendReplicatedCatalogDocument(nil, next, maxReplicatedCatalogBytes)
			if err != nil {
				t.Fatal(err)
			}
			// Both reads finish certification against the same old holder before
			// either publishes. Executing the closures in order fixes the race's
			// winning and losing publishers without relying on goroutine timing.
			_, first, err := authority.prepareReadCatalogCut(t.Context(), next, raw)
			if err != nil {
				t.Fatal(err)
			}
			_, second, err := authority.prepareReadCatalogCut(t.Context(), next, raw)
			if err != nil {
				t.Fatal(err)
			}
			if err = first(); err != nil {
				t.Fatalf("first certified read: %v", err)
			}
			installed := authority.holder.Current()
			if err = second(); err != nil {
				t.Fatalf("concurrent byte-identical certified read: %v", err)
			}
			if authority.holder.Current() != installed {
				t.Fatal("duplicate read republished the installed snapshot")
			}
		})
	}
}

func TestReplicatedCatalogPreparedReadPublicationRejectsDivergentHead(t *testing.T) {
	for _, state := range []string{"empty", "older"} {
		t.Run(state, func(t *testing.T) {
			var current *Snapshot
			if state == "older" {
				current = testCatalogAuthoritySnapshot(t, 1)
			}
			authority := &ReplicatedCatalogAuthority{holder: NewCatalogHolder(current)}
			next := testCatalogAuthoritySnapshot(t, 2)
			raw, err := appendReplicatedCatalogDocument(nil, next, maxReplicatedCatalogBytes)
			if err != nil {
				t.Fatal(err)
			}
			_, publish, err := authority.prepareReadCatalogCut(t.Context(), next, raw)
			if err != nil {
				t.Fatal(err)
			}
			divergent := testCatalogAuthoritySnapshot(t, next.Generation())
			divergent.endpoints["ep-a-native"] = "127.0.0.1:6554"
			if err = authority.holder.publishNewerChecked(divergent); err != nil {
				t.Fatal(err)
			}
			installed := authority.holder.Current()
			if err = publish(); !errors.Is(err, ErrReplicatedCatalogConflict) {
				t.Fatalf("equal-generation divergent head publication: %v", err)
			}
			if authority.holder.Current() != installed {
				t.Fatal("conflicting read replaced the installed snapshot")
			}
		})
	}
}

func TestReplicatedCatalogPreparedReadPublicationRejectsSupersededCut(t *testing.T) {
	authority := &ReplicatedCatalogAuthority{
		holder: NewCatalogHolder(testCatalogAuthoritySnapshot(t, 1)),
	}
	next := testCatalogAuthoritySnapshot(t, 2)
	raw, err := appendReplicatedCatalogDocument(nil, next, maxReplicatedCatalogBytes)
	if err != nil {
		t.Fatal(err)
	}
	_, publish, err := authority.prepareReadCatalogCut(t.Context(), next, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.holder.publishNewerChecked(testCatalogAuthoritySnapshot(t, 3)); err != nil {
		t.Fatal(err)
	}
	installed := authority.holder.Current()
	if err = publish(); !errors.Is(err, ErrCatalogGenerationNotNewer) {
		t.Fatalf("superseded publication: %v", err)
	}
	if authority.holder.Current() != installed {
		t.Fatal("stale read replaced the installed snapshot")
	}
}

func TestReplicatedCatalogAttestedReadPreservesExactCutDuringPublication(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	genesis := testCatalogAuthoritySnapshot(t, 1)
	tracker := &replicatedCatalogRouteSeedTracker{
		immutable: genesis, active: current, activeExists: true,
		shutdown: make(chan struct{}),
	}
	authority.routeSeed.Store(tracker)
	tracker.mu.Lock()
	locked := true
	defer func() {
		if locked {
			tracker.mu.Unlock()
		}
	}()
	type result struct {
		receipt ReplicatedCatalogSeedReceipt
		err     error
	}
	completed := make(chan result, 1)
	go func() {
		receipt, err := authority.ReadAttested(t.Context(), genesis)
		completed <- result{receipt: receipt, err: err}
	}()
	// readAttested acquires authority.mu only after building its exact receipt.
	// Holding tracker.mu then observing authority.mu held proves the read is
	// suspended at tracker observation, before its publication completes.
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for authority.mu.TryLock() {
		authority.mu.Unlock()
		select {
		case got := <-completed:
			t.Fatalf("attested read completed before the publication barrier: %v", got.err)
		case <-deadline.C:
			t.Fatal("attested read did not reach the publication barrier")
		default:
			runtime.Gosched()
		}
	}
	if err := authority.holder.publishNewerChecked(
		testCatalogAuthoritySnapshot(t, current.Generation()+1),
	); err != nil {
		t.Fatal(err)
	}
	tracker.mu.Unlock()
	locked = false
	var got result
	select {
	case got = <-completed:
	case <-deadline.C:
		t.Fatal("attested read did not finish after publication")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	receipt := got.receipt
	if receipt.Snapshot().Generation() != current.Generation() {
		t.Fatalf("receipt changed its certified cut: got generation %d, want %d",
			receipt.Snapshot().Generation(), current.Generation())
	}
	canonical, err := AppendSnapshotDocument(nil, receipt.Snapshot())
	if err != nil || !bytes.Equal(canonical, receipt.canonical) {
		t.Fatalf("receipt snapshot differs from its canonical image: %v", err)
	}
	head, err := appendReplicatedCatalogDocument(nil, receipt.Snapshot(), maxReplicatedCatalogBytes)
	if err != nil || uint64(len(head)) != receipt.headBytes || sha256.Sum256(head) != receipt.headDigest {
		t.Fatalf("receipt snapshot differs from its certified head witness: %v", err)
	}
	if authority.holder.Current().Generation() != current.Generation()+1 {
		t.Fatal("attested read rolled back the concurrent holder publication")
	}
	path := filepath.Join(t.TempDir(), "catalog-route.vibejson")
	if err = authority.StageReplicatedCatalogRouteSeedAfter(path, 0, receipt); err != nil {
		t.Fatalf("exact attested receipt cannot stage its route seed: %v", err)
	}
}
