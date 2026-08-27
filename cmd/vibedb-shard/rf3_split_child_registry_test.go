package main

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestRF3SplitChildRegistryDerivesInjectivePathsWithoutProvisioningOperations(t *testing.T) {
	root := filepath.Join(t.TempDir(), "split-children")
	registry := rf3ManifestSplitChildRegistry{Root: root}
	firstOperation, secondOperation := [32]byte{1}, [32]byte{2}
	first, err := registry.childPaths(firstOperation, 1)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := registry.childPaths(firstOperation, 1)
	if err != nil || retry != first {
		t.Fatalf("exact retry paths = %+v, %v", retry, err)
	}
	sibling, err := registry.childPaths(firstOperation, 2)
	if err != nil || sibling.Root == first.Root || sibling.Database == first.Database || sibling.WAL == first.WAL {
		t.Fatalf("sibling paths = %+v, %v", sibling, err)
	}
	other, err := registry.childPaths(secondOperation, 1)
	if err != nil || other.Root == first.Root {
		t.Fatalf("operation paths = %+v, %v", other, err)
	}
	for _, path := range []string{first.Root, first.Database, first.WAL} {
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) {
			t.Fatalf("derived path escaped registry: %q, %v", path, relErr)
		}
	}
	if entries, readErr := filepath.Glob(filepath.Join(root, "*")); readErr != nil || len(entries) != 0 {
		t.Fatalf("path derivation provisioned operation state: %v, %v", entries, readErr)
	}
	if _, err = registry.childPaths([32]byte{}, 1); err == nil {
		t.Fatal("zero operation accepted")
	}
	if _, err = registry.childPaths(firstOperation, 3); err == nil {
		t.Fatal("out-of-range child accepted")
	}
}

func TestRF3SplitChildTemplateMatchesExactRetainedApply(t *testing.T) {
	limits := durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20}
	ledgerStart, ledgerEnd, ledgerIdentity := [32]byte{0x20}, [32]byte{0x90}, [32]byte{0x5a}
	registry := rf3ManifestSplitChildRegistry{Table: "docs", Apply: rf3ManifestSplitChildApply{
		MaxSessions: 32, RetryWindow: 8, TxnLimits: limits, ShardKey: "id",
		RequestLedgerCapacityBytes: 64 << 20, RequestLedgerCleanupReserveBytes: 8 << 20,
		RequestLedgerRangeStart: ledgerStart, RequestLedgerRangeEnd: ledgerEnd,
		RequestLedgerRangeIdentity: ledgerIdentity,
		TupleVersion:               distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
	}}
	base := sqldriver.ReplicatedShardStoreIdentity{UserTable: "docs"}
	apply := sqldriver.ReplicatedApplyIdentity{
		MaxSessions: 32, RetryWindow: 8, TxnLimits: limits,
		RequestLedgerCapacityBytes: 64 << 20, RequestLedgerCleanupReserveBytes: 8 << 20,
		RequestLedgerRangeStart: ledgerStart, RequestLedgerRangeEnd: ledgerEnd,
		RequestLedgerRangeIdentity: ledgerIdentity,
		Placement: sqldriver.ReplicatedPlacementProfile{
			ShardKey: "id", TupleVersion: distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
		},
	}
	if !rf3SplitChildTemplateMatchesRetained(registry, base, apply) {
		t.Fatal("exact child template rejected")
	}
	registry.Apply.MaxSessions++
	if rf3SplitChildTemplateMatchesRetained(registry, base, apply) {
		t.Fatal("mismatched child capacity accepted")
	}
	registry.Apply.MaxSessions--
	registry.Apply.RequestLedgerRangeIdentity[0]++
	if rf3SplitChildTemplateMatchesRetained(registry, base, apply) {
		t.Fatal("mismatched child ledger authority accepted")
	}
}

func TestRF3SplitChildPathRegistryEnforcesFixedOperationCapacity(t *testing.T) {
	template := rf3ManifestSplitChildRegistry{
		Root: filepath.Join(t.TempDir(), "split-children"), MaxOperations: 1,
	}
	registry, err := newRF3SplitChildPathRegistry(template)
	if err != nil {
		t.Fatal(err)
	}
	first := [32]byte{1}
	if _, err = registry.acquire(first, 1); err != nil {
		t.Fatal(err)
	}
	if _, err = registry.acquire(first, 2); err != nil {
		t.Fatalf("same operation sibling rejected: %v", err)
	}
	second := [32]byte{2}
	if _, err = registry.acquire(second, 1); !errors.Is(err, errRF3SplitChildRegistryBound) {
		t.Fatalf("second operation error = %v", err)
	}
	if !registry.release(first) || registry.release(first) {
		t.Fatal("operation release was not exact")
	}
	if _, err = registry.acquire(second, 1); err != nil {
		t.Fatalf("released slot was not reusable: %v", err)
	}
}
