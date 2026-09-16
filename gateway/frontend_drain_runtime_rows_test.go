package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

type frontendDrainRuntimeRowsFixture struct {
	route FrontendDrainRuntimeCatalogRoute
	rows  map[string]FrontendDrainRuntimeRow
}

func (fixture *frontendDrainRuntimeRowsFixture) ReadFrontendDrainRuntimeCatalogRoute(
	context.Context,
) (FrontendDrainRuntimeCatalogRoute, error) {
	return fixture.route, nil
}

func (fixture *frontendDrainRuntimeRowsFixture) ReadFrontendDrainRuntimeRow(
	_ context.Context, key FrontendDrainRuntimeRowKey,
) (FrontendDrainRuntimeRow, error) {
	encoded, ok := key.EncodedKey()
	if !ok {
		return FrontendDrainRuntimeRow{}, ErrReplicatedCatalog
	}
	row, found := fixture.rows[string(encoded)]
	if !found {
		return FrontendDrainRuntimeRow{Applied: 1, Fence: fixture.rowFence()}, nil
	}
	row.Value = bytes.Clone(row.Value)
	return row, nil
}

func (fixture *frontendDrainRuntimeRowsFixture) rowFence() raftservice.ServingFence {
	return raftservice.ServingFence{Group: fixture.route.Group,
		AllocationGeneration: fixture.route.AllocationGeneration, Command: fixture.route.Command,
		MemberID: 1, StoreID: [16]byte{0x11}, NodeIncarnation: 21, Term: 1}
}

func newFrontendDrainRuntimeRowsFixture(t *testing.T) *frontendDrainRuntimeRowsFixture {
	t.Helper()
	current := testCatalogAuthoritySnapshot(t, 5)
	genesis := testCatalogAuthoritySnapshot(t, 1)
	descriptor := current.ReplicatedShardDescriptors()[0]
	fixture := &frontendDrainRuntimeRowsFixture{
		route: FrontendDrainRuntimeCatalogRoute{Group: descriptor.Group,
			AllocationGeneration: uint64(descriptor.AllocationGeneration), Command: descriptor.Command, Relation: 1},
		rows: make(map[string]FrontendDrainRuntimeRow),
	}
	fence := fixture.rowFence()
	put := func(key FrontendDrainRuntimeRowKey, value []byte) {
		encoded, ok := key.EncodedKey()
		if !ok {
			t.Fatalf("invalid fixture row key: %+v", key)
		}
		fixture.rows[string(encoded)] = FrontendDrainRuntimeRow{Applied: 100, Found: true,
			Value: bytes.Clone(value), Fence: fence}
	}
	node := scalingTestNodeRecord(rafttransport.NodeID{0xe1}, 1, NodeActive, 1)
	// The node row records its own last-write generation. The catalog head
	// below is intentionally newer without rewriting this unchanged row.
	node.CatalogGeneration = 1
	nodeRaw, err := appendScalingNodeRecord(nil, node)
	if err != nil {
		t.Fatal(err)
	}
	nodeDigest := scalingDigest(nodeRaw)
	directoryRaw, err := appendScalingNodeDirectoryAt(nil, []scalingNodeDirectoryEntry{{
		NodeID: node.NodeID[:], Incarnation: node.Incarnation, Revision: node.Revision,
		Digest: nodeDigest[:],
	}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	put(FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeNodeDirectoryRow}, directoryRaw)
	put(FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeNodeRecordRow,
		NodeID: node.NodeID, Incarnation: node.Incarnation}, nodeRaw)
	currentHead, err := appendReplicatedCatalogDocument(nil, current, maxReplicatedCatalogBytes)
	if err != nil {
		t.Fatal(err)
	}
	put(FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogHeadRow}, currentHead)
	genesisHead, err := appendReplicatedCatalogDocument(nil, genesis, maxReplicatedCatalogBytes)
	if err != nil {
		t.Fatal(err)
	}
	genesisProof, err := appendReplicatedCatalogGenesis(nil, genesisHead)
	if err != nil {
		t.Fatal(err)
	}
	put(FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogGenesisRow}, genesisProof)
	witness, err := appendReplicatedCatalogHeadWitness(nil, current.Generation(), currentHead)
	if err != nil {
		t.Fatal(err)
	}
	put(FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogWitnessRow}, witness)
	serviceRaw, err := appendReplicatedServiceDirectory(nil, replicatedServiceDirectory{Revision: 1})
	if err != nil {
		t.Fatal(err)
	}
	put(FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeServiceDirectoryRow}, serviceRaw)
	return fixture
}

func TestReadFrontendDrainRuntimeCutFromRowsUsesOneCertifiedCatalogEpoch(t *testing.T) {
	fixture := newFrontendDrainRuntimeRowsFixture(t)
	cut, err := ReadFrontendDrainRuntimeCutFromRows(context.Background(), fixture)
	if err != nil {
		t.Fatalf("read canonical local source cut: %v", err)
	}
	if cut.Catalog == nil || cut.Catalog.Generation() != 5 || cut.CatalogHeadDigest == (replication.Digest{}) {
		t.Fatalf("catalog cut=%+v", cut)
	}
	if cut.Nodes.Revision != 1 || len(cut.Nodes.Nodes) != 1 || cut.Nodes.CatalogGeneration != 5 {
		t.Fatalf("node cut=%+v", cut.Nodes)
	}
	if cut.ServiceDirectoryRevision != 1 || len(cut.ContinuationGrants) != 0 || len(cut.DrainFences) != 0 {
		t.Fatalf("service cut revision=%d grants=%d fences=%d", cut.ServiceDirectoryRevision,
			len(cut.ContinuationGrants), len(cut.DrainFences))
	}
	if len(cut.CatalogFences) == 0 {
		t.Fatal("canonical catalog projection has no service fences")
	}
}

func TestReadFrontendDrainRuntimeCutFromRowsRejectsFenceAndMissingServiceRows(t *testing.T) {
	t.Run("wrong owner fence", func(t *testing.T) {
		fixture := newFrontendDrainRuntimeRowsFixture(t)
		key := FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogHeadRow}
		encoded, ok := key.EncodedKey()
		if !ok {
			t.Fatal("head key did not encode")
		}
		row := fixture.rows[string(encoded)]
		row.Fence.MemberID++
		fixture.rows[string(encoded)] = row
		if _, err := ReadFrontendDrainRuntimeCutFromRows(context.Background(), fixture); !errors.Is(err, ErrReplicatedCatalogConflict) {
			t.Fatalf("wrong owner fence error=%v", err)
		}
	})
	t.Run("missing committed service directory", func(t *testing.T) {
		fixture := newFrontendDrainRuntimeRowsFixture(t)
		key, ok := (FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeServiceDirectoryRow}).EncodedKey()
		if !ok {
			t.Fatal("service directory key did not encode")
		}
		delete(fixture.rows, string(key))
		if _, err := ReadFrontendDrainRuntimeCutFromRows(context.Background(), fixture); !errors.Is(err, ErrReplicatedCatalogConflict) {
			t.Fatalf("missing service row error=%v", err)
		}
	})
}

func TestFrontendDrainRuntimeRowKeyIsClosedAndCanonical(t *testing.T) {
	valid := []FrontendDrainRuntimeRowKey{
		{Kind: FrontendDrainRuntimeNodeDirectoryRow},
		{Kind: FrontendDrainRuntimeNodeRecordRow, NodeID: rafttransport.NodeID{1}, Incarnation: 1},
		{Kind: FrontendDrainRuntimeCatalogHeadRow},
		{Kind: FrontendDrainRuntimeCatalogGenesisRow},
		{Kind: FrontendDrainRuntimeCatalogWitnessRow},
		{Kind: FrontendDrainRuntimeServiceDirectoryRow},
		{Kind: FrontendDrainRuntimeDrainRecordRow, DrainID: [32]byte{2}},
	}
	for _, key := range valid {
		encoded, ok := key.EncodedKey()
		if !key.Valid() || !ok || len(encoded) == 0 || key.MaxValueBytes() == 0 {
			t.Fatalf("valid closed row key rejected: %+v encoded=%x", key, encoded)
		}
	}
	invalid := []FrontendDrainRuntimeRowKey{
		{Kind: 0},
		{Kind: FrontendDrainRuntimeNodeDirectoryRow, NodeID: rafttransport.NodeID{1}},
		{Kind: FrontendDrainRuntimeNodeRecordRow, Incarnation: 1},
		{Kind: FrontendDrainRuntimeDrainRecordRow, DrainID: [32]byte{}},
		{Kind: FrontendDrainRuntimeCatalogHeadRow, DrainID: [32]byte{3}},
	}
	for _, key := range invalid {
		if key.Valid() {
			t.Fatalf("invalid closed row key accepted: %+v", key)
		}
		if encoded, ok := key.EncodedKey(); ok || encoded != nil {
			t.Fatalf("invalid key encoded: %+v -> %x", key, encoded)
		}
	}
}
