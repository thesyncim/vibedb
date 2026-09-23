package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

// FrontendDrainRuntimeRowKind is the closed set of catalog rows that may be
// read by the local storage bootstrap adapter.  Keeping the row identity
// typed prevents this path from becoming an arbitrary user-data reader.
type FrontendDrainRuntimeRowKind uint8

const (
	FrontendDrainRuntimeNodeDirectoryRow FrontendDrainRuntimeRowKind = iota + 1
	FrontendDrainRuntimeNodeRecordRow
	FrontendDrainRuntimeCatalogHeadRow
	FrontendDrainRuntimeCatalogGenesisRow
	FrontendDrainRuntimeCatalogWitnessRow
	FrontendDrainRuntimeServiceDirectoryRow
	FrontendDrainRuntimeDrainRecordRow
)

func (kind FrontendDrainRuntimeRowKind) valid() bool {
	return kind >= FrontendDrainRuntimeNodeDirectoryRow && kind <= FrontendDrainRuntimeDrainRecordRow
}

// FrontendDrainRuntimeRowKey names one canonical control-plane row.  The
// encoded key is constructed inside this package; callers cannot substitute a
// relation key or a free-form document identifier.
type FrontendDrainRuntimeRowKey struct {
	Kind        FrontendDrainRuntimeRowKind
	NodeID      rafttransport.NodeID
	Incarnation uint64
	DrainID     [32]byte
}

func (key FrontendDrainRuntimeRowKey) valid() bool {
	if !key.Kind.valid() {
		return false
	}
	switch key.Kind {
	case FrontendDrainRuntimeNodeDirectoryRow,
		FrontendDrainRuntimeCatalogHeadRow,
		FrontendDrainRuntimeCatalogGenesisRow,
		FrontendDrainRuntimeCatalogWitnessRow,
		FrontendDrainRuntimeServiceDirectoryRow:
		return key.NodeID == (rafttransport.NodeID{}) && key.Incarnation == 0 && key.DrainID == ([32]byte{})
	case FrontendDrainRuntimeNodeRecordRow:
		return key.NodeID != (rafttransport.NodeID{}) && key.Incarnation != 0 && key.DrainID == ([32]byte{})
	case FrontendDrainRuntimeDrainRecordRow:
		return key.NodeID == (rafttransport.NodeID{}) && key.Incarnation == 0 && key.DrainID != ([32]byte{})
	default:
		return false
	}
}

// Valid reports whether key names one member of the closed row grammar.
func (key FrontendDrainRuntimeRowKey) Valid() bool { return key.valid() }

// EncodedKey returns the canonical control-plane SQL key for key.
func (key FrontendDrainRuntimeRowKey) EncodedKey() ([]byte, bool) {
	if !key.valid() {
		return nil, false
	}
	switch key.Kind {
	case FrontendDrainRuntimeNodeDirectoryRow:
		return bytes.Clone(scalingNodeDirectoryKey), true
	case FrontendDrainRuntimeNodeRecordRow:
		return scalingNodeKey(key.NodeID, key.Incarnation), true
	case FrontendDrainRuntimeCatalogHeadRow:
		return bytes.Clone(replicatedCatalogHeadKey), true
	case FrontendDrainRuntimeCatalogGenesisRow:
		return bytes.Clone(replicatedCatalogGenesisKey), true
	case FrontendDrainRuntimeCatalogWitnessRow:
		return bytes.Clone(replicatedCatalogHeadWitnessKey), true
	case FrontendDrainRuntimeServiceDirectoryRow:
		return bytes.Clone(replicatedServiceDirectoryKey), true
	case FrontendDrainRuntimeDrainRecordRow:
		return frontendDrainDocumentKey(key.DrainID), true
	default:
		return nil, false
	}
}

// MaxValueBytes is the per-row bound used by the local point-read adapter.
func (key FrontendDrainRuntimeRowKey) MaxValueBytes() int {
	switch key.Kind {
	case FrontendDrainRuntimeNodeDirectoryRow:
		return maxScalingNodeDirectoryBytes
	case FrontendDrainRuntimeNodeRecordRow:
		return maxScalingNodeRecordBytes
	case FrontendDrainRuntimeCatalogHeadRow:
		return maxReplicatedCatalogBytes
	case FrontendDrainRuntimeCatalogGenesisRow:
		return maxReplicatedCatalogGenesisBytes
	case FrontendDrainRuntimeCatalogWitnessRow:
		return maxReplicatedCatalogHeadWitnessBytes
	case FrontendDrainRuntimeServiceDirectoryRow, FrontendDrainRuntimeDrainRecordRow:
		return maxReplicatedServiceDirectoryBytes
	default:
		return 0
	}
}

// FrontendDrainRuntimeCatalogRoute is the exact catalog owner coordinate
// used for every local row read. Relation is fixed by the embedded catalog
// configuration; Group, allocation, and Command come from the serving owner.
type FrontendDrainRuntimeCatalogRoute struct {
	Group                raftmember.GroupKey
	AllocationGeneration uint64
	Command              raftservice.CommandFence
	Relation             replication.RelationID
}

func (route FrontendDrainRuntimeCatalogRoute) valid() bool {
	return route.Group != (raftmember.GroupKey{}) && route.AllocationGeneration != 0 &&
		route.Command.Valid() && route.Relation != 0 && route.Relation <= replication.MaxRelationID
}

// Valid reports whether route is a complete local catalog point-read fence.
func (route FrontendDrainRuntimeCatalogRoute) Valid() bool { return route.valid() }

func sameFrontendDrainRuntimeCatalogRoute(left, right FrontendDrainRuntimeCatalogRoute) bool {
	return left.Group == right.Group && left.AllocationGeneration == right.AllocationGeneration &&
		left.Command == right.Command && left.Relation == right.Relation
}

// FrontendDrainRuntimeRow is a point-read result returned by a certified
// local catalog owner. Fence is copied from the same owner cut that served the
// point; it is never supplied by the bootstrap caller.
type FrontendDrainRuntimeRow struct {
	Applied uint64
	Found   bool
	Value   []byte
	Fence   raftservice.ServingFence
}

// FrontendDrainRuntimeCutRowReader is the narrow bootstrap source. It can
// read only the closed control-plane rows above and must return a complete
// serving fence for each point. Implementations must use a quorum/linearizable
// owner read; local applied bytes or a static gateway policy are insufficient.
type FrontendDrainRuntimeCutRowReader interface {
	ReadFrontendDrainRuntimeCatalogRoute(context.Context) (FrontendDrainRuntimeCatalogRoute, error)
	ReadFrontendDrainRuntimeRow(context.Context, FrontendDrainRuntimeRowKey) (FrontendDrainRuntimeRow, error)
}

var errFrontendDrainRuntimeRowsChanged = errors.New("gateway: canonical frontend drain source rows changed")

func servingFenceMatchesFrontendDrainRuntimeRoute(
	row FrontendDrainRuntimeRow, route FrontendDrainRuntimeCatalogRoute,
) bool {
	return row.Applied != 0 && row.Fence.Group == route.Group &&
		row.Fence.AllocationGeneration == route.AllocationGeneration &&
		row.Fence.Command == route.Command && row.Fence.MemberID != 0 &&
		row.Fence.StoreID != ([16]byte{}) && row.Fence.NodeIncarnation != 0 && row.Fence.Term != 0
}

func sameFrontendDrainRuntimeServingFence(left, right raftservice.ServingFence) bool {
	return left == right
}

func readFrontendDrainRuntimeRow(
	ctx context.Context, reader FrontendDrainRuntimeCutRowReader,
	key FrontendDrainRuntimeRowKey, route FrontendDrainRuntimeCatalogRoute,
) (FrontendDrainRuntimeRow, error) {
	if ctx == nil || reader == nil || !key.valid() || !route.valid() {
		return FrontendDrainRuntimeRow{}, ErrReplicatedCatalog
	}
	row, err := reader.ReadFrontendDrainRuntimeRow(ctx, key)
	if err != nil {
		return FrontendDrainRuntimeRow{}, err
	}
	if !servingFenceMatchesFrontendDrainRuntimeRoute(row, route) ||
		(!row.Found && len(row.Value) != 0) || (row.Found && len(row.Value) == 0) {
		return FrontendDrainRuntimeRow{}, errors.Join(ErrReplicatedCatalogConflict, errFrontendDrainRuntimeRowsChanged)
	}
	return row, nil
}

func openFrontendDrainRuntimeNodeDirectory(raw []byte) (scalingNodeDirectory, []scalingNodeDirectoryEntry, error) {
	entries, err := openScalingNodeDirectory(raw)
	if err != nil {
		return scalingNodeDirectory{}, nil, err
	}
	payload, err := openTypedControlPlaneDocument(raw, scalingNodeDirectoryDocumentID[:], maxScalingNodeDirectoryBytes)
	if err != nil {
		return scalingNodeDirectory{}, nil, err
	}
	var directory scalingNodeDirectory
	if err = vibejson.Unmarshal(payload, &directory); err != nil || directory.Revision == 0 {
		return scalingNodeDirectory{}, nil, errors.Join(err, ErrInvalidScalingMetadata)
	}
	return directory, entries, nil
}

func openFrontendDrainRuntimeCatalogHead(raw []byte) (*Snapshot, error) {
	payload, err := openTypedControlPlaneDocument(raw, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes)
	if err != nil {
		return nil, err
	}
	snapshot, err := OpenSnapshotDocument(payload)
	if err != nil || snapshot == nil {
		return nil, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	canonical, err := appendReplicatedCatalogDocument(nil, snapshot, maxReplicatedCatalogBytes)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	return snapshot, nil
}

func readFrontendDrainRuntimeCatalogProof(
	ctx context.Context, reader FrontendDrainRuntimeCutRowReader,
	route FrontendDrainRuntimeCatalogRoute, head FrontendDrainRuntimeRow,
) (*Snapshot, replication.Digest, error) {
	if !head.Found {
		genesis, genesisErr := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogGenesisRow}, route)
		if genesisErr != nil {
			return nil, replication.Digest{}, genesisErr
		}
		witness, witnessErr := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogWitnessRow}, route)
		if witnessErr != nil {
			return nil, replication.Digest{}, witnessErr
		}
		if !genesis.Found && !witness.Found {
			return nil, replication.Digest{}, ErrReplicatedCatalogMissing
		}
		return nil, replication.Digest{}, ErrReplicatedCatalogConflict
	}
	snapshot, err := openFrontendDrainRuntimeCatalogHead(head.Value)
	if err != nil {
		return nil, replication.Digest{}, err
	}
	genesis, err := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogGenesisRow}, route)
	if err != nil || !genesis.Found {
		return nil, replication.Digest{}, errors.Join(err, ErrReplicatedCatalogConflict)
	}
	genesisHead := []byte(nil)
	if snapshot.Generation() == 1 {
		genesisHead = head.Value
	}
	if err = validateReplicatedCatalogGenesis(genesis.Value, genesisHead); err != nil {
		return nil, replication.Digest{}, ErrReplicatedCatalogConflict
	}
	witness, err := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogWitnessRow}, route)
	if err != nil || !witness.Found || validateReplicatedCatalogHeadWitness(witness.Value, snapshot.Generation(), head.Value) != nil {
		return nil, replication.Digest{}, ErrReplicatedCatalogConflict
	}
	if !sameFrontendDrainRuntimeServingFence(head.Fence, genesis.Fence) ||
		!sameFrontendDrainRuntimeServingFence(head.Fence, witness.Fence) {
		return nil, replication.Digest{}, errFrontendDrainRuntimeRowsChanged
	}
	return snapshot, scalingDigest(head.Value), nil
}

func readFrontendDrainRuntimeNodeCut(
	ctx context.Context, reader FrontendDrainRuntimeCutRowReader,
	route FrontendDrainRuntimeCatalogRoute,
) (NodeDirectoryCut, FrontendDrainRuntimeRow, error) {
	row, err := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeNodeDirectoryRow}, route)
	if err != nil {
		return NodeDirectoryCut{}, FrontendDrainRuntimeRow{}, err
	}
	if !row.Found {
		return NodeDirectoryCut{}, FrontendDrainRuntimeRow{}, ErrScalingNodeMissing
	}
	directory, entries, err := openFrontendDrainRuntimeNodeDirectory(row.Value)
	if err != nil {
		return NodeDirectoryCut{}, FrontendDrainRuntimeRow{}, err
	}
	nodes := make([]NodeRecord, len(entries))
	for index, entry := range entries {
		var node rafttransport.NodeID
		copy(node[:], entry.NodeID)
		child, childErr := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{
			Kind: FrontendDrainRuntimeNodeRecordRow, NodeID: node, Incarnation: entry.Incarnation,
		}, route)
		if childErr != nil {
			return NodeDirectoryCut{}, FrontendDrainRuntimeRow{}, childErr
		}
		if !sameFrontendDrainRuntimeServingFence(row.Fence, child.Fence) {
			return NodeDirectoryCut{}, FrontendDrainRuntimeRow{}, errFrontendDrainRuntimeRowsChanged
		}
		if !child.Found || scalingDigest(child.Value) != replication.Digest(entry.Digest) {
			return NodeDirectoryCut{}, FrontendDrainRuntimeRow{}, errFrontendDrainRuntimeRowsChanged
		}
		nodes[index], err = openScalingNodeRecord(child.Value, node, entry.Incarnation)
		if err != nil {
			return NodeDirectoryCut{}, FrontendDrainRuntimeRow{}, err
		}
	}
	cut := NodeDirectoryCut{Revision: directory.Revision, Digest: scalingDigest(row.Value),
		CatalogGeneration: 0, Nodes: nodes}
	for _, node := range nodes {
		if node.CatalogGeneration > cut.CatalogGeneration {
			cut.CatalogGeneration = node.CatalogGeneration
		}
	}
	if !cut.Valid() {
		return NodeDirectoryCut{}, FrontendDrainRuntimeRow{}, ErrInvalidScalingMetadata
	}
	return cut, row, nil
}

func readFrontendDrainRuntimeServiceCut(
	ctx context.Context, reader FrontendDrainRuntimeCutRowReader,
	route FrontendDrainRuntimeCatalogRoute,
) (uint64, []serviceauthz.CommittedFrontendContinuationGrant,
	[]serviceauthz.CommittedFrontendDrainFence, []FrontendDrainRecord,
	FrontendDrainRuntimeRow, error) {
	row, err := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeServiceDirectoryRow}, route)
	if err != nil {
		return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, err
	}
	if !row.Found {
		// An absent service row is a missing committed source, not an empty
		// directory. The genuine genesis path is handled by catalog bootstrap.
		return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, ErrReplicatedCatalogConflict
	}
	directory, err := openReplicatedServiceDirectory(row.Value)
	if err != nil {
		return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, err
	}
	records := make([]FrontendDrainRecord, len(directory.Drains))
	for index, entry := range directory.Drains {
		drainID, ok := frontendDrainEntryID(entry)
		if !ok {
			return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, ErrReplicatedCatalogConflict
		}
		child, childErr := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{
			Kind: FrontendDrainRuntimeDrainRecordRow, DrainID: drainID,
		}, route)
		if childErr != nil {
			return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, childErr
		}
		if !sameFrontendDrainRuntimeServingFence(row.Fence, child.Fence) {
			return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, errFrontendDrainRuntimeRowsChanged
		}
		if !child.Found || scalingDigest(child.Value) != replication.Digest(entry.Digest) {
			return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, errFrontendDrainRuntimeRowsChanged
		}
		records[index], err = openReplicatedFrontendDrainDocument(child.Value)
		if err != nil || records[index].DrainID != drainID || records[index].Revision != entry.Revision {
			return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, ErrReplicatedCatalogConflict
		}
	}
	grants, fences, err := frontendDrainServiceMaterial(records)
	if err != nil {
		return 0, nil, nil, nil, FrontendDrainRuntimeRow{}, err
	}
	return directory.Revision, grants, fences, records, row, nil
}

// ReadFrontendDrainRuntimeCutFromRows reads the same coherent source epoch as
// ReplicatedCatalogAuthority.ReadFrontendDrainRuntimeCut, using only the
// certified local catalog row reader. Aggregate rows are re-read after all
// children, and every point carries the exact owner fence; a changed route or
// aggregate retries the entire projection.
func ReadFrontendDrainRuntimeCutFromRows(
	ctx context.Context, reader FrontendDrainRuntimeCutRowReader,
) (FrontendDrainRuntimeCut, error) {
	if ctx == nil || reader == nil {
		return FrontendDrainRuntimeCut{}, ErrReplicatedCatalog
	}
	lastRetry := ""
	for attempt := 0; attempt < 4; attempt++ {
		route, err := reader.ReadFrontendDrainRuntimeCatalogRoute(ctx)
		if err != nil {
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows route attempt %d: %w", attempt+1, err)
		}
		if !route.valid() {
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows route attempt %d invalid: %w", attempt+1, ErrReplicatedCatalogConflict)
		}
		nodes, nodeRow, err := readFrontendDrainRuntimeNodeCut(ctx, reader, route)
		if err != nil {
			if errors.Is(err, errFrontendDrainRuntimeRowsChanged) {
				lastRetry = fmt.Sprintf("attempt %d node cut changed: %v", attempt+1, err)
				continue
			}
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows node cut attempt %d route=%+v: %w", attempt+1, route, err)
		}
		head, err := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogHeadRow}, route)
		if err != nil {
			if errors.Is(err, errFrontendDrainRuntimeRowsChanged) {
				lastRetry = fmt.Sprintf("attempt %d catalog head changed: %v", attempt+1, err)
				continue
			}
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows catalog head attempt %d route=%+v: %w", attempt+1, route, err)
		}
		catalog, headDigest, err := readFrontendDrainRuntimeCatalogProof(ctx, reader, route, head)
		if err != nil {
			if errors.Is(err, errFrontendDrainRuntimeRowsChanged) {
				lastRetry = fmt.Sprintf("attempt %d catalog proof changed: %v", attempt+1, err)
				continue
			}
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows catalog proof attempt %d route=%+v: %w", attempt+1, route, err)
		}
		if !sameFrontendDrainRuntimeServingFence(nodeRow.Fence, head.Fence) {
			lastRetry = fmt.Sprintf("attempt %d node/head serving fence changed: node=%+v head=%+v", attempt+1, nodeRow.Fence, head.Fence)
			continue
		}
		if catalog.Generation() < nodes.CatalogGeneration {
			lastRetry = fmt.Sprintf("attempt %d catalog generation regressed: catalog=%d nodes=%d", attempt+1, catalog.Generation(), nodes.CatalogGeneration)
			continue
		}
		// NodeRecord.CatalogGeneration is the generation in which that
		// particular row last changed.  A catalog-only publication can advance
		// the certified head without rewriting the node directory.  Carry the
		// head generation on the effective aggregate cut while retaining each
		// raw row's generation for its own history and digest checks.
		nodes.CatalogGeneration = catalog.Generation()
		fences, generation, err := catalogServiceFencesForFrontendDrainRuntimeRoute(catalog, route)
		if err != nil {
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows service fences attempt %d route=%+v: %w", attempt+1, route, err)
		}
		if generation != catalog.Generation() {
			lastRetry = fmt.Sprintf("attempt %d service fence generation mismatch: fences=%d catalog=%d", attempt+1, generation, catalog.Generation())
			continue
		}
		serviceRevision, grants, drainFences, records, serviceRow, err := readFrontendDrainRuntimeServiceCut(ctx, reader, route)
		if err != nil {
			if errors.Is(err, errFrontendDrainRuntimeRowsChanged) {
				lastRetry = fmt.Sprintf("attempt %d service cut changed: %v", attempt+1, err)
				continue
			}
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows service cut attempt %d route=%+v: %w", attempt+1, route, err)
		}
		if !sameFrontendDrainRuntimeServingFence(nodeRow.Fence, serviceRow.Fence) {
			lastRetry = fmt.Sprintf("attempt %d node/service serving fence changed: node=%+v service=%+v", attempt+1, nodeRow.Fence, serviceRow.Fence)
			continue
		}
		latestRoute, err := reader.ReadFrontendDrainRuntimeCatalogRoute(ctx)
		if err != nil {
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows final route attempt %d: %w", attempt+1, err)
		}
		latestNode, err := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeNodeDirectoryRow}, route)
		if err != nil {
			if errors.Is(err, errFrontendDrainRuntimeRowsChanged) {
				lastRetry = fmt.Sprintf("attempt %d final node changed: %v", attempt+1, err)
				continue
			}
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows final node attempt %d route=%+v: %w", attempt+1, route, err)
		}
		latestHead, err := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeCatalogHeadRow}, route)
		if err != nil {
			if errors.Is(err, errFrontendDrainRuntimeRowsChanged) {
				lastRetry = fmt.Sprintf("attempt %d final head changed: %v", attempt+1, err)
				continue
			}
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows final head attempt %d route=%+v: %w", attempt+1, route, err)
		}
		latestService, err := readFrontendDrainRuntimeRow(ctx, reader, FrontendDrainRuntimeRowKey{Kind: FrontendDrainRuntimeServiceDirectoryRow}, route)
		if err != nil {
			if errors.Is(err, errFrontendDrainRuntimeRowsChanged) {
				lastRetry = fmt.Sprintf("attempt %d final service changed: %v", attempt+1, err)
				continue
			}
			return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows final service attempt %d route=%+v: %w", attempt+1, route, err)
		}
		if !sameFrontendDrainRuntimeCatalogRoute(route, latestRoute) ||
			!sameFrontendDrainRuntimeServingFence(nodeRow.Fence, latestNode.Fence) ||
			!sameFrontendDrainRuntimeServingFence(nodeRow.Fence, latestHead.Fence) ||
			!sameFrontendDrainRuntimeServingFence(nodeRow.Fence, latestService.Fence) ||
			!bytes.Equal(nodeRow.Value, latestNode.Value) ||
			!bytes.Equal(head.Value, latestHead.Value) ||
			!bytes.Equal(serviceRow.Value, latestService.Value) {
			lastRetry = fmt.Sprintf("attempt %d final canonical rows changed: route=%+v latest=%+v nodeFence=%+v latestNodeFence=%+v headFence=%+v latestHeadFence=%+v serviceFence=%+v latestServiceFence=%+v", attempt+1, route, latestRoute, nodeRow.Fence, latestNode.Fence, head.Fence, latestHead.Fence, serviceRow.Fence, latestService.Fence)
			continue
		}
		return FrontendDrainRuntimeCut{
			Nodes: nodes, Catalog: catalog, CatalogHeadDigest: headDigest,
			CatalogFences: slices.Clone(fences), ServiceDirectoryRevision: serviceRevision,
			ContinuationGrants: slices.Clone(grants), DrainFences: slices.Clone(drainFences),
			DrainRecords: slices.Clone(records),
		}, nil
	}
	if lastRetry == "" {
		lastRetry = "no coherent row epoch"
	}
	return FrontendDrainRuntimeCut{}, fmt.Errorf("canonical rows conflict after four attempts: %s: %w", lastRetry, ErrReplicatedCatalogConflict)
}
