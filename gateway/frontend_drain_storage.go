package gateway

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	vibejson "github.com/thesyncim/vibejson"
)

// FrontendDrainRecordReader reads the per-drain rows and their bounded index.
// The index revision is independent from a node revision, but every returned
// record is verified against the index digest before it is exposed.
type FrontendDrainRecordReader interface {
	ReadFrontendDrainRecord(context.Context, [32]byte) (FrontendDrainRecord, error)
	ReadFrontendDrainRecordCut(context.Context) (uint64, []FrontendDrainRecord, error)
}

// FrontendDrainServiceCutReader returns the service-directory material from
// one verified aggregate-row epoch. The continuation grants, empty-drain
// fences, and child index are coupled: projecting a mixture of their separate
// reads can publish a revision that no committed catalog state ever held.
type FrontendDrainServiceCutReader interface {
	ReadFrontendDrainServiceCut(context.Context) (
		uint64, []serviceauthz.CommittedFrontendContinuationGrant,
		[]serviceauthz.CommittedFrontendDrainFence, []FrontendDrainRecord, error,
	)
}

// FrontendDrainRuntimeCut is the one source epoch used to project a live
// service directory. Node lifecycle, catalog resource fences, and child-owned
// drain proof are verified together; callers must not combine independently
// refreshed fragments into a synthetic revision.
type FrontendDrainRuntimeCut struct {
	Nodes                    NodeDirectoryCut
	Catalog                  *Snapshot
	CatalogHeadDigest        replication.Digest
	CatalogFences            []serviceauthz.ServiceFence
	ServiceDirectoryRevision uint64
	ContinuationGrants       []serviceauthz.CommittedFrontendContinuationGrant
	DrainFences              []serviceauthz.CommittedFrontendDrainFence
	DrainRecords             []FrontendDrainRecord
}

// FrontendDrainRuntimeCutReader supplies a complete read-verified projection
// source to gateway runtimes.
type FrontendDrainRuntimeCutReader interface {
	ReadFrontendDrainRuntimeCut(context.Context) (FrontendDrainRuntimeCut, error)
}

// FrontendDrainRecordWriter persists the Prepared proof. The expected
// revision is the record revision, with zero reserved for create-only.
// Enforcing and Retired transitions use the node authority below so a caller
// cannot publish a service proof without the matching node lifecycle CAS.
type FrontendDrainRecordWriter interface {
	PutFrontendDrainRecord(context.Context, FrontendDrainRecord, uint64) error
}

// FrontendDrainCapacityReserver reserves a canonical directory index slot
// before local admission is closed. The reservation is idempotent by drainID
// and is consumed by the create-only Prepared child mutation.
type FrontendDrainCapacityReserver interface {
	ReserveFrontendDrainCapacity(context.Context, [32]byte) error
}

// FrontendDrainRecordFitsStorage checks the exact canonical child encoding
// against the replicated mutation limit. Coordinators use a worst-case token
// shape before closing admission so a later serialization cannot strand an
// otherwise Active frontend.
func FrontendDrainRecordFitsStorage(record FrontendDrainRecord) bool {
	raw, err := appendReplicatedFrontendDrainDocument(nil, record)
	return err == nil && len(raw) <= maxReplicatedServiceDirectoryBytes
}

// FrontendDrainLifecycleEnforcer atomically advances a prepared drain and its
// exact node record to the enforcing state. It is deliberately separate from
// FrontendDrainRecordWriter: standalone row updates cannot open a draining
// node's service binding safely.
type FrontendDrainLifecycleEnforcer interface {
	EnforceFrontendDrain(context.Context, [32]byte, rafttransport.NodeID, uint64, uint64) error
}

// EnforceFrontendDrain atomically advances the prepared frontend proof and
// the owning node from Active to Draining. The caller supplies the immutable
// drain identity and the node CAS revision; neither is inferred from a local
// process flag.
func (authority *ReplicatedCatalogAuthority) EnforceFrontendDrain(
	ctx context.Context, drainID [32]byte, nodeID rafttransport.NodeID,
	incarnation uint64, expectedRevision uint64,
) error {
	if authority == nil || ctx == nil || drainID == ([32]byte{}) ||
		nodeID == (rafttransport.NodeID{}) || incarnation == 0 || expectedRevision == 0 {
		return ErrScalingState
	}
	node, err := authority.ReadNode(ctx, nodeID, incarnation)
	if err != nil {
		return err
	}
	if node.Lifecycle != NodeActive || node.Revision != expectedRevision || node.Roles&NodeRoleGateway == 0 {
		return ErrScalingState
	}
	next := node
	next.Lifecycle = NodeDraining
	if next.Revision == ^uint64(0) {
		return ErrScalingRevision
	}
	next.Revision++
	return authority.putNodeWithExtra(ctx, next, expectedRevision, nil, nil,
		func(appendCtx context.Context, prior, current NodeRecord, directoryRevision uint64, directoryDigest replication.Digest) ([]NativeMutation, error) {
			return authority.appendEnforcedFrontendDrainMutations(appendCtx, drainID, prior, current, directoryRevision, directoryDigest)
		})
}

func (authority *ReplicatedCatalogAuthority) readFrontendDrainDirectory(
	ctx context.Context,
) (replicatedServiceDirectory, ReplicatedPointResult, error) {
	if authority == nil || ctx == nil {
		return replicatedServiceDirectory{}, ReplicatedPointResult{}, ErrReplicatedCatalog
	}
	result, err := authority.readRaw(ctx, replicatedServiceDirectoryKey,
		maxReplicatedServiceDirectoryBytes)
	if err != nil {
		return replicatedServiceDirectory{}, result, err
	}
	if !result.Found {
		return replicatedServiceDirectory{}, result, nil
	}
	directory, err := openReplicatedServiceDirectory(result.Value)
	if err != nil {
		return replicatedServiceDirectory{}, result, err
	}
	return directory, result, nil
}

func findFrontendDrainEntry(
	directory replicatedServiceDirectory, drainID [32]byte,
) (replicatedFrontendDrainEntry, int, bool) {
	for index, entry := range directory.Drains {
		id, ok := frontendDrainEntryID(entry)
		if !ok {
			return replicatedFrontendDrainEntry{}, -1, false
		}
		if id == drainID {
			return entry, index, true
		}
	}
	return replicatedFrontendDrainEntry{}, -1, false
}

func findFrontendDrainReservation(directory replicatedServiceDirectory, drainID [32]byte) (int, bool) {
	for index, reservation := range directory.Reservations {
		if len(reservation.DrainID) != len(drainID) {
			return -1, false
		}
		if bytes.Equal(reservation.DrainID, drainID[:]) {
			return index, true
		}
	}
	return -1, false
}

// retiredFrontendDrainAcknowledged is the durable GC fence for a terminal
// child. RetireNode's node witness proves the lifecycle CAS, but it does not
// prove that every serving receiver installed the terminal exact cut. That
// acknowledgement belongs to the parent scaling intent and must remain
// readable until the child is compacted.
func (authority *ReplicatedCatalogAuthority) retiredFrontendDrainAcknowledged(
	ctx context.Context, record FrontendDrainRecord,
) (bool, error) {
	if authority == nil || ctx == nil || !record.Valid() || record.Lifecycle != FrontendDrainRetired {
		return false, ErrReplicatedCatalogConflict
	}
	result, err := authority.readRaw(ctx, scalingIntentKey(record.IntentID), maxScalingIntentRecordBytes)
	if err != nil {
		return false, err
	}
	if !result.Found {
		return false, nil
	}
	intent, err := openScalingIntentRecord(result.Value, record.IntentID)
	if err != nil {
		return false, err
	}
	if intent.State != ScalingComplete || intent.Request.Kind != ScalingDecommission || intent.Request.Drain != (NodeReference{
		NodeID: record.PhysicalNode, Incarnation: record.PhysicalIncarnation,
	}) {
		return false, ErrReplicatedCatalogConflict
	}
	return intent.Evidence.NodeID == record.PhysicalNode &&
		intent.Evidence.NodeIncarnation == record.PhysicalIncarnation &&
		intent.Evidence.RetiredAcknowledged && intent.Evidence.SafeToStop(), nil
}

// ReserveFrontendDrainCapacity creates a compact, durable reservation before
// BeginFrontendDrain. It does not alter node lifecycle or listener state.
func (authority *ReplicatedCatalogAuthority) ReserveFrontendDrainCapacity(ctx context.Context, drainID [32]byte) error {
	if authority == nil || authority.session == nil || ctx == nil || drainID == ([32]byte{}) {
		return ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	directory, current, err := authority.readFrontendDrainDirectory(ctx)
	if err != nil {
		return err
	}
	if _, _, found := findFrontendDrainEntry(directory, drainID); found {
		return nil
	}
	if _, found := findFrontendDrainReservation(directory, drainID); found {
		return nil
	}
	// The scaling-intent guard is the first admission fence, but retain the
	// same check here for a restart or an authority that is resuming a durable
	// intent whose reservation has not yet been consumed. This call still runs
	// before appending the candidate reservation, and the directory digest CAS
	// below serializes two authorities that race from one stale cut.
	activeGatewayDrains, err := authority.activeGatewayFrontendDrainIDs(ctx)
	if err != nil {
		return err
	}
	if _, candidateIsGateway := activeGatewayDrains[drainID]; candidateIsGateway {
		for _, entry := range directory.Drains {
			entryID, ok := frontendDrainEntryID(entry)
			if !ok {
				return ErrReplicatedCatalog
			}
			if entryID != drainID {
				if _, active := activeGatewayDrains[entryID]; active {
					return errors.Join(ErrScalingState, ErrConcurrentFrontendDrain)
				}
			}
		}
		for _, reservation := range directory.Reservations {
			if len(reservation.DrainID) != 32 {
				return ErrReplicatedCatalog
			}
			var reservationID [32]byte
			copy(reservationID[:], reservation.DrainID)
			if reservationID != drainID {
				if _, active := activeGatewayDrains[reservationID]; active {
					return errors.Join(ErrScalingState, ErrConcurrentFrontendDrain)
				}
			}
		}
	}
	var compactedID [32]byte
	var compactedRaw []byte
	if len(directory.Drains)+len(directory.Reservations) >= maxReplicatedFrontendDrainEntries {
		for index, entry := range directory.Drains {
			record, raw, readErr := frontendDrainRecordFromEntry(ctx, authority, entry)
			if readErr != nil {
				return readErr
			}
			if record.Lifecycle != FrontendDrainRetired {
				continue
			}
			acknowledged, ackErr := authority.retiredFrontendDrainAcknowledged(ctx, record)
			if ackErr != nil {
				return ackErr
			}
			if !acknowledged {
				continue
			}
			// A compacted child is already terminal. Existing gates retain its
			// retired denial; fresh/restarted gates have no grant and therefore
			// deny every envelope. It can never become an admission authority.
			compactedID, compactedRaw = record.DrainID, raw
			directory.Drains = slices.Delete(directory.Drains, index, index+1)
			break
		}
		if compactedRaw == nil {
			return ErrReplicatedCatalogConflict
		}
	}
	if directory.Revision == ^uint64(0) {
		return ErrReplicatedCatalogConflict
	}
	directory.Reservations = append(directory.Reservations, replicatedFrontendDrainReservation{DrainID: append([]byte(nil), drainID[:]...)})
	slices.SortFunc(directory.Reservations, func(left, right replicatedFrontendDrainReservation) int {
		return bytes.Compare(left.DrainID, right.DrainID)
	})
	if directory.Revision == 0 {
		directory.Revision = 1
	} else {
		directory.Revision++
	}
	raw, err := appendReplicatedServiceDirectory(nil, directory)
	if err != nil {
		return err
	}
	mutations := []NativeMutation{scalingDirectoryMutation(current, replicatedServiceDirectoryKey, raw)}
	if compactedRaw != nil {
		mutations = append(mutations, NativeMutation{Kind: replication.MutationDeleteDigestEqual,
			Key: frontendDrainDocumentKey(compactedID), ExpectedValueLength: uint64(len(compactedRaw)),
			ExpectedValueDigest: scalingDigest(compactedRaw)})
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	return scalingMutationError(result, err, authority.session)
}

func frontendDrainRecordFromEntry(
	ctx context.Context, authority *ReplicatedCatalogAuthority,
	entry replicatedFrontendDrainEntry,
) (FrontendDrainRecord, []byte, error) {
	drainID, ok := frontendDrainEntryID(entry)
	if !ok {
		return FrontendDrainRecord{}, nil, ErrReplicatedCatalog
	}
	result, err := authority.readRaw(ctx, frontendDrainDocumentKey(drainID),
		maxReplicatedServiceDirectoryBytes)
	if err != nil {
		return FrontendDrainRecord{}, nil, err
	}
	if !result.Found || scalingDigest(result.Value) != replication.Digest(entry.Digest) {
		return FrontendDrainRecord{}, nil, ErrReplicatedCatalogConflict
	}
	record, err := openReplicatedFrontendDrainDocument(result.Value)
	if err != nil {
		return FrontendDrainRecord{}, nil, err
	}
	if record.DrainID != drainID || record.Revision != entry.Revision {
		return FrontendDrainRecord{}, nil, ErrReplicatedCatalogConflict
	}
	return record, result.Value, nil
}

// ReadFrontendDrainRecordCut reads the index and all child rows under a
// stable index digest. A child replacement, deletion, or index advance causes
// a bounded retry instead of returning a mixed drain epoch.
func (authority *ReplicatedCatalogAuthority) ReadFrontendDrainRecordCut(
	ctx context.Context,
) (uint64, []FrontendDrainRecord, error) {
	if authority == nil || ctx == nil || authority.executor == nil {
		return 0, nil, ErrReplicatedCatalog
	}
	for attempt := 0; attempt < authority.executor.maxAttempts; attempt++ {
		directory, indexResult, err := authority.readFrontendDrainDirectory(ctx)
		if err != nil {
			return 0, nil, err
		}
		if !indexResult.Found {
			return 0, nil, nil
		}
		records := make([]FrontendDrainRecord, len(directory.Drains))
		for index, entry := range directory.Drains {
			record, _, readErr := frontendDrainRecordFromEntry(ctx, authority, entry)
			if errors.Is(readErr, ErrReplicatedCatalogConflict) {
				records = nil
				break
			}
			if readErr != nil {
				return 0, nil, readErr
			}
			records[index] = record
		}
		if records == nil {
			continue
		}
		latest, err := authority.readRaw(ctx, replicatedServiceDirectoryKey,
			maxReplicatedServiceDirectoryBytes)
		if err != nil {
			return 0, nil, err
		}
		if latest.Found && bytes.Equal(latest.Value, indexResult.Value) {
			return directory.Revision, records, nil
		}
	}
	return 0, nil, ErrReplicatedCatalogConflict
}

// ReadFrontendDrainServiceCut reads the aggregate directory once, resolves
// every indexed child against that exact directory digest, then verifies the
// aggregate did not advance before returning. It is the production read path
// for service-directory projection; callers never have to select a maximum
// among independently read grant/fence/index revisions.
func (authority *ReplicatedCatalogAuthority) ReadFrontendDrainServiceCut(
	ctx context.Context,
) (uint64, []serviceauthz.CommittedFrontendContinuationGrant,
	[]serviceauthz.CommittedFrontendDrainFence, []FrontendDrainRecord, error) {
	if authority == nil || ctx == nil || authority.executor == nil {
		return 0, nil, nil, nil, ErrReplicatedCatalog
	}
	for attempt := 0; attempt < authority.executor.maxAttempts; attempt++ {
		directory, indexResult, err := authority.readFrontendDrainDirectory(ctx)
		if err != nil {
			return 0, nil, nil, nil, err
		}
		if !indexResult.Found {
			return 0, nil, nil, nil, nil
		}
		records := make([]FrontendDrainRecord, len(directory.Drains))
		complete := true
		for index, entry := range directory.Drains {
			record, _, readErr := frontendDrainRecordFromEntry(ctx, authority, entry)
			if errors.Is(readErr, ErrReplicatedCatalogConflict) {
				complete = false
				break
			}
			if readErr != nil {
				return 0, nil, nil, nil, readErr
			}
			records[index] = record
		}
		if !complete {
			continue
		}
		latest, err := authority.readRaw(ctx, replicatedServiceDirectoryKey,
			maxReplicatedServiceDirectoryBytes)
		if err != nil {
			return 0, nil, nil, nil, err
		}
		if latest.Found && bytes.Equal(latest.Value, indexResult.Value) {
			grants, fences, materialErr := frontendDrainServiceMaterial(records)
			if materialErr != nil {
				return 0, nil, nil, nil, materialErr
			}
			return directory.Revision, grants, fences, records, nil
		}
	}
	return 0, nil, nil, nil, ErrReplicatedCatalogConflict
}

// ReadFrontendDrainRuntimeCut reads all three durable sources and verifies
// both the node directory and catalog head again before returning. A writer
// that commits between any fragment reads causes a bounded retry; no runtime
// receives a mixed node/catalog/drain epoch.
func (authority *ReplicatedCatalogAuthority) ReadFrontendDrainRuntimeCut(ctx context.Context) (FrontendDrainRuntimeCut, error) {
	if authority == nil || ctx == nil || authority.executor == nil {
		return FrontendDrainRuntimeCut{}, ErrReplicatedCatalog
	}
	// Every component read, including the private catalog-fence projection,
	// must carry the authority's fixed internal principal. The public readers
	// bind this context themselves, but this aggregate reader calls the private
	// helper directly; binding once here prevents a mixed authorized/anonymous
	// cut during runtime service-directory startup.
	var err error
	ctx, err = authority.authorizedContext(ctx)
	if err != nil {
		return FrontendDrainRuntimeCut{}, err
	}
	for attempt := 0; attempt < authority.executor.maxAttempts; attempt++ {
		nodes, err := authority.ReadNodeDirectoryCut(ctx)
		if err != nil {
			return FrontendDrainRuntimeCut{}, err
		}
		catalog, headDigest, err := authority.ReadReplicatedCatalogHead(ctx)
		if err != nil || catalog == nil || headDigest == (replication.Digest{}) ||
			catalog.Generation() < nodes.CatalogGeneration {
			if err != nil {
				return FrontendDrainRuntimeCut{}, err
			}
			continue
		}
		// A node row records the last generation that changed that row.  The
		// catalog head may advance independently, so the effective coherent cut
		// carries the certified head generation while retaining the directory
		// digest and per-node historical coordinates.
		nodes.CatalogGeneration = catalog.Generation()
		fences, generation, err := authority.catalogServiceFences(ctx, catalog)
		if err != nil || generation != catalog.Generation() {
			if err != nil {
				return FrontendDrainRuntimeCut{}, err
			}
			continue
		}
		serviceRevision, grants, drainFences, records, err := authority.ReadFrontendDrainServiceCut(ctx)
		if err != nil {
			return FrontendDrainRuntimeCut{}, err
		}
		latestNodes, err := authority.ReadNodeDirectoryCut(ctx)
		if err != nil {
			return FrontendDrainRuntimeCut{}, err
		}
		latestCatalog, latestHead, err := authority.ReadReplicatedCatalogHead(ctx)
		if err != nil {
			return FrontendDrainRuntimeCut{}, err
		}
		if latestNodes.Revision != nodes.Revision || latestNodes.Digest != nodes.Digest ||
			latestNodes.CatalogGeneration > nodes.CatalogGeneration || latestCatalog == nil ||
			latestHead != headDigest || latestCatalog.Generation() != catalog.Generation() {
			continue
		}
		return FrontendDrainRuntimeCut{Nodes: nodes, Catalog: catalog, CatalogHeadDigest: headDigest,
			CatalogFences: slices.Clone(fences), ServiceDirectoryRevision: serviceRevision,
			ContinuationGrants: slices.Clone(grants), DrainFences: slices.Clone(drainFences),
			DrainRecords: slices.Clone(records)}, nil
	}
	return FrontendDrainRuntimeCut{}, ErrReplicatedCatalogConflict
}

// frontendDrainServiceMaterial projects service authorization solely from the
// indexed child documents. The directory row is an index, not a second copy
// of potentially multi-megabyte accepted-token grants. Keeping one owner for
// the proof makes every returned grant/fence inseparable from its drain phase
// and the child digest verified by ReadFrontendDrainServiceCut.
func frontendDrainServiceMaterial(records []FrontendDrainRecord) (
	[]serviceauthz.CommittedFrontendContinuationGrant,
	[]serviceauthz.CommittedFrontendDrainFence, error,
) {
	grants := make([]serviceauthz.CommittedFrontendContinuationGrant, 0, len(records))
	fences := make([]serviceauthz.CommittedFrontendDrainFence, 0, len(records))
	for _, record := range records {
		if !record.Valid() {
			return nil, nil, ErrReplicatedCatalogConflict
		}
		if record.ContinuationGrant != nil {
			grant := *record.ContinuationGrant
			if !grant.Valid() {
				return nil, nil, ErrReplicatedCatalogConflict
			}
			grants = append(grants, grant)
			continue
		}
		marker := serviceauthz.CommittedFrontendDrainFence{
			TrustDomain: record.TrustDomain, PhysicalNode: record.PhysicalNode,
			PhysicalIncarnation: record.PhysicalIncarnation, PeerKeyDigest: [32]byte(record.keyDigest()),
			GatewayServiceID: record.GatewayServiceID, GatewaySessionID: record.GatewaySessionID,
			GatewaySessionRevision: record.GatewaySessionRevision, DrainID: record.DrainID,
			Revision: record.NodeRevision, Fence: record.DrainFence,
		}
		if !marker.Valid() {
			return nil, nil, ErrReplicatedCatalogConflict
		}
		fences = append(fences, marker)
	}
	slices.SortFunc(grants, func(left, right serviceauthz.CommittedFrontendContinuationGrant) int {
		return bytes.Compare(left.GrantDigest[:], right.GrantDigest[:])
	})
	slices.SortFunc(fences, func(left, right serviceauthz.CommittedFrontendDrainFence) int {
		return bytes.Compare(left.DrainID[:], right.DrainID[:])
	})
	for index := 1; index < len(grants); index++ {
		if grants[index-1].GrantDigest == grants[index].GrantDigest {
			return nil, nil, ErrReplicatedCatalogConflict
		}
	}
	for index := 1; index < len(fences); index++ {
		if fences[index-1].DrainID == fences[index].DrainID {
			return nil, nil, ErrReplicatedCatalogConflict
		}
	}
	return grants, fences, nil
}

func (authority *ReplicatedCatalogAuthority) ReadFrontendDrainRecord(
	ctx context.Context, drainID [32]byte,
) (FrontendDrainRecord, error) {
	if authority == nil || ctx == nil || drainID == ([32]byte{}) {
		return FrontendDrainRecord{}, ErrReplicatedCatalog
	}
	_, records, err := authority.ReadFrontendDrainRecordCut(ctx)
	if err != nil {
		return FrontendDrainRecord{}, err
	}
	for _, record := range records {
		if record.DrainID == drainID {
			return record, nil
		}
	}
	return FrontendDrainRecord{}, ErrReplicatedCatalogMissing
}

func (authority *ReplicatedCatalogAuthority) ListFrontendDrainRecords(
	ctx context.Context,
) ([]FrontendDrainRecord, error) {
	_, records, err := authority.ReadFrontendDrainRecordCut(ctx)
	return records, err
}

func (authority *ReplicatedCatalogAuthority) PutFrontendDrainRecord(
	ctx context.Context, record FrontendDrainRecord, expectedRevision uint64,
) error {
	if authority == nil || authority.session == nil || ctx == nil {
		return ErrReplicatedCatalog
	}
	record = record.normalized()
	if !record.Valid() || record.Lifecycle != FrontendDrainPrepared ||
		record.Revision == 0 {
		return ErrScalingState
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return err
	}
	authority.mu.Lock()
	defer authority.mu.Unlock()
	if err = authority.requireRouteSeedServingLocked(); err != nil {
		return err
	}
	if authority.session.Status().Pending {
		return ErrReplicatedCatalogPending
	}
	nodeResult, err := authority.readRaw(ctx, scalingNodeKey(record.PhysicalNode, record.PhysicalIncarnation),
		maxScalingNodeRecordBytes)
	if err != nil {
		return err
	}
	if !nodeResult.Found {
		return ErrScalingNodeMissing
	}
	node, err := openScalingNodeRecord(nodeResult.Value, record.PhysicalNode, record.PhysicalIncarnation)
	if err != nil {
		return err
	}
	if node.Lifecycle != NodeActive || !record.ValidForNode(node) ||
		record.DrainID != NewFrontendDrainID(record.IntentID, NodeReference{NodeID: node.NodeID, Incarnation: node.Incarnation}) {
		return ErrScalingIdentity
	}
	intentResult, err := authority.readRaw(ctx, scalingIntentKey(record.IntentID), maxScalingIntentRecordBytes)
	if err != nil {
		return err
	}
	if !intentResult.Found {
		return ErrScalingIntentMissing
	}
	intent, err := openScalingIntentRecord(intentResult.Value, record.IntentID)
	if err != nil {
		return err
	}
	if intent.Request.Kind != ScalingDecommission ||
		intent.Request.Drain != (NodeReference{NodeID: node.NodeID, Incarnation: node.Incarnation}) ||
		(intent.State != ScalingReserved && intent.State != ScalingRunning) {
		return ErrScalingState
	}
	directory, directoryResult, err := authority.readFrontendDrainDirectory(ctx)
	if err != nil {
		return err
	}
	reservationIndex, reserved := findFrontendDrainReservation(directory, record.DrainID)
	childKey := frontendDrainDocumentKey(record.DrainID)
	childResult, err := authority.readRaw(ctx, childKey, maxReplicatedServiceDirectoryBytes)
	if err != nil {
		return err
	}
	childRaw, err := appendReplicatedFrontendDrainDocument(nil, record)
	if err != nil {
		return err
	}
	if priorEntry, entryIndex, found := findFrontendDrainEntry(directory, record.DrainID); found {
		if !childResult.Found || scalingDigest(childResult.Value) != replication.Digest(priorEntry.Digest) {
			return ErrReplicatedCatalogConflict
		}
		prior, openErr := openReplicatedFrontendDrainDocument(childResult.Value)
		if openErr != nil {
			return openErr
		}
		if bytes.Equal(childResult.Value, childRaw) && priorEntry.Revision == record.Revision {
			return nil
		}
		if expectedRevision == 0 || expectedRevision != prior.Revision || record.Revision != prior.Revision+1 ||
			!samePreparedFrontendDrainImmutable(prior, record) {
			return ErrReplicatedCatalogConflict
		}
		if directory.Revision == ^uint64(0) {
			return ErrReplicatedCatalog
		}
		directory.Drains[entryIndex] = frontendDrainEntry(record, childRaw)
		directory.Revision++
		indexRaw, marshalErr := appendReplicatedServiceDirectory(nil, directory)
		if marshalErr != nil {
			return marshalErr
		}
		result, mutateErr := authority.session.MutateBatch(ctx, []NativeMutation{
			scalingDirectoryMutation(childResult, childKey, childRaw),
			scalingDirectoryMutation(directoryResult, replicatedServiceDirectoryKey, indexRaw),
		})
		return scalingMutationError(result, mutateErr, authority.session)
	}
	if childResult.Found || expectedRevision != 0 {
		return ErrReplicatedCatalogConflict
	}
	if !reserved {
		return ErrReplicatedCatalogConflict
	}
	if directory.Revision == ^uint64(0) {
		return ErrReplicatedCatalog
	}
	directory.Reservations = slices.Delete(directory.Reservations, reservationIndex, reservationIndex+1)
	directory.Drains = append(directory.Drains, frontendDrainEntry(record, childRaw))
	slices.SortFunc(directory.Drains, func(left, right replicatedFrontendDrainEntry) int {
		return bytes.Compare(left.DrainID, right.DrainID)
	})
	if directory.Revision == 0 {
		directory.Revision = 1
	} else {
		directory.Revision++
	}
	indexRaw, err := appendReplicatedServiceDirectory(nil, directory)
	if err != nil {
		return err
	}
	mutations := []NativeMutation{
		scalingDirectoryMutation(childResult, childKey, childRaw),
		scalingDirectoryMutation(directoryResult, replicatedServiceDirectoryKey, indexRaw),
	}
	result, err := authority.session.MutateBatch(ctx, mutations)
	return scalingMutationError(result, err, authority.session)
}

// samePreparedFrontendDrainImmutable admits only a receiver-roster fence
// refresh while a drain remains Prepared. The accepted tokens, scopes,
// admission proof, node/session identity, and drain intent stay byte-for-byte
// fixed across a retry after a directory change.
func samePreparedFrontendDrainImmutable(left, right FrontendDrainRecord) bool {
	left, right = left.normalized(), right.normalized()
	left.ReceiverDirectoryRevision, left.ReceiverDirectoryDigest = 0, replication.Digest{}
	left.ReceiverCatalogGeneration, left.ReceiverCatalogHeadDigest = 0, replication.Digest{}
	right.ReceiverDirectoryRevision, right.ReceiverDirectoryDigest = 0, replication.Digest{}
	right.ReceiverCatalogGeneration, right.ReceiverCatalogHeadDigest = 0, replication.Digest{}
	left.Revision, right.Revision = 0, 0
	encodedLeft, leftErr := vibejson.Marshal(&left)
	encodedRight, rightErr := vibejson.Marshal(&right)
	return leftErr == nil && rightErr == nil && bytes.Equal(encodedLeft, encodedRight)
}

// appendRetiredFrontendDrainMutations advances the durable frontend proof in
// the same batch as the Draining -> Decommissioned node CAS. Storage-only nodes
// have no frontend-drain child row because they carry no gateway admission
// proof and therefore need no extra mutation. A gateway row is mandatory;
// once present, it is the restart-visible tombstone for the exact gateway
// session that was scanned before retirement.
func (authority *ReplicatedCatalogAuthority) appendRetiredFrontendDrainMutations(
	ctx context.Context, prior NodeRecord, terminal NodeRecord,
	_ uint64, _ replication.Digest,
) ([]NativeMutation, error) {
	if authority == nil || ctx == nil {
		return nil, ErrReplicatedCatalog
	}
	if prior.Roles&NodeRoleGateway == 0 {
		return nil, nil
	}
	directory, directoryResult, err := authority.readFrontendDrainDirectory(ctx)
	if err != nil {
		return nil, err
	}
	// Gateway retirement is only valid with the canonical child that proves its
	// exact prepared/enforcing admission cut. A missing index or child must not
	// silently turn a gateway lifecycle CAS into a bare node retirement.
	if !directoryResult.Found || len(directory.Drains) == 0 {
		return nil, ErrScalingState
	}

	var entryIndex int
	var record FrontendDrainRecord
	var childRaw []byte
	found := false
	physicalMatch := false
	identityMatch := false
	for index, candidate := range directory.Drains {
		candidateRecord, raw, readErr := frontendDrainRecordFromEntry(ctx, authority, candidate)
		if readErr != nil {
			return nil, readErr
		}
		if candidateRecord.PhysicalNode != prior.NodeID || candidateRecord.PhysicalIncarnation != prior.Incarnation {
			continue
		}
		physicalMatch = true
		if candidateRecord.GatewayServiceID != prior.Gateway.NodeID ||
			candidateRecord.GatewayIncarnation != prior.Gateway.Incarnation ||
			candidateRecord.GatewayIdentityServiceID != prior.Gateway.ServiceID ||
			candidateRecord.GatewaySessionID != prior.Gateway.SessionID ||
			candidateRecord.GatewaySessionRevision != prior.Gateway.SessionRevision {
			continue
		}
		identityMatch = true
		if candidateRecord.NodeRevision != prior.Revision {
			continue
		}
		_, entryIndex, record, childRaw, found = candidate, index, candidateRecord, raw, true
		break
	}
	if !found {
		if identityMatch {
			return nil, ErrScalingRevision
		}
		if physicalMatch {
			return nil, ErrScalingIdentity
		}
		return nil, ErrScalingState
	}
	if terminal.Lifecycle != NodeDecommissioned || !record.ValidForNode(prior) {
		return nil, ErrScalingIdentity
	}
	// A Prepared proof has not crossed the admission fence and cannot be
	// silently retired. The controller must first commit Enforcing together
	// with the node's Draining CAS.
	if record.Lifecycle != FrontendDrainEnforcing {
		if record.Lifecycle == FrontendDrainRetired {
			return nil, nil
		}
		return nil, ErrScalingState
	}

	retired := record
	retired.Lifecycle = FrontendDrainRetired
	if retired.Revision == ^uint64(0) {
		return nil, ErrReplicatedCatalog
	}
	retired.Revision++
	if retired.ContinuationGrant != nil {
		grant := *retired.ContinuationGrant
		if grant.State != serviceauthz.ContinuationGrantEnforcing {
			return nil, ErrScalingState
		}
		grant.State = serviceauthz.ContinuationGrantRetired
		grant.GrantDigest = grant.Digest()
		if !grant.Valid() {
			return nil, ErrScalingState
		}
		retired.ContinuationGrant = &grant
	}
	retired = retired.normalized()
	if !retired.Valid() {
		return nil, ErrScalingState
	}
	retiredRaw, err := appendReplicatedFrontendDrainDocument(nil, retired)
	if err != nil {
		return nil, err
	}
	directory.Drains[entryIndex] = frontendDrainEntry(retired, retiredRaw)
	if directory.Revision == ^uint64(0) {
		return nil, ErrReplicatedCatalog
	}
	directory.Revision++
	directoryRaw, err := appendReplicatedServiceDirectory(nil, directory)
	if err != nil {
		return nil, err
	}
	return []NativeMutation{
		scalingDirectoryMutation(ReplicatedPointResult{Found: true, Value: childRaw}, frontendDrainDocumentKey(record.DrainID), retiredRaw),
		scalingDirectoryMutation(directoryResult, replicatedServiceDirectoryKey, directoryRaw),
	}, nil
}

// appendEnforcedFrontendDrainMutations is the companion to
// EnforceFrontendDrain. It runs while the node-authority mutex is held, so
// the child row, service-directory proof, and node-directory CAS form one
// replicated batch. A Prepared proof cannot be skipped or replaced by a
// process-local marker.
func (authority *ReplicatedCatalogAuthority) appendEnforcedFrontendDrainMutations(
	ctx context.Context, drainID [32]byte, prior NodeRecord, current NodeRecord,
	nodeDirectoryRevision uint64, nodeDirectoryDigest replication.Digest,
) ([]NativeMutation, error) {
	if authority == nil || ctx == nil || drainID == ([32]byte{}) ||
		prior.Lifecycle != NodeActive || current.Lifecycle != NodeDraining ||
		prior.Revision+1 != current.Revision || prior.Roles&NodeRoleGateway == 0 {
		return nil, ErrScalingState
	}
	directory, directoryResult, err := authority.readFrontendDrainDirectory(ctx)
	if err != nil {
		return nil, err
	}
	if !directoryResult.Found {
		return nil, ErrScalingState
	}
	entry, entryIndex, found := findFrontendDrainEntry(directory, drainID)
	if !found {
		return nil, ErrScalingState
	}
	record, childRaw, err := frontendDrainRecordFromEntry(ctx, authority, entry)
	if err != nil {
		return nil, err
	}
	if record.Lifecycle != FrontendDrainPrepared || !record.ValidForNode(prior) ||
		record.PhysicalNode != prior.NodeID || record.PhysicalIncarnation != prior.Incarnation ||
		record.GatewayServiceID != prior.Gateway.NodeID || record.GatewayIncarnation != prior.Gateway.Incarnation ||
		record.DrainID != drainID {
		return nil, ErrScalingIdentity
	}
	if record.ReceiverDirectoryRevision == 0 {
		return nil, ErrScalingRevision
	}
	// Use the exact node directory already fenced by this mutation. Reading
	// a runtime projection here would re-enter the authority mutex through
	// route-seed publication. The raw catalog head is the only other source
	// needed: its digest is compared again by the same atomic batch below.
	if nodeDirectoryRevision != record.ReceiverDirectoryRevision ||
		nodeDirectoryDigest != record.ReceiverDirectoryDigest {
		return nil, ErrScalingRevision
	}
	receiverHead, headErr := authority.readRaw(ctx, replicatedCatalogHeadKey, maxReplicatedCatalogBytes)
	if headErr != nil || !receiverHead.Found || scalingDigest(receiverHead.Value) != record.ReceiverCatalogHeadDigest {
		return nil, ErrScalingRevision
	}
	payload, openErr := openTypedControlPlaneDocument(receiverHead.Value, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes)
	if openErr != nil {
		return nil, openErr
	}
	snapshot, openErr := OpenSnapshotDocument(payload)
	if openErr != nil || snapshot.Generation() != record.ReceiverCatalogGeneration {
		return nil, errors.Join(ErrScalingRevision, openErr)
	}
	enforcing := record
	enforcing.Lifecycle = FrontendDrainEnforcing
	if enforcing.Revision == ^uint64(0) {
		return nil, ErrReplicatedCatalog
	}
	enforcing.Revision++
	enforcing.NodeRevision = current.Revision
	if enforcing.ContinuationGrant != nil {
		grant := *enforcing.ContinuationGrant
		if grant.State != serviceauthz.ContinuationGrantPrepared {
			return nil, ErrScalingState
		}
		grant.State = serviceauthz.ContinuationGrantEnforcing
		grant.GrantDigest = grant.Digest()
		if !grant.Valid() {
			return nil, ErrScalingState
		}
		enforcing.ContinuationGrant = &grant
	}
	if !enforcing.Valid() || !enforcing.ValidForNode(current) {
		return nil, ErrScalingIdentity
	}
	enforcingRaw, err := appendReplicatedFrontendDrainDocument(nil, enforcing)
	if err != nil {
		return nil, err
	}
	directory.Drains[entryIndex] = frontendDrainEntry(enforcing, enforcingRaw)
	if directory.Revision == ^uint64(0) {
		return nil, ErrReplicatedCatalog
	}
	directory.Revision++
	directoryRaw, err := appendReplicatedServiceDirectory(nil, directory)
	if err != nil {
		return nil, err
	}
	mutations := []NativeMutation{
		scalingDirectoryMutation(ReplicatedPointResult{Found: true, Value: childRaw}, frontendDrainDocumentKey(drainID), enforcingRaw),
		scalingDirectoryMutation(directoryResult, replicatedServiceDirectoryKey, directoryRaw),
	}
	if receiverHead.Found {
		mutations = append(mutations, NativeMutation{Kind: replication.MutationPutDigestEqual,
			Key: replicatedCatalogHeadKey, Value: receiverHead.Value,
			ExpectedValueLength: uint64(len(receiverHead.Value)), ExpectedValueDigest: scalingDigest(receiverHead.Value)})
	}
	return mutations, nil
}

var _ FrontendDrainRecordReader = (*ReplicatedCatalogAuthority)(nil)
var _ FrontendDrainRecordWriter = (*ReplicatedCatalogAuthority)(nil)
var _ FrontendDrainLifecycleEnforcer = (*ReplicatedCatalogAuthority)(nil)
