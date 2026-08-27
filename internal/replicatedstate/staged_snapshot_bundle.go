package replicatedstate

import (
	"crypto/sha256"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// PrepareStagedSnapshotBundle authenticates an exclusively owned, non-serving
// dense relation image and synthesizes fresh replicated state for binding and
// bootstrap. Hidden source state is deliberately not an input. Like the
// singleton preparation, it does not publish a state row or serving authority.
func PrepareStagedSnapshotBundle(
	binding Binding,
	bootstrap *pb.Snapshot,
	system CollectionTarget,
	relations []RelationCollection,
	txnLog *durable.TxnLog,
	options Options,
	cut StagedSnapshotCut,
	artifactOptions SnapshotArtifactOptions,
) (*StagedSnapshotPreparation, error) {
	if len(relations) < 2 || cut.ImageAudit.Visit != nil || cut.ImageAudit.Finish != nil {
		return nil, ErrStagedSnapshot
	}
	prepared, err := prepareOpenInputs(
		binding, bootstrap, system,
		UserCollection{Name: relations[0].Name, Target: relations[0].Target},
		txnLog, options, true,
	)
	_, artifactErr := normalizeSnapshotArtifactOptions(artifactOptions)
	if err != nil || options.TransitionCapture != nil ||
		prepared.options.TransitionCaptureTarget.Collection == nil ||
		prepared.options.TransitionCaptureTarget.Name != TransitionCaptureCollectionName ||
		cut.Applied <= 1 || cut.Applied == math.MaxUint64 || cut.Term == 0 ||
		cut.Term == math.MaxUint64 || cut.EntryDigest == ([sha256.Size]byte{}) ||
		len(bootstrap.GetMetadata().GetConfState().GetVoters()) == 0 || artifactErr != nil {
		return nil, errors.Join(ErrStagedSnapshot, err, artifactErr)
	}

	preparedRelations, manifestDigest, err := prepareRelationCollections(binding, relations)
	if err != nil {
		return nil, errors.Join(ErrStagedSnapshot, err)
	}
	contract, err := bundleApplyContractDigest(
		manifestDigest, preparedRelations, options.MaxSessions, options.RetryWindow,
		options.RequestLedgerCapacityBytes, options.RequestLedgerCleanupReserveBytes,
		options.RequestLedgerRange, routeGateRecordLimit(),
	)
	if err != nil {
		return nil, errors.Join(ErrStagedSnapshot, err)
	}
	prepared.relations = preparedRelations
	prepared.manifestDigest = manifestDigest
	prepared.applyContract = contract
	prepared.members = make([]durable.NamedCollection, 1, len(preparedRelations)+2)
	prepared.members[0] = durable.NamedCollection{Name: systemCollectionName, Collection: system.Collection}
	for i := range preparedRelations {
		prepared.members = append(prepared.members, durable.NamedCollection{
			Name: preparedRelations[i].name, Collection: preparedRelations[i].target.Collection,
		})
	}
	capture := prepared.options.TransitionCaptureTarget
	if !validReservedTransitionCaptureTarget(capture, system, preparedRelations) {
		return nil, ErrStagedSnapshot
	}
	prepared.members = append(prepared.members, durable.NamedCollection{
		Name: capture.Name, Collection: capture.Collection,
	})
	if options.CheckpointGroup != nil && !options.CheckpointGroup.Owns(prepared.members) {
		return nil, ErrStagedSnapshot
	}
	if err := txnLog.ValidateCollections(prepared.members); err != nil {
		return nil, errors.Join(ErrStagedSnapshot, err)
	}
	if err := validateBundleTransactionProfile(system, preparedRelations, options); err != nil {
		return nil, errors.Join(ErrStagedSnapshot, err)
	}

	snapshots, err := durable.SnapshotCollections(prepared.members)
	if err != nil {
		return nil, err
	}
	defer func() { _ = snapshots.Close() }()
	systemSnapshot, systemOK := snapshots.Collection(systemCollectionName)
	captureSnapshot, captureOK := snapshots.Collection(capture.Name)
	if !systemOK || systemSnapshot == nil || !captureOK || captureSnapshot == nil ||
		captureSnapshot.Generation() == 0 || captureSnapshot.Len() != 0 {
		return nil, ErrStagedSnapshot
	}
	captureDigest, err := snapshotArtifactOpaqueImageDigest(captureSnapshot)
	if err != nil || captureDigest != snapshotArtifactEmptyCaptureImageDigest() {
		return nil, errors.Join(ErrStagedSnapshot, err)
	}

	relationRows := make([]uint64, len(preparedRelations))
	relationGenerations := make([]uint64, len(preparedRelations))
	relationImages := make([][sha256.Size]byte, len(preparedRelations))
	relationPlacements := make([]relationPlacementAccumulator, len(preparedRelations))
	manifestRelations := make([]SnapshotArtifactRelation, len(preparedRelations))
	var userRows uint64
	for i := range preparedRelations {
		relation := &preparedRelations[i]
		snapshot, ok := snapshots.Collection(relation.name)
		if !ok || snapshot == nil || snapshot.Generation() == 0 {
			return nil, ErrStagedSnapshot
		}
		if err := validateRelationIndexCatalog(snapshot, relation.localIndexes); err != nil {
			return nil, errors.Join(ErrStagedSnapshot, err)
		}
		image, placement, err := openedRelationImageDigest(relation, snapshot, binding.OwnedRange)
		if err != nil || userRows > math.MaxUint64-snapshot.Len() {
			return nil, errors.Join(ErrStagedSnapshot, err)
		}
		relationRows[i], relationGenerations[i], relationImages[i], relationPlacements[i] =
			snapshot.Len(), snapshot.Generation(), image, placement
		userRows += snapshot.Len()
		relation.openedImage = image
		relation.openedGen = snapshot.Generation()
		relation.placement = placement
		relation.placementGen = snapshot.Generation()
		manifestRelations[i] = SnapshotArtifactRelation{
			Relation: relation.id, Kind: relation.kind, Collection: []byte(relation.name),
			Rows: snapshot.Len(), ImageDigest: image,
		}
	}
	imageDigest := canonicalBundleImageDigest(manifestRelations)
	dataChainDigest, err := dataChainSeedDigest(prepared.applyContract, imageDigest)
	if err != nil {
		return nil, err
	}
	state := State{
		Binding: prepared.binding, Applied: cut.Applied, LastTerm: cut.Term,
		LastKind: RecordImportedSnapshot, LastEntryType: pb.EntryNormal,
		LastEntryDigest: cut.EntryDigest, DataChainDigest: dataChainDigest,
		ApplyContractDigest: prepared.applyContract,
		ConfState:           cloneConfState(bootstrap.GetMetadata().GetConfState()),
		ReplicaSetVersion:   1, BootstrapDigest: prepared.bootstrapDigest,
		SnapshotBaseDigest: prepared.bootstrapDigest, SessionEpochHighWater: cut.Applied,
	}
	state.RelationPlacementDigest = relationPlacementStateDigest(
		binding.SchemaGeneration, manifestDigest, preparedRelations,
	)
	if err := validateState(state); err != nil {
		return nil, errors.Join(ErrStagedSnapshot, err)
	}
	current, present, sessions, slots, authorities, _, scanErr := scanSessionSystemSnapshot(
		systemSnapshot, options.MaxSessions, options.RetryWindow,
		options.RequestLedgerCapacityBytes, options.RequestLedgerCleanupReserveBytes,
		options.RequestLedgerRange, routeGateRecordLimit(),
	)
	if scanErr != nil || sessions != 0 || slots != 0 || authorities != 0 ||
		present && !equalState(current, state) {
		return nil, errors.Join(ErrStagedSnapshot, scanErr)
	}
	stateEnvelope, err := AppendState(nil, state)
	if err != nil {
		return nil, err
	}
	manifest := SnapshotArtifactManifest{
		State: cloneState(state), UserCollection: []byte(prepared.userName), Bundle: true,
		RelationManifestDigest: manifestDigest, Relations: manifestRelations,
		SystemRows: 1, UserRows: userRows, ImageDigest: imageDigest,
		CaptureImageDigest: captureDigest,
	}
	manifest.Digest = bundleSnapshotManifestDigest(
		stateEnvelope, manifestDigest, manifest.SystemRows, manifestRelations,
		imageDigest, 0, captureDigest,
	)
	if err := validateSnapshotBaseManifest(manifest); err != nil {
		return nil, errors.Join(ErrStagedSnapshot, err)
	}
	return &StagedSnapshotPreparation{
		prepared: prepared, staticBootstrap: proto.Clone(bootstrap).(*pb.Snapshot), state: cloneState(state),
		stateEnvelope: stateEnvelope, imageDigest: imageDigest, userRows: relationRows[0],
		userGeneration: relationGenerations[0], captureGeneration: captureSnapshot.Generation(),
		statePresent: present, manifest: manifest, relationRows: relationRows,
		relationGenerations: relationGenerations, relationImages: relationImages,
		relationPlacements: relationPlacements,
	}, nil
}
