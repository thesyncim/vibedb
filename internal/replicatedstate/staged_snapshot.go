package replicatedstate

import (
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// StagedSnapshotCut is the synthetic local Raft prefix assigned to one
// independently bootstrapped, already durable user image.
type StagedSnapshotCut struct {
	Applied     uint64
	Term        uint64
	EntryDigest [32]byte
	// ImageAudit, when present, observes the exact same pinned user-image rows
	// as the canonical digest pass. Visit borrows key and value for the call;
	// Finish must authenticate the complete observed image. Supplying only one
	// callback is invalid. This lets an importing subsystem bind its own sealed
	// image proof without a second full collection scan.
	ImageAudit StagedSnapshotImageAudit
}

// StagedSnapshotImageAudit binds a subsystem-specific image proof to the one
// canonical staged-snapshot scan. Callbacks must not retain their borrowed row
// slices. Finish is called only after every row was visited successfully.
type StagedSnapshotImageAudit struct {
	Visit  func(key, value []byte) error
	Finish func() error
}

// StagedSnapshotPreparation is the immutable result of the sole complete
// staged-image audit. The image must remain exclusively owned and non-serving
// until Finish returns. Preparing first lets the checkpoint-group certificate
// commit to the exact state envelope before that envelope is published.
type StagedSnapshotPreparation struct {
	prepared        openInputs
	staticBootstrap *pb.Snapshot
	state           State
	stateEnvelope   []byte
	imageDigest     [32]byte
	userRows        uint64
	userGeneration  uint64
	statePresent    bool
	manifest        SnapshotArtifactManifest
}

// PrepareStagedSnapshot validates one coherent system/user cut and computes
// the canonical user-image digest and row count in a single user-row pass. It
// does not mutate either collection and grants no serving authority.
func PrepareStagedSnapshot(
	binding Binding,
	bootstrap *pb.Snapshot,
	system CollectionTarget,
	user UserCollection,
	txnLog *durable.TxnLog,
	options Options,
	cut StagedSnapshotCut,
	artifactOptions SnapshotArtifactOptions,
) (*StagedSnapshotPreparation, error) {
	prepared, err := prepareOpenInputs(
		binding, bootstrap, system, user, txnLog, options, false,
	)
	_, artifactErr := normalizeSnapshotArtifactOptions(artifactOptions)
	auditValid := (cut.ImageAudit.Visit == nil) == (cut.ImageAudit.Finish == nil)
	if err != nil || options.TransitionCapture != nil || cut.Applied <= 1 ||
		cut.Applied == math.MaxUint64 || cut.Term == 0 || cut.Term == math.MaxUint64 ||
		cut.EntryDigest == ([32]byte{}) ||
		len(bootstrap.GetMetadata().GetConfState().GetVoters()) == 0 || artifactErr != nil ||
		!auditValid {
		return nil, errors.Join(ErrStagedSnapshot, err, artifactErr)
	}

	cutSnapshot, err := durable.SnapshotCollections([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: prepared.system.Collection},
		{Name: prepared.userName, Collection: prepared.user.Collection},
	})
	if err != nil {
		return nil, err
	}
	userSnapshot, userOK := cutSnapshot.Collection(prepared.userName)
	systemSnapshot, systemOK := cutSnapshot.Collection(systemCollectionName)
	if !userOK || userSnapshot == nil || !systemOK || systemSnapshot == nil {
		return nil, errors.Join(ErrStagedSnapshot, cutSnapshot.Close())
	}
	userGeneration := userSnapshot.Generation()
	if userGeneration == 0 {
		return nil, errors.Join(ErrStagedSnapshot, cutSnapshot.Close())
	}

	imageHasher, err := newCanonicalImageHasher(
		prepared.userName, prepared.user.Validation,
		prepared.user.ValidationDigest, prepared.user.Validator,
	)
	var userRows uint64
	if err == nil {
		err = userSnapshot.RangeRaw(func(key, value []byte) error {
			if userRows == math.MaxUint64 {
				return ErrStagedSnapshot
			}
			if err := imageHasher.add(key, value); err != nil {
				return err
			}
			if cut.ImageAudit.Visit != nil {
				if err := cut.ImageAudit.Visit(key, value); err != nil {
					return err
				}
			}
			userRows++
			return nil
		})
	}
	if err == nil && cut.ImageAudit.Finish != nil {
		err = cut.ImageAudit.Finish()
	}
	if err != nil {
		return nil, errors.Join(err, cutSnapshot.Close())
	}
	imageDigest := imageHasher.sum()
	dataChainDigest, err := dataChainSeedDigest(prepared.applyContract, imageDigest)
	if err != nil {
		return nil, errors.Join(err, cutSnapshot.Close())
	}
	state := State{
		Binding: prepared.binding, Applied: cut.Applied, LastTerm: cut.Term,
		LastKind: RecordImportedSnapshot, LastEntryType: pb.EntryNormal,
		LastEntryDigest: cut.EntryDigest, DataChainDigest: dataChainDigest,
		ApplyContractDigest: prepared.applyContract,
		ConfState:           cloneConfState(bootstrap.GetMetadata().GetConfState()),
		ReplicaSetVersion:   1, BootstrapDigest: prepared.bootstrapDigest,
		SnapshotBaseDigest: prepared.bootstrapDigest,
		// Split children do not inherit the source session image yet. Fence every
		// epoch that could have been allocated before this certified source cut,
		// so a delayed command cannot create a fresh child-local session.
		SessionEpochHighWater: cut.Applied,
	}
	if err := validateState(state); err != nil {
		return nil, errors.Join(
			fmt.Errorf("%w: %v", ErrStagedSnapshot, err), cutSnapshot.Close(),
		)
	}
	current, present, sessions, slots, authorities, scanErr := scanSessionSystemSnapshot(
		systemSnapshot, options.MaxSessions, options.RetryWindow,
	)
	closeErr := cutSnapshot.Close()
	if scanErr != nil || closeErr != nil || sessions != 0 || slots != 0 || authorities != 0 ||
		present && !equalState(current, state) {
		return nil, errors.Join(ErrStagedSnapshot, scanErr, closeErr)
	}
	stateEnvelope, err := AppendState(nil, state)
	if err != nil {
		return nil, err
	}
	manifest := SnapshotArtifactManifest{
		State:          cloneState(state),
		UserCollection: []byte(prepared.userName),
		Seeded:         true,
		UserRows:       userRows,
		ImageDigest:    imageDigest,
	}
	manifest.Digest = seededSnapshotManifestDigest(
		stateEnvelope, manifest.UserCollection, imageDigest, userRows,
	)
	if err := validateSnapshotBaseManifest(manifest); err != nil {
		return nil, errors.Join(ErrStagedSnapshot, err)
	}
	return &StagedSnapshotPreparation{
		prepared:        prepared,
		staticBootstrap: proto.Clone(bootstrap).(*pb.Snapshot),
		state:           cloneState(state), stateEnvelope: stateEnvelope,
		imageDigest: imageDigest, userRows: userRows, userGeneration: userGeneration,
		statePresent: present, manifest: manifest,
	}, nil
}

// AppendSeedEnvelope appends a detached copy of the exact imported State
// envelope that a seeded checkpoint-group certificate must authenticate.
func (p *StagedSnapshotPreparation) AppendSeedEnvelope(dst []byte) []byte {
	if p == nil {
		return dst
	}
	return append(dst, p.stateEnvelope...)
}

// AppendSeedKey appends a detached copy of the hidden imported-State key.
func (p *StagedSnapshotPreparation) AppendSeedKey(dst []byte) []byte {
	if p == nil {
		return dst
	}
	return append(dst, stateKey...)
}

// AppliedIndex returns the imported Raft cut bound by the seed envelope.
func (p *StagedSnapshotPreparation) AppliedIndex() uint64 {
	if p == nil {
		return 0
	}
	return p.state.Applied
}

// SeedMember returns the fixed member that owns the imported State row.
func (p *StagedSnapshotPreparation) SeedMember() string {
	if p == nil {
		return ""
	}
	return systemCollectionName
}

// UserMember returns the imported collection whose exact pinned generation was
// authenticated by the preparation scan.
func (p *StagedSnapshotPreparation) UserMember() string {
	if p == nil {
		return ""
	}
	return p.prepared.userName
}

// UserGeneration returns the exact imported collection generation pinned by
// the preparation scan. Seeded checkpoint-group activation uses it as the
// atomic scan-to-ownership handoff fence.
func (p *StagedSnapshotPreparation) UserGeneration() uint64 {
	if p == nil {
		return 0
	}
	return p.userGeneration
}

// NeedsSeed reports whether the coherent preparation cut lacked its exact
// imported State row.
func (p *StagedSnapshotPreparation) NeedsSeed() bool {
	return p != nil && !p.statePresent
}

// Initialize preserves the direct, ungrouped activation API. Replicated SQL
// activation instead creates an authenticated seeded checkpoint group, calls
// CheckpointGroup.Seed, and then calls Finish with that group.
func (p *StagedSnapshotPreparation) Initialize() (
	*Machine,
	*pb.Snapshot,
	SnapshotArtifactManifest,
	error,
) {
	if p == nil {
		return nil, nil, SnapshotArtifactManifest{}, ErrStagedSnapshot
	}
	if !p.statePresent {
		if p.prepared.checkpointGroup != nil {
			return nil, nil, SnapshotArtifactManifest{}, fmt.Errorf(
				"%w: checkpoint group must publish the prepared seed", ErrStagedSnapshot,
			)
		}
		if err := p.prepared.system.Collection.Update(func(batch *durable.WriteBatch) error {
			return batch.Put(stateKey, p.stateEnvelope)
		}); err != nil {
			return nil, nil, SnapshotArtifactManifest{}, err
		}
	}
	return p.Finish(p.prepared.checkpointGroup)
}

// Finish constructs the initialized Machine and its compact snapshot-base
// certificate without reopening or rescanning the user image. A supplied
// checkpoint group must own the exact fixed membership, authenticate this seed
// envelope, and have durably certified the imported cut before Finish succeeds.
func (p *StagedSnapshotPreparation) Finish(
	group *durable.CheckpointGroup,
) (*Machine, *pb.Snapshot, SnapshotArtifactManifest, error) {
	if p == nil {
		return nil, nil, SnapshotArtifactManifest{}, ErrStagedSnapshot
	}
	if p.prepared.checkpointGroup != nil {
		if group == nil {
			group = p.prepared.checkpointGroup
		} else if group != p.prepared.checkpointGroup {
			return nil, nil, SnapshotArtifactManifest{}, fmt.Errorf(
				"%w: checkpoint group changed after preparation", ErrStagedSnapshot,
			)
		}
	}
	members := []durable.NamedCollection{
		{Name: systemCollectionName, Collection: p.prepared.system.Collection},
		{Name: p.prepared.userName, Collection: p.prepared.user.Collection},
	}
	if group != nil {
		seeded, err := group.ValidateSeedState(
			p.state.Applied, systemCollectionName, p.stateEnvelope,
		)
		if err != nil || !seeded || !group.Owns(members) || group.SeedPending() ||
			group.AppliedIndex() != p.state.Applied ||
			group.CheckpointAppliedIndex() != p.state.Applied {
			return nil, nil, SnapshotArtifactManifest{}, errors.Join(
				ErrStagedSnapshot, err,
			)
		}
	}

	systemSnapshot, err := p.prepared.system.Collection.Snapshot()
	if err != nil {
		return nil, nil, SnapshotArtifactManifest{}, err
	}
	current, present, sessions, slots, authorities, scanErr := scanSessionSystemSnapshot(
		systemSnapshot, p.prepared.options.MaxSessions, p.prepared.options.RetryWindow,
	)
	closeErr := systemSnapshot.Close()
	if scanErr != nil || closeErr != nil || !present || sessions != 0 || slots != 0 ||
		authorities != 0 ||
		!equalState(current, p.state) || p.prepared.user.Collection.Len() != p.userRows {
		return nil, nil, SnapshotArtifactManifest{}, errors.Join(
			ErrStagedSnapshot, scanErr, closeErr,
		)
	}
	if p.prepared.user.Collection.Generation() != p.userGeneration {
		return nil, nil, SnapshotArtifactManifest{}, ErrStagedSnapshot
	}

	prepared := p.prepared
	prepared.checkpointGroup = group
	prepared.options.CheckpointGroup = group
	machine := newMachineFromOpenInputs(prepared)
	machine.state = cloneState(p.state)
	machine.openedImageDigest = p.imageDigest
	machine.openedImageApplied = p.state.Applied
	machine.openedImageGeneration = p.userGeneration
	machine.relations[0].openedImage = p.imageDigest
	machine.relations[0].openedApplied = p.state.Applied
	machine.relations[0].openedGen = p.userGeneration
	machine.binding = p.state.Binding
	machine.distribution = []byte(p.state.Binding.Distribution)
	machine.shard = []byte(p.state.Binding.Shard)
	machine.initialized = true
	machine.publication = publicationFromState(p.state)

	manifest := cloneSnapshotArtifactManifest(p.manifest)
	base, err := BuildSnapshotBase(manifest, p.staticBootstrap)
	if err != nil {
		return nil, nil, SnapshotArtifactManifest{}, err
	}
	opened, err := OpenSnapshotBase(base)
	if err != nil || !equalSnapshotArtifactManifest(opened.Manifest, manifest) ||
		!proto.Equal(opened.StaticBootstrap, p.staticBootstrap) {
		return nil, nil, SnapshotArtifactManifest{}, errors.Join(ErrStagedSnapshot, err)
	}
	return machine, base, manifest, nil
}

// InitializeStagedSnapshot converts an exclusively owned, non-serving user
// collection into an initialized replicated-state candidate without rewriting
// its rows. It is the ungrouped compatibility wrapper around the two-phase
// PrepareStagedSnapshot/Finish activation seam.
func InitializeStagedSnapshot(
	binding Binding,
	bootstrap *pb.Snapshot,
	system CollectionTarget,
	user UserCollection,
	txnLog *durable.TxnLog,
	options Options,
	cut StagedSnapshotCut,
	artifactOptions SnapshotArtifactOptions,
) (*Machine, *pb.Snapshot, SnapshotArtifactManifest, error) {
	prepared, err := PrepareStagedSnapshot(
		binding, bootstrap, system, user, txnLog, options, cut, artifactOptions,
	)
	if err != nil {
		return nil, nil, SnapshotArtifactManifest{}, err
	}
	return prepared.Initialize()
}
