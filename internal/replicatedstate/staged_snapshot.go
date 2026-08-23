package replicatedstate

import (
	"errors"
	"fmt"
	"io"
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
}

// InitializeStagedSnapshot converts an exclusively owned, non-serving user
// collection into an initialized replicated-state candidate without rewriting
// its rows. It writes only the hidden state row, then emits the standard small
// Raft snapshot-base certificate that must still be installed by the Raft
// runtime before serving. Exact retries before that install are idempotent.
func InitializeStagedSnapshot(
	binding Binding,
	bootstrap *pb.Snapshot,
	system CollectionTarget,
	user UserCollection,
	txnLog *durable.TxnLog,
	options Options,
	cut StagedSnapshotCut,
	artifactOptions SnapshotArtifactOptions,
) (
	machine *Machine,
	base *pb.Snapshot,
	manifest SnapshotArtifactManifest,
	resultErr error,
) {
	prepared, err := prepareOpenInputs(
		binding, bootstrap, system, user, txnLog, options,
	)
	_, artifactErr := normalizeSnapshotArtifactOptions(artifactOptions)
	if err != nil || options.TransitionCapture != nil || cut.Applied <= 1 ||
		cut.Applied == math.MaxUint64 || cut.Term == 0 || cut.Term == math.MaxUint64 ||
		cut.EntryDigest == ([32]byte{}) ||
		len(bootstrap.GetMetadata().GetConfState().GetVoters()) == 0 || artifactErr != nil {
		return nil, nil, SnapshotArtifactManifest{},
			errors.Join(ErrStagedSnapshot, err, artifactErr)
	}
	cutSnapshot, err := durable.SnapshotCollections([]durable.NamedCollection{
		{Name: systemCollectionName, Collection: prepared.system.Collection},
		{Name: prepared.userName, Collection: prepared.user.Collection},
	})
	if err != nil {
		return nil, nil, SnapshotArtifactManifest{}, err
	}
	userSnapshot, userOK := cutSnapshot.Collection(prepared.userName)
	systemSnapshot, systemOK := cutSnapshot.Collection(systemCollectionName)
	if !userOK || userSnapshot == nil || !systemOK || systemSnapshot == nil {
		_ = cutSnapshot.Close()
		return nil, nil, SnapshotArtifactManifest{}, ErrStagedSnapshot
	}
	imageDigest, err := canonicalImageDigest(
		prepared.userName, prepared.user.Validation, prepared.user.ValidationDigest,
		prepared.user.Validator, userSnapshot, nil,
	)
	if err != nil {
		_ = cutSnapshot.Close()
		return nil, nil, SnapshotArtifactManifest{}, err
	}
	dataChainDigest, err := dataChainSeedDigest(prepared.applyContract, imageDigest)
	if err != nil {
		_ = cutSnapshot.Close()
		return nil, nil, SnapshotArtifactManifest{}, err
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
		_ = cutSnapshot.Close()
		return nil, nil, SnapshotArtifactManifest{},
			fmt.Errorf("%w: %v", ErrStagedSnapshot, err)
	}
	current, present, sessions, slots, scanErr := scanSessionSystemSnapshot(
		systemSnapshot, options.MaxSessions, options.RetryWindow,
	)
	closeErr := cutSnapshot.Close()
	if scanErr != nil || closeErr != nil || sessions != 0 || slots != 0 ||
		present && !equalState(current, state) {
		return nil, nil, SnapshotArtifactManifest{}, errors.Join(
			ErrStagedSnapshot, scanErr, closeErr,
		)
	}
	if !present {
		envelope, appendErr := AppendState(nil, state)
		if appendErr != nil {
			return nil, nil, SnapshotArtifactManifest{}, appendErr
		}
		if err := prepared.system.Collection.Update(func(batch *durable.WriteBatch) error {
			return batch.Put(stateKey, envelope)
		}); err != nil {
			return nil, nil, SnapshotArtifactManifest{}, err
		}
	}
	machine, err = Open(
		prepared.binding, bootstrap, prepared.system,
		UserCollection{Name: prepared.userName, Target: prepared.user},
		prepared.txnLog, prepared.options,
	)
	if err != nil {
		return nil, nil, SnapshotArtifactManifest{},
			fmt.Errorf("%w: reopen initialized image: %v", ErrStagedSnapshot, err)
	}
	snapshot, err := machine.Snapshot(prepared.userName)
	if err != nil {
		return nil, nil, SnapshotArtifactManifest{}, err
	}
	manifest, err = WriteSnapshotArtifact(io.Discard, snapshot, artifactOptions)
	closeErr = snapshot.Close()
	if err != nil || closeErr != nil || !equalState(manifest.State, state) {
		return nil, nil, SnapshotArtifactManifest{}, errors.Join(
			ErrStagedSnapshot, err, closeErr,
		)
	}
	base, err = BuildSnapshotBase(manifest, bootstrap)
	if err != nil {
		return nil, nil, SnapshotArtifactManifest{}, err
	}
	opened, err := OpenSnapshotBase(base)
	if err != nil || !equalState(opened.Manifest.State, state) ||
		!proto.Equal(opened.StaticBootstrap, bootstrap) {
		return nil, nil, SnapshotArtifactManifest{},
			errors.Join(ErrStagedSnapshot, err)
	}
	return machine, base, manifest, nil
}
