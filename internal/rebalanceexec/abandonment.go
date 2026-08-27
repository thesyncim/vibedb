package rebalanceexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	vibejson "github.com/thesyncim/vibejson"
)

var ErrSnapshotAbandonment = errors.New("rebalanceexec: snapshot abandonment conflicts with catalog authority")

type abandonmentEnvelope struct {
	Witness []byte `json:"witness"`
}

// CatalogAbandonmentJournal is implemented by ReplicatedCatalogAuthority. A
// cancelled move record is therefore the sole authority; process-local age or
// missing source responses never synthesize abandonment.
type CatalogAbandonmentJournal interface {
	ReadOperationIDs(context.Context) ([][32]byte, error)
	ReadOperation(context.Context, [32]byte) (gateway.ReplicatedOperationRecord, error)
	PublishOperation(context.Context, uint64, gateway.ReplicatedOperationRecord) error
	PublishReplicaMoveAbandonment(context.Context, uint64, gateway.ReplicatedOperationRecord) error
	DeleteOperation(context.Context, [32]byte, uint64) error
}

type CatalogAbandonmentAuthority struct{ Journal CatalogAbandonmentJournal }

func (authority CatalogAbandonmentAuthority) ReadArtifactAbandonment(
	ctx context.Context, operation [32]byte,
) (snapshottransfer.ArtifactAbandonmentWitness, bool, error) {
	if ctx == nil || operation == ([32]byte{}) || authority.Journal == nil {
		return snapshottransfer.ArtifactAbandonmentWitness{}, false, ErrSnapshotAbandonment
	}
	record, err := authority.Journal.ReadOperation(ctx, operation)
	if errors.Is(err, gateway.ErrReplicatedOperationMissing) {
		return snapshottransfer.ArtifactAbandonmentWitness{}, false, nil
	}
	if err != nil {
		return snapshottransfer.ArtifactAbandonmentWitness{}, false, err
	}
	if record.ID != operation || record.Kind != gateway.ReplicatedOperationMove ||
		record.State != gateway.ReplicatedOperationCancelled {
		return snapshottransfer.ArtifactAbandonmentWitness{}, false, nil
	}
	var envelope abandonmentEnvelope
	if err = vibejson.Unmarshal(record.Intent, &envelope); err != nil {
		return snapshottransfer.ArtifactAbandonmentWitness{}, false, errors.Join(err, ErrSnapshotAbandonment)
	}
	canonical, canonicalErr := vibejson.Marshal(&envelope)
	if canonicalErr != nil || !bytes.Equal(canonical, record.Intent) ||
		sha256.Sum256(record.Intent) != record.IntentDigest {
		return snapshottransfer.ArtifactAbandonmentWitness{}, false, errors.Join(canonicalErr, ErrSnapshotAbandonment)
	}
	witness, err := snapshottransfer.OpenAbandonmentWitness(envelope.Witness)
	if err != nil || witness.Operation != operation || witness.AuthorityRevision != record.Revision ||
		witness.OwnerEpoch != record.CatalogGeneration || witness.LeaseRevision+1 != record.Revision ||
		witness.LeaseAppliedThrough+1 != witness.AbandonedAppliedThrough {
		return snapshottransfer.ArtifactAbandonmentWitness{}, false, errors.Join(err, ErrSnapshotAbandonment)
	}
	return witness, true, nil
}

// Publish records the exact cancellation and owner-lease retirement as one
// catalog RF3 CAS. The caller supplies the artifact identity observed from the
// source; the journal revision and catalog generation are the monotonic lease
// fence, and cannot be selected from a local clock.
func (authority CatalogAbandonmentAuthority) Publish(
	ctx context.Context, expected uint64, witness snapshottransfer.ArtifactAbandonmentWitness,
) (snapshottransfer.ArtifactAbandonmentWitness, error) {
	if ctx == nil || authority.Journal == nil || expected == 0 ||
		witness.Operation == ([32]byte{}) || witness.Step == ([32]byte{}) ||
		witness.Artifact == ([32]byte{}) || witness.Owner == ([16]byte{}) ||
		!witness.Descriptor.Valid() || witness.Artifact != witness.Descriptor.ArtifactHash ||
		witness.TargetStore != witness.Descriptor.TargetStore ||
		witness.TargetIncarnation != witness.Descriptor.TargetIncarnation ||
		witness.SchemaGeneration != witness.Descriptor.SchemaGeneration ||
		witness.ReplicaSetVersion != witness.Descriptor.ReplicaSetVersion {
		return snapshottransfer.ArtifactAbandonmentWitness{}, ErrSnapshotAbandonment
	}
	record, err := authority.Journal.ReadOperation(ctx, witness.Operation)
	if err != nil || record.ID != witness.Operation || record.Kind != gateway.ReplicatedOperationMove ||
		record.State == gateway.ReplicatedOperationComplete ||
		record.State == gateway.ReplicatedOperationCancelled || record.Revision != expected ||
		record.Revision == ^uint64(0) {
		return snapshottransfer.ArtifactAbandonmentWitness{}, errors.Join(err, ErrSnapshotAbandonment)
	}
	witness.OwnerEpoch = record.CatalogGeneration
	witness.LeaseRevision = record.Revision
	witness.LeaseAppliedThrough = record.Revision
	witness.AbandonedAppliedThrough = record.Revision + 1
	witness.AuthorityRevision = record.Revision + 1
	if !witness.Valid() {
		return snapshottransfer.ArtifactAbandonmentWitness{}, ErrSnapshotAbandonment
	}
	raw, err := snapshottransfer.AppendAbandonmentWitness(nil, witness)
	if err != nil {
		return snapshottransfer.ArtifactAbandonmentWitness{}, err
	}
	intent, err := vibejson.Marshal(&abandonmentEnvelope{Witness: raw})
	if err != nil {
		return snapshottransfer.ArtifactAbandonmentWitness{}, err
	}
	next := record
	next.State, next.Revision = gateway.ReplicatedOperationCancelled, record.Revision+1
	next.Intent = intent
	next.IntentDigest = sha256.Sum256(intent)
	next.Proof = next.IntentDigest
	next.Cursor = [8]uint64{witness.LeaseRevision, witness.AbandonedAppliedThrough}
	if err = authority.Journal.PublishReplicaMoveAbandonment(ctx, record.Revision, next); err != nil {
		settled, found, readErr := authority.ReadArtifactAbandonment(ctx, witness.Operation)
		if readErr == nil && found && settled == witness {
			return witness, nil
		}
		return snapshottransfer.ArtifactAbandonmentWitness{}, errors.Join(err, readErr)
	}
	return witness, nil
}

type SnapshotAbandonmentClient interface {
	AbandonReplicaMoveSnapshot(context.Context, snapshottransfer.SourceControlRequest,
		snapshottransfer.ArtifactAbandonmentWitness) error
}

type AbandonmentScheduler struct {
	Directory  CatalogAbandonmentJournal
	Authority  CatalogAbandonmentAuthority
	Source     SnapshotAbandonmentClient
	MaxRecords int
	MaxBytes   uint64
}

type AbandonmentSchedulerCursor struct{ AfterOperation [32]byte }

type AbandonmentSchedulerPass struct {
	Cursor                                  AbandonmentSchedulerCursor
	Discovered, Scanned, Witnessed, Deleted uint32
	ScheduledBytes                          uint64
	Done                                    bool
}

func (scheduler *AbandonmentScheduler) RunPass(
	ctx context.Context, cursor AbandonmentSchedulerCursor,
) (AbandonmentSchedulerPass, error) {
	pass := AbandonmentSchedulerPass{Cursor: cursor}
	if scheduler == nil || ctx == nil || scheduler.Directory == nil ||
		scheduler.Authority.Journal == nil || scheduler.Source == nil ||
		scheduler.MaxRecords <= 0 || scheduler.MaxRecords > 4096 || scheduler.MaxBytes == 0 {
		return pass, ErrSnapshotAbandonment
	}
	ids, err := scheduler.Directory.ReadOperationIDs(ctx)
	if err != nil {
		return pass, err
	}
	pass.Discovered = uint32(len(ids))
	for _, operation := range ids {
		if bytes.Compare(operation[:], cursor.AfterOperation[:]) <= 0 {
			continue
		}
		if pass.Scanned == uint32(scheduler.MaxRecords) {
			return pass, nil
		}
		pass.Scanned++
		witness, found, readErr := scheduler.Authority.ReadArtifactAbandonment(ctx, operation)
		if readErr != nil {
			return pass, readErr
		}
		if !found {
			pass.Cursor.AfterOperation = operation
			continue
		}
		pass.Witnessed++
		artifactBytes := uint64(snapshottransfer.DescriptorBytes) + witness.Descriptor.ArtifactBytes
		if artifactBytes > scheduler.MaxBytes-pass.ScheduledBytes {
			return pass, nil
		}
		request := snapshottransfer.SourceControlRequest{
			Operation: witness.Operation, Step: witness.Step, Group: witness.Descriptor.Group,
			SourceMember: witness.Descriptor.SourceMember, TargetMember: witness.Descriptor.TargetMember,
			TargetStore: witness.TargetStore, TargetIncarnation: witness.TargetIncarnation,
			ReplicaSetVersion: witness.ReplicaSetVersion, SourceNode: witness.Owner,
		}
		if err = scheduler.Source.AbandonReplicaMoveSnapshot(ctx, request, witness); err != nil {
			return pass, err
		}
		pass.Deleted++
		pass.ScheduledBytes += artifactBytes
		if err = scheduler.Directory.DeleteOperation(ctx, operation, witness.AuthorityRevision); err != nil {
			return pass, err
		}
		pass.Cursor.AfterOperation = operation
	}
	pass.Done = true
	return pass, nil
}
