package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/schemachange"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

// VerifiedReplicatedSchemaTarget owns opened, audited target files at exact
// durable generations. Construct it before taking the final write fence; its
// Prepare method performs no row scan or target open. Call Close on every
// outcome. It holds only shard-local DDL ownership, never a source write lock.
// The zero value cannot authorize preparation, and the handle is not durable
// across process replacement: restart must re-audit outside the write fence.
type VerifiedReplicatedSchemaTarget struct {
	mu              sync.Mutex
	owner           *ReplicatedApply
	raw             []byte
	expectedApplied uint64
	sourceDigest    [32]byte
	shadow          *schemaDDLShadowRecord
	staged          *database
	target          ReplicatedShardStoreIdentity
	proof           ReplicatedSchemaTargetProof
	opened          []*table
	images          []durable.ImageIdentity
	audit           *replicatedstate.SchemaImageAudit
	unlock          func()
	closed          bool
}

// PreflightReplicatedSchemaTarget opens and audits the complete target before
// a write fence is acquired. expectedApplied is the cut that produced the
// target, never a current-index substitute. Mutable shadow targets are accepted
// only under their exact ready journal/capture cursor and remain exclusively
// owned until this handle closes. Source writes may continue during the audit;
// Prepare refuses the stale handle if any publication overtakes its cut.
func (a *ReplicatedApply) PreflightReplicatedSchemaTarget(ctx context.Context, raw []byte, expectedApplied uint64) (result *VerifiedReplicatedSchemaTarget, resultErr error) {
	if expectedApplied == 0 {
		return nil, ErrReplicatedSchemaCatalogImage
	}
	root, unlock, err := a.lockSchemaDDLShadow(ctx)
	if err != nil {
		return nil, err
	}
	verified := &VerifiedReplicatedSchemaTarget{owner: a, raw: bytes.Clone(raw), expectedApplied: expectedApplied, unlock: unlock}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, verified.Close())
		}
	}()
	source, generation, err := a.schemaDDLShadowSource()
	if err != nil {
		return nil, err
	}
	verified.sourceDigest = sha256.Sum256(source)
	shadow, found, err := readSchemaDDLShadowRecord(root)
	if err != nil {
		return nil, err
	}
	if found {
		if !shadow.Ready || !bytes.Equal(shadow.Shadow.Catalog, raw) || shadow.SourceGeneration != generation ||
			shadow.SourceDigest != verified.sourceDigest || shadow.Shadow.Cursor.Publication.Applied != expectedApplied {
			return nil, ErrReplicatedSchemaDDLConflict
		}
		verified.shadow = &shadow
		if err := verified.checkCapture(); err != nil {
			return nil, err
		}
	} else if record, found, err := readSchemaDDLBuildRecord(root); err != nil {
		return nil, err
	} else if found && bytes.Equal(record.Target.Catalog, raw) && (!record.Ready || record.Applied != expectedApplied) {
		return nil, ErrReplicatedSchemaDDLConflict
	}
	if a.Applied() != expectedApplied {
		return nil, fmt.Errorf("schema target source applied advanced: current=%d target=%d: %w",
			a.Applied(), expectedApplied, ErrTransactionConflict)
	}
	if _, err := a.certifyReplicatedSchemaTargetWithHandoff(verified.raw, nil, verified); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return verified, nil
}

func (v *VerifiedReplicatedSchemaTarget) checkCapture() error {
	if v.shadow == nil {
		return nil
	}
	d, err := v.owner.ReplicatedSchemaCaptureDescriptor(v.shadow.Shadow.Operation)
	if err != nil || d.Config != v.shadow.Capture || d.Abort != schemachange.NotAborted || d.Head != v.shadow.Shadow.Cursor {
		return errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	return nil
}

// Prepare rechecks source/capture and every opaque durable target identity
// before publishing checkpoint membership. Its work is bounded by catalog and
// relation count, not row count. The caller must acquire its distributed write
// fence after Preflight and hold it through the schema transition. This method
// does not itself provide that cross-replica fence or activate serving handles.
func (v *VerifiedReplicatedSchemaTarget) Prepare(ctx context.Context, authorization [32]byte) (ReplicatedSchemaTargetProof, error) {
	if v == nil || ctx == nil || authorization == [32]byte{} {
		return ReplicatedSchemaTargetProof{}, ErrReplicatedSchemaCatalogImage
	}
	if err := ctx.Err(); err != nil {
		return ReplicatedSchemaTargetProof{}, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.owner == nil || v.staged == nil || len(v.opened) == 0 || len(v.opened) != len(v.images) {
		return ReplicatedSchemaTargetProof{}, ErrReplicatedSchemaCatalogImage
	}
	if v.shadow != nil && authorization != v.shadow.Shadow.Operation {
		return ReplicatedSchemaTargetProof{}, ErrReplicatedSchemaDDLConflict
	}
	if err := v.checkCapture(); err != nil {
		return ReplicatedSchemaTargetProof{}, err
	}
	source, _, err := v.owner.schemaDDLShadowSource()
	if err != nil || sha256.Sum256(source) != v.sourceDigest {
		return ReplicatedSchemaTargetProof{}, fmt.Errorf("schema target source catalog changed after preflight: %w",
			errors.Join(err, ErrTransactionConflict))
	}
	for i, candidate := range v.opened {
		if !candidate.collection.MatchesDurableImage(v.images[i]) {
			return ReplicatedSchemaTargetProof{}, fmt.Errorf("schema target relation %d changed after preflight: %w",
				i, ErrTransactionConflict)
		}
	}
	proof := v.proof
	if err := v.owner.prepareVerifiedSchemaMembership(v.raw, v.expectedApplied, authorization, v.staged, v.target, &proof); err != nil {
		return ReplicatedSchemaTargetProof{}, err
	}
	v.proof = proof
	return proof, nil
}

// DetachedTarget returns the audited, unpublished build receipt before
// membership preparation. It grants no serving authority and remains valid
// only for the exact source cut captured by this handle. The caller retains
// the external write fence until it durably journals the receipt.
func (v *VerifiedReplicatedSchemaTarget) DetachedTarget() (ReplicatedSchemaDDLTarget, error) {
	if v == nil {
		return ReplicatedSchemaDDLTarget{}, ErrReplicatedSchemaCatalogImage
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed || v.owner == nil || v.staged == nil || len(v.raw) == 0 ||
		v.proof.Membership != (durable.CheckpointMembershipWitness{}) {
		return ReplicatedSchemaDDLTarget{}, ErrReplicatedSchemaCatalogImage
	}
	target := ReplicatedSchemaDDLTarget{Catalog: bytes.Clone(v.raw), Proof: v.proof}
	if err := ValidateReplicatedSchemaDDLTarget(target, v.expectedApplied,
		target.Proof.Catalog.SchemaGeneration-1); err != nil {
		return ReplicatedSchemaDDLTarget{}, err
	}
	return target, nil
}

func (v *VerifiedReplicatedSchemaTarget) Close() error {
	if v == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.closed {
		return nil
	}
	v.closed = true
	var result error
	for i := len(v.opened) - 1; i >= 0; i-- {
		result = errors.Join(result, v.opened[i].collection.Close(), v.opened[i].file.Close())
	}
	clear(v.opened)
	v.opened, v.images, v.staged = nil, nil, nil
	v.owner, v.shadow, v.raw = nil, nil, nil
	if v.unlock != nil {
		v.unlock()
		v.unlock = nil
	}
	return result
}

// ResumePrepared reuses the audit of a closed, successfully prepared target
// for command construction. The durable stage marker prevents shadow replay;
// the exact source catalog/applied cut and preparation witness are rechecked.
// This does not acquire the distributed write fence or publish a transition.
func (v *VerifiedReplicatedSchemaTarget) ResumePrepared(ctx context.Context, a *ReplicatedApply, raw []byte, request [32]byte) (ReplicatedSchemaTargetProof, error) {
	return v.resumePrepared(ctx, a, raw, request, 0)
}

// ResumePreparedAfterEmptySuffix permits a later machine index only when the
// shard owner has independently proved every intervening WAL entry empty and
// normal. The returned proof deliberately retains its original prepared cut.
func (v *VerifiedReplicatedSchemaTarget) ResumePreparedAfterEmptySuffix(
	ctx context.Context, a *ReplicatedApply, raw []byte, request [32]byte,
	preCommandApplied uint64,
) (ReplicatedSchemaTargetProof, error) {
	if v == nil || preCommandApplied <= v.expectedApplied {
		return ReplicatedSchemaTargetProof{}, ErrReplicatedSchemaCatalogImage
	}
	return v.resumePrepared(ctx, a, raw, request, preCommandApplied)
}

func (v *VerifiedReplicatedSchemaTarget) resumePrepared(ctx context.Context, a *ReplicatedApply, raw []byte, request [32]byte, preCommandApplied uint64) (ReplicatedSchemaTargetProof, error) {
	if v == nil || ctx == nil || a == nil || a.database == nil || request == ([32]byte{}) {
		return ReplicatedSchemaTargetProof{}, ErrReplicatedSchemaCatalogImage
	}
	if err := ctx.Err(); err != nil {
		return ReplicatedSchemaTargetProof{}, err
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.closed || v.audit == nil || v.proof.Membership.Sequence == 0 ||
		v.proof.Relations != v.audit.Certificate() || v.proof.Catalog.Digest != sha256.Sum256(raw) {
		return ReplicatedSchemaTargetProof{}, ErrReplicatedSchemaCatalogImage
	}
	wantApplied := v.expectedApplied
	if preCommandApplied != 0 {
		wantApplied = preCommandApplied
	}
	source, _, err := a.schemaDDLShadowSource()
	if err != nil || sha256.Sum256(source) != v.sourceDigest || a.Applied() != wantApplied {
		return ReplicatedSchemaTargetProof{}, errors.Join(err, ErrTransactionConflict)
	}
	proof, err := a.recoverPreparedSchemaTargetProof(raw, request, v.proof)
	if err != nil || proof != v.proof {
		return ReplicatedSchemaTargetProof{}, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return proof, nil
}

// OpenActivatedApply reuses this target's row audit after its files have been
// closed and the exact committed schema has been published and selected. It
// does not publish, select or quiesce a generation. The ordinary open path still
// validates checkpoint membership, committed transition and all session state.
// A different image fails rather than rescanning under the caller's fence.
func (v *VerifiedReplicatedSchemaTarget) OpenActivatedApply(d *Database, expected ReplicatedShardStoreIdentity,
	bootstrap *pb.Snapshot, options ReplicatedApplyOptions,
) (*ReplicatedApply, ReplicatedApplyIdentity, error) {
	if v == nil {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedSchemaCatalogImage
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.closed || v.audit == nil || v.proof.Membership.Sequence == 0 || !v.target.Equal(expected) {
		return nil, ReplicatedApplyIdentity{}, ErrReplicatedSchemaCatalogImage
	}
	return d.openReplicatedApplyWithSchemaAudit(expected, bootstrap, options, nil, v.audit)
}
