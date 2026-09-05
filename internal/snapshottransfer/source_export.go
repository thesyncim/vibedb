package snapshottransfer

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// SourceExportPlan binds one already-pinned replicated-state cut to the exact
// learner that may receive it. The two workspaces are caller-owned and keep
// both artifact framing and repository publication bounded without retaining
// the complete artifact in memory.
type SourceExportPlan struct {
	Repository *Repository
	Snapshot   *replicatedstate.ReadSnapshot
	// Context carries the source-control cancellation into the actual two-pass
	// export. Nil means Background for callers that use this low-level API.
	Context context.Context
	// Budget is one physical-node budget shared by every group served by the
	// process. It is optional for embedders that intentionally run without a
	// migration controller.
	Budget *migrationbudget.Budget
	// lease is acquired by a retained provider before it materializes a
	// chunk-sized workspace. Direct low-level callers leave it nil and the
	// export acquires its own lease below.
	lease *migrationbudget.Lease

	ExpectedFence     replicatedstate.SnapshotFence
	Group             raftmember.GroupKey
	SourceMember      uint64
	TargetMember      uint64
	TargetStore       [16]byte
	TargetIncarnation uint64
	ChunkBytes        uint32

	ArtifactWorkspace []byte
	TransferWorkspace []byte
	// Release returns caller-owned workspace or other bounded plan resources.
	// Snapshot ownership remains separate and is always closed by the exporter.
	Release func()
}

// ExportPinnedSnapshot publishes one deterministic artifact from the same
// immutable cut used to derive its descriptor. It scans that cut twice: once
// to derive the exact byte hash and geometry, then once to stream bounded
// chunks into Repository. This avoids both a full-artifact allocation and a
// second temporary artifact on disk. Snapshot ownership remains with caller.
// An exact retry resumes at Repository's durable cursor.
func ExportPinnedSnapshot(plan SourceExportPlan) (
	Descriptor,
	replicatedstate.SnapshotArtifactManifest,
	error,
) {
	if err := validateSourceExportPlan(plan); err != nil {
		return Descriptor{}, replicatedstate.SnapshotArtifactManifest{}, err
	}
	fence := plan.Snapshot.Fence()
	publication := plan.Snapshot.Publication()
	if fence != plan.ExpectedFence || publication.ConfState == nil ||
		publication.ReplicaSetVersion != fence.ReplicaSetVersion ||
		publication.Applied != fence.Applied ||
		!exactLearnerConfState(publication.ConfState, plan.SourceMember, plan.TargetMember) {
		return Descriptor{}, replicatedstate.SnapshotArtifactManifest{}, ErrStaleFence
	}
	ctx := budgetContext(plan.Context)
	lease := plan.lease
	if plan.Budget != nil && lease == nil {
		var err error
		lease, err = plan.Budget.Acquire(ctx)
		if err != nil {
			return Descriptor{}, replicatedstate.SnapshotArtifactManifest{}, err
		}
	}
	if lease != nil {
		// Provider plans also release this lease from Release. Lease.Release is
		// idempotent, while this defer keeps direct low-level callers from
		// leaking capacity when they return before invoking that callback.
		defer lease.Release()
	}

	options := replicatedstate.SnapshotArtifactOptions{
		TargetChunkBytes: int(plan.ChunkBytes),
		PayloadBuffer:    plan.ArtifactWorkspace[:0],
	}
	digest := sha256.New()
	firstPass := budgetedWriter{
		ctx: ctx, budget: plan.Budget, lease: lease, writer: digest,
		cost: func(bytes uint64) migrationbudget.Cost {
			return migrationbudget.Cost{CPU: bytes}
		},
	}
	manifest, err := replicatedstate.WriteSnapshotArtifact(firstPass, plan.Snapshot, options)
	if err != nil {
		return Descriptor{}, replicatedstate.SnapshotArtifactManifest{}, err
	}
	descriptor := sourceDescriptor(plan, fence, manifest, digest)
	if !descriptor.Valid() {
		return Descriptor{}, replicatedstate.SnapshotArtifactManifest{}, ErrDescriptor
	}

	offset, complete, err := plan.Repository.OffsetContextWithLease(ctx, lease, descriptor)
	if err != nil {
		return Descriptor{}, replicatedstate.SnapshotArtifactManifest{}, err
	}
	if complete {
		return descriptor, manifest, nil
	}
	writer := repositoryExportWriter{
		repository: plan.Repository,
		descriptor: descriptor,
		workspace:  plan.TransferWorkspace[:0],
		offset:     offset,
		ctx:        ctx,
		budget:     plan.Budget,
		lease:      lease,
	}
	secondPass := budgetedWriter{
		ctx: ctx, budget: plan.Budget, lease: lease, writer: &writer,
		cost: func(bytes uint64) migrationbudget.Cost {
			return migrationbudget.Cost{CPU: bytes}
		},
	}
	written, writeErr := replicatedstate.WriteSnapshotArtifact(secondPass, plan.Snapshot, options)
	if writeErr == nil {
		writeErr = writer.finish()
	}
	if writeErr != nil {
		return descriptor, replicatedstate.SnapshotArtifactManifest{}, writeErr
	}
	if written.EncodedBytes != manifest.EncodedBytes || written.Digest != manifest.Digest ||
		written.ImageDigest != manifest.ImageDigest ||
		written.CaptureImageDigest != manifest.CaptureImageDigest {
		return descriptor, replicatedstate.SnapshotArtifactManifest{}, ErrStaleFence
	}
	return descriptor, manifest, nil
}

func validateSourceExportPlan(plan SourceExportPlan) error {
	if plan.Repository == nil || plan.Snapshot == nil ||
		plan.Group == (raftmember.GroupKey{}) || plan.SourceMember == 0 ||
		plan.TargetMember == 0 || plan.SourceMember == plan.TargetMember ||
		plan.TargetStore == ([16]byte{}) || plan.TargetIncarnation == 0 ||
		plan.ChunkBytes < MinChunkBytes || plan.ChunkBytes > AbsoluteMaxChunkBytes ||
		cap(plan.ArtifactWorkspace) < int(plan.ChunkBytes) ||
		cap(plan.TransferWorkspace) < int(plan.ChunkBytes) {
		return ErrBound
	}
	fence := plan.ExpectedFence
	binding := fence.Binding
	if fence.ReplicaSetVersion == 0 || fence.Applied == 0 || fence.LastTerm == 0 ||
		fence.LastEntryDigest == ([sha256.Size]byte{}) ||
		fence.RelationManifestDigest == ([sha256.Size]byte{}) ||
		binding.SchemaGeneration == 0 ||
		binding.ClusterID != plan.Group.ClusterID ||
		binding.ClusterIncarnation != plan.Group.ClusterIncarnation ||
		binding.TopologyRecoveryEpoch != plan.Group.TopologyRecoveryEpoch ||
		binding.ShardIncarnation != plan.Group.ShardIncarnation ||
		binding.GroupID != plan.Group.GroupID {
		return ErrDescriptor
	}
	return replicatedstate.ValidateSnapshotArtifactOptions(
		replicatedstate.SnapshotArtifactOptions{
			TargetChunkBytes: int(plan.ChunkBytes), PayloadBuffer: plan.ArtifactWorkspace[:0],
		},
	)
}

func sourceDescriptor(
	plan SourceExportPlan,
	fence replicatedstate.SnapshotFence,
	manifest replicatedstate.SnapshotArtifactManifest,
	digest hash.Hash,
) Descriptor {
	var artifactHash [sha256.Size]byte
	copy(artifactHash[:], digest.Sum(nil))
	return Descriptor{
		Group: plan.Group, SourceMember: plan.SourceMember,
		TargetMember: plan.TargetMember, TargetStore: plan.TargetStore,
		TargetIncarnation: plan.TargetIncarnation,
		SchemaGeneration:  fence.Binding.SchemaGeneration,
		ReplicaSetVersion: fence.ReplicaSetVersion,
		SnapshotIndex:     fence.Applied, SnapshotTerm: fence.LastTerm,
		Lineage: fence.LastEntryDigest, ArtifactHash: artifactHash,
		ArtifactBytes: manifest.EncodedBytes, ChunkBytes: plan.ChunkBytes,
	}
}

type repositoryExportWriter struct {
	repository *Repository
	descriptor Descriptor
	workspace  []byte
	offset     uint64
	streamed   uint64
	ctx        context.Context
	budget     *migrationbudget.Budget
	lease      *migrationbudget.Lease
}

func (writer *repositoryExportWriter) Write(src []byte) (int, error) {
	original := len(src)
	if writer.streamed < writer.offset {
		skip := min(uint64(len(src)), writer.offset-writer.streamed)
		writer.streamed += skip
		src = src[skip:]
	}
	for len(src) != 0 {
		available := cap(writer.workspace) - len(writer.workspace)
		if available == 0 {
			if err := writer.flush(); err != nil {
				return original - len(src), err
			}
			available = cap(writer.workspace)
		}
		count := min(len(src), available)
		writer.workspace = append(writer.workspace, src[:count]...)
		writer.streamed += uint64(count)
		src = src[count:]
	}
	return original, nil
}

func (writer *repositoryExportWriter) finish() error {
	if writer.streamed != writer.descriptor.ArtifactBytes {
		return io.ErrUnexpectedEOF
	}
	if err := writer.flush(); err != nil {
		return err
	}
	offset, complete, err := writer.repository.OffsetContextWithLease(writer.ctx, writer.lease, writer.descriptor)
	if err != nil || !complete || offset != writer.descriptor.ArtifactBytes {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	return nil
}

func (writer *repositoryExportWriter) flush() error {
	if len(writer.workspace) == 0 {
		return nil
	}
	remaining := writer.workspace
	for len(remaining) != 0 {
		count := len(remaining)
		if writer.budget != nil {
			bounded, err := consumeBudgetBytes(writer.ctx, writer.budget, writer.lease,
				uint64(count), func(bytes uint64) migrationbudget.Cost {
					return migrationbudget.Cost{CPU: bytes * 2, DiskWrite: bytes}
				})
			if err != nil {
				return err
			}
			count = int(bounded)
		}
		chunk := remaining[:count]
		start := writer.offset
		end := start + uint64(len(chunk))
		next, _, err := writer.repository.AppendContextWithLease(writer.ctx, writer.lease,
			writer.descriptor, start, chunk, sha256.Sum256(chunk),
		)
		if err != nil {
			settled, _, settleErr := writer.repository.OffsetContextWithLease(writer.ctx, writer.lease, writer.descriptor)
			if settleErr == nil && settled >= end {
				next, err = settled, nil
			} else {
				return errors.Join(err, settleErr)
			}
		}
		if next < end {
			return ErrOutcomeUnknown
		}
		writer.offset = next
		remaining = remaining[count:]
	}
	// Keep the original backing array and capacity. Shrinking the field itself
	// would make the next Write observe cap==0 and repeatedly flush an empty
	// workspace forever.
	writer.workspace = writer.workspace[:0]
	return nil
}
