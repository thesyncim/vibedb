package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// DurableRequestDynamicPayload owns the exact physical bytes for one wave.
// Target and Command alias Bytes; retaining this value therefore retains one
// bounded allocation rather than three copies.
type DurableRequestDynamicPayload struct {
	Build   requestledger.PayloadBuildRecord
	Step    requestledger.StepRef
	Bytes   []byte
	Target  []byte
	Command []byte
}

// DurableRequestDynamicPayloadStore stages and reopens the exact physical
// bytes named by a dynamic StepRef. It deliberately has no route resolver:
// once a build exists, a retry must reopen its authenticated chunks instead of
// rebuilding a command from possibly newer topology.
type DurableRequestDynamicPayloadStore struct {
	ledger DurableRequestLedger
}

func NewDurableRequestDynamicPayloadStore(
	ledger DurableRequestLedger,
) (*DurableRequestDynamicPayloadStore, error) {
	if ledger == nil {
		return nil, ErrDurableRequest
	}
	return &DurableRequestDynamicPayloadStore{ledger: ledger}, nil
}

// Stage seals target||command under the current head wave. Repeated calls with
// the same bytes resume the winning build; different bytes lose deterministically.
func (store *DurableRequestDynamicPayloadStore) Stage(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
	target []byte,
	command []byte,
) (DurableRequestDynamicPayload, error) {
	if store == nil || store.ledger == nil || ctx == nil || !key.Valid() ||
		len(target) == 0 || len(target) > requestledger.MaxTargetBytes ||
		len(command) == 0 || len(command) > replication.MaxCommandBytes ||
		uint64(len(target)) > requestledger.MaxDynamicWavePayloadBytes-uint64(len(command)) {
		return DurableRequestDynamicPayload{}, ErrDurableRequest
	}
	exact := make([]byte, len(target)+len(command))
	copy(exact, target)
	copy(exact[len(target):], command)
	root, err := durableRequestDynamicPayloadRoot(key, exact)
	if err != nil {
		return DurableRequestDynamicPayload{}, err
	}

	headRow, err := store.readHead(ctx, home, key)
	if err != nil {
		return DurableRequestDynamicPayload{}, err
	}
	head := headRow.Head
	buildRow, err := store.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
		Key: key, Kind: replicatedstate.RequestLedgerReadPayloadBuild,
		MinimumApplied: headRow.Applied,
	})
	if err != nil {
		return DurableRequestDynamicPayload{}, err
	}
	var build requestledger.PayloadBuildRecord
	if buildRow.Found {
		if buildRow.Kind != replicatedstate.RequestLedgerReadPayloadBuild {
			return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
		}
		build = buildRow.PayloadBuild
	} else {
		build, err = requestledger.NewPayloadBuild(
			head, root, uint64(len(exact)),
			(uint64(len(exact))+requestledger.MaxPlanPageBytes-1)/requestledger.MaxPlanPageBytes,
		)
		if err != nil {
			return DurableRequestDynamicPayload{}, errors.Join(err, ErrDurableRequestConflict)
		}
		result, applyErr := store.ledger.ApplyCAS(ctx, home, key, DurableRequestLifecycleCAS{
			Operation:        requestledger.OperationBeginPayloadBuild,
			ExpectedRevision: head.Revision, Revision: build.Revision,
			PayloadBuild: build,
		})
		if applyErr != nil {
			return DurableRequestDynamicPayload{}, applyErr
		}
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
		}
	}
	if !durableRequestDynamicBuildMatches(build, head, root, uint64(len(exact))) {
		return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
	}

	for build.Phase == requestledger.PayloadBuildStaging && build.NextChunkOrdinal < build.ChunkCount {
		offset := build.StagedBytes
		end := min(offset+requestledger.MaxPlanPageBytes, uint64(len(exact)))
		chunk, chunkErr := requestledger.NewPayloadChunk(build, exact[offset:end])
		if chunkErr != nil {
			return DurableRequestDynamicPayload{}, errors.Join(chunkErr, ErrDurableRequestConflict)
		}
		next, advanceErr := requestledger.AdvancePayloadBuild(build, chunk, build.Revision+1)
		if advanceErr != nil {
			return DurableRequestDynamicPayload{}, errors.Join(advanceErr, ErrDurableRequestConflict)
		}
		result, applyErr := store.ledger.ApplyCAS(ctx, home, key, DurableRequestLifecycleCAS{
			Operation:        requestledger.OperationStagePayloadChunk,
			ExpectedRevision: build.Revision, Revision: next.Revision,
			Head: head, PayloadChunk: chunk,
		})
		if applyErr != nil {
			return DurableRequestDynamicPayload{}, applyErr
		}
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
		}
		build = next
	}
	if build.Phase == requestledger.PayloadBuildStaging {
		sealed, sealErr := requestledger.SealPayloadBuild(build, build.Revision+1)
		if sealErr != nil {
			return DurableRequestDynamicPayload{}, errors.Join(sealErr, ErrDurableRequestConflict)
		}
		result, applyErr := store.ledger.ApplyCAS(ctx, home, key, DurableRequestLifecycleCAS{
			Operation:        requestledger.OperationSealPayload,
			ExpectedRevision: build.Revision, Revision: sealed.Revision,
			PayloadBuild: sealed,
		})
		if applyErr != nil {
			return DurableRequestDynamicPayload{}, applyErr
		}
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
		}
		build = sealed
	}
	return durableRequestDynamicPayload(build, exact, uint64(len(target)))
}

// Open reassembles one sealed build from authenticated chunks. The caller
// supplies the persisted build-digest and StepRef witnesses, never
// reconstructed routing bytes.
func (store *DurableRequestDynamicPayloadStore) Open(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
	buildDigest requestledger.Digest,
	step requestledger.StepRef,
	minimumApplied uint64,
) (DurableRequestDynamicPayload, error) {
	if store == nil || store.ledger == nil || ctx == nil || !key.Valid() ||
		minimumApplied == 0 || buildDigest == (requestledger.Digest{}) ||
		step.TargetSource != requestledger.PayloadSourceDynamic ||
		step.CommandSource != requestledger.PayloadSourceDynamic ||
		step.TargetOffset != 0 || step.CommandOffset != step.TargetLength {
		return DurableRequestDynamicPayload{}, ErrDurableRequest
	}
	buildRow, err := store.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
		Key: key, Kind: replicatedstate.RequestLedgerReadPayloadBuild,
		MinimumApplied: minimumApplied,
	})
	if err != nil {
		return DurableRequestDynamicPayload{}, err
	}
	if !buildRow.Found || buildRow.Kind != replicatedstate.RequestLedgerReadPayloadBuild ||
		buildRow.PayloadBuild.BuildDigest != buildDigest {
		return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
	}
	build := buildRow.PayloadBuild
	if build.Phase != requestledger.PayloadBuildSealed ||
		build.TotalBytes > requestledger.MaxDynamicWavePayloadBytes ||
		step.CommandOffset > build.TotalBytes ||
		step.CommandLength > build.TotalBytes-step.CommandOffset ||
		step.CommandOffset+step.CommandLength != build.TotalBytes {
		return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
	}
	keyDigest, err := requestledger.KeyDigest(key)
	if err != nil || keyDigest != build.KeyDigest {
		return DurableRequestDynamicPayload{}, errors.Join(err, ErrDurableRequestConflict)
	}
	exact := make([]byte, 0, build.TotalBytes)
	var previous requestledger.Digest
	for ordinal := uint64(0); ordinal < build.ChunkCount; ordinal++ {
		row, readErr := store.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
			Key: key, Kind: replicatedstate.RequestLedgerReadPayloadChunk,
			Ordinal: ordinal, ContentRoot: build.ContentRoot,
			MinimumApplied: minimumApplied,
		})
		if readErr != nil {
			return DurableRequestDynamicPayload{}, readErr
		}
		chunk := row.PayloadChunk
		if !row.Found || row.Kind != replicatedstate.RequestLedgerReadPayloadChunk ||
			chunk.KeyDigest != build.KeyDigest || chunk.PlanRoot != build.PlanRoot ||
			chunk.BuildDigest != build.BuildDigest || chunk.ContentRoot != build.ContentRoot ||
			chunk.Ordinal != ordinal || chunk.Count != build.ChunkCount ||
			chunk.TotalBytes != build.TotalBytes || chunk.PreviousChain != previous ||
			chunk.Offset != uint64(len(exact)) {
			return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
		}
		exact = append(exact, chunk.Data...)
		previous = chunk.Chain
	}
	if uint64(len(exact)) != build.TotalBytes || previous != build.ContentRoot {
		return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
	}
	return durableRequestDynamicPayloadWithStep(build, step, exact)
}

// Cleanup retires an advanced wave's dynamic chunks in bounded replicated
// batches. It is safe to call after every restart and is a no-op when no
// cleanup remains.
func (store *DurableRequestDynamicPayloadStore) Cleanup(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
) (uint64, error) {
	if store == nil || store.ledger == nil || ctx == nil || !key.Valid() {
		return 0, ErrDurableRequest
	}
	var revision uint64
	for {
		headRow, err := store.readHead(ctx, home, key)
		if err != nil {
			return 0, err
		}
		head := headRow.Head
		revision = head.Revision
		if head.CleanupBuildDigest == (requestledger.Digest{}) {
			return revision, nil
		}
		request, requestErr := requestledger.NewPayloadCleanupRequest(
			head, requestledger.MaxAckGCDeleteRows, math.MaxUint32,
		)
		if requestErr != nil {
			return 0, errors.Join(requestErr, ErrDurableRequestConflict)
		}
		chunk, planErr := requestledger.PlanPayloadCleanup(head, request)
		if planErr != nil {
			return 0, errors.Join(planErr, ErrDurableRequestConflict)
		}
		result, applyErr := store.ledger.ApplyCAS(ctx, home, key, DurableRequestLifecycleCAS{
			Operation:        requestledger.OperationCleanupPayload,
			ExpectedRevision: head.Revision, Revision: head.Revision + 1,
			Head: head, PayloadCleanup: request,
		})
		if applyErr != nil {
			return 0, applyErr
		}
		if result.Ledger.ResultCode != replicatedstate.ResultApplied {
			return 0, ErrDurableRequestConflict
		}
		next, advanceErr := requestledger.AdvancePayloadCleanup(
			head, request, chunk, head.Revision+1,
		)
		if advanceErr != nil {
			return 0, errors.Join(advanceErr, ErrDurableRequestConflict)
		}
		revision = next.Revision
	}
}

func (store *DurableRequestDynamicPayloadStore) readHead(
	ctx context.Context,
	home DurableRequestLedgerHome,
	key requestledger.RequestKey,
) (DurableRequestLifecycleRow, error) {
	row, err := store.ledger.ReadRow(ctx, home, DurableRequestLifecycleRead{
		Key: key, Kind: replicatedstate.RequestLedgerReadHead, MinimumApplied: 1,
	})
	if err != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadHead {
		return DurableRequestLifecycleRow{}, errors.Join(err, ErrDurableRequestConflict)
	}
	return row, nil
}

func durableRequestDynamicPayloadRoot(
	key requestledger.RequestKey,
	exact []byte,
) (requestledger.Digest, error) {
	keyDigest, err := requestledger.KeyDigest(key)
	if err != nil {
		return requestledger.Digest{}, errors.Join(err, ErrDurableRequest)
	}
	accumulator, err := requestledger.NewPayloadRootAccumulator(keyDigest, uint64(len(exact)))
	if err != nil {
		return requestledger.Digest{}, errors.Join(err, ErrDurableRequest)
	}
	for offset := 0; offset < len(exact); offset += requestledger.MaxPlanPageBytes {
		end := min(offset+requestledger.MaxPlanPageBytes, len(exact))
		if err = accumulator.Append(exact[offset:end]); err != nil {
			return requestledger.Digest{}, errors.Join(err, ErrDurableRequest)
		}
	}
	root, err := accumulator.Root()
	return root, err
}

func durableRequestDynamicBuildMatches(
	build requestledger.PayloadBuildRecord,
	head requestledger.HeadRecord,
	root requestledger.Digest,
	total uint64,
) bool {
	return build.KeyDigest == head.KeyDigest && build.RequestDigest == head.RequestDigest &&
		build.PlanRoot == head.PlanRoot &&
		build.PriorContinuationDigest == head.ContinuationDigest &&
		build.WaveOrdinal == head.NextStepOrdinal && build.ContentRoot == root &&
		build.TotalBytes == total &&
		build.ChunkCount == (total+requestledger.MaxPlanPageBytes-1)/requestledger.MaxPlanPageBytes
}

func durableRequestDynamicPayload(
	build requestledger.PayloadBuildRecord,
	exact []byte,
	targetLength uint64,
) (DurableRequestDynamicPayload, error) {
	step := requestledger.StepRef{
		TargetSource: requestledger.PayloadSourceDynamic, CommandSource: requestledger.PayloadSourceDynamic,
		TargetOffset: 0, TargetLength: targetLength,
		CommandOffset: targetLength, CommandLength: uint64(len(exact)) - targetLength,
		TargetDigest:  requestledger.Digest(sha256.Sum256(exact[:targetLength])),
		CommandDigest: requestledger.Digest(sha256.Sum256(exact[targetLength:])),
	}
	return durableRequestDynamicPayloadWithStep(build, step, exact)
}

func durableRequestDynamicPayloadWithStep(
	build requestledger.PayloadBuildRecord,
	step requestledger.StepRef,
	exact []byte,
) (DurableRequestDynamicPayload, error) {
	if build.Phase != requestledger.PayloadBuildSealed ||
		step.TargetOffset > uint64(len(exact)) || step.TargetLength > uint64(len(exact))-step.TargetOffset ||
		step.CommandOffset > uint64(len(exact)) || step.CommandLength > uint64(len(exact))-step.CommandOffset {
		return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
	}
	target := exact[step.TargetOffset : step.TargetOffset+step.TargetLength]
	command := exact[step.CommandOffset : step.CommandOffset+step.CommandLength]
	if requestledger.Digest(sha256.Sum256(target)) != step.TargetDigest ||
		requestledger.Digest(sha256.Sum256(command)) != step.CommandDigest {
		return DurableRequestDynamicPayload{}, ErrDurableRequestConflict
	}
	return DurableRequestDynamicPayload{
		Build: build, Step: step, Bytes: exact,
		Target: target, Command: command,
	}, nil
}
