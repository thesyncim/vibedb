package splitcontroller

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rangesplit"
)

// SplitCatalogAuthority is the single catalog-Raft publication capability.
// RetryPending must settle only the publisher's byte-identical pending command.
type SplitCatalogAuthority interface {
	Publish(context.Context, uint64, *gateway.Snapshot) error
	RetryPending(context.Context) error
	Read(context.Context) (*gateway.Snapshot, error)
}

// ExecutePublishCatalog installs the certified child routes with one catalog
// generation CAS. It never prunes source data; the reconciler cannot emit that
// action until this generation is visible and its older leases are drained.
func ExecutePublishCatalog(
	ctx context.Context,
	plan *Plan,
	observed Observation,
	authority SplitCatalogAuthority,
) error {
	if ctx == nil || plan == nil || authority == nil || observed.Catalog == nil ||
		observed.Certificate == nil {
		return ErrInvalidPlan
	}
	action, err := Reconcile(plan, observed)
	if err != nil || action.Kind != ActionPublishCatalog ||
		action.CatalogGeneration != plan.next {
		return errors.Join(ErrTopologyConflict, err)
	}
	next, err := plan.BuildCatalogTransition(
		observed.Catalog, observed.SourceState, *observed.Certificate,
	)
	if err != nil {
		return err
	}
	err = authority.Publish(ctx, plan.current, next)
	if errors.Is(err, gateway.ErrReplicatedCatalogPending) {
		err = authority.RetryPending(ctx)
	}
	if err != nil && !errors.Is(err, gateway.ErrCatalogGenerationMismatch) &&
		!errors.Is(err, gateway.ErrCatalogGenerationNotNewer) {
		return err
	}
	settled, readErr := authority.Read(ctx)
	wantDigest, wantErr := gateway.CatalogSnapshotDigest(next)
	gotDigest, gotErr := gateway.CatalogSnapshotDigest(settled)
	if readErr != nil || wantErr != nil || gotErr != nil || settled == nil ||
		settled.Generation() != plan.next || gotDigest != wantDigest {
		return errors.Join(ErrTopologyConflict, err, readErr, wantErr, gotErr)
	}
	return nil
}

// RetainedPruneAuthoritySource mints the sealed destructive capability only
// after the catalog publication is durable and every older serving lease has
// drained.
type RetainedPruneAuthoritySource interface {
	AuthorizeRetainedPrune(
		context.Context,
		distribution.DistributionName,
		[32]byte,
		[32]byte,
	) (gateway.RetainedPruneAuthority, error)
}

// RetainedPruneProposer admits one bounded, digest-addressed delete batch via
// the retained source Raft group. It must consume/copy the borrowed iterator
// before returning.
type RetainedPruneProposer interface {
	ProposeRetainedPrune(
		context.Context,
		OperationID,
		raftservice.ServingFence,
		rangesplit.RetainedPruneBatch,
	) error
}

// ExecutePruneRetained advances at most one bounded scan/verification step or
// admits one bounded replicated delete batch. The cursor is durable before
// admission, making response loss and process death replay-safe. This method
// is deliberately callable only after Reconcile proves publish-before-prune.
func (a *LocalSourceActions) ExecutePruneRetained(
	ctx context.Context,
	plan *Plan,
	observed Observation,
	serving raftservice.ServingState,
	authorities RetainedPruneAuthoritySource,
	proposer RetainedPruneProposer,
	limits rangesplit.RetainedPruneLimits,
) error {
	if a == nil || ctx == nil || plan == nil || authorities == nil || proposer == nil ||
		plan.operation != a.store.operation || observed.Catalog == nil ||
		observed.Certificate == nil {
		return ErrInvalidPlan
	}
	action, err := Reconcile(plan, observed)
	if err != nil || action.Kind != ActionPruneRetained {
		return errors.Join(ErrTopologyConflict, err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !activeCaptureMatchesObservation(a.active, observed) {
		return ErrTopologyConflict
	}
	manifest, err := a.runtime.RangeSplitRelationManifestDigest()
	if err != nil || !sourceServingStateMatches(
		observed.SourceState, serving, manifest,
	) {
		return errors.Join(ErrTopologyConflict, err)
	}
	certificate := *observed.Certificate
	authority, err := authorities.AuthorizeRetainedPrune(
		ctx, plan.source.Distribution, [32]byte(plan.operation), certificate.Digest(),
	)
	if err != nil {
		return err
	}
	proof, err := rangesplit.NewRetainedPruneAuthorityProof(
		plan.partitioner, certificate, authority.Manifest(), authority.Generation(), authority.Operation(),
	)
	if err != nil || authority.Certificate() != certificate.Digest() {
		return errors.Join(ErrTopologyConflict, err)
	}
	return a.executeCertifiedPruneLocked(ctx, plan, observed, serving, proposer, limits, certificate, proof)
}

// ExecuteCertifiedPruneRetained is the shipped gateway-to-shard path. The
// shard validates the fixed external certificate against its admitted Plan,
// exact published catalog, and cutover before rangesplit issues its immutable
// manifest authority proof.
func (a *LocalSourceActions) ExecuteCertifiedPruneRetained(
	ctx context.Context,
	plan *Plan,
	observed Observation,
	serving raftservice.ServingState,
	proposer RetainedPruneProposer,
	limits rangesplit.RetainedPruneLimits,
) error {
	if a == nil || ctx == nil || plan == nil || proposer == nil ||
		plan.operation != a.store.operation || observed.Catalog == nil || observed.Certificate == nil ||
		!validRetainedPruneCertificate(
			plan, observed.Catalog, *observed.Certificate, observed.RetainedPruneCertificate,
		) {
		return ErrInvalidPlan
	}
	// Publication and the exact drain certificate already establish the
	// gateway's child-readiness decision. Do not require remote runtime cuts
	// again in this source-local, authenticated action.
	stage, err := plan.catalogStage(observed.Catalog)
	if err != nil || stage != catalogTarget || plan.validateSourceObservation(observed) != nil {
		return errors.Join(ErrTopologyConflict, err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !activeCaptureContainsObservation(a.active, observed) {
		return ErrTopologyConflict
	}
	manifestDigest, err := a.runtime.RangeSplitRelationManifestDigest()
	if err != nil || !sourceServingStateMatches(observed.SourceState, serving, manifestDigest) {
		return errors.Join(ErrTopologyConflict, err)
	}
	manifest, ok := observed.Catalog.Manifest(plan.source.Distribution)
	if !ok {
		return ErrTopologyConflict
	}
	certificate := *observed.Certificate
	proof, err := rangesplit.NewRetainedPruneAuthorityProof(
		plan.partitioner, certificate, manifest, observed.Catalog.Generation(), [32]byte(plan.operation),
	)
	if err != nil {
		return errors.Join(ErrTopologyConflict, err)
	}
	return a.executeCertifiedPruneLocked(ctx, plan, observed, serving, proposer, limits, certificate, proof)
}

// A topology-session Open (or a prior unknown prune outcome) can advance the
// retained source after the gateway freezes its witness. Verify its exact
// ancestor in the capture, not just a monotonic index. The pruner then consumes
// the authenticated suffix in bounded steps before planning any more deletes.
func activeCaptureContainsObservation(active *rangesplit.SourceCapture, observed Observation) bool {
	if active == nil {
		return false
	}
	head, ok := observationCaptureHead(observed)
	state := observed.SourceState
	if !ok || head != state.Applied {
		return false
	}
	descriptor, err := active.Descriptor()
	if err != nil {
		return false
	}
	descriptor.Head = rangesplit.ChildArtifactSourceCut{
		Applied: state.Applied, Term: state.LastTerm, EntryDigest: state.LastEntryDigest,
		DataChainDigest: state.DataChainDigest, BaseDigest: state.SnapshotBaseDigest,
		RouteGeneration: state.Binding.RouteGeneration,
	}
	descriptor.Coordinates = rangesplit.TailSourceCoordinates{
		OwnershipEpoch: state.Binding.OwnershipEpoch, RoutingVersion: state.Binding.RoutingVersion,
		RouteGeneration: state.Binding.RouteGeneration,
	}
	var workspace rangesplit.SourceCaptureWorkspace
	return active.ValidateDescriptorAncestor(descriptor, &workspace) == nil
}

func activeCaptureMatchesObservation(active *rangesplit.SourceCapture, observed Observation) bool {
	if active == nil {
		return false
	}
	head, ok := observationCaptureHead(observed)
	if !ok {
		return false
	}
	descriptor, err := active.Descriptor()
	return err == nil && descriptor.Head.Applied == head
}

func (a *LocalSourceActions) executeCertifiedPruneLocked(
	ctx context.Context,
	plan *Plan,
	observed Observation,
	serving raftservice.ServingState,
	proposer RetainedPruneProposer,
	limits rangesplit.RetainedPruneLimits,
	certificate rangesplit.CutoverCertificate,
	proof rangesplit.RetainedPruneAuthorityProof,
) error {
	cursor, revision, present, err := a.store.LoadRetainedPrune(
		plan.partitioner, certificate,
	)
	if err != nil {
		return err
	}
	var persisted []byte
	if present {
		persisted, err = rangesplit.AppendRetainedPruneCursor(nil, &cursor)
		if err != nil {
			return errors.Join(ErrRuntimeStore, err)
		}
	}
	if present != (observed.Prune != nil) {
		return ErrTopologyConflict
	}
	if observed.Prune != nil {
		observedRaw, encodeErr := rangesplit.AppendRetainedPruneCursor(nil, observed.Prune)
		if encodeErr != nil || !bytes.Equal(observedRaw, persisted) {
			return errors.Join(ErrTopologyConflict, encodeErr)
		}
	}
	pruner, err := rangesplit.NewCertifiedRetainedPruner(
		plan.partitioner, certificate, proof, persisted,
	)
	if err != nil {
		return err
	}
	cut, err := a.runtime.RangeSplitSnapshot()
	if err != nil {
		return err
	}
	var workspace rangesplit.RetainedPruneWorkspace
	persist := func(raw []byte) error {
		next, openErr := rangesplit.OpenRetainedPruneCursor(raw)
		if openErr != nil || next == nil || revision == ^uint64(0) {
			return errors.Join(ErrRuntimeStore, openErr)
		}
		revision++
		return a.store.PersistRetainedPrune(revision, *next)
	}
	batch, hasBatch, advanceErr := pruner.Advance(
		cut, a.active, limits, persist, &workspace,
	)
	closeErr := cut.Close()
	if advanceErr != nil || closeErr != nil {
		return errors.Join(advanceErr, closeErr)
	}
	if !hasBatch {
		return nil
	}
	return proposer.ProposeRetainedPrune(
		ctx, plan.operation, serving.Fence(), batch,
	)
}
