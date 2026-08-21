// Package splitcontroller derives one safe next step for an online range
// split from independently durable authorities. Given the same caller-retained
// immutable Plan, the reconciler needs no separate progress journal: after a
// crash, the catalog, capture, stage cursors, SQL binding, WAL binding, and Raft
// runtime reconstruct the same answer.
package splitcontroller

import (
	"errors"
	"math"
	"strings"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"go.etcd.io/raft/v3"
)

var (
	ErrInvalidPlan      = errors.New("splitcontroller: invalid online split plan")
	ErrTopologyConflict = errors.New("splitcontroller: observed authority conflicts with split plan")
)

// ActionKind identifies the one safe next operation derived by Reconcile.
// Await and Complete mutate nothing. Every other action must still use the
// exact proof-checking primitive and generation CAS named by the plan.
type ActionKind uint8

const (
	ActionAwaitSourceLeader ActionKind = iota + 1
	ActionStartCapture
	ActionBuildArtifacts
	ActionStageChild
	ActionCatchUpTail
	ActionSealSource
	ActionCertifyCutover
	ActionActivateChild
	ActionCreateChildWAL
	ActionAdoptChildRuntime
	ActionAwaitChildReady
	ActionPruneRetained
	ActionPublishCatalog
	ActionAwaitCatalogDrain
	ActionComplete
)

// Action is a constant-size next-step result. Child is populated only for a
// destination action. CatalogGeneration is populated for publish and drain.
type Action struct {
	Kind              ActionKind
	Child             uint8
	CatalogGeneration uint64
}

// ChildTarget freezes the prepared first-leader destination for one
// non-retained child. SQL must already be bound to the exact final WAL identity
// derived by BindingForNewWAL. None of these fields grants serving authority.
type ChildTarget struct {
	Child                 uint8
	Endpoint              distribution.EndpointID
	WAL                   raftstore.Identity
	TopologyRecoveryEpoch uint64
	Authority             sqldriver.ReplicatedAuthorityProfile
	SQL                   sqldriver.ReplicatedShardStoreIdentity
}

// Plan binds one source catalog generation, exact split geometry, and every
// non-retained child to its final SQL/Raft identity. It is immutable after
// construction.
type Plan struct {
	partitioner    *rangesplit.Partitioner
	sourceManifest *distribution.Manifest
	targetManifest *distribution.Manifest
	source         autosplit.SourceIdentity
	children       [autosplit.MaxSplitChildren]autosplit.SplitChildIdentity
	leaderCounts   [autosplit.MaxSplitChildren]uint16
	childCount     uint8
	retained       uint8
	current        uint64
	next           uint64
	targets        [autosplit.MaxSplitChildren]ChildTarget
}

// NewPlan validates the complete cold control-plane intent before any source
// capture or destination work begins.
func NewPlan(
	current *gateway.Snapshot,
	split *autosplit.SplitPlan,
	partitioner *rangesplit.Partitioner,
	targets []ChildTarget,
) (*Plan, error) {
	if current == nil || split == nil {
		return nil, ErrInvalidPlan
	}
	sourceManifest, ok := current.Manifest(split.Source.Distribution)
	if !ok {
		return nil, ErrInvalidPlan
	}
	return newPlan(
		sourceManifest, current.Generation(), split, partitioner, targets,
	)
}

// RecoverPlan reconstructs the immutable plan against either the exact source
// catalog generation or its already-published successor. After publication it
// collapses the authenticated child sequence back into the one original source
// descriptor, then re-runs the normal manifest and target-identity validation.
// The caller still retains split and targets. No old catalog snapshot is needed.
func RecoverPlan(
	current *gateway.Snapshot,
	sourceGeneration uint64,
	split *autosplit.SplitPlan,
	partitioner *rangesplit.Partitioner,
	targets []ChildTarget,
) (*Plan, error) {
	if current == nil || split == nil || sourceGeneration == math.MaxUint64 {
		return nil, ErrInvalidPlan
	}
	switch current.Generation() {
	case sourceGeneration:
		return NewPlan(current, split, partitioner, targets)
	case sourceGeneration + 1:
		target, ok := current.Manifest(split.Source.Distribution)
		if !ok || split.Manifest() == nil || !target.Equal(split.Manifest()) {
			return nil, ErrTopologyConflict
		}
		source, err := reconstructSourceManifest(split)
		if err != nil {
			return nil, err
		}
		return newPlan(source, sourceGeneration, split, partitioner, targets)
	default:
		return nil, ErrTopologyConflict
	}
}

func newPlan(
	sourceManifest *distribution.Manifest,
	sourceGeneration uint64,
	split *autosplit.SplitPlan,
	partitioner *rangesplit.Partitioner,
	targets []ChildTarget,
) (*Plan, error) {
	if sourceManifest == nil || split == nil || split.Manifest() == nil || partitioner == nil ||
		sourceGeneration == math.MaxUint64 || split.ChildCount < 2 ||
		split.ChildCount > autosplit.MaxSplitChildren ||
		split.RetainedChild >= split.ChildCount ||
		len(targets) != int(split.ChildCount)-1 {
		return nil, ErrInvalidPlan
	}
	if sourceManifest.Version() == ^distribution.RoutingVersion(0) ||
		split.Manifest().Version() != sourceManifest.Version()+1 ||
		partitioner.Digest() == ([32]byte{}) {
		return nil, ErrInvalidPlan
	}
	digest, err := rangesplit.SplitPlanDigest(split)
	if err != nil || digest != partitioner.Digest() ||
		partitioner.ValidateManifestTransition(sourceManifest, split.Manifest()) != nil {
		return nil, errors.Join(ErrInvalidPlan, err)
	}

	plan := &Plan{
		partitioner:    partitioner,
		sourceManifest: sourceManifest, targetManifest: split.Manifest(),
		source:     cloneSourceIdentity(split.Source),
		childCount: split.ChildCount, retained: split.RetainedChild,
		current: sourceGeneration, next: sourceGeneration + 1,
	}
	for child := 0; child < int(split.ChildCount); child++ {
		identity, identityOK := split.ChildIdentity(child)
		descriptor, descriptorOK := split.Child(child)
		if !identityOK || !descriptorOK || len(descriptor.Leaders) == 0 ||
			len(descriptor.Leaders) > math.MaxUint16 {
			return nil, ErrInvalidPlan
		}
		identity.Shard = distribution.ShardID(strings.Clone(string(identity.Shard)))
		plan.children[child] = identity
		plan.leaderCounts[child] = uint16(len(descriptor.Leaders))
	}
	var seen [autosplit.MaxSplitChildren]bool
	for index := range targets {
		target := cloneChildTarget(targets[index])
		child := int(target.Child)
		descriptor, childOK := split.ChildIdentity(child)
		leader, leaderOK := split.ChildLeader(child, 0)
		if !childOK || descriptor.Retained || seen[child] ||
			!leaderOK || target.Endpoint != leader ||
			target.WAL.Distribution != string(split.Source.Distribution) ||
			target.WAL.Shard != string(descriptor.Shard) ||
			target.WAL.AllocationGeneration != uint64(descriptor.AllocationGeneration) ||
			target.SQL.LogID == ([16]byte{}) ||
			target.SQL.UserTable != partitioner.CollectionName() {
			return nil, ErrInvalidPlan
		}
		planned, bindingErr := raftmember.BindingForNewWAL(
			target.WAL, target.TopologyRecoveryEpoch, target.Authority,
		)
		if bindingErr != nil || planned != target.SQL.Binding ||
			target.Authority.OwnershipEpoch != uint64(descriptor.OwnershipEpoch) ||
			target.Authority.RoutingVersion != uint64(split.Manifest().Version()) ||
			target.Authority.RouteGeneration != plan.next {
			return nil, errors.Join(ErrInvalidPlan, bindingErr)
		}
		seen[child] = true
		plan.targets[child] = target
	}
	for child := 0; child < int(split.ChildCount); child++ {
		descriptor, _ := split.ChildIdentity(child)
		if descriptor.Retained == seen[child] {
			return nil, ErrInvalidPlan
		}
	}
	return plan, nil
}

func reconstructSourceManifest(split *autosplit.SplitPlan) (*distribution.Manifest, error) {
	if split == nil || split.Manifest() == nil || split.ChildCount < 2 ||
		split.ChildCount > autosplit.MaxSplitChildren || split.RetainedChild >= split.ChildCount ||
		split.Source.RoutingVersion == ^distribution.RoutingVersion(0) ||
		split.Manifest().Version() != split.Source.RoutingVersion+1 {
		return nil, ErrInvalidPlan
	}
	target := split.Manifest()
	start := -1
	for ordinal := 0; ordinal < target.ShardCount(); ordinal++ {
		metadata, ok := target.ShardMetadataAt(ordinal)
		if ok && metadata.Range.Start == split.Source.Range.Start {
			start = ordinal
			break
		}
	}
	if start < 0 || start+int(split.ChildCount) > target.ShardCount() {
		return nil, ErrTopologyConflict
	}
	for child := 0; child < int(split.ChildCount); child++ {
		identity, ok := split.ChildIdentity(child)
		descriptor, descriptorOK := split.Child(child)
		metadata, metadataOK := target.ShardMetadataAt(start + child)
		if !ok || !descriptorOK || !metadataOK || metadata.ID != identity.Shard ||
			metadata.AllocationGeneration != identity.AllocationGeneration ||
			metadata.Range != identity.Range || metadata.Epoch != identity.OwnershipEpoch ||
			!target.ShardLeadersEqual(start+child, descriptor.Leaders) {
			return nil, ErrTopologyConflict
		}
	}

	shards := make([]distribution.Shard, 0, target.ShardCount()-int(split.ChildCount)+1)
	for ordinal := 0; ordinal < start; ordinal++ {
		shard, _ := target.ShardInfo(ordinal)
		shards = append(shards, shard)
	}
	retained, ok := split.Child(int(split.RetainedChild))
	if !ok || len(retained.Leaders) == 0 {
		return nil, ErrTopologyConflict
	}
	shards = append(shards, distribution.Shard{
		ID: split.Source.Shard, AllocationGeneration: split.Source.AllocationGeneration,
		Range: split.Source.Range, Leaders: retained.Leaders, Epoch: split.Source.OwnershipEpoch,
	})
	for ordinal := start + int(split.ChildCount); ordinal < target.ShardCount(); ordinal++ {
		shard, _ := target.ShardInfo(ordinal)
		shards = append(shards, shard)
	}
	source, err := distribution.NewManifest(
		split.Source.Distribution, split.Source.RoutingVersion, shards,
	)
	if err != nil {
		return nil, errors.Join(ErrTopologyConflict, err)
	}
	return source, nil
}

// CatalogGeneration returns the exact source generation and its only valid
// successor for this operation.
func (p *Plan) CatalogGeneration() (current, next uint64) {
	if p == nil {
		return 0, 0
	}
	return p.current, p.next
}

// Target returns a detached child target.
func (p *Plan) Target(child uint8) (ChildTarget, bool) {
	if p == nil || int(child) >= int(p.childCount) {
		return ChildTarget{}, false
	}
	target := p.targets[child]
	if target.Endpoint == "" {
		return ChildTarget{}, false
	}
	return cloneChildTarget(target), true
}

// ChildObservation is a detached scheduler cut for one non-retained child.
// Each later flag requires all earlier exact identities to remain present.
type ChildObservation struct {
	Child uint8

	Activated     bool
	ApplyIdentity sqldriver.ReplicatedApplyIdentity
	ApplyProfile  sqldriver.ReplicatedApplyCapacityProfile

	WALCreated bool
	WALBinding sqldriver.ReplicatedShardStoreBinding

	RuntimeAdopted  bool
	RuntimeIdentity raftmember.RuntimeIdentity
	RuntimeStatus   raftmember.RuntimeStatus
}

// Observation is one detached control-loop cut. Pointers distinguish absent
// durable evidence from valid zero-free proof values. Child slots are indexed
// by split-child ordinal, not by target order.
type Observation struct {
	Catalog      *gateway.Snapshot
	SourceState  replicatedstate.State
	SourceStatus raftmember.RuntimeStatus
	Capture      *rangesplit.SourceCapture

	Artifacts   *rangesplit.ChildArtifactSet
	Tail        *rangesplit.TailCursor
	Stages      [autosplit.MaxSplitChildren]*rangesplit.ChildStageCursor
	Certificate *rangesplit.CutoverCertificate
	Children    [autosplit.MaxSplitChildren]*ChildObservation
	Prune       *rangesplit.RetainedPruneCursor

	OlderCatalogDrained bool
}

// Reconcile proves the one safe next step from independently durable state.
// It performs no I/O, publication, proposal, allocation, or ownership change.
func Reconcile(plan *Plan, observed Observation) (Action, error) {
	if plan == nil || observed.Catalog == nil {
		return Action{}, ErrInvalidPlan
	}
	catalog, err := plan.catalogStage(observed.Catalog)
	if err != nil {
		return Action{}, err
	}
	if catalog == catalogTarget {
		if err := plan.validatePublishedCompletion(observed); err != nil {
			return Action{}, err
		}
		if !observed.OlderCatalogDrained {
			return Action{Kind: ActionAwaitCatalogDrain, CatalogGeneration: plan.next}, nil
		}
		return Action{Kind: ActionComplete}, nil
	}

	if err := plan.validateSourceObservation(observed); err != nil {
		return Action{}, err
	}
	if observed.Capture == nil || observed.Capture.Head() == 0 {
		if !sourceLeader(observed.SourceStatus, observed.SourceState) {
			return Action{Kind: ActionAwaitSourceLeader}, nil
		}
		return Action{Kind: ActionStartCapture}, nil
	}
	if observed.Capture.Head() != observed.SourceState.Applied {
		return Action{}, ErrTopologyConflict
	}
	if observed.Artifacts == nil {
		return Action{Kind: ActionBuildArtifacts}, nil
	}
	initial, err := plan.partitioner.InitialTailCursor(*observed.Artifacts)
	if err != nil || initial.SourceCoordinates().RouteGeneration != plan.current {
		return Action{}, errors.Join(ErrTopologyConflict, err)
	}
	if action, ok, err := plan.stageAction(observed, initial); ok || err != nil {
		return action, err
	}
	if observed.Tail == nil {
		return Action{Kind: ActionCatchUpTail}, nil
	}
	tail := *observed.Tail
	if plan.partitioner.ValidateTailCursor(tail) != nil ||
		tail.SourceCut().Applied < initial.SourceCut().Applied {
		return Action{}, ErrTopologyConflict
	}
	if !tail.Sealed() {
		if !plan.sourceStateMatchesCut(observed.SourceState, tail) ||
			observed.Capture.Head() < tail.SourceCut().Applied {
			return Action{}, ErrTopologyConflict
		}
		if observed.Capture.Head() > tail.SourceCut().Applied ||
			!plan.stagesMatchTail(observed, tail, false) {
			return Action{Kind: ActionCatchUpTail}, nil
		}
		if !sourceLeader(observed.SourceStatus, observed.SourceState) {
			return Action{Kind: ActionAwaitSourceLeader}, nil
		}
		return Action{Kind: ActionSealSource}, nil
	}
	if !plan.stagesMatchTail(observed, tail, true) {
		return Action{Kind: ActionCatchUpTail}, nil
	}
	if observed.Certificate == nil {
		if observed.Capture.Head() != tail.SourceCut().Applied ||
			!plan.sourceStateMatchesCut(observed.SourceState, tail) {
			return Action{}, ErrTopologyConflict
		}
		return Action{Kind: ActionCertifyCutover}, nil
	}
	certificate := *observed.Certificate
	if plan.partitioner.VerifyCutoverCertificate(certificate) != nil ||
		certificate.SourceCut() != tail.SourceCut() ||
		certificate.SourceCoordinates() != tail.SourceCoordinates() {
		return Action{}, ErrTopologyConflict
	}
	if !plan.sourceStateAfterCutover(observed.SourceState, certificate, observed.Prune) {
		return Action{}, ErrTopologyConflict
	}
	if action, ok, err := plan.childAction(observed, certificate); ok || err != nil {
		return action, err
	}
	if observed.Prune == nil || observed.Prune.Phase() != rangesplit.RetainedPruneComplete {
		if !sourceLeader(observed.SourceStatus, observed.SourceState) {
			return Action{Kind: ActionAwaitSourceLeader}, nil
		}
		return Action{Kind: ActionPruneRetained}, nil
	}
	prune := *observed.Prune
	if plan.partitioner.VerifyRetainedPruneCompletion(certificate, prune) != nil ||
		plan.partitioner.ValidatePublicationTransition(
			plan.sourceManifest, plan.targetManifest, plan.current, plan.next,
			certificate, prune,
		) != nil {
		return Action{}, ErrTopologyConflict
	}
	return Action{Kind: ActionPublishCatalog, CatalogGeneration: plan.next}, nil
}

type catalogStage uint8

const (
	catalogSource catalogStage = iota + 1
	catalogTarget
)

func (p *Plan) catalogStage(snapshot *gateway.Snapshot) (catalogStage, error) {
	manifest, ok := snapshot.Manifest(p.source.Distribution)
	if !ok {
		return 0, ErrTopologyConflict
	}
	switch snapshot.Generation() {
	case p.current:
		if !manifest.Equal(p.sourceManifest) {
			return 0, ErrTopologyConflict
		}
		return catalogSource, nil
	case p.next:
		if !manifest.Equal(p.targetManifest) {
			return 0, ErrTopologyConflict
		}
		return catalogTarget, nil
	default:
		return 0, ErrTopologyConflict
	}
}

func (p *Plan) validateSourceObservation(observed Observation) error {
	state := observed.SourceState
	binding := state.Binding
	if state.Applied == 0 || state.LastTerm == 0 ||
		state.LogicalDigest == ([32]byte{}) || state.LastEntryDigest == ([32]byte{}) ||
		state.SnapshotBaseDigest == ([32]byte{}) ||
		binding.Distribution != string(p.source.Distribution) ||
		binding.Shard != string(p.source.Shard) ||
		binding.AllocationGeneration != uint64(p.source.AllocationGeneration) {
		return ErrTopologyConflict
	}
	sealed := observed.Certificate != nil || observed.Tail != nil && observed.Tail.Sealed()
	retained := p.children[p.retained]
	if sealed {
		if binding.OwnershipEpoch != uint64(retained.OwnershipEpoch) ||
			binding.RoutingVersion != uint64(p.targetManifest.Version()) ||
			binding.RouteGeneration != p.next {
			return ErrTopologyConflict
		}
		return nil
	}
	if binding.OwnershipEpoch != uint64(p.source.OwnershipEpoch) ||
		binding.RoutingVersion != uint64(p.source.RoutingVersion) ||
		binding.RouteGeneration != p.current {
		return ErrTopologyConflict
	}
	return nil
}

func (p *Plan) sourceStateMatchesCut(state replicatedstate.State, tail rangesplit.TailCursor) bool {
	cut := tail.SourceCut()
	coordinates := tail.SourceCoordinates()
	return state.Applied == cut.Applied && state.LastTerm == cut.Term &&
		state.LogicalDigest == cut.LogicalDigest && state.LastEntryDigest == cut.EntryDigest &&
		state.SnapshotBaseDigest == cut.BaseDigest &&
		state.Binding.OwnershipEpoch == coordinates.OwnershipEpoch &&
		state.Binding.RoutingVersion == coordinates.RoutingVersion &&
		state.Binding.RouteGeneration == coordinates.RouteGeneration
}

func (p *Plan) sourceStateAfterCutover(
	state replicatedstate.State,
	certificate rangesplit.CutoverCertificate,
	prune *rangesplit.RetainedPruneCursor,
) bool {
	cut := certificate.SourceCut()
	coordinates := certificate.SourceCoordinates()
	if state.Binding.OwnershipEpoch != coordinates.OwnershipEpoch ||
		state.Binding.RoutingVersion != coordinates.RoutingVersion ||
		state.Binding.RouteGeneration != coordinates.RouteGeneration ||
		state.SnapshotBaseDigest != cut.BaseDigest || state.Applied < cut.Applied {
		return false
	}
	if prune == nil {
		return state.Applied == cut.Applied && state.LastTerm == cut.Term &&
			state.LogicalDigest == cut.LogicalDigest && state.LastEntryDigest == cut.EntryDigest
	}
	pruneCut := prune.SourceCut()
	return state.Applied >= pruneCut.Applied
}

func (p *Plan) stageAction(
	observed Observation,
	initial rangesplit.TailCursor,
) (Action, bool, error) {
	placement := initial.PlacementDigest()
	for child := 0; child < int(p.childCount); child++ {
		descriptor := p.children[child]
		if descriptor.Retained {
			if observed.Stages[child] != nil {
				return Action{}, false, ErrTopologyConflict
			}
			continue
		}
		cursor := observed.Stages[child]
		artifact := observed.Artifacts.Children[child]
		if cursor == nil {
			return Action{Kind: ActionStageChild, Child: uint8(child)}, true, nil
		}
		if cursor.Child() != uint8(child) || cursor.PlanDigest() != p.partitioner.Digest() ||
			cursor.PlacementDigest() != placement || cursor.ArtifactDigest() != artifact.Digest ||
			cursor.SourceCut().Applied < artifact.Source.Applied {
			return Action{}, false, ErrTopologyConflict
		}
		switch cursor.Phase() {
		case rangesplit.ChildStageArtifact:
			if cursor.SourceCut() != artifact.Source {
				return Action{}, false, ErrTopologyConflict
			}
			return Action{Kind: ActionStageChild, Child: uint8(child)}, true, nil
		case rangesplit.ChildStageTail, rangesplit.ChildStageSealed:
		default:
			return Action{}, false, ErrTopologyConflict
		}
	}
	return Action{}, false, nil
}

func (p *Plan) stagesMatchTail(
	observed Observation,
	tail rangesplit.TailCursor,
	sealed bool,
) bool {
	for child := 0; child < int(p.childCount); child++ {
		descriptor := p.children[child]
		if descriptor.Retained {
			continue
		}
		cursor := observed.Stages[child]
		if cursor == nil || cursor.SourceCut() != tail.SourceCut() {
			return false
		}
		if sealed && cursor.Phase() != rangesplit.ChildStageSealed {
			return false
		}
		if !sealed && cursor.Phase() != rangesplit.ChildStageTail {
			return false
		}
	}
	return true
}

func (p *Plan) childAction(
	observed Observation,
	certificate rangesplit.CutoverCertificate,
) (Action, bool, error) {
	for child := 0; child < int(p.childCount); child++ {
		descriptor := p.children[child]
		if descriptor.Retained {
			if observed.Children[child] != nil {
				return Action{}, false, ErrTopologyConflict
			}
			continue
		}
		target := p.targets[child]
		status := observed.Children[child]
		if status == nil || !status.Activated {
			if status != nil && (status.WALCreated || status.RuntimeAdopted) {
				return Action{}, false, ErrTopologyConflict
			}
			return Action{Kind: ActionActivateChild, Child: uint8(child)}, true, nil
		}
		if status.Child != uint8(child) || status.ApplyIdentity.Storage == "" ||
			status.ApplyIdentity.ValidationDigest == ([32]byte{}) ||
			status.ApplyProfile.Binding != target.SQL.Binding ||
			!status.ApplyProfile.Initialized ||
			status.ApplyProfile.Applied < certificate.SourceCut().Applied ||
			status.ApplyProfile.MaxCompletions != status.ApplyIdentity.MaxCompletions {
			return Action{}, false, ErrTopologyConflict
		}
		if !status.WALCreated {
			if status.RuntimeAdopted ||
				status.ApplyProfile.Applied != certificate.SourceCut().Applied ||
				status.ApplyProfile.CompletionCount != 0 {
				return Action{}, false, ErrTopologyConflict
			}
			return Action{Kind: ActionCreateChildWAL, Child: uint8(child)}, true, nil
		}
		if status.WALBinding != target.SQL.Binding {
			return Action{}, false, ErrTopologyConflict
		}
		if !status.RuntimeAdopted {
			return Action{Kind: ActionAdoptChildRuntime, Child: uint8(child)}, true, nil
		}
		if !runtimeIdentityMatches(target, status.RuntimeIdentity) {
			return Action{}, false, ErrTopologyConflict
		}
		runtime := status.RuntimeStatus
		if runtime.MemberID != target.WAL.MemberID || runtime.Term == 0 ||
			runtime.Applied > runtime.Commit {
			return Action{}, false, ErrTopologyConflict
		}
		if runtime.LeaderID != runtime.MemberID || runtime.RaftState != raft.StateLeader ||
			runtime.Applied < certificate.SourceCut().Applied {
			return Action{Kind: ActionAwaitChildReady, Child: uint8(child)}, true, nil
		}
	}
	return Action{}, false, nil
}

func (p *Plan) validatePublishedCompletion(observed Observation) error {
	if observed.Certificate == nil || observed.Prune == nil ||
		observed.Prune.Phase() != rangesplit.RetainedPruneComplete {
		return ErrTopologyConflict
	}
	certificate, prune := *observed.Certificate, *observed.Prune
	if p.partitioner.ValidatePublicationTransition(
		p.sourceManifest, p.targetManifest, p.current, p.next, certificate, prune,
	) != nil {
		return ErrTopologyConflict
	}
	if _, ok, err := p.childAction(observed, certificate); ok || err != nil {
		if err != nil {
			return err
		}
		return ErrTopologyConflict
	}
	return nil
}

func sourceLeader(status raftmember.RuntimeStatus, state replicatedstate.State) bool {
	return status.MemberID != 0 && status.MemberID == status.LeaderID &&
		status.RaftState == raft.StateLeader && status.Term != 0 &&
		status.Applied == state.Applied && status.Applied <= status.Commit
}

func runtimeIdentityMatches(target ChildTarget, identity raftmember.RuntimeIdentity) bool {
	return identity.Group.ClusterID == target.WAL.ClusterID &&
		identity.Group.ClusterIncarnation == target.WAL.ClusterIncarnation &&
		identity.Group.TopologyRecoveryEpoch == target.TopologyRecoveryEpoch &&
		identity.Group.ShardIncarnation == target.WAL.ShardIncarnation &&
		identity.Group.GroupID == target.WAL.GroupID &&
		identity.Distribution == target.WAL.Distribution &&
		identity.Shard == target.WAL.Shard &&
		identity.AllocationGeneration == target.WAL.AllocationGeneration &&
		identity.MemberID == target.WAL.MemberID && identity.StoreID == target.WAL.StoreID &&
		identity.NodeIncarnation != 0
}

func cloneChildTarget(target ChildTarget) ChildTarget {
	target.Endpoint = distribution.EndpointID(strings.Clone(string(target.Endpoint)))
	target.WAL.Distribution = strings.Clone(target.WAL.Distribution)
	target.WAL.Shard = strings.Clone(target.WAL.Shard)
	target.SQL.Binding.Distribution = strings.Clone(target.SQL.Binding.Distribution)
	target.SQL.Binding.Shard = strings.Clone(target.SQL.Binding.Shard)
	target.SQL.UserTable = strings.Clone(target.SQL.UserTable)
	target.SQL.UserStorage = strings.Clone(target.SQL.UserStorage)
	target.SQL.UserPrimaryKey = strings.Clone(target.SQL.UserPrimaryKey)
	return target
}

func cloneSourceIdentity(source autosplit.SourceIdentity) autosplit.SourceIdentity {
	source.Distribution = distribution.DistributionName(
		strings.Clone(string(source.Distribution)),
	)
	source.Shard = distribution.ShardID(strings.Clone(string(source.Shard)))
	return source
}
