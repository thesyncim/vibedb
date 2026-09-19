package gateway

import (
	"bytes"
	"context"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// FrontendDrainRuntimeCatalogRowReader is the local canonical source used
// during embedded-gateway recovery. It obtains the serving fence from the
// catalog owner itself and admits each fixed control-plane point through the
// linearizable read lane. It deliberately has no arbitrary relation or key
// escape hatch.
type FrontendDrainRuntimeCatalogRowReader struct {
	owner   frontendDrainRuntimeCatalogOwner
	group   FrontendDrainRuntimeCatalogRoute
	command raftservice.CommandFence
}

// frontendDrainRuntimeCatalogOwner is kept narrow so the row reader's
// serialization and fence rules can be tested without constructing a full
// peer. Production construction uses *raftservice.ExecutionOwners.
type frontendDrainRuntimeCatalogOwner interface {
	Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	ReadLinearizablePointInto(context.Context, raftservice.LinearizablePointReadRequest, *raftservice.LinearizablePointReadCut) error
}

// NewFrontendDrainRuntimeCatalogRowReader binds one local ExecutionOwners
// instance to the reserved catalog route. route must be the authenticated
// catalog route loaded from the immutable/active route seed; its command is
// used as the lower bound for owner command progression.
func NewFrontendDrainRuntimeCatalogRowReader(
	owners *raftservice.ExecutionOwners, route ReplicatedRoute, relation replication.RelationID,
) (*FrontendDrainRuntimeCatalogRowReader, error) {
	if owners == nil || !catalogBootstrapRoute(route) || relation == 0 || relation > replication.MaxRelationID ||
		!route.Command.Valid() || route.AllocationGeneration == 0 {
		return nil, ErrReplicatedCatalog
	}
	return newFrontendDrainRuntimeCatalogRowReader(owners, FrontendDrainRuntimeCatalogRoute{
		Group: route.Group, AllocationGeneration: route.AllocationGeneration,
		Command: route.Command, Relation: relation,
	})
}

// NewFrontendDrainRuntimeCatalogRowReaderForCatalogRoute binds the fixed
// control-plane row grammar directly to a certified catalog route. Physical
// storage startup has the retained RF3 group and command fence before it has a
// gateway manifest, so it must not manufacture the other public route digests
// merely to construct this reader.
func NewFrontendDrainRuntimeCatalogRowReaderForCatalogRoute(
	owners *raftservice.ExecutionOwners, route FrontendDrainRuntimeCatalogRoute,
) (*FrontendDrainRuntimeCatalogRowReader, error) {
	if owners == nil || !route.Valid() {
		return nil, ErrReplicatedCatalog
	}
	return newFrontendDrainRuntimeCatalogRowReader(owners, route)
}

func newFrontendDrainRuntimeCatalogRowReader(
	owner frontendDrainRuntimeCatalogOwner, route FrontendDrainRuntimeCatalogRoute,
) (*FrontendDrainRuntimeCatalogRowReader, error) {
	if owner == nil || !route.Valid() {
		return nil, ErrReplicatedCatalog
	}
	return &FrontendDrainRuntimeCatalogRowReader{owner: owner, group: route, command: route.Command}, nil
}

func (reader *FrontendDrainRuntimeCatalogRowReader) probe(
	ctx context.Context,
) (raftservice.ServingState, FrontendDrainRuntimeCatalogRoute, error) {
	if reader == nil || reader.owner == nil || ctx == nil || !reader.group.Valid() || !reader.command.Valid() {
		return raftservice.ServingState{}, FrontendDrainRuntimeCatalogRoute{}, ErrReplicatedCatalog
	}
	state, err := reader.owner.Probe(ctx, reader.group.Group)
	if err != nil {
		return raftservice.ServingState{}, FrontendDrainRuntimeCatalogRoute{}, err
	}
	fence := state.Fence()
	if fence.Group != reader.group.Group {
		return raftservice.ServingState{}, FrontendDrainRuntimeCatalogRoute{}, fmt.Errorf("%w: catalog row owner group mismatch want=%+v got=%+v", ErrReplicatedCatalogConflict, reader.group.Group, fence.Group)
	}
	if fence.AllocationGeneration != reader.group.AllocationGeneration {
		return raftservice.ServingState{}, FrontendDrainRuntimeCatalogRoute{}, fmt.Errorf("%w: catalog row owner allocation mismatch want=%d got=%d", ErrReplicatedCatalogConflict, reader.group.AllocationGeneration, fence.AllocationGeneration)
	}
	if !fence.Command.Valid() || fence.MemberID == 0 || fence.StoreID == ([16]byte{}) ||
		fence.NodeIncarnation == 0 || fence.Term == 0 {
		return raftservice.ServingState{}, FrontendDrainRuntimeCatalogRoute{}, fmt.Errorf("%w: catalog row owner fence incomplete fence=%+v", ErrReplicatedCatalogConflict, fence)
	}
	if state.Status.MemberID != state.Identity.MemberID {
		return raftservice.ServingState{}, FrontendDrainRuntimeCatalogRoute{}, fmt.Errorf("%w: catalog row owner member mismatch identity=%d status=%d", ErrReplicatedCatalogConflict, state.Identity.MemberID, state.Status.MemberID)
	}
	if state.Status.LeaderID != state.Identity.MemberID {
		return raftservice.ServingState{}, FrontendDrainRuntimeCatalogRoute{}, fmt.Errorf("%w: catalog row owner is not leader identity=%d leader=%d term=%d", ErrReplicatedCatalogConflict, state.Identity.MemberID, state.Status.LeaderID, state.Status.Term)
	}
	if !catalogCommandProgression(reader.command, fence.Command) {
		return raftservice.ServingState{}, FrontendDrainRuntimeCatalogRoute{}, fmt.Errorf("%w: catalog row owner command regressed floor=%+v got=%+v", ErrReplicatedCatalogConflict, reader.command, fence.Command)
	}
	route := reader.group
	route.Command = fence.Command
	return state, route, nil
}

// ReadFrontendDrainRuntimeCatalogRoute returns the current owner command and
// exact catalog route. The command is refreshed from the serialized owner on
// every call; the constructor's command is only a monotonic lower bound.
func (reader *FrontendDrainRuntimeCatalogRowReader) ReadFrontendDrainRuntimeCatalogRoute(
	ctx context.Context,
) (FrontendDrainRuntimeCatalogRoute, error) {
	_, route, err := reader.probe(ctx)
	return route, err
}

// ReadFrontendDrainRuntimeRow reads one closed control-plane row under one
// exact serving fence. The caller receives detached bytes only after the
// point cut has passed its final authority check.
func (reader *FrontendDrainRuntimeCatalogRowReader) ReadFrontendDrainRuntimeRow(
	ctx context.Context, key FrontendDrainRuntimeRowKey,
) (FrontendDrainRuntimeRow, error) {
	if reader == nil || ctx == nil || !key.Valid() {
		return FrontendDrainRuntimeRow{}, ErrReplicatedCatalog
	}
	state, route, err := reader.probe(ctx)
	if err != nil {
		return FrontendDrainRuntimeRow{}, err
	}
	encoded, ok := key.EncodedKey()
	if !ok || len(encoded) == 0 || key.MaxValueBytes() == 0 {
		return FrontendDrainRuntimeRow{}, ErrReplicatedCatalog
	}
	fence := state.Fence()
	var cut raftservice.LinearizablePointReadCut
	err = reader.owner.ReadLinearizablePointInto(ctx, raftservice.LinearizablePointReadRequest{
		Fence: fence, Capability: serviceauthz.CapabilityDataRead,
		Authorize: func(current raftservice.ServingState) bool {
			return current.Fence() == fence
		},
	}, &cut)
	if err != nil {
		return FrontendDrainRuntimeRow{}, err
	}
	defer cut.Close()
	if cut.State().Fence() != fence {
		return FrontendDrainRuntimeRow{}, ErrReplicatedCatalogConflict
	}
	// The machine admits a point read against the relation's frozen document
	// limit, even when this closed control-plane row has a tighter canonical
	// envelope bound. Use the full relation ceiling for admission (the same
	// reservation as ReplicatedCatalogAuthority.readRaw), then enforce the
	// row-specific bound on the detached result below. Passing the smaller
	// envelope bound to PointReadInto would reject every control row on a
	// relation whose user document limit is the canonical 4 MiB ceiling.
	point, err := cut.PointReadInto(ctx, route.Relation, encoded, maxReplicatedCatalogBytes, nil)
	if err != nil {
		return FrontendDrainRuntimeRow{}, err
	}
	if point.Fence.Applied == 0 || (point.Found && len(point.Value) == 0) || (!point.Found && len(point.Value) != 0) {
		return FrontendDrainRuntimeRow{}, ErrReplicatedCatalogConflict
	}
	if len(point.Value) > key.MaxValueBytes() {
		return FrontendDrainRuntimeRow{}, ErrReplicatedCatalog
	}
	return FrontendDrainRuntimeRow{
		Applied: point.Fence.Applied, Found: point.Found, Value: bytes.Clone(point.Value), Fence: fence,
	}, nil
}

var _ FrontendDrainRuntimeCutRowReader = (*FrontendDrainRuntimeCatalogRowReader)(nil)
