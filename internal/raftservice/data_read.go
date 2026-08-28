package raftservice

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// DataReadSource is separate from the privileged backup snapshot source.
// Implementations check live intents and capture an immutable publication at
// the supplied quorum floor into reusable caller-owned storage.
type DataReadSource interface {
	DataReadCutInto([]replication.RelationID, uint64, *replicatedstate.DataReadCut) error
}

type LinearizableDataReadRequest struct {
	Fence      ServingFence
	Capability serviceauthz.Capability
	Relations  []replication.RelationID
}

// LinearizableDataReadCut pins both the storage cut and the serving generation.
// It is single-consumer, must not be copied, and can be reused after Close.
type LinearizableDataReadCut struct {
	source     DataReadSource
	data       replicatedstate.DataReadCut
	generation *ownerGeneration
	owner      *Owner
}

// Source borrows the exact generation's data source for shard-local SQL
// execution. It is valid only while this cut retains its generation lease.
func (cut *LinearizableDataReadCut) Source() DataReadSource {
	if cut == nil || cut.owner == nil {
		return nil
	}
	return cut.source
}

// Data borrows the admitted cut. Its physical rows still require the ownership
// filtering documented by DataReadCut; this is not a backup/export capability.
func (cut *LinearizableDataReadCut) Data() *replicatedstate.DataReadCut {
	if cut == nil || cut.owner == nil {
		return nil
	}
	return &cut.data
}

func (cut *LinearizableDataReadCut) Close() error {
	if cut == nil || cut.owner == nil {
		return nil
	}
	err := cut.data.Close()
	cut.generation.release()
	cut.owner.releasePendingRead(1)
	cut.generation, cut.owner = nil, nil
	cut.source = nil
	return err
}

// ReadLinearizableDataInto acquires a leader ReadIndex, waits for apply, and
// pins the exact serving generation until the caller closes dst. Network
// boundaries must authenticate and authorize the request before calling this
// method; only the dedicated data-read capability is accepted here.
func (owner *Owner) ReadLinearizableDataInto(
	ctx context.Context, request LinearizableDataReadRequest, dst *LinearizableDataReadCut,
) error {
	if owner == nil || ctx == nil || dst == nil || request.Capability != serviceauthz.CapabilityDataRead ||
		request.Relations != nil && len(request.Relations) == 0 || len(request.Relations) > replication.MaxRelationsPerBundle {
		return ErrInvalidOwner
	}
	if dst.owner != nil {
		return replicatedstate.ErrDataReadOpen
	}
	if err := owner.reservePendingRead(1); err != nil {
		return err
	}
	admitted := false
	defer func() {
		if !admitted {
			owner.releasePendingRead(1)
		}
	}()
	delivery := &readDelivery{reply: make(chan ownerReply, 1)}
	reply, err := owner.enqueueRead(ctx, ownerRequest{
		kind: requestReadLinear, group: request.Fence.Group, reply: delivery.reply,
		read: readRequest{fence: request.Fence, delivery: delivery},
	}, delivery)
	if err != nil {
		return err
	}
	defer func() {
		if !admitted {
			reply.read.generation.release()
		}
	}()
	source, ok := reply.read.source.(DataReadSource)
	if !ok {
		return ErrServingFence
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := source.DataReadCutInto(request.Relations, reply.read.minimumApplied, &dst.data); err != nil {
		return err
	}
	if err := context.Cause(ctx); err != nil {
		return errors.Join(err, dst.data.Close())
	}
	fence := dst.data.Fence()
	if !pointReadFenceMatches(fence, request.Fence) || fence.Applied < reply.read.minimumApplied {
		return errors.Join(ErrServingFence, dst.data.Close())
	}
	dst.generation, dst.owner = reply.read.generation, owner
	dst.source = source
	admitted = true
	return nil
}

func (owners *ExecutionOwners) ReadLinearizableDataInto(
	ctx context.Context, request LinearizableDataReadRequest, dst *LinearizableDataReadCut,
) error {
	owner, err := owners.owner(request.Fence.Group)
	if err != nil {
		return err
	}
	return owner.ReadLinearizableDataInto(ctx, request, dst)
}
