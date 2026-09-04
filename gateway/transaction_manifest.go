package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
)

// transactionCoordinatorStager is the narrow gateway side of coordinator
// staging. Segment bytes alias one fixed page arena and must be consumed before
// the method returns. The shard wire adapter supplies the durable operations.
type transactionCoordinatorStager interface {
	stageInlineCoordinator(record []byte) error
	stageManifestCoordinator(record, firstSegment []byte) error
	stageManifestSegment(id distributedtxn.ID, index uint32, record []byte) error
}

type transactionCoordinatorFormat uint8

const (
	transactionCoordinatorInline transactionCoordinatorFormat = iota + 1
	transactionCoordinatorSegmented
)

// stageTransactionCoordinator preserves the byte-identical VTC1 fast path.
// Only an actual inline-size refusal selects VTCM plus a canonical VTM1 stream.
// The first pass computes the authenticated descriptor; the second emits each
// page directly into the durable stager, retaining no aggregate page buffer.
func stageTransactionCoordinator(
	record distributedtxn.CoordinatorRecord,
	stager transactionCoordinatorStager,
) (transactionCoordinatorFormat, error) {
	inline, err := distributedtxn.AppendCoordinator(nil, record)
	if err == nil {
		return transactionCoordinatorInline, stager.stageInlineCoordinator(inline)
	}
	if !errors.Is(err, distributedtxn.ErrTooLarge) {
		return 0, err
	}

	scratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	descriptor, err := buildTransactionManifest(record.Targets, scratch, func(distributedtxn.ManifestSegment) error {
		return nil
	})
	if err != nil {
		return 0, err
	}
	manifest, err := distributedtxn.AppendManifestCoordinator(nil, distributedtxn.ManifestCoordinatorRecord{
		ID: record.ID, State: record.State, Revision: record.Revision,
		CatalogGeneration: record.CatalogGeneration,
		RecoveryDeadline:  record.RecoveryDeadline,
		Manifest:          descriptor,
	})
	if err != nil {
		return 0, err
	}
	begun := false
	_, err = buildTransactionManifest(record.Targets, scratch, func(segment distributedtxn.ManifestSegment) error {
		if !begun {
			begun = true
			return stager.stageManifestCoordinator(manifest, segment.Raw)
		}
		return stager.stageManifestSegment(record.ID, segment.Index, segment.Raw)
	})
	if err == nil && !begun {
		err = distributedtxn.ErrCorrupt
	}
	return transactionCoordinatorSegmented, err
}

func buildTransactionManifest(
	targets []distributedtxn.TransactionTargetRef, scratch []byte,
	emit func(distributedtxn.ManifestSegment) error,
) (distributedtxn.ManifestDescriptor, error) {
	builder, err := distributedtxn.NewManifestBuilder(scratch, emit)
	if err != nil {
		return distributedtxn.ManifestDescriptor{}, err
	}
	for i := range targets {
		if err := builder.Append(targets[i]); err != nil {
			return distributedtxn.ManifestDescriptor{}, err
		}
	}
	return builder.Seal()
}

// gatewayCoordinatorStager adapts the two coordinator encodings to the shard
// wire without weakening the shared routing, deadline, or ownership fence.
type gatewayCoordinatorStager struct {
	executor    *Executor
	ctx         context.Context
	coordinator *transactionTarget
	profile     Profile
}

func (s *gatewayCoordinatorStager) stageInlineCoordinator(record []byte) error {
	return s.stage(shardservice.TransactionStageCoordinator, distributedtxn.ID{}, record)
}

func (s *gatewayCoordinatorStager) stageManifestCoordinator(record, firstSegment []byte) error {
	return s.stageManifestBegin(record, firstSegment)
}

func (s *gatewayCoordinatorStager) stageManifestBegin(record, firstSegment []byte) error {
	request := transactionRequest(
		s.coordinator.call.req, s.profile,
		shardservice.TransactionStageManifestCoordinator,
		distributedtxn.ID{}, 0, record,
	)
	request.Transaction.ManifestSegment = firstSegment
	_, err := s.executor.transactionRoundTrip(
		s.ctx, s.coordinator.call.address, request, s.profile,
	)
	return err
}

func (s *gatewayCoordinatorStager) stageManifestSegment(
	id distributedtxn.ID,
	_ uint32,
	record []byte,
) error {
	return s.stage(shardservice.TransactionStageManifestSegment, id, record)
}

func (s *gatewayCoordinatorStager) stage(
	operation shardservice.TransactionOperation,
	id distributedtxn.ID,
	record []byte,
) error {
	request := transactionRequest(
		s.coordinator.call.req, s.profile, operation, id, 0, record,
	)
	_, err := s.executor.transactionRoundTrip(
		s.ctx, s.coordinator.call.address, request, s.profile,
	)
	return err
}
