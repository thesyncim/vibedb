package snapshottransfer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	ErrSourceControl        = errors.New("snapshottransfer: invalid source export control request")
	ErrSourceUnauthorized   = errors.New("snapshottransfer: source export control is not authorized")
	ErrSourceMissing        = errors.New("snapshottransfer: source export operation is missing")
	ErrSourceConflict       = errors.New("snapshottransfer: source export operation conflicts")
	ErrSourceOutcomeUnknown = errors.New("snapshottransfer: source export outcome is unknown")
)

// SourceControlRequestDiscriminator identifies this fixed grammar on the
// shared authenticated shard-control listener.
func SourceControlRequestDiscriminator() [8]byte { return sourceRequestMagic }

const (
	SourceControlRequestBytes    = 216
	SourceControlResponseBytes   = 336
	AbsoluteMaxSourceConcurrency = 256
)

var (
	sourceRequestMagic  = [8]byte{'V', 'B', 'S', 'R', 'E', 'Q', 0, 0}
	sourceResponseMagic = [8]byte{'V', 'B', 'S', 'R', 'E', 'S', 0, 0}
)

type SourceControlState uint8

const (
	SourceControlRunning SourceControlState = iota + 1
	SourceControlComplete
	SourceControlReleased
)

type sourceControlCommand uint8

const (
	sourceControlPrepare sourceControlCommand = iota
	sourceControlRelease
	sourceControlAbandon
)

// SourceControlRequest is the immutable identity of one journaled replica-move
// snapshot action. ReplicaSetVersion selects the exact learner-bearing Raft
// publication; an exporter must never substitute a later publication.
type SourceControlRequest struct {
	Operation         [32]byte
	Step              [32]byte
	Group             raftmember.GroupKey
	SourceMember      uint64
	TargetMember      uint64
	TargetStore       [16]byte
	TargetIncarnation uint64
	ReplicaSetVersion uint64
	SourceNode        rafttransport.NodeID
}

type SourceControlRecord struct {
	Request    SourceControlRequest
	Revision   uint64
	State      SourceControlState
	Descriptor Descriptor
}

// SourceControlJournal must durably publish before returning nil. A failed
// publication may have committed; the service resolves that uncertainty with
// an exact read before returning.
type SourceControlJournal interface {
	ReadSourceExport(context.Context, [32]byte) (SourceControlRecord, error)
	PublishSourceExport(context.Context, uint64, SourceControlRecord) error
}

// SourceControlExporter is the narrow adapter around ExportPinnedSnapshot and
// its durable Repository. Observe must find an artifact previously exported
// for the exact request, including after process restart. Export must use an
// exact learner-bearing snapshot fence and the repository's resumable cursor.
type SourceControlExporter interface {
	ObserveReplicaMoveSnapshot(context.Context, SourceControlRequest) (Descriptor, bool, error)
	ExportReplicaMoveSnapshot(context.Context, SourceControlRequest) (Descriptor, error)
	ReleaseReplicaMoveSnapshot(context.Context, SourceControlRequest, Descriptor) error
}

type SourceControlAbandoner interface {
	AbandonReplicaMoveSnapshot(context.Context, SourceControlRequest, ArtifactAbandonmentWitness) error
}

// SourceExportPlanProvider owns source-specific Raft ReadIndex fencing and
// durable artifact discovery. PinSourceExport returns a newly owned immutable
// ReadSnapshot; PinnedSourceControlExporter always closes it.
type SourceExportPlanProvider interface {
	ObserveSourceExport(context.Context, SourceControlRequest) (Descriptor, bool, error)
	PinSourceExport(context.Context, SourceControlRequest) (SourceExportPlan, error)
	ReleaseSourceExport(context.Context, SourceControlRequest, Descriptor) error
}

// PinnedSourceControlExporter is the concrete bounded adapter to
// ExportPinnedSnapshot. It prevents command handlers from growing a second
// snapshot encoding/export implementation.
type PinnedSourceControlExporter struct{ Provider SourceExportPlanProvider }

func (exporter PinnedSourceControlExporter) ObserveReplicaMoveSnapshot(
	ctx context.Context,
	request SourceControlRequest,
) (Descriptor, bool, error) {
	if exporter.Provider == nil {
		return Descriptor{}, false, ErrSourceControl
	}
	return exporter.Provider.ObserveSourceExport(ctx, request)
}

func (exporter PinnedSourceControlExporter) ExportReplicaMoveSnapshot(
	ctx context.Context,
	request SourceControlRequest,
) (result Descriptor, resultErr error) {
	if exporter.Provider == nil || ctx == nil || !validSourceControlRequest(request) {
		return Descriptor{}, ErrSourceControl
	}
	plan, err := exporter.Provider.PinSourceExport(ctx, request)
	if err != nil {
		return Descriptor{}, err
	}
	if plan.Release != nil {
		defer plan.Release()
	}
	if plan.Snapshot == nil {
		return Descriptor{}, ErrSourceControl
	}
	defer func() { resultErr = errors.Join(resultErr, plan.Snapshot.Close()) }()
	if plan.Group != request.Group || plan.SourceMember != request.SourceMember ||
		plan.TargetMember != request.TargetMember || plan.TargetStore != request.TargetStore ||
		plan.TargetIncarnation != request.TargetIncarnation ||
		plan.ExpectedFence.ReplicaSetVersion != request.ReplicaSetVersion {
		return Descriptor{}, ErrSourceConflict
	}
	descriptor, _, err := ExportPinnedSnapshot(plan)
	if err != nil {
		return Descriptor{}, err
	}
	if cause := context.Cause(ctx); cause != nil {
		// The repository may already be complete. Returning the cancellation is
		// intentionally outcome-unknown to the service, which observes it.
		return Descriptor{}, cause
	}
	return descriptor, nil
}

func (exporter PinnedSourceControlExporter) ReleaseReplicaMoveSnapshot(
	ctx context.Context,
	request SourceControlRequest,
	descriptor Descriptor,
) error {
	if exporter.Provider == nil || ctx == nil || !validSourceControlRequest(request) ||
		!descriptorMatchesSourceRequest(descriptor, request) {
		return ErrSourceControl
	}
	return exporter.Provider.ReleaseSourceExport(ctx, request, descriptor)
}

func (exporter PinnedSourceControlExporter) AbandonReplicaMoveSnapshot(
	ctx context.Context, request SourceControlRequest, witness ArtifactAbandonmentWitness,
) error {
	provider, ok := exporter.Provider.(interface {
		AbandonSourceExport(context.Context, SourceControlRequest, ArtifactAbandonmentWitness) error
	})
	if !ok {
		return ErrAbandonment
	}
	return provider.AbandonSourceExport(ctx, request, witness)
}

type SourceControlAuthorizeFunc func(rafttransport.PeerIdentity, SourceControlRequest) bool

type SourceControlOptions struct {
	Journal       SourceControlJournal
	Exporter      SourceControlExporter
	Authorize     SourceControlAuthorizeFunc
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

// SourceControlService serves one command per authenticated shard-control
// stream. Fixed stripes serialize retries for the same operation and the
// semaphore bounds snapshot scans and repository IO across operations.
type SourceControlService struct {
	journal       SourceControlJournal
	exporter      SourceControlExporter
	authorize     SourceControlAuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	stripes       []sync.Mutex
}

func NewSourceControlService(options SourceControlOptions) (*SourceControlService, error) {
	if options.Journal == nil || options.Exporter == nil || options.Authorize == nil ||
		options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > AbsoluteMaxSourceConcurrency {
		return nil, ErrSourceControl
	}
	return &SourceControlService{
		journal: options.Journal, exporter: options.Exporter, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots:   make(chan struct{}, options.MaxConcurrent),
		stripes: make([]sync.Mutex, options.MaxConcurrent),
	}, nil
}

func (service *SourceControlService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		return ErrSourceUnauthorized
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := service.readDeadline(); deadline.IsZero() {
		return ErrSourceControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	command, request, err := readSourceControlCommand(connection)
	if err != nil {
		return err
	}
	if command == sourceControlAbandon {
		var raw [AbandonmentWitnessBytes]byte
		if _, err = io.ReadFull(connection, raw[:]); err != nil {
			return err
		}
		witness, openErr := OpenAbandonmentWitness(raw[:])
		if openErr != nil {
			return openErr
		}
		return service.serveAbandonCommand(ctx, connection, request, witness)
	}
	return service.serveCommand(ctx, connection, command, request)
}

func (service *SourceControlService) serveCommand(
	ctx context.Context, connection rafttransport.PeerConnection,
	command sourceControlCommand, request SourceControlRequest,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl ||
		command > sourceControlRelease || !validSourceControlRequest(request) {
		return ErrSourceUnauthorized
	}
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{
		ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation,
	}
	if peer.TrustDomain != wantDomain || !service.authorize(peer, request) {
		return ErrSourceUnauthorized
	}
	var (
		record SourceControlRecord
		err    error
	)
	if command == sourceControlRelease {
		record, err = service.Release(ctx, request)
	} else {
		record, err = service.Execute(ctx, request)
	}
	if err != nil {
		return err
	}
	if deadline := service.writeDeadline(); deadline.IsZero() {
		return ErrSourceControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteSourceControlResponse(connection, record)
}

func (service *SourceControlService) serveAbandonCommand(
	ctx context.Context, connection rafttransport.PeerConnection, request SourceControlRequest,
	witness ArtifactAbandonmentWitness,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl ||
		!validSourceControlRequest(request) || !witness.Valid() {
		return ErrSourceUnauthorized
	}
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	if peer.TrustDomain != wantDomain || !service.authorize(peer, request) {
		return ErrSourceUnauthorized
	}
	record, err := service.Abandon(ctx, request, witness)
	if err != nil {
		return err
	}
	if deadline := service.writeDeadline(); deadline.IsZero() {
		return ErrSourceControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteSourceControlResponse(connection, record)
}

// Execute records immutable intent before invoking the exporter. Unknown
// exporter outcomes are settled through Observe, and unknown journal outcomes
// are settled through an exact record read.
func (service *SourceControlService) Execute(ctx context.Context, request SourceControlRequest) (SourceControlRecord, error) {
	if service == nil || ctx == nil || !validSourceControlRequest(request) {
		return SourceControlRecord{}, ErrSourceControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return SourceControlRecord{}, cause
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return SourceControlRecord{}, ErrBound
	}
	stripe := &service.stripes[binary.LittleEndian.Uint64(request.Operation[:8])%uint64(len(service.stripes))]
	stripe.Lock()
	defer stripe.Unlock()
	record, err := service.loadOrCreate(ctx, request)
	if err != nil || record.State == SourceControlComplete || record.State == SourceControlReleased {
		return record, err
	}
	descriptor, found, err := service.exporter.ObserveReplicaMoveSnapshot(ctx, request)
	if err != nil {
		return SourceControlRecord{}, err
	}
	if !found {
		descriptor, err = service.exporter.ExportReplicaMoveSnapshot(ctx, request)
		if err != nil {
			observed, observedFound, observeErr := service.exporter.ObserveReplicaMoveSnapshot(ctx, request)
			if observeErr != nil || !observedFound {
				return SourceControlRecord{}, errors.Join(err, observeErr)
			}
			descriptor = observed
		}
	}
	if !descriptorMatchesSourceRequest(descriptor, request) {
		return SourceControlRecord{}, ErrSourceConflict
	}
	terminal := record
	terminal.Revision++
	terminal.State = SourceControlComplete
	terminal.Descriptor = descriptor
	if err = service.publishExact(ctx, record.Revision, terminal); err != nil {
		return SourceControlRecord{}, err
	}
	return terminal, nil
}

// Release durably retires the exact completed export. A running export is
// never eligible: the target may still need its resumable source bytes. The
// repository release precedes the Released journal transition, so a crash at
// either boundary is settled by replaying the same operation and step.
func (service *SourceControlService) Release(
	ctx context.Context,
	request SourceControlRequest,
) (SourceControlRecord, error) {
	if service == nil || ctx == nil || !validSourceControlRequest(request) {
		return SourceControlRecord{}, ErrSourceControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return SourceControlRecord{}, cause
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return SourceControlRecord{}, ErrBound
	}
	stripe := &service.stripes[binary.LittleEndian.Uint64(request.Operation[:8])%uint64(len(service.stripes))]
	stripe.Lock()
	defer stripe.Unlock()
	record, err := service.journal.ReadSourceExport(ctx, request.Operation)
	if err != nil {
		return SourceControlRecord{}, err
	}
	if !validSourceControlRecord(record) || record.Request != request {
		return SourceControlRecord{}, ErrSourceConflict
	}
	if record.State == SourceControlReleased {
		return record, nil
	}
	if record.State != SourceControlComplete {
		return SourceControlRecord{}, ErrSourceConflict
	}
	if err := service.exporter.ReleaseReplicaMoveSnapshot(
		ctx, request, record.Descriptor,
	); err != nil {
		return SourceControlRecord{}, err
	}
	released := record
	released.Revision++
	released.State = SourceControlReleased
	if err := service.publishExact(ctx, record.Revision, released); err != nil {
		return SourceControlRecord{}, err
	}
	return released, nil
}

// Abandon accepts no local timeout. Only an exact replicated witness which
// fences the source owner lease may authorize repository deletion.
func (service *SourceControlService) Abandon(
	ctx context.Context, request SourceControlRequest, witness ArtifactAbandonmentWitness,
) (SourceControlRecord, error) {
	if service == nil || ctx == nil || !validSourceControlRequest(request) || !witness.Valid() ||
		witness.Operation != request.Operation || witness.Step != request.Step ||
		witness.Owner != request.SourceNode || !descriptorMatchesSourceRequest(witness.Descriptor, request) {
		return SourceControlRecord{}, ErrAbandonment
	}
	abandoner, ok := service.exporter.(SourceControlAbandoner)
	if !ok {
		return SourceControlRecord{}, ErrAbandonment
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return SourceControlRecord{}, ErrBound
	}
	stripe := &service.stripes[binary.LittleEndian.Uint64(request.Operation[:8])%uint64(len(service.stripes))]
	stripe.Lock()
	defer stripe.Unlock()
	record, err := service.journal.ReadSourceExport(ctx, request.Operation)
	if err != nil || !validSourceControlRecord(record) || record.Request != request {
		return SourceControlRecord{}, errors.Join(err, ErrSourceConflict)
	}
	if record.State == SourceControlReleased {
		if record.Descriptor != witness.Descriptor {
			return SourceControlRecord{}, ErrSourceConflict
		}
		return record, nil
	}
	if err = abandoner.AbandonReplicaMoveSnapshot(ctx, request, witness); err != nil {
		return SourceControlRecord{}, err
	}
	retired := record
	retired.Revision++
	retired.State = SourceControlReleased
	retired.Descriptor = witness.Descriptor
	if err = service.publishExact(ctx, record.Revision, retired); err != nil {
		return SourceControlRecord{}, err
	}
	return retired, nil
}

func (service *SourceControlService) Observe(ctx context.Context, operation [32]byte) (SourceControlRecord, error) {
	if service == nil || ctx == nil || operation == ([32]byte{}) {
		return SourceControlRecord{}, ErrSourceControl
	}
	record, err := service.journal.ReadSourceExport(ctx, operation)
	if err != nil {
		return SourceControlRecord{}, err
	}
	if !validSourceControlRecord(record) || record.Request.Operation != operation {
		return SourceControlRecord{}, ErrSourceConflict
	}
	return record, nil
}

func (service *SourceControlService) loadOrCreate(ctx context.Context, request SourceControlRequest) (SourceControlRecord, error) {
	record, err := service.journal.ReadSourceExport(ctx, request.Operation)
	if errors.Is(err, ErrSourceMissing) {
		record = SourceControlRecord{Request: request, Revision: 1, State: SourceControlRunning}
		if err = service.publishExact(ctx, 0, record); err != nil {
			return SourceControlRecord{}, err
		}
		return record, nil
	}
	if err != nil {
		return SourceControlRecord{}, err
	}
	if !validSourceControlRecord(record) || record.Request != request {
		return SourceControlRecord{}, ErrSourceConflict
	}
	return record, nil
}

func (service *SourceControlService) publishExact(ctx context.Context, expected uint64, desired SourceControlRecord) error {
	err := service.journal.PublishSourceExport(ctx, expected, desired)
	if err == nil {
		return nil
	}
	settled, readErr := service.journal.ReadSourceExport(ctx, desired.Request.Operation)
	if readErr == nil && settled == desired {
		return nil
	}
	return errors.Join(ErrSourceOutcomeUnknown, err, readErr)
}

func validSourceControlRequest(request SourceControlRequest) bool {
	group := request.Group
	return request.Operation != ([32]byte{}) && request.Step != ([32]byte{}) &&
		group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{}) && request.SourceMember != 0 &&
		request.TargetMember != 0 && request.SourceMember != request.TargetMember &&
		request.TargetStore != ([16]byte{}) && request.TargetIncarnation != 0 &&
		request.ReplicaSetVersion != 0 && request.SourceNode != (rafttransport.NodeID{})
}

func validSourceControlRecord(record SourceControlRecord) bool {
	if !validSourceControlRequest(record.Request) || record.Revision == 0 ||
		record.State < SourceControlRunning || record.State > SourceControlReleased {
		return false
	}
	if record.State == SourceControlRunning {
		return record.Descriptor == (Descriptor{})
	}
	return descriptorMatchesSourceRequest(record.Descriptor, record.Request)
}

func descriptorMatchesSourceRequest(descriptor Descriptor, request SourceControlRequest) bool {
	return descriptor.Valid() && descriptor.Group == request.Group &&
		descriptor.SourceMember == request.SourceMember && descriptor.TargetMember == request.TargetMember &&
		descriptor.TargetStore == request.TargetStore &&
		descriptor.TargetIncarnation == request.TargetIncarnation &&
		descriptor.ReplicaSetVersion == request.ReplicaSetVersion
}

func AppendSourceControlRequest(dst []byte, request SourceControlRequest) ([]byte, error) {
	if !validSourceControlRequest(request) || len(dst) > math.MaxInt-SourceControlRequestBytes {
		return dst, ErrSourceControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, SourceControlRequestBytes)...)
	b := dst[start:]
	copy(b[:8], sourceRequestMagic[:])
	copy(b[8:40], request.Operation[:])
	copy(b[40:72], request.Step[:])
	copy(b[72:88], request.Group.ClusterID[:])
	copy(b[88:104], request.Group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(b[104:112], request.Group.TopologyRecoveryEpoch)
	copy(b[112:128], request.Group.ShardIncarnation[:])
	copy(b[128:144], request.Group.GroupID[:])
	binary.BigEndian.PutUint64(b[144:152], request.SourceMember)
	binary.BigEndian.PutUint64(b[152:160], request.TargetMember)
	copy(b[160:176], request.TargetStore[:])
	binary.BigEndian.PutUint64(b[176:184], request.TargetIncarnation)
	binary.BigEndian.PutUint64(b[184:192], request.ReplicaSetVersion)
	copy(b[192:208], request.SourceNode[:])
	// 208:216 is the required zero canonical tail.
	return dst, nil
}

func OpenSourceControlRequest(raw []byte) (SourceControlRequest, error) {
	if len(raw) != SourceControlRequestBytes || !bytes.Equal(raw[:8], sourceRequestMagic[:]) ||
		binary.BigEndian.Uint64(raw[208:216]) != 0 {
		return SourceControlRequest{}, ErrSourceControl
	}
	var request SourceControlRequest
	copy(request.Operation[:], raw[8:40])
	copy(request.Step[:], raw[40:72])
	copy(request.Group.ClusterID[:], raw[72:88])
	copy(request.Group.ClusterIncarnation[:], raw[88:104])
	request.Group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(raw[104:112])
	copy(request.Group.ShardIncarnation[:], raw[112:128])
	copy(request.Group.GroupID[:], raw[128:144])
	request.SourceMember = binary.BigEndian.Uint64(raw[144:152])
	request.TargetMember = binary.BigEndian.Uint64(raw[152:160])
	copy(request.TargetStore[:], raw[160:176])
	request.TargetIncarnation = binary.BigEndian.Uint64(raw[176:184])
	request.ReplicaSetVersion = binary.BigEndian.Uint64(raw[184:192])
	copy(request.SourceNode[:], raw[192:208])
	if !validSourceControlRequest(request) {
		return SourceControlRequest{}, ErrSourceControl
	}
	return request, nil
}

func appendSourceControlReleaseRequest(dst []byte, request SourceControlRequest) ([]byte, error) {
	dst, err := AppendSourceControlRequest(dst, request)
	if err != nil {
		return dst, err
	}
	dst[len(dst)-8] = byte(sourceControlRelease)
	return dst, nil
}

func appendSourceControlAbandonRequest(
	dst []byte, request SourceControlRequest, witness ArtifactAbandonmentWitness,
) ([]byte, error) {
	if !witness.Valid() || witness.Operation != request.Operation || witness.Step != request.Step ||
		witness.Owner != request.SourceNode || !descriptorMatchesSourceRequest(witness.Descriptor, request) {
		return dst, ErrAbandonment
	}
	start := len(dst)
	dst, err := AppendSourceControlRequest(dst, request)
	if err != nil {
		return dst[:start], err
	}
	dst[len(dst)-8] = byte(sourceControlAbandon)
	dst, err = AppendAbandonmentWitness(dst, witness)
	if err != nil {
		return dst[:start], err
	}
	return dst, nil
}

func readSourceControlCommand(reader io.Reader) (
	sourceControlCommand, SourceControlRequest, error,
) {
	var raw [SourceControlRequestBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return 0, SourceControlRequest{}, err
	}
	return openSourceControlCommand(raw)
}

func openSourceControlCommand(raw [SourceControlRequestBytes]byte) (
	sourceControlCommand, SourceControlRequest, error,
) {
	command := sourceControlCommand(raw[208])
	if command > sourceControlAbandon || !allZero(raw[209:216]) {
		return 0, SourceControlRequest{}, ErrSourceControl
	}
	raw[208] = 0
	request, err := OpenSourceControlRequest(raw[:])
	return command, request, err
}

func ReadSourceControlRequest(reader io.Reader) (SourceControlRequest, error) {
	var raw [SourceControlRequestBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return SourceControlRequest{}, err
	}
	return OpenSourceControlRequest(raw[:])
}

func WriteSourceControlRequest(writer io.Writer, request SourceControlRequest) error {
	var raw [SourceControlRequestBytes]byte
	encoded, err := AppendSourceControlRequest(raw[:0], request)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func AppendSourceControlResponse(dst []byte, record SourceControlRecord) ([]byte, error) {
	if !validSourceControlRecord(record) ||
		(record.State != SourceControlComplete && record.State != SourceControlReleased) ||
		len(dst) > math.MaxInt-SourceControlResponseBytes {
		return dst, ErrSourceControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, SourceControlResponseBytes)...)
	b := dst[start:]
	copy(b[:8], sourceResponseMagic[:])
	b[8] = byte(record.State)
	binary.BigEndian.PutUint64(b[16:24], record.Revision)
	copy(b[24:56], record.Request.Operation[:])
	copy(b[56:88], record.Request.Step[:])
	encoded, err := AppendDescriptor(b[88:88], record.Descriptor)
	if err != nil || len(encoded) != DescriptorBytes {
		return dst[:start], errors.Join(ErrSourceControl, err)
	}
	copy(b[88:], encoded)
	copy(b[320:336], record.Request.SourceNode[:])
	return dst, nil
}

func OpenSourceControlResponse(raw []byte) (SourceControlRecord, error) {
	if len(raw) != SourceControlResponseBytes || !bytes.Equal(raw[:8], sourceResponseMagic[:]) ||
		(raw[8] != byte(SourceControlComplete) && raw[8] != byte(SourceControlReleased)) ||
		!allZero(raw[9:16]) {
		return SourceControlRecord{}, ErrSourceControl
	}
	var record SourceControlRecord
	record.State = SourceControlState(raw[8])
	record.Revision = binary.BigEndian.Uint64(raw[16:24])
	copy(record.Request.Operation[:], raw[24:56])
	copy(record.Request.Step[:], raw[56:88])
	descriptor, err := OpenDescriptor(raw[88:320])
	if err != nil {
		return SourceControlRecord{}, errors.Join(ErrSourceControl, err)
	}
	record.Descriptor = descriptor
	record.Request.Group = descriptor.Group
	record.Request.SourceMember = descriptor.SourceMember
	record.Request.TargetMember = descriptor.TargetMember
	record.Request.TargetStore = descriptor.TargetStore
	record.Request.TargetIncarnation = descriptor.TargetIncarnation
	record.Request.ReplicaSetVersion = descriptor.ReplicaSetVersion
	copy(record.Request.SourceNode[:], raw[320:336])
	if !validSourceControlRecord(record) {
		return SourceControlRecord{}, ErrSourceControl
	}
	return record, nil
}

func ReadSourceControlResponse(reader io.Reader) (SourceControlRecord, error) {
	var raw [SourceControlResponseBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return SourceControlRecord{}, err
	}
	return OpenSourceControlResponse(raw[:])
}

func WriteSourceControlResponse(writer io.Writer, record SourceControlRecord) error {
	var raw [SourceControlResponseBytes]byte
	encoded, err := AppendSourceControlResponse(raw[:0], record)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}
