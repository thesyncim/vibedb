package snapshottransfer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	ErrBootstrapControl        = errors.New("snapshottransfer: invalid bootstrap control request")
	ErrBootstrapUnauthorized   = errors.New("snapshottransfer: bootstrap control is not authorized")
	ErrBootstrapMissing        = errors.New("snapshottransfer: bootstrap operation is missing")
	ErrBootstrapConflict       = errors.New("snapshottransfer: bootstrap operation conflicts")
	ErrBootstrapOutcomeUnknown = errors.New("snapshottransfer: bootstrap outcome is unknown")
)

const (
	BootstrapRequestBytes           = 8 + 32 + 32 + DescriptorBytes
	bootstrapResponseBase           = 468
	MaxBootstrapResponseBytes       = bootstrapResponseBase + 2*replication.MaxIdentityBytes
	AbsoluteMaxBootstrapConcurrency = 256
)

var (
	bootstrapRequestMagic  = [8]byte{'V', 'B', 'B', 'O', 'O', 'T', 0, 0}
	bootstrapResponseMagic = [8]byte{'V', 'B', 'B', 'R', 'E', 'S', 0, 0}
)

// BootstrapRequestDiscriminator identifies this fixed grammar when the cold
// learner shares its authenticated control listener with grant installation.
func BootstrapRequestDiscriminator() [8]byte { return bootstrapRequestMagic }

type BootstrapState uint8

const (
	BootstrapRunning BootstrapState = iota + 1
	BootstrapComplete
)

// BootstrapRequest is one immutable controller command. Operation identifies
// the durable move and Step identifies its exact reconciled action.
type BootstrapRequest struct {
	Operation  [32]byte
	Step       [32]byte
	Descriptor Descriptor
}

type BootstrapRecord struct {
	Request  BootstrapRequest
	Revision uint64
	State    BootstrapState
	Identity raftmember.RuntimeIdentity
}

// BootstrapJournal must make Publish durable before returning nil. A CAS
// mismatch returns ErrBootstrapConflict; an absent Read returns
// ErrBootstrapMissing. Implementations may return an outcome-unknown error:
// the service always re-reads and accepts only the exact desired record.
type BootstrapJournal interface {
	ReadBootstrap(context.Context, [32]byte) (BootstrapRecord, error)
	PublishBootstrap(context.Context, uint64, BootstrapRecord) error
}

type BootstrapReceiver interface {
	Receive(context.Context, rafttransport.NodeID, Descriptor) error
}

// BootstrapInstaller wraps InstallPublishedLearner and its post-crash
// observation. ObserveInstalled is mandatory so a crash after Host.Add but
// before terminal journal publication cannot cause a second unsafe install.
type BootstrapInstaller interface {
	ObserveInstalled(context.Context, Descriptor) (raftmember.RuntimeIdentity, bool, error)
	InstallPublishedLearner(context.Context, Descriptor) (raftmember.RuntimeIdentity, error)
}

type BootstrapAuthorizeFunc func(rafttransport.PeerIdentity, BootstrapRequest) bool
type BootstrapSourceNodeFunc func(Descriptor) (rafttransport.NodeID, bool)

type BootstrapControlOptions struct {
	Journal       BootstrapJournal
	Receiver      BootstrapReceiver
	Installer     BootstrapInstaller
	Authorize     BootstrapAuthorizeFunc
	SourceNode    BootstrapSourceNodeFunc
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

// BootstrapControlService is a pre-Raft capability: it owns no Host and grants
// no serving authority. Fixed stripes serialize exact-operation retries while
// a bounded semaphore limits independent bootstrap work.
type BootstrapControlService struct {
	journal       BootstrapJournal
	receiver      BootstrapReceiver
	installer     BootstrapInstaller
	authorize     BootstrapAuthorizeFunc
	sourceNode    BootstrapSourceNodeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	stripes       []sync.Mutex
}

func NewBootstrapControlService(options BootstrapControlOptions) (*BootstrapControlService, error) {
	if options.Journal == nil || options.Receiver == nil || options.Installer == nil ||
		options.Authorize == nil || options.SourceNode == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > AbsoluteMaxBootstrapConcurrency {
		return nil, ErrBootstrapControl
	}
	return &BootstrapControlService{
		journal: options.Journal, receiver: options.Receiver, installer: options.Installer,
		authorize: options.Authorize, sourceNode: options.SourceNode,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots:   make(chan struct{}, options.MaxConcurrent),
		stripes: make([]sync.Mutex, options.MaxConcurrent),
	}, nil
}

// Serve handles exactly one fixed-width command on one already-authenticated
// shard-control stream and returns one bounded terminal observation.
func (service *BootstrapControlService) Serve(
	ctx context.Context,
	connection rafttransport.PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		return ErrBootstrapUnauthorized
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := service.readDeadline(); deadline.IsZero() {
		return ErrBootstrapControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	request, err := ReadBootstrapRequest(connection)
	if err != nil {
		return err
	}
	identity := connection.PeerIdentity()
	if !service.authorize(identity, request) {
		return ErrBootstrapUnauthorized
	}
	record, err := service.Execute(ctx, request)
	if err != nil {
		return err
	}
	if deadline := service.writeDeadline(); deadline.IsZero() {
		return ErrBootstrapControl
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteBootstrapResponse(connection, record)
}

// Execute durably records exact intent before network or install work. Every
// retry either resumes the same descriptor or returns the already-settled
// terminal identity.
func (service *BootstrapControlService) Execute(
	ctx context.Context,
	request BootstrapRequest,
) (BootstrapRecord, error) {
	if service == nil || ctx == nil || !validBootstrapRequest(request) {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return BootstrapRecord{}, cause
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return BootstrapRecord{}, ErrBound
	}
	stripe := &service.stripes[binary.LittleEndian.Uint64(request.Operation[:8])%uint64(len(service.stripes))]
	stripe.Lock()
	defer stripe.Unlock()

	record, err := service.loadOrCreate(ctx, request)
	if err != nil || record.State == BootstrapComplete {
		return record, err
	}
	installed, found, err := service.installer.ObserveInstalled(ctx, request.Descriptor)
	if err != nil {
		return BootstrapRecord{}, err
	}
	if !found {
		source, ok := service.sourceNode(request.Descriptor)
		if !ok || source == (rafttransport.NodeID{}) {
			return BootstrapRecord{}, ErrBootstrapUnauthorized
		}
		if err = service.receiver.Receive(ctx, source, request.Descriptor); err != nil {
			return BootstrapRecord{}, err
		}
		installed, err = service.installer.InstallPublishedLearner(ctx, request.Descriptor)
		if err != nil {
			observed, observedFound, observeErr := service.installer.ObserveInstalled(ctx, request.Descriptor)
			if observeErr != nil || !observedFound {
				return BootstrapRecord{}, errors.Join(err, observeErr)
			}
			installed = observed
		}
	}
	if !runtimeMatchesDescriptor(installed, request.Descriptor) {
		return BootstrapRecord{}, ErrBootstrapConflict
	}
	terminal := record
	terminal.Revision++
	terminal.State = BootstrapComplete
	terminal.Identity = installed
	if err = service.publishExact(ctx, record.Revision, terminal); err != nil {
		return BootstrapRecord{}, err
	}
	return terminal, nil
}

func (service *BootstrapControlService) Observe(
	ctx context.Context,
	operation [32]byte,
) (BootstrapRecord, error) {
	if service == nil || ctx == nil || operation == ([32]byte{}) {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	record, err := service.journal.ReadBootstrap(ctx, operation)
	if err != nil {
		return BootstrapRecord{}, err
	}
	if !validBootstrapRecord(record) || record.Request.Operation != operation {
		return BootstrapRecord{}, ErrBootstrapConflict
	}
	return record, nil
}

func (service *BootstrapControlService) loadOrCreate(
	ctx context.Context,
	request BootstrapRequest,
) (BootstrapRecord, error) {
	record, err := service.journal.ReadBootstrap(ctx, request.Operation)
	if errors.Is(err, ErrBootstrapMissing) {
		record = BootstrapRecord{Request: request, Revision: 1, State: BootstrapRunning}
		if err = service.publishExact(ctx, 0, record); err != nil {
			return BootstrapRecord{}, err
		}
		return record, nil
	}
	if err != nil {
		return BootstrapRecord{}, err
	}
	if !validBootstrapRecord(record) || record.Request != request {
		return BootstrapRecord{}, ErrBootstrapConflict
	}
	return record, nil
}

func (service *BootstrapControlService) publishExact(
	ctx context.Context,
	expected uint64,
	desired BootstrapRecord,
) error {
	err := service.journal.PublishBootstrap(ctx, expected, desired)
	if err == nil {
		return nil
	}
	settled, readErr := service.journal.ReadBootstrap(ctx, desired.Request.Operation)
	if readErr == nil && equalBootstrapRecord(settled, desired) {
		return nil
	}
	return errors.Join(ErrBootstrapOutcomeUnknown, err, readErr)
}

func validBootstrapRequest(request BootstrapRequest) bool {
	return request.Operation != ([32]byte{}) && request.Step != ([32]byte{}) &&
		request.Descriptor.Valid()
}

func validBootstrapRecord(record BootstrapRecord) bool {
	if !validBootstrapRequest(record.Request) || record.Revision == 0 ||
		record.State < BootstrapRunning || record.State > BootstrapComplete {
		return false
	}
	if record.State == BootstrapRunning {
		return record.Identity == (raftmember.RuntimeIdentity{})
	}
	return runtimeMatchesDescriptor(record.Identity, record.Request.Descriptor)
}

func equalBootstrapRecord(left, right BootstrapRecord) bool {
	return left == right
}

func runtimeMatchesDescriptor(identity raftmember.RuntimeIdentity, descriptor Descriptor) bool {
	return runtimeIdentityValid(identity) && identity.Group == descriptor.Group &&
		identity.MemberID == descriptor.TargetMember && identity.StoreID == descriptor.TargetStore &&
		identity.NodeIncarnation == descriptor.TargetIncarnation
}

// AppendBootstrapRequest appends the sole canonical request grammar.
func AppendBootstrapRequest(dst []byte, request BootstrapRequest) ([]byte, error) {
	if !validBootstrapRequest(request) || len(dst) > math.MaxInt-BootstrapRequestBytes {
		return dst, ErrBootstrapControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, BootstrapRequestBytes)...)
	copy(dst[start:start+8], bootstrapRequestMagic[:])
	copy(dst[start+8:start+40], request.Operation[:])
	copy(dst[start+40:start+72], request.Step[:])
	encoded, err := AppendDescriptor(dst[start+72:start+72], request.Descriptor)
	if err != nil || len(encoded) != DescriptorBytes {
		return dst[:start], errors.Join(ErrBootstrapControl, err)
	}
	copy(dst[start+72:], encoded)
	return dst, nil
}

func OpenBootstrapRequest(raw []byte) (BootstrapRequest, error) {
	if len(raw) != BootstrapRequestBytes || !bytes.Equal(raw[:8], bootstrapRequestMagic[:]) {
		return BootstrapRequest{}, ErrBootstrapControl
	}
	var request BootstrapRequest
	copy(request.Operation[:], raw[8:40])
	copy(request.Step[:], raw[40:72])
	descriptor, err := OpenDescriptor(raw[72:])
	request.Descriptor = descriptor
	if err != nil || !validBootstrapRequest(request) {
		return BootstrapRequest{}, errors.Join(ErrBootstrapControl, err)
	}
	return request, nil
}

func ReadBootstrapRequest(reader io.Reader) (BootstrapRequest, error) {
	var raw [BootstrapRequestBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return BootstrapRequest{}, err
	}
	return OpenBootstrapRequest(raw[:])
}

func WriteBootstrapRequest(writer io.Writer, request BootstrapRequest) error {
	var raw [BootstrapRequestBytes]byte
	encoded, err := AppendBootstrapRequest(raw[:0], request)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func AppendBootstrapResponse(dst []byte, record BootstrapRecord) ([]byte, error) {
	if !validBootstrapRecord(record) || record.State != BootstrapComplete {
		return dst, ErrBootstrapControl
	}
	identity := record.Identity
	length := bootstrapResponseBase + len(identity.Distribution) + len(identity.Shard)
	if len(dst) > math.MaxInt-length {
		return dst, ErrBound
	}
	start := len(dst)
	dst = append(dst, make([]byte, length)...)
	b := dst[start:]
	copy(b[:8], bootstrapResponseMagic[:])
	b[8] = byte(record.State)
	binary.BigEndian.PutUint64(b[16:24], record.Revision)
	copy(b[24:56], record.Request.Operation[:])
	copy(b[56:88], record.Request.Step[:])
	encodedDescriptor, err := AppendDescriptor(b[88:88], record.Request.Descriptor)
	if err != nil || len(encodedDescriptor) != DescriptorBytes {
		return dst[:start], errors.Join(ErrBootstrapControl, err)
	}
	copy(b[88:320], encodedDescriptor)
	appendRuntimeIdentity(b[320:464], identity)
	binary.BigEndian.PutUint16(b[464:466], uint16(len(identity.Distribution)))
	binary.BigEndian.PutUint16(b[466:468], uint16(len(identity.Shard)))
	copy(b[bootstrapResponseBase:], identity.Distribution)
	copy(b[bootstrapResponseBase+len(identity.Distribution):], identity.Shard)
	return dst, nil
}

func OpenBootstrapResponse(raw []byte) (BootstrapRecord, error) {
	if len(raw) < bootstrapResponseBase || len(raw) > MaxBootstrapResponseBytes ||
		!bytes.Equal(raw[:8], bootstrapResponseMagic[:]) || raw[8] != byte(BootstrapComplete) ||
		!allZero(raw[9:16]) {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	distributionBytes := int(binary.BigEndian.Uint16(raw[464:466]))
	shardBytes := int(binary.BigEndian.Uint16(raw[466:468]))
	if distributionBytes == 0 || shardBytes == 0 ||
		distributionBytes > replication.MaxIdentityBytes || shardBytes > replication.MaxIdentityBytes ||
		len(raw) != bootstrapResponseBase+distributionBytes+shardBytes {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	var record BootstrapRecord
	record.State = BootstrapComplete
	record.Revision = binary.BigEndian.Uint64(raw[16:24])
	copy(record.Request.Operation[:], raw[24:56])
	copy(record.Request.Step[:], raw[56:88])
	descriptor, err := OpenDescriptor(raw[88:320])
	if err != nil {
		return BootstrapRecord{}, errors.Join(ErrBootstrapControl, err)
	}
	record.Request.Descriptor = descriptor
	record.Identity = openRuntimeIdentity(raw[320:464])
	record.Identity.Distribution = string(raw[bootstrapResponseBase : bootstrapResponseBase+distributionBytes])
	record.Identity.Shard = string(raw[bootstrapResponseBase+distributionBytes:])
	if !validBootstrapRecord(record) {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	return record, nil
}

func ReadBootstrapResponse(reader io.Reader) (BootstrapRecord, error) {
	var prefix [bootstrapResponseBase]byte
	if _, err := io.ReadFull(reader, prefix[:]); err != nil {
		return BootstrapRecord{}, err
	}
	distributionBytes := int(binary.BigEndian.Uint16(prefix[464:466]))
	shardBytes := int(binary.BigEndian.Uint16(prefix[466:468]))
	if distributionBytes > replication.MaxIdentityBytes || shardBytes > replication.MaxIdentityBytes {
		return BootstrapRecord{}, ErrBootstrapControl
	}
	raw := make([]byte, bootstrapResponseBase+distributionBytes+shardBytes)
	copy(raw, prefix[:])
	if _, err := io.ReadFull(reader, raw[bootstrapResponseBase:]); err != nil {
		return BootstrapRecord{}, err
	}
	return OpenBootstrapResponse(raw)
}

func WriteBootstrapResponse(writer io.Writer, record BootstrapRecord) error {
	var raw [MaxBootstrapResponseBytes]byte
	encoded, err := AppendBootstrapResponse(raw[:0], record)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func appendRuntimeIdentity(dst []byte, identity raftmember.RuntimeIdentity) {
	copy(dst[0:16], identity.Group.ClusterID[:])
	copy(dst[16:32], identity.Group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(dst[32:40], identity.Group.TopologyRecoveryEpoch)
	copy(dst[40:56], identity.Group.ShardIncarnation[:])
	copy(dst[56:72], identity.Group.GroupID[:])
	binary.BigEndian.PutUint64(dst[72:80], identity.AllocationGeneration)
	binary.BigEndian.PutUint64(dst[80:88], identity.MemberID)
	copy(dst[88:104], identity.StoreID[:])
	binary.BigEndian.PutUint64(dst[104:112], identity.NodeIncarnation)
	copy(dst[112:144], identity.RelationManifestDigest[:])
}

func openRuntimeIdentity(src []byte) (identity raftmember.RuntimeIdentity) {
	copy(identity.Group.ClusterID[:], src[0:16])
	copy(identity.Group.ClusterIncarnation[:], src[16:32])
	identity.Group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(src[32:40])
	copy(identity.Group.ShardIncarnation[:], src[40:56])
	copy(identity.Group.GroupID[:], src[56:72])
	identity.AllocationGeneration = binary.BigEndian.Uint64(src[72:80])
	identity.MemberID = binary.BigEndian.Uint64(src[80:88])
	copy(identity.StoreID[:], src[88:104])
	identity.NodeIncarnation = binary.BigEndian.Uint64(src[104:112])
	copy(identity.RelationManifestDigest[:], src[112:144])
	return identity
}

func runtimeIdentityValid(identity raftmember.RuntimeIdentity) bool {
	return identity.Group != (raftmember.GroupKey{}) && identity.AllocationGeneration != 0 &&
		identity.MemberID != 0 && identity.StoreID != ([16]byte{}) && identity.NodeIncarnation != 0 &&
		identity.RelationManifestDigest != ([32]byte{}) && identity.Distribution != "" && identity.Shard != "" &&
		len(identity.Distribution) <= replication.MaxIdentityBytes && len(identity.Shard) <= replication.MaxIdentityBytes &&
		utf8.ValidString(identity.Distribution) && utf8.ValidString(identity.Shard) &&
		!strings.ContainsRune(identity.Distribution, 0) && !strings.ContainsRune(identity.Shard, 0)
}

func allZero(src []byte) bool {
	for _, value := range src {
		if value != 0 {
			return false
		}
	}
	return true
}
