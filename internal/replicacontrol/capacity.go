package replicacontrol

// This file is the capacity/demand half of replica control.  It deliberately
// has a separate discriminator and wire grammar from the Raft observation
// protocol: capacity is a bounded accounting cut and must never accidentally
// be accepted as a membership/serving observation.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	// ErrCapacityUnavailable means that the local runtime cannot produce a
	// measured cut.  Callers must block placement until a later cold-budgeted
	// observation succeeds; they must not turn this into zero demand.
	ErrCapacityUnavailable = errors.New("replicacontrol: capacity evidence unavailable")
	// ErrCapacityStale means that a source cut no longer matches the requested
	// identity, generation, applied floor, or freshness floor.
	ErrCapacityStale = errors.New("replicacontrol: capacity evidence is stale")
)

const (
	// CapacityRequestBytes and CapacityObservationBytes are fixed so an
	// authenticated capacity round has no attacker-controlled allocation before
	// its bounds and identity have been checked.
	CapacityRequestBytes         = 224
	CapacityObservationBytes     = 568
	capacityPayloadBytes         = CapacityObservationBytes - 32
	AbsoluteMaxCapacityObservers = 256
)

// CapacityDemandKind says whether Demand is a measured resident cut or a
// conservative, physically enforced upper bound. A conservative bound is
// safe for placement because the planner reserves the entire value; callers
// must never present it as an observed size.
type CapacityDemandKind uint8

const (
	CapacityDemandMeasured CapacityDemandKind = iota + 1
	CapacityDemandConservative
)

var (
	capacityRequestMagic  = [8]byte{'V', 'B', 'C', 'A', 'P', 'R', 0, 1}
	capacityResponseMagic = [8]byte{'V', 'B', 'C', 'A', 'P', 'S', 0, 1}
)

// CapacityRequest asks one exact local member for a detached storage cut.
// ExpectedCatalogGeneration is supplied by the controller's immutable route
// snapshot.  The shard does not invent a catalog generation locally; it
// authenticates the request and echoes the exact fence into the reply.
// MinimumApplied and MinimumSourceRevision make retries monotonic without
// requiring an in-flight request to predict the next apply index exactly.
type CapacityRequest struct {
	// Round binds every group reply on one node to the same bounded snapshot.
	// Zero requests a fresh standalone observation.
	Round                     [32]byte
	Operation                 [32]byte
	Step                      [32]byte
	Group                     raftmember.GroupKey
	TargetMember              uint64
	ExpectedCatalogGeneration uint64
	MinimumApplied            uint64
	MinimumSourceRevision     uint64
}

// NodeCapacity is the node-wide actual capacity cut returned alongside every
// group observation.  Capacity/Used are storage accounting vectors; only
// ResourceLiveBytes is required by the scaling planner, while the remaining
// dimensions are retained for future resource admission.  Revision and
// Incarnation fence a node report against ABA after restart or re-registration.
type NodeCapacity struct {
	NodeID            rafttransport.NodeID
	NodeIncarnation   uint64
	Revision          uint64
	Capacity          autosplit.CapacityVector
	Used              autosplit.CapacityVector
	MigrationCapacity uint64
	MigrationUsed     uint64
	MaxReceives       uint32
	ActiveReceives    uint32
}

// CapacityObservation is a complete, detached measurement cut.  Demand is
// the resident physical reservation of the requested replica and
// MigrationBytes is a conservative transfer bound for making a simultaneous
// source+target copy.  KnownEmpty is authenticated zero-size evidence; a
// missing cut is never represented by a zero value.
type CapacityObservation struct {
	Request           CapacityRequest
	Identity          raftmember.RuntimeIdentity
	CatalogGeneration uint64
	Applied           uint64
	SourceRevision    uint64
	Demand            autosplit.CapacityVector
	MigrationBytes    uint64
	KnownEmpty        bool
	DemandKind        CapacityDemandKind
	Node              NodeCapacity
	ObservationDigest [32]byte
}

// CapacityObserver is implemented by the live and cold RF3 providers.  The
// callback must do bounded metadata/counter work and honor ctx cancellation;
// it must not scan user rows on a foreground request.
type CapacityObserver interface {
	ObserveReplicaCapacity(context.Context, CapacityRequest) (CapacityObservation, error)
}

// CapacityAuthorizeFunc is evaluated after mutual TLS identifies the peer and
// before the potentially expensive local measurement starts.
type CapacityAuthorizeFunc func(rafttransport.PeerIdentity, CapacityRequest) bool

// CapacityServiceOptions configures one authenticated capacity endpoint.
type CapacityServiceOptions struct {
	Observer      CapacityObserver
	Authorize     CapacityAuthorizeFunc
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

// CapacityService serves one bounded capacity request per connection.
type CapacityService struct {
	observer      CapacityObserver
	authorize     CapacityAuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	stripes       []sync.Mutex
}

// CapacityRequestDiscriminator identifies the capacity grammar on the shared
// authenticated shard-control listener.
func CapacityRequestDiscriminator() [8]byte { return capacityRequestMagic }

// NewCapacityService constructs the authenticated capacity endpoint.
func NewCapacityService(options CapacityServiceOptions) (*CapacityService, error) {
	if options.Observer == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > AbsoluteMaxCapacityObservers {
		return nil, ErrControl
	}
	return &CapacityService{
		observer: options.Observer, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots:   make(chan struct{}, options.MaxConcurrent),
		stripes: make([]sync.Mutex, options.MaxConcurrent),
	}, nil
}

// Serve reads one request and writes one complete capacity cut.  The service
// is intentionally one-shot so a controller cannot retain an idle stream while
// local storage accounting is being updated.
func (service *CapacityService) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrUnauthorized
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := boundedDeadline(ctx, service.readDeadline()); deadline.IsZero() {
		return ErrControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	request, err := ReadCapacityRequest(connection)
	if err != nil {
		return err
	}
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	if peer.TrustDomain != wantDomain || !service.authorize(peer, request) {
		return ErrUnauthorized
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrBound
	}
	stripe := &service.stripes[binary.BigEndian.Uint64(request.Operation[:8])%uint64(len(service.stripes))]
	stripe.Lock()
	cut, observeErr := service.observer.ObserveReplicaCapacity(ctx, request)
	stripe.Unlock()
	if observeErr != nil {
		return observeErr
	}
	if cut.Request != request || !validCapacityObservation(cut) {
		return ErrCapacityStale
	}
	if deadline := boundedDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
		return ErrControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteCapacityObservation(connection, cut)
}

// CapacityClient is the authenticated one-shot client for capacity rounds.
type CapacityClient struct {
	opener        StreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

// NewCapacityClient constructs a capacity client sharing the shard-control
// transport opener used by replica control.
func NewCapacityClient(options ClientOptions) (*CapacityClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrControl
	}
	return &CapacityClient{opener: options.Opener, readDeadline: options.ReadDeadline,
		writeDeadline: options.WriteDeadline}, nil
}

// Observe performs one read-only capacity attempt.  The exact request can be
// replayed after a transport failure because the provider has no mutation
// authority.
func (client *CapacityClient) Observe(ctx context.Context, node rafttransport.NodeID, request CapacityRequest) (CapacityObservation, error) {
	if client == nil || ctx == nil || node == (rafttransport.NodeID{}) || !validCapacityRequest(request) {
		return CapacityObservation{}, ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return CapacityObservation{}, cause
	}
	connection, err := client.opener.OpenShardControl(ctx, node)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return CapacityObservation{}, err
	}
	if connection == nil {
		return CapacityObservation{}, ErrControl
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID,
		ClusterIncarnation: request.Group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl || peer.TrustDomain != wantDomain {
		return CapacityObservation{}, ErrUnauthorized
	}
	if deadline := boundedDeadline(ctx, client.writeDeadline()); deadline.IsZero() {
		return CapacityObservation{}, ErrControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return CapacityObservation{}, err
	}
	if err = WriteCapacityRequest(connection, request); err != nil {
		return CapacityObservation{}, err
	}
	if deadline := boundedDeadline(ctx, client.readDeadline()); deadline.IsZero() {
		return CapacityObservation{}, ErrControl
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return CapacityObservation{}, err
	}
	observation, err := ReadCapacityObservation(connection)
	if err != nil {
		return CapacityObservation{}, err
	}
	if observation.Request != request || !validCapacityObservation(observation) {
		return CapacityObservation{}, errors.Join(ErrCapacityStale, ErrControl)
	}
	return observation, nil
}

func AppendCapacityRequest(dst []byte, request CapacityRequest) ([]byte, error) {
	if !validCapacityRequest(request) || len(dst) > math.MaxInt-CapacityRequestBytes {
		return dst, ErrControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, CapacityRequestBytes)...)
	b := dst[start:]
	copy(b[:8], capacityRequestMagic[:])
	copy(b[8:40], request.Operation[:])
	copy(b[40:72], request.Step[:])
	appendGroup(b[72:160], request.Group)
	binary.BigEndian.PutUint64(b[160:168], request.TargetMember)
	binary.BigEndian.PutUint64(b[168:176], request.ExpectedCatalogGeneration)
	binary.BigEndian.PutUint64(b[176:184], request.MinimumApplied)
	binary.BigEndian.PutUint64(b[184:192], request.MinimumSourceRevision)
	copy(b[192:224], request.Round[:])
	return dst, nil
}

func OpenCapacityRequest(raw []byte) (CapacityRequest, error) {
	if len(raw) != CapacityRequestBytes || !bytes.Equal(raw[:8], capacityRequestMagic[:]) {
		return CapacityRequest{}, ErrControl
	}
	var request CapacityRequest
	copy(request.Operation[:], raw[8:40])
	copy(request.Step[:], raw[40:72])
	request.Group = openGroup(raw[72:160])
	request.TargetMember = binary.BigEndian.Uint64(raw[160:168])
	request.ExpectedCatalogGeneration = binary.BigEndian.Uint64(raw[168:176])
	request.MinimumApplied = binary.BigEndian.Uint64(raw[176:184])
	request.MinimumSourceRevision = binary.BigEndian.Uint64(raw[184:192])
	copy(request.Round[:], raw[192:224])
	if !validCapacityRequest(request) {
		return CapacityRequest{}, ErrControl
	}
	return request, nil
}

func ReadCapacityRequest(reader io.Reader) (CapacityRequest, error) {
	var raw [CapacityRequestBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return CapacityRequest{}, err
	}
	return OpenCapacityRequest(raw[:])
}

func WriteCapacityRequest(writer io.Writer, request CapacityRequest) error {
	var raw [CapacityRequestBytes]byte
	encoded, err := AppendCapacityRequest(raw[:0], request)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func AppendCapacityObservation(dst []byte, observation CapacityObservation) ([]byte, error) {
	if !validCapacityObservation(observation) || len(dst) > math.MaxInt-CapacityObservationBytes {
		return dst, ErrControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, CapacityObservationBytes)...)
	payload := dst[start : start+capacityPayloadBytes]
	encodeCapacityObservationPayload(payload, observation)
	digest := sha256.Sum256(payload)
	if observation.ObservationDigest != digest {
		return dst[:start], ErrCapacityStale
	}
	copy(dst[start+capacityPayloadBytes:start+CapacityObservationBytes], digest[:])
	return dst, nil
}

func OpenCapacityObservation(raw []byte) (CapacityObservation, error) {
	if len(raw) != CapacityObservationBytes || !bytes.Equal(raw[:8], capacityResponseMagic[:]) ||
		raw[8] != 1 || raw[9] > 1 || !allZero(raw[10:16]) || !allZero(raw[497:504]) {
		return CapacityObservation{}, ErrControl
	}
	var observation CapacityObservation
	decodeCapacityObservationPayload(raw[:capacityPayloadBytes], &observation)
	copy(observation.ObservationDigest[:], raw[capacityPayloadBytes:])
	if !validCapacityObservation(observation) {
		return CapacityObservation{}, ErrCapacityStale
	}
	if sha256.Sum256(raw[:capacityPayloadBytes]) != observation.ObservationDigest {
		return CapacityObservation{}, ErrCapacityStale
	}
	return observation, nil
}

func ReadCapacityObservation(reader io.Reader) (CapacityObservation, error) {
	var raw [CapacityObservationBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return CapacityObservation{}, err
	}
	return OpenCapacityObservation(raw[:])
}

func WriteCapacityObservation(writer io.Writer, observation CapacityObservation) error {
	var raw [CapacityObservationBytes]byte
	encoded, err := AppendCapacityObservation(raw[:0], observation)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func encodeCapacityObservationPayload(b []byte, observation CapacityObservation) {
	clear(b)
	copy(b[:8], capacityResponseMagic[:])
	b[8] = 1
	if observation.KnownEmpty {
		b[9] = 1
	}
	copy(b[16:48], observation.Request.Operation[:])
	copy(b[48:80], observation.Request.Step[:])
	appendGroup(b[80:168], observation.Request.Group)
	binary.BigEndian.PutUint64(b[168:176], observation.Request.TargetMember)
	binary.BigEndian.PutUint64(b[176:184], observation.CatalogGeneration)
	binary.BigEndian.PutUint64(b[184:192], observation.Applied)
	binary.BigEndian.PutUint64(b[192:200], observation.SourceRevision)
	binary.BigEndian.PutUint64(b[200:208], observation.Request.ExpectedCatalogGeneration)
	binary.BigEndian.PutUint64(b[208:216], observation.Request.MinimumApplied)
	binary.BigEndian.PutUint64(b[216:224], observation.Request.MinimumSourceRevision)
	binary.BigEndian.PutUint64(b[224:232], observation.Identity.AllocationGeneration)
	binary.BigEndian.PutUint64(b[232:240], observation.Identity.MemberID)
	copy(b[240:256], observation.Identity.StoreID[:])
	binary.BigEndian.PutUint64(b[256:264], observation.Identity.NodeIncarnation)
	copy(b[264:280], observation.Node.NodeID[:])
	binary.BigEndian.PutUint64(b[280:288], observation.Node.NodeIncarnation)
	binary.BigEndian.PutUint64(b[288:296], observation.Node.Revision)
	putVector(b[296:352], observation.Demand)
	binary.BigEndian.PutUint64(b[352:360], observation.MigrationBytes)
	putVector(b[360:416], observation.Node.Capacity)
	putVector(b[416:472], observation.Node.Used)
	binary.BigEndian.PutUint64(b[472:480], observation.Node.MigrationCapacity)
	binary.BigEndian.PutUint64(b[480:488], observation.Node.MigrationUsed)
	binary.BigEndian.PutUint32(b[488:492], observation.Node.MaxReceives)
	binary.BigEndian.PutUint32(b[492:496], observation.Node.ActiveReceives)
	b[496] = byte(observation.DemandKind)
	// b[497:504] is canonical zero padding.
	copy(b[504:536], observation.Request.Round[:])
}

func decodeCapacityObservationPayload(b []byte, observation *CapacityObservation) {
	observation.Request.Operation = [32]byte{}
	copy(observation.Request.Operation[:], b[16:48])
	copy(observation.Request.Step[:], b[48:80])
	observation.Request.Group = openGroup(b[80:168])
	observation.Request.TargetMember = binary.BigEndian.Uint64(b[168:176])
	observation.Request.ExpectedCatalogGeneration = binary.BigEndian.Uint64(b[200:208])
	observation.Request.MinimumApplied = binary.BigEndian.Uint64(b[208:216])
	observation.Request.MinimumSourceRevision = binary.BigEndian.Uint64(b[216:224])
	copy(observation.Request.Round[:], b[504:536])
	observation.CatalogGeneration = binary.BigEndian.Uint64(b[176:184])
	observation.Applied = binary.BigEndian.Uint64(b[184:192])
	observation.SourceRevision = binary.BigEndian.Uint64(b[192:200])
	observation.Identity.Group = observation.Request.Group
	observation.Identity.AllocationGeneration = binary.BigEndian.Uint64(b[224:232])
	observation.Identity.MemberID = binary.BigEndian.Uint64(b[232:240])
	copy(observation.Identity.StoreID[:], b[240:256])
	observation.Identity.NodeIncarnation = binary.BigEndian.Uint64(b[256:264])
	copy(observation.Node.NodeID[:], b[264:280])
	observation.Node.NodeIncarnation = binary.BigEndian.Uint64(b[280:288])
	observation.Node.Revision = binary.BigEndian.Uint64(b[288:296])
	observation.Demand = getVector(b[296:352])
	observation.MigrationBytes = binary.BigEndian.Uint64(b[352:360])
	observation.Node.Capacity = getVector(b[360:416])
	observation.Node.Used = getVector(b[416:472])
	observation.Node.MigrationCapacity = binary.BigEndian.Uint64(b[472:480])
	observation.Node.MigrationUsed = binary.BigEndian.Uint64(b[480:488])
	observation.Node.MaxReceives = binary.BigEndian.Uint32(b[488:492])
	observation.Node.ActiveReceives = binary.BigEndian.Uint32(b[492:496])
	observation.DemandKind = CapacityDemandKind(b[496])
	observation.KnownEmpty = b[9] == 1
}

func putVector(dst []byte, vector autosplit.CapacityVector) {
	for index, value := range vector {
		binary.BigEndian.PutUint64(dst[index*8:index*8+8], value)
	}
}

func getVector(src []byte) (vector autosplit.CapacityVector) {
	for index := range vector {
		vector[index] = binary.BigEndian.Uint64(src[index*8 : index*8+8])
	}
	return vector
}

func validCapacityRequest(request CapacityRequest) bool {
	return request.Operation != ([32]byte{}) && request.Step != ([32]byte{}) &&
		validGroup(request.Group) && request.TargetMember != 0 &&
		request.ExpectedCatalogGeneration != 0
}

func validCapacityObservation(observation CapacityObservation) bool {
	request := observation.Request
	if !validCapacityRequest(request) || observation.CatalogGeneration != request.ExpectedCatalogGeneration ||
		observation.Applied == 0 || observation.SourceRevision == 0 ||
		observation.Applied < request.MinimumApplied || observation.SourceRevision < request.MinimumSourceRevision ||
		observation.Identity.Group != request.Group || observation.Identity.MemberID != request.TargetMember ||
		observation.Identity.AllocationGeneration == 0 || observation.Identity.StoreID == ([16]byte{}) ||
		observation.Identity.NodeIncarnation == 0 ||
		observation.Node.NodeID == (rafttransport.NodeID{}) || observation.Node.NodeIncarnation == 0 ||
		observation.Node.Revision == 0 || observation.Node.NodeIncarnation != observation.Identity.NodeIncarnation ||
		observation.Node.MaxReceives == 0 || observation.Node.ActiveReceives > observation.Node.MaxReceives ||
		(observation.DemandKind != CapacityDemandMeasured && observation.DemandKind != CapacityDemandConservative) ||
		observation.Node.Capacity[autosplit.ResourceLiveBytes] == 0 ||
		observation.Node.MigrationCapacity == 0 || observation.Node.MigrationUsed > observation.Node.MigrationCapacity {
		return false
	}
	for resource := range autosplit.ResourceCount {
		if observation.Node.Used[resource] > observation.Node.Capacity[resource] {
			return false
		}
	}
	if observation.KnownEmpty {
		if observation.DemandKind != CapacityDemandMeasured || observation.MigrationBytes != 0 || nonzeroCapacityVector(observation.Demand) {
			return false
		}
	} else if observation.Demand[autosplit.ResourceLiveBytes] == 0 || observation.MigrationBytes == 0 {
		return false
	}
	return true
}

func nonzeroCapacityVector(vector autosplit.CapacityVector) bool {
	for _, value := range vector {
		if value != 0 {
			return true
		}
	}
	return false
}

// CapacityObservationDigest returns the canonical measurement digest. Runtime
// providers should call it after filling every field except ObservationDigest.
func CapacityObservationDigest(observation CapacityObservation) [32]byte {
	var payload [capacityPayloadBytes]byte
	encodeCapacityObservationPayload(payload[:], observation)
	return sha256.Sum256(payload[:])
}

// NewCapacityObservation fills the canonical digest after validating the
// caller's measurement. It is useful for live and cold providers that share
// the wire package but own different storage handles.
func NewCapacityObservation(observation CapacityObservation) (CapacityObservation, error) {
	observation.ObservationDigest = CapacityObservationDigest(observation)
	if !validCapacityObservation(observation) {
		return CapacityObservation{}, ErrCapacityUnavailable
	}
	return observation, nil
}
