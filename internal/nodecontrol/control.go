// Package nodecontrol implements the authenticated, durable control plane used
// to prepare a replica on an empty physical node and to adopt it after the
// replicated membership grant is committed.
//
// The package deliberately has no placement or consensus authority.  A node
// receives an opaque request, re-reads the exact GroupEnrollmentIntent from a
// committed directory, and only then invokes its local durable preparation or
// runtime adoption callback.  The controller is therefore unable to create a
// serving member by presenting a signature or a stale copy of an intent.
package nodecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

var (
	ErrControl         = errors.New("nodecontrol: invalid control request")
	ErrUnauthorized    = errors.New("nodecontrol: request is not authorized")
	ErrMissing         = errors.New("nodecontrol: enrollment intent or journal record is missing")
	ErrConflict        = errors.New("nodecontrol: enrollment request conflicts with durable state")
	ErrStale           = errors.New("nodecontrol: enrollment intent is stale or not committed")
	ErrOutcomeUnknown  = errors.New("nodecontrol: durable outcome is unknown")
	ErrBound           = errors.New("nodecontrol: concurrency bound exceeded")
	ErrNotPrepared     = errors.New("nodecontrol: replica is not durably prepared")
	ErrNotCommitted    = errors.New("nodecontrol: membership is not committed")
	ErrInvalidProof    = errors.New("nodecontrol: prepared proof is invalid")
	ErrJournalCorrupt  = errors.New("nodecontrol: durable journal is corrupt")
	ErrUnsupportedPeer = errors.New("nodecontrol: unsupported peer connection")
)

const (
	// MaxPayloadBytes bounds a canonical preparation manifest.  It is kept
	// below the command's complete manifest limit so one control request never
	// consumes the whole node's retained request budget.
	MaxPayloadBytes = 4 << 20
	MaxProofBytes   = 128 << 10
	MaxJournalBytes = 256 << 10

	requestHeaderBytes = 212
	// magic, version/state/reserved, revision, intent ID, preparation digest,
	// adoption digest, proof length, and reserved bytes.
	responseHeaderBytes = 124
	maxOperationStripes = 256

	requestVersion  = 1
	responseVersion = 1
)

var (
	requestMagic  = [8]byte{'V', 'D', 'N', 'C', 'T', 'R', 'L', 0}
	responseMagic = [8]byte{'V', 'D', 'N', 'C', 'R', 'E', 'S', 0}
)

// Phase identifies the side effect requested from a physical node.
type Phase uint8

const (
	PhasePrepare Phase = iota + 1
	PhaseAdopt
)

func (phase Phase) valid() bool { return phase == PhasePrepare || phase == PhaseAdopt }

// Request is immutable for one control round trip. Payload is the canonical
// group preparation document. It is retained in the request rather than
// reconstructed from endpoint capacity; the node callback must validate its
// identities against the committed intent before creating any files.
type Request struct {
	Phase                 Phase
	IntentID              [32]byte
	IntentDigest          replication.Digest
	Group                 raftmember.GroupKey
	TargetMember          uint64
	TargetNode            rafttransport.NodeID
	TargetNodeIncarnation uint64
	TargetStoreID         [16]byte
	TargetNodeRevision    uint64
	Payload               []byte
}

// NewRequest derives every identity in a wire request from the authoritative
// intent. Callers should use it instead of filling the tuple by hand.
func NewRequest(phase Phase, intent gateway.GroupEnrollmentIntent, payload []byte) (Request, error) {
	if !intent.Valid() || !phase.valid() || len(payload) == 0 || len(payload) > MaxPayloadBytes {
		return Request{}, ErrControl
	}
	if sha256.Sum256(payload) != intent.ExpectedManifestDigest {
		return Request{}, ErrStale
	}
	return Request{
		Phase: phase, IntentID: intent.IntentID, IntentDigest: intent.Digest(), Group: intent.Group,
		TargetMember: intent.Target.Member, TargetNode: intent.Target.Node,
		TargetNodeIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID,
		TargetNodeRevision: intent.TargetNodeRevision, Payload: bytes.Clone(payload),
	}, nil
}

// PrepareVariant changes only the phase. It is used when an adopt retry has to
// reconstruct a local preparation record after a process restart.
func (request Request) PrepareVariant() Request {
	request.Phase = PhasePrepare
	request.Payload = bytes.Clone(request.Payload)
	return request
}

// State is the node-local durable saga. Preparing and Adopting are retained
// across crashes so an observer can resolve a side effect before retrying it.
type State uint8

const (
	StatePreparing State = iota + 1
	StatePrepared
	StateAdopting
	StateAdopted
)

func (state State) valid() bool { return state >= StatePreparing && state <= StateAdopted }

// Record is the bounded durable state held by a Journal. The preparation and
// adoption request digests are separate because adoption is authorized by a
// later committed intent state while referring to the same immutable target.
type Record struct {
	IntentID          [32]byte
	PreparationDigest [32]byte
	AdoptionDigest    [32]byte
	Revision          uint64
	State             State
	Proof             gateway.PreparedReplicaProof
}

func (record Record) valid() bool {
	if record.IntentID == ([32]byte{}) || record.Revision == 0 || !record.State.valid() ||
		record.PreparationDigest == ([32]byte{}) {
		return false
	}
	switch record.State {
	case StatePreparing:
		return record.AdoptionDigest == ([32]byte{}) && record.Proof == (gateway.PreparedReplicaProof{})
	case StatePrepared:
		return record.AdoptionDigest == ([32]byte{}) && record.Proof.Valid() && record.Proof.IntentID == record.IntentID
	case StateAdopting, StateAdopted:
		return record.AdoptionDigest != ([32]byte{}) && record.Proof.Valid() && record.Proof.IntentID == record.IntentID
	default:
		return false
	}
}

func validTransition(previous, next Record) bool {
	if !previous.valid() || !next.valid() || previous.IntentID != next.IntentID ||
		next.Revision != previous.Revision+1 || previous.PreparationDigest != next.PreparationDigest {
		return false
	}
	switch previous.State {
	case StatePreparing:
		return next.State == StatePrepared && next.AdoptionDigest == ([32]byte{})
	case StatePrepared:
		return next.State == StateAdopting && next.AdoptionDigest != ([32]byte{}) && next.Proof == previous.Proof
	case StateAdopting:
		return next.State == StateAdopted && next.AdoptionDigest == previous.AdoptionDigest && next.Proof == previous.Proof
	default:
		return false
	}
}

// IntentReader is the sole authority used by Service. Implementations must
// read from the committed replicated directory, not from a controller request
// cache or a process-local scheduler.
type IntentReader interface {
	ReadEnrollmentIntent(context.Context, [32]byte) (gateway.GroupEnrollmentIntent, error)
}

// IntentReaderSlot is the handoff point between the pre-group node process and
// the authenticated committed-directory reader. An empty physical node can
// bind its control listener before it has a gateway or group roster; the slot
// remains fail-closed until a seed/bootstrap reader has observed the node's
// committed Joining record. Swapping the reader is a local wiring operation,
// never an authority publication.
type IntentReaderSlot struct {
	mu     sync.RWMutex
	reader IntentReader
}

func (slot *IntentReaderSlot) Set(reader IntentReader) error {
	if slot == nil || reader == nil {
		return ErrControl
	}
	slot.mu.Lock()
	slot.reader = reader
	slot.mu.Unlock()
	return nil
}

func (slot *IntentReaderSlot) ReadEnrollmentIntent(
	ctx context.Context, intentID [32]byte,
) (gateway.GroupEnrollmentIntent, error) {
	if slot == nil {
		return gateway.GroupEnrollmentIntent{}, ErrNotCommitted
	}
	slot.mu.RLock()
	reader := slot.reader
	slot.mu.RUnlock()
	if reader == nil {
		return gateway.GroupEnrollmentIntent{}, ErrNotCommitted
	}
	return reader.ReadEnrollmentIntent(ctx, intentID)
}

// Journal makes each state transition durable before returning nil. A missing
// key must return ErrMissing; a compare-and-swap mismatch must return
// ErrConflict. Implementations may return an outcome-unknown error, after
// which Service re-reads and accepts only the exact desired record.
type Journal interface {
	Read(context.Context, [32]byte) (Record, error)
	Publish(context.Context, uint64, Record) error
}

// Preparer owns real storage side effects. Prepare must fsync all artifacts
// before returning a proof. ObservePrepared is required for crash recovery and
// must inspect the durable artifact rather than trusting a process flag.
type Preparer interface {
	Prepare(context.Context, gateway.GroupEnrollmentIntent, []byte) (gateway.PreparedReplicaProof, error)
	ObservePrepared(context.Context, gateway.GroupEnrollmentIntent, []byte) (gateway.PreparedReplicaProof, bool, error)
}

// Adopter owns the serialized runtime owner. Adopt must publish no serving
// route until the caller has already observed EnrollmentEnrolled in the
// committed directory. ObserveAdopted resolves a crash after adoption but
// before the local journal publication.
type Adopter interface {
	Adopt(context.Context, gateway.GroupEnrollmentIntent, gateway.PreparedReplicaProof) error
	ObserveAdopted(context.Context, gateway.GroupEnrollmentIntent, gateway.PreparedReplicaProof) (bool, error)
}

// AuthorizeFunc is called on the bounded fixed header before the potentially
// large payload is allocated. It must make its decision from the authenticated
// peer and immutable request coordinates; Request.Payload is nil at that
// point. Manifest-specific checks belong in PayloadValidator.
type AuthorizeFunc func(rafttransport.PeerIdentity, Request) bool
type PayloadValidator func(context.Context, gateway.GroupEnrollmentIntent, []byte) error

// ServiceOptions configures one authenticated node-control endpoint.
type ServiceOptions struct {
	Reader    IntentReader
	Journal   Journal
	Preparer  Preparer
	Adopter   Adopter
	Authorize AuthorizeFunc
	// ValidatePayload must reject paths and identities which are not owned by
	// the target node. The service always checks canonical JSON first; this
	// hook performs the manifest-specific path and schema checks before the
	// preparer sees the payload.
	ValidatePayload  PayloadValidator
	LocalNode        rafttransport.NodeID
	LocalIncarnation uint64
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
}

// Service is safe for concurrent authenticated connections. Exact operation
// retries are serialized by a bounded stripe array and independent requests
// are limited by MaxConcurrent.
type Service struct {
	reader           IntentReader
	journal          Journal
	preparer         Preparer
	adopter          Adopter
	authorize        AuthorizeFunc
	validatePayload  PayloadValidator
	localNode        rafttransport.NodeID
	localIncarnation uint64
	readDeadline     rafttransport.DeadlineFunc
	writeDeadline    rafttransport.DeadlineFunc
	slots            chan struct{}
	stripes          []sync.Mutex
	requests         atomic.Uint64
	completions      atomic.Uint64
	faults           atomic.Uint64
}

type Metrics struct {
	Requests, Completions, Faults uint64
	Inflight                      int
}

func (service *Service) Metrics() Metrics {
	if service == nil {
		return Metrics{}
	}
	return Metrics{Requests: service.requests.Load(), Completions: service.completions.Load(), Faults: service.faults.Load()}
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Reader == nil || options.Journal == nil || options.Preparer == nil || options.Adopter == nil ||
		options.Authorize == nil || options.ValidatePayload == nil || options.LocalNode == (rafttransport.NodeID{}) ||
		options.ReadDeadline == nil || options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > maxOperationStripes {
		return nil, ErrControl
	}
	if options.LocalIncarnation == 0 {
		return nil, ErrControl
	}
	return &Service{
		reader: options.Reader, journal: options.Journal, preparer: options.Preparer, adopter: options.Adopter,
		authorize: options.Authorize, localNode: options.LocalNode, localIncarnation: options.LocalIncarnation,
		validatePayload: options.ValidatePayload,
		readDeadline:    options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots: make(chan struct{}, options.MaxConcurrent), stripes: make([]sync.Mutex, options.MaxConcurrent),
	}, nil
}

// Serve handles one complete request on an already TLS-authenticated
// TrafficShardControl stream. The caller may put this handler behind
// internal/shardcontrol.Mux; the fixed request magic is intentionally replayed
// by that mux wrapper.
func (service *Service) Serve(ctx context.Context, connection rafttransport.PeerConnection) (resultErr error) {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		return ErrUnsupportedPeer
	}
	service.requests.Add(1)
	defer func() {
		if resultErr == nil {
			service.completions.Add(1)
		} else {
			service.faults.Add(1)
		}
		_ = connection.Close()
	}()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	release, ok := service.acquire()
	if !ok {
		return ErrBound
	}
	defer release()
	if deadline := service.readDeadline(); deadline.IsZero() {
		return ErrControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	request, payloadBytes, err := readRequestHeader(connection)
	if err != nil {
		return err
	}
	if !service.authorize(connection.PeerIdentity(), request) {
		return ErrUnauthorized
	}
	request.Payload = make([]byte, payloadBytes)
	if _, err = io.ReadFull(connection, request.Payload); err != nil {
		return err
	}
	if !request.valid() {
		return ErrControl
	}
	record, err := service.executeHeld(ctx, request)
	if err != nil {
		return err
	}
	if deadline := service.writeDeadline(); deadline.IsZero() {
		return ErrControl
	} else if err := connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteResponse(connection, record)
}

// Execute validates the committed directory before any journal or storage
// side effect, then resumes or performs the exact requested phase.
func (service *Service) Execute(ctx context.Context, request Request) (Record, error) {
	if service == nil || ctx == nil || !request.valid() {
		return Record{}, ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return Record{}, cause
	}
	release, ok := service.acquire()
	if !ok {
		return Record{}, ErrBound
	}
	defer release()
	return service.executeHeld(ctx, request)
}

func (service *Service) acquire() (func(), bool) {
	if service == nil {
		return nil, false
	}
	select {
	case service.slots <- struct{}{}:
		return func() { <-service.slots }, true
	default:
		return nil, false
	}
}

func (service *Service) executeHeld(ctx context.Context, request Request) (Record, error) {
	if service == nil || ctx == nil || !request.valid() {
		return Record{}, ErrControl
	}
	stripe := &service.stripes[binary.BigEndian.Uint64(request.IntentID[:8])%uint64(len(service.stripes))]
	stripe.Lock()
	defer stripe.Unlock()

	intent, err := service.reader.ReadEnrollmentIntent(ctx, request.IntentID)
	if err != nil {
		return Record{}, errors.Join(ErrStale, err)
	}
	if err = validateRequestIntent(request, intent, service.localNode, service.localIncarnation); err != nil {
		return Record{}, err
	}
	canonical, canonicalErr := vibejson.AppendCanonicalize(nil, request.Payload)
	if canonicalErr != nil || !bytes.Equal(canonical, request.Payload) {
		return Record{}, ErrControl
	}
	if sha256.Sum256(request.Payload) != intent.ExpectedManifestDigest {
		return Record{}, ErrStale
	}
	if err = service.validatePayload(ctx, intent, request.Payload); err != nil {
		return Record{}, errors.Join(ErrControl, err)
	}
	// Cancellation is a terminal authority decision.  It must never be
	// treated as an older enrollment state by either phase: doing so would let
	// a node with a durable journal retry storage preparation after the
	// replicated controller has revoked the intent.
	if intent.State == gateway.EnrollmentCancelled {
		return Record{}, ErrStale
	}
	if request.Phase == PhasePrepare {
		return service.executePrepare(ctx, request, intent)
	}
	if intent.State == gateway.EnrollmentComplete {
		// Completed intents remain useful for audit and exact observation, but
		// never authorize bringing a retired runtime back into service.
		return service.observeCompletedAdoption(ctx, request, intent)
	}
	if intent.State < gateway.EnrollmentEnrolled {
		return Record{}, ErrNotCommitted
	}
	return service.executeAdopt(ctx, request, intent)
}

func (service *Service) observeCompletedAdoption(ctx context.Context, request Request, intent gateway.GroupEnrollmentIntent) (Record, error) {
	record, err := service.journal.Read(ctx, request.IntentID)
	if err != nil {
		return Record{}, errors.Join(ErrStale, err)
	}
	if !record.valid() || record.IntentID != request.IntentID || record.State != StateAdopted ||
		record.AdoptionDigest != request.Digest() || record.PreparationDigest != request.PrepareVariant().Digest() ||
		!proofMatchesIntent(record.Proof, intent) {
		return Record{}, ErrStale
	}
	return record, nil
}

func (service *Service) executePrepare(ctx context.Context, request Request, intent gateway.GroupEnrollmentIntent) (Record, error) {
	digest := request.Digest()
	record, err := service.journal.Read(ctx, request.IntentID)
	if errors.Is(err, ErrMissing) {
		record = Record{IntentID: request.IntentID, PreparationDigest: digest, Revision: 1, State: StatePreparing}
		if err = service.publishExact(ctx, 0, record); err != nil {
			return Record{}, err
		}
	} else if err != nil {
		return Record{}, err
	} else if !record.valid() || record.IntentID != request.IntentID || record.PreparationDigest != digest {
		return Record{}, ErrConflict
	}
	if record.State == StatePrepared || record.State == StateAdopting || record.State == StateAdopted {
		if !proofMatchesIntent(record.Proof, intent) {
			return Record{}, ErrConflict
		}
		return record, nil
	}
	proof, found, err := service.preparer.ObservePrepared(ctx, intent, request.Payload)
	if err != nil {
		return Record{}, err
	}
	if !found {
		proof, err = service.preparer.Prepare(ctx, intent, request.Payload)
		if err != nil {
			return Record{}, err
		}
	}
	if !proofMatchesIntent(proof, intent) {
		return Record{}, ErrInvalidProof
	}
	terminal := record
	terminal.Revision++
	terminal.State = StatePrepared
	terminal.Proof = proof
	if err = service.publishExact(ctx, record.Revision, terminal); err != nil {
		return Record{}, err
	}
	return terminal, nil
}

func (service *Service) executeAdopt(ctx context.Context, request Request, intent gateway.GroupEnrollmentIntent) (Record, error) {
	prepare := request.PrepareVariant()
	prepareDigest := prepare.Digest()
	adoptDigest := request.Digest()
	record, err := service.journal.Read(ctx, request.IntentID)
	if errors.Is(err, ErrMissing) {
		// A lost process-local cache is not a lost preparation. Reconstruct the
		// exact durable proof from the artifact before permitting adoption.
		record = Record{IntentID: request.IntentID, PreparationDigest: prepareDigest, Revision: 1, State: StatePreparing}
		if err = service.publishExact(ctx, 0, record); err != nil {
			return Record{}, err
		}
	} else if err != nil {
		return Record{}, err
	}
	if !record.valid() || record.IntentID != request.IntentID || record.PreparationDigest != prepareDigest {
		return Record{}, ErrConflict
	}
	if record.State == StatePreparing {
		proof, found, observeErr := service.preparer.ObservePrepared(ctx, intent, request.Payload)
		if observeErr != nil {
			return Record{}, observeErr
		}
		if !found {
			return Record{}, ErrNotPrepared
		}
		if !proofMatchesIntent(proof, intent) {
			return Record{}, ErrInvalidProof
		}
		prepared := record
		prepared.Revision++
		prepared.State = StatePrepared
		prepared.Proof = proof
		if err = service.publishExact(ctx, record.Revision, prepared); err != nil {
			return Record{}, err
		}
		record = prepared
	}
	if !proofMatchesIntent(record.Proof, intent) {
		return Record{}, ErrInvalidProof
	}
	if record.State == StateAdopted {
		if record.AdoptionDigest != adoptDigest {
			return Record{}, ErrConflict
		}
		return record, nil
	}
	if record.State == StateAdopting && record.AdoptionDigest != adoptDigest {
		return Record{}, ErrConflict
	}
	if record.State == StatePrepared {
		adopting := record
		adopting.Revision++
		adopting.State = StateAdopting
		adopting.AdoptionDigest = adoptDigest
		if err = service.publishExact(ctx, record.Revision, adopting); err != nil {
			return Record{}, err
		}
		record = adopting
	}
	if adopted, observeErr := service.adopter.ObserveAdopted(ctx, intent, record.Proof); observeErr != nil {
		return Record{}, observeErr
	} else if !adopted {
		if err = service.adopter.Adopt(ctx, intent, record.Proof); err != nil {
			return Record{}, err
		}
	}
	terminal := record
	terminal.Revision++
	terminal.State = StateAdopted
	if err = service.publishExact(ctx, record.Revision, terminal); err != nil {
		return Record{}, err
	}
	return terminal, nil
}

func (service *Service) publishExact(ctx context.Context, expected uint64, desired Record) error {
	if err := service.journal.Publish(ctx, expected, desired); err != nil {
		// Visibility after a rename is not a durability proof. Keep the outcome
		// unknown and let the next exact retry resolve the journal state.
		return errors.Join(ErrOutcomeUnknown, err)
	}
	return nil
}

func validateRequestIntent(request Request, intent gateway.GroupEnrollmentIntent, local rafttransport.NodeID, localIncarnation uint64) error {
	if !intent.Valid() || request.IntentID != intent.IntentID || request.IntentDigest != intent.Digest() ||
		request.Group != intent.Group || request.TargetMember != intent.Target.Member ||
		request.TargetNode != intent.Target.Node || request.TargetNodeIncarnation != intent.Target.NodeIncarnation ||
		request.TargetStoreID != intent.Target.StoreID || request.TargetNodeRevision != intent.TargetNodeRevision {
		return ErrStale
	}
	if request.TargetNode != local || request.TargetNodeIncarnation != localIncarnation {
		return ErrUnauthorized
	}
	// Keep this check adjacent to the immutable identity checks so callers
	// cannot reach a side effect by changing only the phase byte.
	if intent.State == gateway.EnrollmentCancelled {
		return ErrStale
	}
	if request.Phase == PhasePrepare {
		// Preparation is admitted only by the controller's committed
		// same-state claim.  A caller cannot manufacture that fence in the
		// request payload, and a later enrollment state must not reopen the
		// preparation side effect after the controller has advanced the saga.
		switch intent.State {
		case gateway.EnrollmentReserved:
			claim := gateway.EnrollmentPreparationClaim(intent)
			if intent.PreparationClaim == ([32]byte{}) || intent.PreparationClaim != claim {
				return ErrNotCommitted
			}
		case gateway.EnrollmentPrepared:
			// An exact retry may resolve a node crash after the durable
			// preparation record was written.  executePrepare still requires
			// the matching local proof before returning success.
		default:
			return ErrStale
		}
	} else if intent.State < gateway.EnrollmentEnrolled {
		return ErrNotCommitted
	}
	return nil
}

func proofMatchesIntent(proof gateway.PreparedReplicaProof, intent gateway.GroupEnrollmentIntent) bool {
	if !proof.Valid() || proof.IntentID != intent.IntentID || proof.Group != intent.Group ||
		proof.Distribution != intent.Distribution || proof.Shard != intent.Shard ||
		proof.ReplicaOrdinal != intent.ReplicaOrdinal || proof.AllocationGeneration != intent.AllocationGeneration ||
		proof.CatalogGeneration != intent.CatalogGeneration || proof.TargetMember != intent.Target.Member ||
		proof.TargetNode != intent.Target.Node || proof.TargetNodeIncarnation != intent.Target.NodeIncarnation ||
		proof.TargetStoreID != intent.Target.StoreID || proof.TargetEndpoint != intent.Target.Endpoint ||
		proof.TargetNativeEndpoint != intent.Target.NativeEndpoint || proof.TargetControlEndpoint != intent.Target.ControlEndpoint ||
		proof.ExpectedRosterDigest != intent.ExpectedRosterDigest || proof.ExpectedDescriptorDigest != intent.ExpectedDescriptorDigest ||
		proof.ExpectedManifestDigest != intent.ExpectedManifestDigest ||
		proof.DescriptorDigest != intent.ExpectedDescriptorDigest || proof.ManifestDigest != intent.ExpectedManifestDigest ||
		proof.Command != intent.ExpectedCommand || proof.CertifiedDirectoryRevision != intent.TargetNodeRevision ||
		proof.RelationManifestDigest != proof.Command.RelationManifestDigest || !proof.InstallationFenceValid() {
		return false
	}
	return proof.EnrollmentDigest == proof.ComputedEnrollmentDigest()
}

// Digest returns a stable hash over the exact wire request, including its
// payload. It is persisted before callbacks so a different manifest cannot be
// retried under the same intent ID.
func (request Request) Digest() [32]byte {
	raw, err := AppendRequest(nil, request)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(raw)
}

func (request Request) valid() bool {
	return request.metadataValid() && len(request.Payload) > 0 && len(request.Payload) <= MaxPayloadBytes
}

func validGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) && group.GroupID != ([16]byte{})
}

// RequestDiscriminator identifies the grammar for internal/shardcontrol.Mux.
func RequestDiscriminator() [8]byte { return requestMagic }

func AppendRequest(dst []byte, request Request) ([]byte, error) {
	if !request.valid() || len(dst) > math.MaxInt-requestHeaderBytes-len(request.Payload) {
		return dst, ErrControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, requestHeaderBytes+len(request.Payload))...)
	b := dst[start:]
	copy(b[:8], requestMagic[:])
	b[8], b[9] = requestVersion, byte(request.Phase)
	copy(b[12:44], request.IntentID[:])
	copy(b[44:76], request.IntentDigest[:])
	appendGroup(b[76:148], request.Group)
	binary.BigEndian.PutUint64(b[148:156], request.TargetMember)
	copy(b[156:172], request.TargetNode[:])
	binary.BigEndian.PutUint64(b[172:180], request.TargetNodeIncarnation)
	copy(b[180:196], request.TargetStoreID[:])
	binary.BigEndian.PutUint64(b[196:204], request.TargetNodeRevision)
	binary.BigEndian.PutUint32(b[204:208], uint32(len(request.Payload)))
	copy(b[requestHeaderBytes:], request.Payload)
	return dst, nil
}

func openRequestHeader(raw []byte) (Request, int, error) {
	if len(raw) < requestHeaderBytes || !bytes.Equal(raw[:8], requestMagic[:]) || raw[8] != requestVersion ||
		raw[10] != 0 || raw[11] != 0 || binary.BigEndian.Uint32(raw[208:212]) != 0 {
		return Request{}, 0, ErrControl
	}
	payloadBytes := int(binary.BigEndian.Uint32(raw[204:208]))
	if payloadBytes == 0 || payloadBytes > MaxPayloadBytes {
		return Request{}, 0, ErrControl
	}
	var request Request
	request.Phase = Phase(raw[9])
	copy(request.IntentID[:], raw[12:44])
	copy(request.IntentDigest[:], raw[44:76])
	request.Group = openGroup(raw[76:148])
	request.TargetMember = binary.BigEndian.Uint64(raw[148:156])
	copy(request.TargetNode[:], raw[156:172])
	request.TargetNodeIncarnation = binary.BigEndian.Uint64(raw[172:180])
	copy(request.TargetStoreID[:], raw[180:196])
	request.TargetNodeRevision = binary.BigEndian.Uint64(raw[196:204])
	if !request.metadataValid() {
		return Request{}, 0, ErrControl
	}
	return request, payloadBytes, nil
}

func (request Request) metadataValid() bool {
	return request.Phase.valid() && request.IntentID != ([32]byte{}) && request.IntentDigest != (replication.Digest{}) &&
		validGroup(request.Group) && request.TargetMember != 0 && request.TargetNode != (rafttransport.NodeID{}) &&
		request.TargetNodeIncarnation != 0 && request.TargetStoreID != ([16]byte{}) && request.TargetNodeRevision != 0
}

func OpenRequest(raw []byte) (Request, error) {
	request, payloadBytes, err := openRequestHeader(raw)
	if err != nil || len(raw) != requestHeaderBytes+payloadBytes {
		return Request{}, ErrControl
	}
	request.Payload = bytes.Clone(raw[requestHeaderBytes:])
	if !request.valid() {
		return Request{}, ErrControl
	}
	return request, nil
}

func ReadRequest(reader io.Reader) (Request, error) {
	var header [requestHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return Request{}, err
	}
	request, payloadBytes, err := readRequestHeaderFromBytes(header[:])
	if err != nil {
		return Request{}, err
	}
	request.Payload = make([]byte, payloadBytes)
	if _, err = io.ReadFull(reader, request.Payload); err != nil {
		return Request{}, err
	}
	if !request.valid() {
		return Request{}, ErrControl
	}
	return request, nil
}

// readRequestHeader reads and validates only the fixed portion. Service.Serve
// uses this before allocating the request payload so the authenticated
// endpoint's concurrency bound covers hostile large frames as well.
func readRequestHeader(reader io.Reader) (Request, int, error) {
	var header [requestHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return Request{}, 0, err
	}
	return readRequestHeaderFromBytes(header[:])
}

func readRequestHeaderFromBytes(header []byte) (Request, int, error) {
	return openRequestHeader(header)
}

func WriteRequest(writer io.Writer, request Request) error {
	raw, err := AppendRequest(nil, request)
	if err != nil {
		return err
	}
	return writeFull(writer, raw)
}

func AppendResponse(dst []byte, record Record) ([]byte, error) {
	if !record.valid() || (record.State != StatePrepared && record.State != StateAdopted) {
		return dst, ErrControl
	}
	proof, err := vibejson.Marshal(&record.Proof)
	if err != nil || len(proof) == 0 || len(proof) > MaxProofBytes || len(dst) > math.MaxInt-responseHeaderBytes-len(proof) {
		return dst, errors.Join(ErrControl, err)
	}
	start := len(dst)
	dst = append(dst, make([]byte, responseHeaderBytes+len(proof))...)
	b := dst[start:]
	copy(b[:8], responseMagic[:])
	b[8], b[9] = responseVersion, byte(record.State)
	binary.BigEndian.PutUint64(b[12:20], record.Revision)
	copy(b[20:52], record.IntentID[:])
	copy(b[52:84], record.PreparationDigest[:])
	copy(b[84:116], record.AdoptionDigest[:])
	binary.BigEndian.PutUint32(b[116:120], uint32(len(proof)))
	// b[120:124] is reserved and remains zero.
	copy(b[responseHeaderBytes:], proof)
	return dst, nil
}

func OpenResponse(raw []byte) (Record, error) {
	if len(raw) < responseHeaderBytes || !bytes.Equal(raw[:8], responseMagic[:]) || raw[8] != responseVersion ||
		raw[10] != 0 || raw[11] != 0 {
		return Record{}, ErrControl
	}
	if !allZero(raw[120:124]) {
		return Record{}, ErrControl
	}
	proofBytes := int(binary.BigEndian.Uint32(raw[116:120]))
	if proofBytes == 0 || proofBytes > MaxProofBytes || len(raw) != responseHeaderBytes+proofBytes {
		return Record{}, ErrControl
	}
	var record Record
	record.State = State(raw[9])
	record.Revision = binary.BigEndian.Uint64(raw[12:20])
	copy(record.IntentID[:], raw[20:52])
	copy(record.PreparationDigest[:], raw[52:84])
	copy(record.AdoptionDigest[:], raw[84:116])
	if err := vibejson.Unmarshal(raw[responseHeaderBytes:], &record.Proof); err != nil {
		return Record{}, errors.Join(ErrControl, err)
	}
	canonical, err := vibejson.Marshal(&record.Proof)
	if err != nil || !bytes.Equal(canonical, raw[responseHeaderBytes:]) {
		return Record{}, errors.Join(ErrControl, err)
	}
	if !record.valid() || (record.State != StatePrepared && record.State != StateAdopted) || record.Proof.IntentID != record.IntentID {
		return Record{}, ErrControl
	}
	return record, nil
}

func ReadResponse(reader io.Reader) (Record, error) {
	var header [responseHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return Record{}, err
	}
	proofBytes := int(binary.BigEndian.Uint32(header[116:120]))
	if proofBytes == 0 || proofBytes > MaxProofBytes {
		return Record{}, ErrControl
	}
	raw := make([]byte, responseHeaderBytes+proofBytes)
	copy(raw, header[:])
	if _, err := io.ReadFull(reader, raw[responseHeaderBytes:]); err != nil {
		return Record{}, err
	}
	return OpenResponse(raw)
}

func WriteResponse(writer io.Writer, record Record) error {
	raw, err := AppendResponse(nil, record)
	if err != nil {
		return err
	}
	return writeFull(writer, raw)
}

func allZero(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func appendGroup(dst []byte, group raftmember.GroupKey) {
	copy(dst[0:16], group.ClusterID[:])
	copy(dst[16:32], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(dst[32:40], group.TopologyRecoveryEpoch)
	copy(dst[40:56], group.ShardIncarnation[:])
	copy(dst[56:72], group.GroupID[:])
}

func openGroup(src []byte) (group raftmember.GroupKey) {
	copy(group.ClusterID[:], src[0:16])
	copy(group.ClusterIncarnation[:], src[16:32])
	group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(src[32:40])
	copy(group.ShardIncarnation[:], src[40:56])
	copy(group.GroupID[:], src[56:72])
	return group
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) != 0 {
		written, err := writer.Write(data)
		if written > 0 {
			data = data[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// FileJournal is a crash-safe local journal. Every record is canonical JSON,
// written to a synced temporary inode, renamed into place, then followed by a
// directory sync. The lock also serializes two node-control servers sharing a
// root in one process.
type FileJournal struct {
	mu   sync.Mutex
	root string
	lock *os.File
}

func NewFileJournal(root string) (*FileJournal, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root || root == string(filepath.Separator) {
		return nil, ErrControl
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrControl
	}
	lock, err := os.OpenFile(filepath.Join(root, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		return nil, errors.Join(ErrConflict, err)
	}
	return &FileJournal{root: root, lock: lock}, nil
}

// Close releases the process and OS writer lease held for the journal root.
// A node supervisor should close it after stopping the authenticated service.
func (journal *FileJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if journal.lock == nil {
		return nil
	}
	err := storeio.UnlockWriter(journal.lock)
	closeErr := journal.lock.Close()
	journal.lock = nil
	return errors.Join(err, closeErr)
}

func (journal *FileJournal) path(intentID [32]byte) string {
	return filepath.Join(journal.root, fmt.Sprintf("%x.state", intentID[:]))
}

func (journal *FileJournal) Read(ctx context.Context, intentID [32]byte) (Record, error) {
	if journal == nil || ctx == nil || intentID == ([32]byte{}) {
		return Record{}, ErrControl
	}
	if err := context.Cause(ctx); err != nil {
		return Record{}, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	return journal.readLocked(ctx, intentID)
}

func (journal *FileJournal) readLocked(ctx context.Context, intentID [32]byte) (Record, error) {
	file, err := os.Open(journal.path(intentID))
	if errors.Is(err, os.ErrNotExist) {
		return Record{}, ErrMissing
	}
	if err != nil {
		return Record{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > MaxJournalBytes {
		return Record{}, ErrJournalCorrupt
	}
	raw := make([]byte, int(info.Size()))
	if _, err = io.ReadFull(file, raw); err != nil {
		return Record{}, ErrJournalCorrupt
	}
	var extra [1]byte
	if count, readErr := file.Read(extra[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return Record{}, ErrJournalCorrupt
	}
	var record Record
	if err = vibejson.Unmarshal(raw, &record); err != nil || !record.valid() || record.IntentID != intentID {
		return Record{}, errors.Join(ErrJournalCorrupt, err)
	}
	canonical, err := vibejson.Marshal(&record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Record{}, errors.Join(ErrJournalCorrupt, err)
	}
	return record, nil
}

func (journal *FileJournal) Publish(ctx context.Context, expected uint64, desired Record) error {
	if journal == nil || ctx == nil || !desired.valid() || (expected == 0 && desired.Revision != 1) ||
		(expected != 0 && desired.Revision != expected+1) {
		return ErrControl
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	current, err := journal.readLocked(ctx, desired.IntentID)
	if errors.Is(err, ErrMissing) {
		if expected != 0 {
			return ErrConflict
		}
	} else if err != nil {
		return err
	} else if expected == 0 || current.Revision != expected {
		return ErrConflict
	}
	if expected != 0 && !validTransition(current, desired) {
		return ErrConflict
	}
	raw, err := vibejson.Marshal(&desired)
	if err != nil || len(raw) > MaxJournalBytes {
		return errors.Join(ErrControl, err)
	}
	temporary, err := os.CreateTemp(journal.root, ".nodecontrol-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(raw)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(temporaryName, journal.path(desired.IntentID)); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	if err = syncDirectory(journal.root); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	return nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	err = directory.Sync()
	closeErr := directory.Close()
	return errors.Join(err, closeErr)
}

// MemoryJournal is a bounded test and single-process implementation. It keeps
// the same CAS semantics as FileJournal and is useful for embedding a journal
// in a supervisor that already owns durable storage.
type MemoryJournal struct {
	mu      sync.Mutex
	records map[[32]byte]Record
	max     int
}

func NewMemoryJournal() *MemoryJournal {
	return NewMemoryJournalWithLimit(maxOperationStripes)
}

func NewMemoryJournalWithLimit(maxRecords int) *MemoryJournal {
	if maxRecords <= 0 {
		maxRecords = maxOperationStripes
	}
	return &MemoryJournal{records: make(map[[32]byte]Record), max: maxRecords}
}

func (journal *MemoryJournal) Read(ctx context.Context, intentID [32]byte) (Record, error) {
	if journal == nil || ctx == nil || intentID == ([32]byte{}) {
		return Record{}, ErrControl
	}
	if err := context.Cause(ctx); err != nil {
		return Record{}, err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	record, found := journal.records[intentID]
	if !found {
		return Record{}, ErrMissing
	}
	return record, nil
}

func (journal *MemoryJournal) Publish(ctx context.Context, expected uint64, desired Record) error {
	if journal == nil || ctx == nil || !desired.valid() {
		return ErrControl
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	current, found := journal.records[desired.IntentID]
	if !found {
		if expected != 0 || desired.Revision != 1 {
			return ErrConflict
		}
		if journal.max > 0 && len(journal.records) >= journal.max {
			return ErrBound
		}
	} else if current.Revision != expected || desired.Revision != expected+1 {
		return ErrConflict
	}
	if found && !validTransition(current, desired) {
		return ErrConflict
	}
	journal.records[desired.IntentID] = desired
	return nil
}

// Client performs one bounded authenticated request. It deliberately leaves
// retries to the caller, because a transport failure after the write is an
// unknown outcome that must be resolved by the durable journal.
type StreamOpener interface {
	OpenShardControl(context.Context, rafttransport.NodeID) (rafttransport.PeerConnection, error)
}

type ClientOptions struct {
	Opener        StreamOpener
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
}

type Client struct {
	opener        StreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
}

func NewClient(options ClientOptions) (*Client, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil {
		return nil, ErrControl
	}
	return &Client{opener: options.Opener, readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline}, nil
}

func (client *Client) Execute(ctx context.Context, target rafttransport.NodeID, request Request) (Record, error) {
	if client == nil || ctx == nil || target == (rafttransport.NodeID{}) || !request.valid() || request.TargetNode != target {
		return Record{}, ErrControl
	}
	connection, err := client.opener.OpenShardControl(ctx, target)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return Record{}, err
	}
	if connection == nil {
		return Record{}, ErrControl
	}
	defer connection.Close()
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation}
	if connection.TrafficClass() != rafttransport.TrafficShardControl || peer.TrustDomain != wantDomain || peer.Node != target {
		return Record{}, ErrUnauthorized
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := client.writeDeadline(); deadline.IsZero() {
		return Record{}, ErrControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return Record{}, err
	}
	if err = WriteRequest(connection, request); err != nil {
		return Record{}, errors.Join(ErrOutcomeUnknown, err)
	}
	if deadline := client.readDeadline(); deadline.IsZero() {
		return Record{}, ErrOutcomeUnknown
	} else if err = connection.SetReadDeadline(deadline); err != nil {
		return Record{}, errors.Join(ErrOutcomeUnknown, err)
	}
	record, err := ReadResponse(connection)
	if err != nil {
		return Record{}, errors.Join(ErrOutcomeUnknown, err)
	}
	if record.IntentID != request.IntentID || !proofMatchesRequest(record.Proof, request) ||
		record.PreparationDigest != request.PrepareVariant().Digest() ||
		(request.Phase == PhasePrepare && record.State != StatePrepared && record.State != StateAdopted) ||
		(request.Phase == PhaseAdopt && (record.State != StateAdopted || record.AdoptionDigest != request.Digest())) {
		return Record{}, ErrConflict
	}
	return record, nil
}

func proofMatchesRequest(proof gateway.PreparedReplicaProof, request Request) bool {
	return proof.Valid() && proof.IntentID == request.IntentID && proof.Group == request.Group &&
		proof.TargetMember == request.TargetMember && proof.TargetNode == request.TargetNode &&
		proof.TargetNodeIncarnation == request.TargetNodeIncarnation && proof.TargetStoreID == request.TargetStoreID
}
