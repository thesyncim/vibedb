// Package requestledger defines the sole byte-canonical durable request ledger
// grammar. It contains no transport, SQL, JSON, or storage-engine dependency.
package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	// MaxCommandBytes is the repository-wide admission bound copied here to keep
	// the kernel below replication (which consumes this package). A contract test
	// at the integration boundary must keep the two constants equal.
	MaxCommandBytes = 16 << 20

	// MaxInlinePlanBytes is the only canonical inline/paged split. Larger plans
	// are stored in independently authenticated pages.
	MaxInlinePlanBytes = 32 << 10
	// MaxPlanPageBytes bounds page payload, not its small fixed record frame.
	MaxPlanPageBytes = 512 << 10
	// MaxPlanBytes is the shipped aggregate transaction recipe bound. It is a
	// byte bound, never a participant-count ceiling. Individual replicated
	// ledger commands remain bounded by MaxCommandBytes and stream these bytes
	// in page batches.
	MaxPlanBytes = 1 << 30
	// MaxTargetBytes bounds one portable routing target. It is intentionally a
	// byte bound, not a shard, participant, or topology-count policy.
	MaxTargetBytes = 64 << 10

	checksumBytes = 4
)

var (
	ErrCorrupt      = errors.New("request ledger record is corrupt")
	ErrTooLarge     = errors.New("request ledger record exceeds its byte bound")
	ErrInvalidKey   = errors.New("request ledger key is invalid")
	ErrInvalidState = errors.New("request ledger state transition is invalid")
	ErrRevision     = errors.New("request ledger revision is not monotone")
	ErrIncomplete   = errors.New("request ledger plan is incomplete")
	ErrAlreadyAcked = errors.New("request ledger request is permanently acknowledged")
	ErrBucketBits   = errors.New("request ledger bucket width is invalid")
)

// ScopeKind states how Principal was authenticated. Both scopes require a
// nonzero fixed principal; the local scope must use a persisted installation
// identity and may never collapse independent processes onto a zero identity.
type ScopeKind uint8

const (
	ScopeInvalid ScopeKind = iota
	ScopeAuthenticated
	ScopeLocalInstall
)

type PrincipalID [16]byte
type RequestID [16]byte
type PinID [16]byte
type IssuerLane [8]byte
type AckToken [32]byte
type Digest [sha256.Size]byte

// RequestKey is the complete authenticated idempotency identity. No textual
// UUID or tenant representation participates in durable equality.
type RequestKey struct {
	Scope          ScopeKind
	Principal      PrincipalID
	Request        RequestID
	TenantDigest   Digest
	IssuerEpoch    uint64
	IssuerSequence uint64
	IssuerLane     IssuerLane
}

func (key RequestKey) Valid() bool {
	return (key.Scope == ScopeAuthenticated || key.Scope == ScopeLocalInstall) &&
		key.Principal != (PrincipalID{}) && key.Request != (RequestID{}) &&
		nonzeroDigest(key.TenantDigest) &&
		((key.IssuerEpoch == 0 && key.IssuerSequence == 0 && key.IssuerLane == (IssuerLane{})) ||
			(key.IssuerEpoch != 0 && key.IssuerSequence != 0 && key.IssuerLane != (IssuerLane{})))
}

var keyDigestDomain = []byte("vibedb/request-ledger/key\x00")

// KeyDigest returns the fixed durable key. The seven zero framing bytes after
// Scope are intentional: they make the scope field a canonical 64-bit lane.
func KeyDigest(key RequestKey) (Digest, error) {
	if !key.Valid() {
		return Digest{}, ErrInvalidKey
	}
	var framed [len("vibedb/request-ledger/key\x00") + 8 + 32 + 16 + 16 + 16 + 8]byte
	at := copy(framed[:], keyDigestDomain)
	framed[at] = byte(key.Scope)
	at += 8
	at += copy(framed[at:], key.TenantDigest[:])
	at += copy(framed[at:], key.Principal[:])
	at += copy(framed[at:], key.Request[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], key.IssuerEpoch)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], key.IssuerSequence)
	copy(framed[at+16:at+24], key.IssuerLane[:])
	return Digest(sha256.Sum256(framed[:])), nil
}

// LedgerHome is a distinct distributed keyspace home. It must never be
// substituted for replication.RetryHome: request-result retention and shard
// session retry retention have different ownership and lifetime contracts.
type LedgerHome [sha256.Size]byte

// Home derives the stable keyspace input. Topology maps this input to a range;
// the kernel deliberately does not bake in a shard count.
func Home(key RequestKey) (LedgerHome, error) {
	digest, err := KeyDigest(key)
	if err != nil || key.IssuerEpoch == 0 {
		return LedgerHome(digest), err
	}
	const domain = "vibedb/request-ledger/issuer-home\x00"
	var framed [len(domain) + 8 + 32 + 16 + 8 + 8]byte
	at := copy(framed[:], domain)
	framed[at] = byte(key.Scope)
	at += 8
	at += copy(framed[at:], key.TenantDigest[:])
	at += copy(framed[at:], key.Principal[:])
	binary.LittleEndian.PutUint64(framed[at:at+8], key.IssuerEpoch)
	at += 8
	copy(framed[at:], key.IssuerLane[:])
	return LedgerHome(sha256.Sum256(framed[:])), nil
}

// Bucket extracts the high bucketBits from the uniformly distributed home.
// The explicit bit-width is the only mapping input; changing physical shard
// count is a catalog/range operation and does not change the home.
func (home LedgerHome) Bucket(bucketBits uint8) (uint32, error) {
	if bucketBits == 0 || bucketBits > 24 {
		return 0, ErrBucketBits
	}
	word := uint32(home[0])<<16 | uint32(home[1])<<8 | uint32(home[2])
	return word >> (24 - bucketBits), nil
}

// Phase is monotone. Dispatch progress advances Revision while remaining in
// PhaseSealed; it does not invent intermediate externally visible states.
type Phase uint8

const (
	PhaseInvalid Phase = iota
	PhasePlanning
	PhaseExpired
	PhaseSealed
	PhasePrepared
	PhaseTerminal
	PhaseAcked
)

func (phase Phase) Valid() bool { return phase >= PhasePlanning && phase <= PhaseAcked }

func (phase Phase) CanTransitionTo(next Phase) bool {
	if !phase.Valid() || !next.Valid() || next < phase {
		return false
	}
	switch phase {
	case PhasePlanning:
		return next == PhasePlanning || next == PhaseExpired || next == PhaseSealed
	case PhaseExpired:
		return next == PhaseExpired || next == PhasePlanning
	case PhaseSealed:
		return next == PhaseSealed || next == PhasePrepared
	case PhasePrepared:
		return next == PhasePrepared || next == PhaseTerminal
	case PhaseTerminal:
		return next == PhaseTerminal || next == PhaseAcked
	case PhaseAcked:
		return next == PhaseAcked
	default:
		return false
	}
}

// Outcome is the terminal execution result class. Result remains an opaque,
// byte-canonical response owned by the caller's protocol.
type Outcome uint8

const (
	OutcomeInvalid Outcome = iota
	OutcomeCommitted
	OutcomeAborted
)

func (outcome Outcome) Valid() bool {
	return outcome == OutcomeCommitted || outcome == OutcomeAborted
}

// Usage is exact encoded-record accounting. It excludes storage-engine page,
// key, and allocator overhead because those are accounted by the owner.
type Usage struct {
	HeadBytes         uint64
	PlanPageBytes     uint64
	PendingBytes      uint64
	ContinuationBytes uint64
	PayloadBytes      uint64
	RoutePinBytes     uint64
	PreparedBytes     uint64
	SchemaPinBytes    uint64
	ReadyBytes        uint64
	ExpiryBytes       uint64
	TerminalBytes     uint64
	AckBytes          uint64
}

func (usage Usage) DurableBytes() (uint64, error) {
	total := uint64(0)
	for _, value := range [...]uint64{
		usage.HeadBytes, usage.PlanPageBytes, usage.PendingBytes,
		usage.ContinuationBytes, usage.PayloadBytes, usage.RoutePinBytes,
		usage.PreparedBytes, usage.SchemaPinBytes, usage.ReadyBytes, usage.ExpiryBytes,
		usage.TerminalBytes, usage.AckBytes,
	} {
		if total > ^uint64(0)-value {
			return 0, ErrTooLarge
		}
		total += value
	}
	return total, nil
}

func addUsage(current *uint64, encoded []byte) error {
	value := uint64(len(encoded))
	if *current > ^uint64(0)-value {
		return ErrTooLarge
	}
	*current += value
	return nil
}

func (usage *Usage) AddHead(encoded []byte) error     { return addUsage(&usage.HeadBytes, encoded) }
func (usage *Usage) AddPlanPage(encoded []byte) error { return addUsage(&usage.PlanPageBytes, encoded) }
func (usage *Usage) AddPending(encoded []byte) error  { return addUsage(&usage.PendingBytes, encoded) }
func (usage *Usage) AddContinuation(encoded []byte) error {
	return addUsage(&usage.ContinuationBytes, encoded)
}
func (usage *Usage) AddPayload(encoded []byte) error  { return addUsage(&usage.PayloadBytes, encoded) }
func (usage *Usage) AddRoutePin(encoded []byte) error { return addUsage(&usage.RoutePinBytes, encoded) }
func (usage *Usage) AddPrepared(encoded []byte) error { return addUsage(&usage.PreparedBytes, encoded) }
func (usage *Usage) AddSchemaPin(encoded []byte) error {
	return addUsage(&usage.SchemaPinBytes, encoded)
}
func (usage *Usage) AddReady(encoded []byte) error    { return addUsage(&usage.ReadyBytes, encoded) }
func (usage *Usage) AddExpiry(encoded []byte) error   { return addUsage(&usage.ExpiryBytes, encoded) }
func (usage *Usage) AddTerminal(encoded []byte) error { return addUsage(&usage.TerminalBytes, encoded) }
func (usage *Usage) AddAck(encoded []byte) error      { return addUsage(&usage.AckBytes, encoded) }

func nonzeroDigest(digest Digest) bool { return digest != (Digest{}) }

func nextRevision(expected, next uint64) bool {
	return expected != ^uint64(0) && next == expected+1
}

func appendU64(dst []byte, value uint64) []byte {
	return binary.LittleEndian.AppendUint64(dst, value)
}
