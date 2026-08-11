// Package replicatedstate provides the bounded, unserved replicated apply
// adapter. It deliberately owns no SQL planning, Raft transport, or serving
// authority.
package replicatedstate

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	// ResultFormatMutationV1 is the fixed, empty-payload result grammar used by
	// this low-level unconditional mutation adapter.
	ResultFormatMutationV1 uint16 = 1

	ResultApplied uint32 = iota
	ResultStaleFence
	ResultUnknownCollection
	ResultInvalidDocument
	ResultTargetBound

	// MaxStateEnvelopeBytes and MaxCompletionRecordBytes bound the two binary
	// records before their canonical hexadecimal JSON wrapping.
	MaxStateEnvelopeBytes           = 64 << 10
	MaxCompletionRecordBytes        = 256 << 10
	MaxRetainedCompletionsV1        = 1 << 20
	MaxStaticBootstrapBytes         = 1 << 20
	MaxStaticBootstrapEnvelopeBytes = MaxStaticBootstrapBytes + MaxStateEnvelopeBytes
	MaxDistinctMutationsV1          = 64
	MaxStaticBootstrapMembersV1     = 64
)

var (
	ErrInvalidBinding       = errors.New("replicatedstate: invalid shard binding")
	ErrInvalidOptions       = errors.New("replicatedstate: invalid options")
	ErrInvalidCollection    = errors.New("replicatedstate: invalid collection profile")
	ErrStateCorrupt         = errors.New("replicatedstate: corrupt state record")
	ErrCompletionCorrupt    = errors.New("replicatedstate: corrupt completion record")
	ErrCompletionNotFound   = errors.New("replicatedstate: completion not found")
	ErrRequestConflict      = errors.New("replicatedstate: client sequence conflicts with retained command")
	ErrStaleCommand         = errors.New("replicatedstate: command has a stale mutable fence")
	ErrWrongBinding         = errors.New("replicatedstate: command belongs to another shard binding")
	ErrApplySequence        = errors.New("replicatedstate: invalid apply sequence")
	ErrApplyPoisoned        = errors.New("replicatedstate: machine is poisoned; reopen required")
	ErrStaticSnapshotOnly   = errors.New("replicatedstate: only the exact static bootstrap snapshot is supported")
	ErrAdmissionBound       = errors.New("replicatedstate: command exceeds the frozen apply profile")
	ErrSchemaProfile        = errors.New("replicatedstate: v1 requires a schema-free JSON collection")
	ErrInconsistentSnapshot = errors.New("replicatedstate: coherent snapshot disagrees with publication")
	ErrCodecAlias           = errors.New("replicatedstate: codec input aliases destination append region")
)

// Binding is the exact shard and recovery lineage owned by one Machine.
// ReplicaSetVersion is intentionally absent: it is applied publication state.
type Binding struct {
	ClusterID             replication.ID128
	ClusterIncarnation    replication.ID128
	TopologyRecoveryEpoch uint64
	Distribution          string
	Shard                 string
	AllocationGeneration  uint64
	ShardIncarnation      replication.ID128
	GroupID               replication.ID128

	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	OwnershipEpoch         uint64
	SchemaGeneration       uint64
	RoutingVersion         uint64
	RouteGeneration        uint64
}

func (b Binding) validate() error {
	if b.ClusterID == (replication.ID128{}) ||
		b.ClusterIncarnation == (replication.ID128{}) ||
		b.ShardIncarnation == (replication.ID128{}) ||
		b.GroupID == (replication.ID128{}) ||
		b.TopologyRecoveryEpoch == 0 || b.AllocationGeneration == 0 ||
		b.ActivePolicyGeneration == 0 || b.ProtectionEpoch == 0 ||
		b.OwnershipEpoch == 0 || b.SchemaGeneration == 0 ||
		b.RoutingVersion == 0 || b.RouteGeneration == 0 {
		return ErrInvalidBinding
	}
	if b.Distribution == "" || len(b.Distribution) > replication.MaxIdentityBytes ||
		!utf8.ValidString(b.Distribution) || strings.IndexByte(b.Distribution, 0) >= 0 ||
		b.Shard == "" || len(b.Shard) > replication.MaxIdentityBytes ||
		!utf8.ValidString(b.Shard) || strings.IndexByte(b.Shard, 0) >= 0 {
		return ErrInvalidBinding
	}
	return nil
}

// ValidationProfile is a construction-time assertion about the exact
// collection validation contract. V1 accepts only schema-free JSON.
type ValidationProfile uint8

const ValidationSchemaFreeJSONV1 ValidationProfile = 1

// CollectionLimits repeats the collection's frozen durable bounds at the
// integration boundary. Open compares every value to the live handle, so a
// catalog/profile mismatch fails before any command can be proposed.
type CollectionLimits struct {
	MaxKeyBytes          int
	MaxDocumentBytes     int
	MaxDistinctMutations int
	MaxBatchBytes        int
}

// CollectionTarget binds a durable handle to its deterministic validation and
// admission profile.
type CollectionTarget struct {
	Collection *durable.Collection
	Validation ValidationProfile
	Limits     CollectionLimits
}

// UserCollection is the single logical collection owned by a v1 Machine.
type UserCollection struct {
	Name   string
	Target CollectionTarget
}

func (t CollectionTarget) validate() error {
	if t.Collection == nil || t.Validation != ValidationSchemaFreeJSONV1 ||
		t.Collection.HasSchema() || t.Collection.HasIndexes() ||
		!t.Collection.HasSynchronousDurability() || !t.Collection.SupportsUpdate() {
		return ErrSchemaProfile
	}
	l := t.Limits
	if l.MaxKeyBytes <= 0 || l.MaxDocumentBytes <= 0 ||
		l.MaxDistinctMutations <= 0 || l.MaxBatchBytes <= 0 {
		return ErrInvalidCollection
	}
	if l.MaxKeyBytes != t.Collection.MaxKeyBytes() ||
		l.MaxDocumentBytes != t.Collection.MaxDocumentBytes() ||
		l.MaxDistinctMutations != t.Collection.MaxBatchDocuments() ||
		l.MaxBatchBytes != t.Collection.MaxBatchBytes() {
		return fmt.Errorf("%w: explicit limits differ from durable handle", ErrInvalidCollection)
	}
	return nil
}

// Options fixes the cross-collection and completion-retention admission
// profile. Zero values fail closed.
type Options struct {
	TxnLimits      durable.TxnLimits
	MaxCompletions uint64
}

func (o Options) validate() error {
	if o.MaxCompletions == 0 || o.MaxCompletions > MaxRetainedCompletionsV1 ||
		o.TxnLimits.MaxCollections < 2 ||
		o.TxnLimits.MaxDocuments < 3 || o.TxnLimits.MaxBytes <= 0 {
		return ErrInvalidOptions
	}
	return nil
}

// RequestConflictError identifies the retained completion key involved in a
// tuple reuse. The original record remains authoritative and is not replaced.
type RequestConflictError struct {
	Key [32]byte
}

func (e *RequestConflictError) Error() string {
	return fmt.Sprintf("%v: %x", ErrRequestConflict, e.Key)
}

func (e *RequestConflictError) Is(target error) bool { return target == ErrRequestConflict }

// CompletionLookup owns an exact completion envelope detached from storage.
type CompletionLookup struct {
	Key             [32]byte
	Bytes           []byte
	AppliedSequence uint64
}
