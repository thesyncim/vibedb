package replication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/splitcapture"
)

const (
	// MaxRouteGateCommandBytes is the exact largest outer replicated route-gate
	// command, including three maximum-width identities and envelope checksum.
	MaxRouteGateCommandBytes = commandHeaderBytes + envelopeChecksumBytes +
		routegate.CommandBytes + 3*MaxIdentityBytes
	// MaxExecutionPinCommandBytes is the corresponding exact bound for the
	// fixed logical execution-pin command carried by schema-release evidence.
	MaxExecutionPinCommandBytes = commandHeaderBytes + envelopeChecksumBytes +
		executionpin.CommandBytes + 3*MaxIdentityBytes
)

var commandMagic = [8]byte{'V', 'D', 'B', 'C', 'M', 'D', 0, 0}

var transactionMutationDigestDomain = [...]byte{
	'V', 'i', 'b', 'e', 'D', 'B', '/', 't', 'x', 'n', '/', 'r', 'e', 'l', '/', '1', 0,
}

var executionPinAuthorityDigestDomain = []byte("vibedb/execution-pin/catalog-authority\x00")

const (
	transactionCoordinatorEpoch       = uint64(1)
	transactionParticipantEpoch       = uint64(2)
	transactionCoordinatorPulseTag    = uint64(1) << 62
	transactionCoordinatorDecisionTag = uint64(2) << 62
	transactionCoordinatorRetireTag   = uint64(3) << 62
	transactionCoordinatorRevisionMax = uint64(1) << 62
)

// Command is one deterministic relation-bundle state-machine operation.
// It contains no Raft term, index, physical root generation, SQL text, local
// clock reading, or serialized execution plan. Lease deadlines are explicit
// replicated inputs; this codec never samples time. Fingerprint is the already-
// canonical request fingerprint minted by the request layer; this codec treats
// it as opaque.
// Relation batches are strictly identity ordered. Mutation order within each
// batch, including duplicate keys, is semantic and preserved exactly; upstream
// fingerprinting must bind every relation and mutation ordinal. A session
// retirement, release, open, renew, or revoke carries no mutations. AckThrough
// is the greatest earlier client sequence that will never be retried; zero
// acknowledges nothing. A session-open request has the canonical client tuple
// (epoch 0, sequence 1, acknowledgement 0); the state machine returns its
// allocated epoch. Session lease commands carry exactly two signed, positive-
// domain Unix-nanosecond scalars: Open=(0,D), Renew=(E,D) where 0<E<D, and
// Revoke=(E,0). All other command kinds require both scalars to be zero.
type Command struct {
	Kind           CommandKind
	AuthorityClass CommandAuthorityClass

	ClusterID             ID128
	ClusterIncarnation    ID128
	TopologyRecoveryEpoch uint64

	Distribution         string
	Shard                string
	AllocationGeneration uint64
	ShardIncarnation     ID128
	GroupID              ID128

	ReplicaSetVersion      uint64
	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	OwnershipEpoch         uint64
	SchemaGeneration       uint64
	RoutingVersion         uint64
	RouteGeneration        uint64

	Tenant         []byte
	ClientID       ID128
	ClientEpoch    uint64
	ClientSequence uint64
	AckThrough     uint64
	Fingerprint    Digest
	RetryHome      RetryHome
	// ExpectedDeadlineUnixNano is the exact compare-and-swap lease fence.
	ExpectedDeadlineUnixNano int64
	// NextDeadlineUnixNano is the deadline to publish, or zero for revoke.
	NextDeadlineUnixNano int64

	// Transaction is one exact canonical distributed transaction control body.
	// It is present only for CommandTransaction. Fused participant preparation
	// mutations remain in Batches so the native relation codec is never
	// duplicated.
	Transaction []byte
	// RequestLedger is one canonical internal/requestledger command body. It is
	// present only for CommandRequestLedger and is carried without another
	// redundant outer length because it consumes the complete command payload.
	RequestLedger []byte
	// RouteGate is one exact fixed routegate command. It is present only for
	// CommandRouteGate and is ordered by this data shard's existing Raft log.
	RouteGate []byte
	// ExecutionPin is one fixed canonical logical-pin command. It is present
	// only for CommandExecutionPin and carries no relation mutations.
	ExecutionPin           []byte
	SplitCaptureActivation []byte
	// Batches retains the shared multi-relation mutation payload used by data
	// and transaction commands; control commands require it to be empty.
	Batches []RelationMutationBatch
	// RetainedPrune is present only for CommandRetainedPrune.
	RetainedPrune RetainedPruneProof
}

// CommandView is a checksum- and semantics-validated borrowed command. Its
// byte slices alias the OpenCommand input and must be treated as immutable.
// Copying or retaining the view retains the complete bounded input envelope.
type CommandView struct {
	kind           CommandKind
	AuthorityClass CommandAuthorityClass

	ClusterID             ID128
	ClusterIncarnation    ID128
	TopologyRecoveryEpoch uint64

	Distribution         []byte
	Shard                []byte
	AllocationGeneration uint64
	ShardIncarnation     ID128
	GroupID              ID128

	ReplicaSetVersion      uint64
	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	OwnershipEpoch         uint64
	SchemaGeneration       uint64
	RoutingVersion         uint64
	RouteGeneration        uint64

	Tenant                   []byte
	ClientID                 ID128
	ClientEpoch              uint64
	ClientSequence           uint64
	AckThrough               uint64
	Fingerprint              Digest
	RetryHome                RetryHome
	ExpectedDeadlineUnixNano int64
	NextDeadlineUnixNano     int64

	raw                []byte
	transactionBytes   []byte
	requestLedgerBytes []byte
	routeGateBytes     []byte
	executionPinBytes  []byte
	splitCaptureBytes  []byte
	retainedPrune      RetainedPruneProof
	relationBytes      []byte
	mutationCount      uint32
	relationCount      uint16
	inlineRelationID   RelationID
	transactionRole    distributedtxn.ReplicatedRole
	transactionOp      distributedtxn.ReplicatedOperation
}

// Bytes returns the exact validated envelope. The result aliases the decoder
// input and is read-only for the view's lifetime.
func (v CommandView) Bytes() []byte {
	return v.raw[:len(v.raw):len(v.raw)]
}

// TransactionBytes returns the exact validated transaction control body. The
// result aliases the decoder input and is read-only for the view's lifetime.
// Non-transaction commands return nil.
func (v CommandView) TransactionBytes() []byte {
	return v.transactionBytes[:len(v.transactionBytes):len(v.transactionBytes)]
}

// RequestLedgerBytes returns the exact validated request-ledger control body.
// The result aliases the command envelope. Non-ledger commands return nil.
func (v CommandView) RequestLedgerBytes() []byte {
	return v.requestLedgerBytes[:len(v.requestLedgerBytes):len(v.requestLedgerBytes)]
}

// OpenRequestLedgerInto reopens the already outer-validated request-ledger
// command into caller-owned pending-wave scratch. The returned payload aliases
// the outer command envelope. Supplying MaxPendingWaveSteps entries keeps the
// operation allocation-free.
func (v CommandView) OpenRequestLedgerInto(
	steps []requestledger.StepRef,
) (requestledger.CommandView, error) {
	if v.kind != CommandRequestLedger || len(v.requestLedgerBytes) == 0 {
		return requestledger.CommandView{}, ErrEnvelopeSemantic
	}
	return requestledger.OpenCommandInto(v.requestLedgerBytes, steps)
}

// RouteGateBytes returns the exact validated route-gate command, borrowing the
// outer envelope. Non-route-gate commands return nil.
func (v CommandView) RouteGateBytes() []byte {
	return v.routeGateBytes[:len(v.routeGateBytes):len(v.routeGateBytes)]
}

// OpenRouteGate reopens the already outer-validated fixed command.
func (v CommandView) OpenRouteGate() (routegate.Command, error) {
	if v.kind != CommandRouteGate || len(v.routeGateBytes) != routegate.CommandBytes {
		return routegate.Command{}, ErrEnvelopeSemantic
	}
	return routegate.OpenCommand(v.routeGateBytes)
}

var routeGatePhysicalWitnessDomain = []byte(
	"vibedb/replication/route-gate-physical-witness\x00",
)

// RouteGatePhysicalWitness returns the canonical physical shard authority
// addressed by one route-gate command. It deliberately excludes request and
// operation identity, so acquire and release of the same pin produce one
// stable witness; those logical fields are bound by the nested gate command.
func RouteGatePhysicalWitness(command CommandView) (Digest, bool) {
	if command.Kind() != CommandRouteGate ||
		command.AuthorityClass != CommandAuthorityData {
		return Digest{}, false
	}
	var storage [2*MaxIdentityBytes + 256]byte
	framed := append(storage[:0], routeGatePhysicalWitnessDomain...)
	framed = append(framed, command.ClusterID[:]...)
	framed = append(framed, command.ClusterIncarnation[:]...)
	framed = binary.LittleEndian.AppendUint64(framed, command.TopologyRecoveryEpoch)
	framed = binary.LittleEndian.AppendUint16(framed, uint16(len(command.Distribution)))
	framed = append(framed, command.Distribution...)
	framed = binary.LittleEndian.AppendUint16(framed, uint16(len(command.Shard)))
	framed = append(framed, command.Shard...)
	framed = append(framed, command.ShardIncarnation[:]...)
	framed = append(framed, command.GroupID[:]...)
	for _, value := range [...]uint64{
		command.AllocationGeneration, command.ReplicaSetVersion,
		command.ActivePolicyGeneration, command.ProtectionEpoch,
		command.OwnershipEpoch, command.SchemaGeneration,
		command.RoutingVersion, command.RouteGeneration,
	} {
		framed = binary.LittleEndian.AppendUint64(framed, value)
	}
	return Digest(sha256.Sum256(framed)), true
}

// TransactionIdentity reports the role and operation from the already
// validated transaction control. It does not reopen the nested control or
// materialize intent scopes. Non-transaction commands return ok=false.
func (v CommandView) TransactionIdentity() (
	distributedtxn.ReplicatedRole,
	distributedtxn.ReplicatedOperation,
	bool,
) {
	if v.kind != CommandTransaction || len(v.transactionBytes) == 0 ||
		v.transactionRole == distributedtxn.ReplicatedRoleInvalid ||
		v.transactionOp == distributedtxn.ReplicatedOperationInvalid {
		return distributedtxn.ReplicatedRoleInvalid,
			distributedtxn.ReplicatedOperationInvalid, false
	}
	return v.transactionRole, v.transactionOp, true
}

// OpenTransactionInto reopens the already outer-validated transaction control
// into caller-owned intent-scope scratch. The returned payload aliases the
// command envelope. A MaxIntentScopes scratch slice keeps this allocation-free.
func (v CommandView) OpenTransactionInto(
	scopes []distributedtxn.IntentScope,
) (distributedtxn.ReplicatedCommandView, error) {
	if v.kind != CommandTransaction || len(v.transactionBytes) == 0 {
		return distributedtxn.ReplicatedCommandView{}, ErrEnvelopeSemantic
	}
	return distributedtxn.OpenReplicatedCommandInto(v.transactionBytes, scopes)
}

// ExecutionPinBytes returns the exact validated nested command, borrowing the
// outer envelope. Non-execution-pin commands return nil.
func (v CommandView) ExecutionPinBytes() []byte {
	return v.executionPinBytes[:len(v.executionPinBytes):len(v.executionPinBytes)]
}

func (v CommandView) OpenExecutionPin() (executionpin.Command, error) {
	if v.kind != CommandExecutionPin || len(v.executionPinBytes) != executionpin.CommandBytes {
		return executionpin.Command{}, ErrEnvelopeSemantic
	}
	return executionpin.OpenCommand(v.executionPinBytes)
}

func (v CommandView) SplitCaptureActivationBytes() []byte {
	return v.splitCaptureBytes[:len(v.splitCaptureBytes):len(v.splitCaptureBytes)]
}

func (v CommandView) OpenSplitCaptureActivation() (splitcapture.View, error) {
	if v.kind != CommandSplitCaptureActivate {
		return splitcapture.View{}, ErrEnvelopeSemantic
	}
	return splitcapture.OpenCommand(v.splitCaptureBytes)
}

// RetainedPruneProof returns the validated fixed proof carried by a retained
// prune command. Other command kinds return ok=false.
func (v CommandView) RetainedPruneProof() (RetainedPruneProof, bool) {
	return v.retainedPrune, v.kind == CommandRetainedPrune && v.retainedPrune.Valid()
}

// ExecutionPinAuthorityDigest returns the portable authority witness sealed in
// execution-pin certificates. The shard wire first proves that the nested
// principal equals the authenticated transport Authority and that the class is
// the dedicated execution-pin capability; this digest then binds that
// principal to the exact catalog route fences observed by replicated apply.
func ExecutionPinAuthorityDigest(command CommandView) (Digest, bool) {
	if command.Kind() != CommandExecutionPin ||
		command.AuthorityClass != CommandAuthorityExecutionPin {
		return Digest{}, false
	}
	nested, err := command.OpenExecutionPin()
	if err != nil {
		return Digest{}, false
	}
	h := sha256.New()
	_, _ = h.Write(executionPinAuthorityDigestDomain)
	_, _ = h.Write(command.ClusterID[:])
	_, _ = h.Write(command.ClusterIncarnation[:])
	_, _ = h.Write(command.ShardIncarnation[:])
	_, _ = h.Write(command.GroupID[:])
	_, _ = h.Write([]byte{byte(command.AuthorityClass)})
	_, _ = h.Write(nested.AuthorityNode[:])
	var scalar [8]byte
	writeScalar := func(value uint64) {
		binary.LittleEndian.PutUint64(scalar[:], value)
		_, _ = h.Write(scalar[:])
	}
	writeBytes := func(value []byte) {
		writeScalar(uint64(len(value)))
		_, _ = h.Write(value)
	}
	writeScalar(nested.AuthorityGeneration)
	for _, value := range [...]uint64{
		command.TopologyRecoveryEpoch, command.AllocationGeneration,
		command.ReplicaSetVersion, command.ActivePolicyGeneration,
		command.ProtectionEpoch, command.OwnershipEpoch, command.SchemaGeneration,
		command.RoutingVersion, command.RouteGeneration,
	} {
		writeScalar(value)
	}
	writeBytes(command.Distribution)
	writeBytes(command.Shard)
	writeBytes(command.Tenant)
	var digest Digest
	_ = h.Sum(digest[:0])
	return digest, digest != (Digest{})
}

// Kind reports the relation-bundle or session-lifecycle operation.
func (v CommandView) Kind() CommandKind { return v.kind }

// MutationCount reports the total number of mutations across all relation
// batches.
func (v CommandView) MutationCount() int { return int(v.mutationCount) }

// RelationCount reports the number of ordered relation batches in the command.
// Session lifecycle commands always report zero.
func (v CommandView) RelationCount() int { return int(v.relationCount) }

// RelationBatchView is one borrowed relation batch. Its mutation bytes alias
// the validated command envelope and remain read-only for the view's lifetime.
type RelationBatchView struct {
	Relation      RelationID
	mutationBytes []byte
	mutationCount uint32
}

// MutationCount reports the number of mutations in this relation batch.
func (v RelationBatchView) MutationCount() int { return int(v.mutationCount) }

// MutationBytes returns the exact canonical mutation-frame payload for this
// relation. The result aliases the validated command and is capacity-clamped.
func (v RelationBatchView) MutationBytes() []byte {
	return v.mutationBytes[:len(v.mutationBytes):len(v.mutationBytes)]
}

// OpenRelationMutationBytes validates one detached canonical mutation-frame
// payload and returns a borrowed relation view without outer command framing.
func OpenRelationMutationBytes(
	relation RelationID,
	mutationCount uint32,
	raw []byte,
) (RelationBatchView, error) {
	if relation == 0 || relation > MaxRelationID || mutationCount == 0 ||
		mutationCount > MaxMutations || len(raw) > MaxCommandBytes {
		return RelationBatchView{}, ErrEnvelopeSemantic
	}
	if err := validateMutationBytes(raw, mutationCount); err != nil {
		return RelationBatchView{}, err
	}
	return RelationBatchView{
		Relation: relation, mutationBytes: raw[:len(raw):len(raw)], mutationCount: mutationCount,
	}, nil
}

// MutationView is one borrowed, validated mutation. Key and Value are
// capacity-clamped, read-only aliases into the command envelope.
type MutationView struct {
	Kind  MutationKind
	Key   []byte
	Value []byte
	// Compare aliases the canonical fixed compare payload for
	// digest-equal mutations and is empty for every other mutation.
	Compare []byte

	ExpectedValueLength uint64
	ExpectedValueDigest Digest
}

// MutationIterator walks a validated command without materializing a slice.
type MutationIterator struct {
	remaining uint32
	b         []byte
	current   MutationView
}

// Mutations returns a fresh iterator positioned before the first mutation in
// this relation batch.
func (v RelationBatchView) Mutations() MutationIterator {
	b := v.mutationBytes
	return MutationIterator{
		remaining: v.mutationCount,
		b:         b[:len(b):len(b)],
	}
}

// RelationBatchIterator walks validated relation batches without materializing
// a slice. Multi-relation framing carries exact payload lengths, so advancing
// between batches is constant time and does not rescan mutation frames.
type RelationBatchIterator struct {
	remaining uint16
	b         []byte
	inlineID  RelationID
	inlineN   uint32
	current   RelationBatchView
}

// RelationBatches returns a fresh iterator positioned before the first batch.
func (v CommandView) RelationBatches() RelationBatchIterator {
	b := v.relationBytes
	return RelationBatchIterator{
		remaining: v.relationCount,
		b:         b[:len(b):len(b)],
		inlineID:  v.inlineRelationID,
		inlineN:   v.mutationCount,
	}
}

// Next advances and reports whether one relation batch is available.
func (i *RelationBatchIterator) Next() bool {
	if i == nil || i.remaining == 0 {
		return false
	}
	if i.inlineID != 0 {
		i.current = RelationBatchView{
			Relation: i.inlineID, mutationBytes: i.b, mutationCount: i.inlineN,
		}
		i.b = i.b[len(i.b):len(i.b):len(i.b)]
		i.inlineID = 0
		i.inlineN = 0
		i.remaining--
		return true
	}
	header := i.b[:relationBatchHeaderBytes]
	count := binary.LittleEndian.Uint16(header[2:4])
	payloadBytes := int(binary.LittleEndian.Uint32(header[4:8]))
	start := relationBatchHeaderBytes
	end := start + payloadBytes
	i.current = RelationBatchView{
		Relation:      RelationID(binary.LittleEndian.Uint16(header[0:2])),
		mutationBytes: i.b[start:end:end],
		mutationCount: uint32(count),
	}
	i.b = i.b[end:len(i.b):len(i.b)]
	i.remaining--
	return true
}

// Batch returns the current relation batch. It is zero before the first
// successful Next and remains the final batch after exhaustion.
func (i *RelationBatchIterator) Batch() RelationBatchView {
	if i == nil {
		return RelationBatchView{}
	}
	return i.current
}

// Next advances and reports whether one mutation is available.
func (i *MutationIterator) Next() bool {
	if i == nil || i.remaining == 0 {
		return false
	}
	kind := MutationKind(i.b[0])
	keyLen := int(binary.LittleEndian.Uint16(i.b[2:4]))
	valueLen := int(binary.LittleEndian.Uint32(i.b[4:8]))
	keyStart := mutationHeaderBytes
	valueStart := keyStart + keyLen
	end := valueStart + valueLen
	i.current = MutationView{
		Kind:  kind,
		Key:   i.b[keyStart:valueStart:valueStart],
		Value: i.b[valueStart:end:end],
	}
	if kind == MutationDeleteDigestEqual || kind == MutationPutDigestEqual {
		i.current.Compare = i.b[valueStart:end:end]
		i.current.ExpectedValueLength = binary.LittleEndian.Uint64(i.b[valueStart : valueStart+8])
		copy(i.current.ExpectedValueDigest[:], i.b[valueStart+8:valueStart+mutationDigestCompareBytes])
		if kind == MutationDeleteDigestEqual {
			i.current.Value = nil
		} else {
			i.current.Compare = i.b[valueStart : valueStart+mutationDigestCompareBytes : valueStart+mutationDigestCompareBytes]
			i.current.Value = i.b[valueStart+mutationDigestCompareBytes : end : end]
		}
	}
	i.b = i.b[end:len(i.b):len(i.b)]
	i.remaining--
	return true
}

// Mutation returns the current mutation. It is zero before the first successful
// Next and remains the final mutation after exhaustion.
func (i *MutationIterator) Mutation() MutationView {
	if i == nil {
		return MutationView{}
	}
	return i.current
}

// AppendCommand appends one deterministic command. On validation failure it
// returns the original dst unchanged. A correctly pre-sized dst incurs no heap
// allocation. Input slices must not overlap the writable append region in dst's
// current backing array; such aliases are rejected before dst is modified.
func AppendCommand(dst []byte, command Command) ([]byte, error) {
	total, err := measureCommand(command)
	if err != nil {
		return dst, err
	}
	if commandOverlapsAppendRegion(dst, total, command) {
		return dst, semantic("command input aliases destination append region")
	}
	start := len(dst)
	dst = extendZeroed(dst, total)
	frame := dst[start:]

	copy(frame[0:8], commandMagic[:])
	appendU16(frame, 8, commandCodecSentinel)
	frame[10] = commandWireKind(command.Kind)
	frame[11] = byte(command.AuthorityClass)
	appendU16(frame, 12, commandHeaderBytes)
	appendU32(frame, 16, uint32(total))
	appendU32(frame, 20, uint32(total-commandHeaderBytes-envelopeChecksumBytes))
	mutationCount := commandMutationCount(command)
	appendU32(frame, 24, uint32(mutationCount))
	appendU16(frame, 28, uint16(len(command.Batches)))
	if len(command.Batches) == 1 {
		appendU16(frame, 30, uint16(command.Batches[0].Relation))
	}
	copy(frame[32:48], command.ClusterID[:])
	copy(frame[48:64], command.ClusterIncarnation[:])
	appendU64(frame, 64, command.TopologyRecoveryEpoch)
	copy(frame[72:88], command.ShardIncarnation[:])
	copy(frame[88:104], command.GroupID[:])
	appendU64(frame, 104, command.AllocationGeneration)
	appendU64(frame, 112, command.ReplicaSetVersion)
	appendU64(frame, 120, command.ActivePolicyGeneration)
	appendU64(frame, 128, command.ProtectionEpoch)
	appendU64(frame, 136, command.OwnershipEpoch)
	appendU64(frame, 144, command.SchemaGeneration)
	appendU64(frame, 152, command.RoutingVersion)
	appendU64(frame, 160, command.RouteGeneration)
	copy(frame[168:184], command.ClientID[:])
	appendU64(frame, 184, command.ClientEpoch)
	appendU64(frame, 192, command.ClientSequence)
	copy(frame[200:232], command.Fingerprint[:])
	copy(frame[232:240], command.RetryHome[:])
	appendU16(frame, 240, uint16(len(command.Tenant)))
	appendU16(frame, 242, uint16(len(command.Distribution)))
	appendU16(frame, 244, uint16(len(command.Shard)))
	appendU64(frame, 248, command.AckThrough)

	cursor := commandHeaderBytes
	cursor += copy(frame[cursor:], command.Tenant)
	cursor += copy(frame[cursor:], command.Distribution)
	cursor += copy(frame[cursor:], command.Shard)
	if commandCarriesLeaseBody(command.Kind) {
		appendU64(frame, cursor, uint64(command.ExpectedDeadlineUnixNano))
		appendU64(frame, cursor+8, uint64(command.NextDeadlineUnixNano))
		cursor += sessionLeaseBodyBytes
	}
	if command.Kind == CommandTransaction {
		appendU32(frame, cursor, uint32(len(command.Transaction)))
		cursor += transactionLengthBytes
		cursor += copy(frame[cursor:], command.Transaction)
	}
	if command.Kind == CommandRequestLedger {
		cursor += copy(frame[cursor:], command.RequestLedger)
	}
	if command.Kind == CommandRouteGate {
		cursor += copy(frame[cursor:], command.RouteGate)
	}
	if command.Kind == CommandExecutionPin {
		cursor += copy(frame[cursor:], command.ExecutionPin)
	}
	if command.Kind == CommandRetainedPrune {
		putRetainedPruneProof(frame[cursor:cursor+retainedPruneProofBytes], command.RetainedPrune)
		cursor += retainedPruneProofBytes
	}
	if command.Kind == CommandSplitCaptureActivate {
		cursor += copy(frame[cursor:], command.SplitCaptureActivation)
	}
	for batchIndex := range command.Batches {
		batch := &command.Batches[batchIndex]
		headerAt := -1
		if len(command.Batches) > 1 {
			headerAt = cursor
			appendU16(frame, cursor, uint16(batch.Relation))
			appendU16(frame, cursor+2, uint16(len(batch.Mutations)))
			cursor += relationBatchHeaderBytes
		}
		for mutationIndex := range batch.Mutations {
			mutation := &batch.Mutations[mutationIndex]
			valueBytes := mutationWireValueBytes(*mutation)
			frame[cursor] = byte(mutation.Kind)
			appendU16(frame, cursor+2, uint16(len(mutation.Key)))
			appendU32(frame, cursor+4, uint32(valueBytes))
			cursor += mutationHeaderBytes
			cursor += copy(frame[cursor:], mutation.Key)
			if mutation.Kind == MutationDeleteDigestEqual || mutation.Kind == MutationPutDigestEqual {
				appendU64(frame, cursor, mutation.ExpectedValueLength)
				copy(frame[cursor+8:cursor+mutationDigestCompareBytes], mutation.ExpectedValueDigest[:])
				cursor += mutationDigestCompareBytes
			}
			if mutation.Kind != MutationDeleteDigestEqual {
				cursor += copy(frame[cursor:], mutation.Value)
			}
		}
		if headerAt >= 0 {
			appendU32(
				frame, headerAt+4, uint32(cursor-headerAt-relationBatchHeaderBytes),
			)
		}
	}
	if cursor != total-envelopeChecksumBytes {
		panic("replication: measured command size diverged during encode")
	}
	sealEnvelope(frame)
	return dst, nil
}

// CommandSize returns the exact canonical envelope size without allocating or
// encoding. Callers with bounded reusable buffers can fail admission before an
// append would grow beyond their local byte budget.
func CommandSize(command Command) (int, error) {
	return measureCommand(command)
}

// TransactionCommandSize returns the exact final outer envelope size before
// the canonical transaction control body has been encoded. command.Transaction
// must be empty and transactionBytes is the exact future control-body length.
//
// This is a size-only admission preflight: it validates every outer identity,
// generation, client tuple, lease field, relation batch, and mutation, but it
// cannot parse or cross-bind the absent control body. Fingerprint may therefore
// be zero because the native fingerprint binds those future bytes. CommandSize
// and AppendCommand remain the final strict semantic authority after control
// encoding and fingerprint construction.
func TransactionCommandSize(command Command, transactionBytes int) (int, error) {
	if command.Kind != CommandTransaction || len(command.Transaction) != 0 ||
		transactionBytes <= 0 {
		return 0, semantic("transaction command size preflight")
	}
	if transactionBytes > distributedtxn.MaxReplicatedCommandBytes {
		return 0, ErrEnvelopeTooLarge
	}
	if len(command.Batches) != 0 {
		if err := validateRelationBatches(command.Batches); err != nil {
			return 0, err
		}
	}
	if err := validateCommandEnvelopeMetadata(command, false); err != nil {
		return 0, err
	}
	return measureValidatedCommand(command, transactionBytes)
}

func commandOverlapsAppendRegion(dst []byte, total int, command Command) bool {
	region := writableAppendRegion(dst, total)
	if len(region) == 0 {
		return false
	}
	if byteSlicesOverlap(region, command.Tenant) ||
		byteSliceStringOverlap(region, command.Distribution) ||
		byteSliceStringOverlap(region, command.Shard) ||
		byteSlicesOverlap(region, command.Transaction) ||
		byteSlicesOverlap(region, command.RequestLedger) ||
		byteSlicesOverlap(region, command.RouteGate) ||
		byteSlicesOverlap(region, command.ExecutionPin) ||
		byteSlicesOverlap(region, command.SplitCaptureActivation) ||
		typedSliceOverlapsBytes(region, command.Batches) {
		return true
	}
	for batchIndex := range command.Batches {
		if typedSliceOverlapsBytes(region, command.Batches[batchIndex].Mutations) {
			return true
		}
		for mutationIndex := range command.Batches[batchIndex].Mutations {
			mutation := &command.Batches[batchIndex].Mutations[mutationIndex]
			if byteSlicesOverlap(region, mutation.Key) ||
				byteSlicesOverlap(region, mutation.Value) {
				return true
			}
		}
	}
	return false
}

func commandWireKind(kind CommandKind) uint8 {
	switch kind {
	case CommandMutationBatch:
		return commandWireMutationBatch
	case CommandSessionRetire:
		return commandWireSessionRetire
	case CommandSessionRelease:
		return commandWireSessionRelease
	case CommandSessionOpen:
		return commandWireSessionOpen
	case CommandSessionRenew:
		return commandWireSessionRenew
	case CommandSessionRevoke:
		return commandWireSessionRevoke
	case CommandTransaction:
		return commandWireTransaction
	case CommandRequestLedger:
		return commandWireRequestLedger
	case CommandRouteGate:
		return commandWireRouteGate
	case CommandExecutionPin:
		return commandWireExecutionPin
	case CommandRetainedPrune:
		return commandWireRetainedPrune
	case CommandSplitCaptureActivate:
		return commandWireSplitCaptureActivate
	default:
		panic("replication: validated command kind has no wire encoding")
	}
}

func measureCommand(command Command) (int, error) {
	if err := validateCommandHeader(command); err != nil {
		return 0, err
	}
	return measureValidatedCommand(command, len(command.Transaction))
}

func measureValidatedCommand(command Command, transactionBytes int) (int, error) {
	total := uint64(commandHeaderBytes + envelopeChecksumBytes)
	for _, size := range [...]int{
		len(command.Tenant), len(command.Distribution),
		len(command.Shard),
	} {
		var ok bool
		total, ok = checkedAdd(total, uint64(size), MaxCommandBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	if commandCarriesLeaseBody(command.Kind) {
		var ok bool
		total, ok = checkedAdd(total, sessionLeaseBodyBytes, MaxCommandBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	if command.Kind == CommandTransaction {
		var ok bool
		total, ok = checkedAdd(
			total, uint64(transactionLengthBytes+transactionBytes), MaxCommandBytes,
		)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	if command.Kind == CommandRequestLedger {
		var ok bool
		total, ok = checkedAdd(total, uint64(len(command.RequestLedger)), MaxCommandBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	if command.Kind == CommandRouteGate {
		var ok bool
		total, ok = checkedAdd(total, routegate.CommandBytes, MaxCommandBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	if command.Kind == CommandExecutionPin {
		var ok bool
		total, ok = checkedAdd(total, executionpin.CommandBytes, MaxCommandBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	if command.Kind == CommandRetainedPrune {
		var ok bool
		total, ok = checkedAdd(total, retainedPruneProofBytes, MaxCommandBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	if command.Kind == CommandSplitCaptureActivate {
		var ok bool
		total, ok = checkedAdd(total, uint64(len(command.SplitCaptureActivation)), MaxCommandBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	if len(command.Batches) > 1 {
		var ok bool
		total, ok = checkedAdd(
			total, uint64(len(command.Batches)*relationBatchHeaderBytes), MaxCommandBytes,
		)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	for batchIndex := range command.Batches {
		batch := &command.Batches[batchIndex]
		for mutationIndex := range batch.Mutations {
			mutation := &batch.Mutations[mutationIndex]
			if err := validateMutation(*mutation); err != nil {
				return 0, err
			}
			var ok bool
			total, ok = checkedAdd(total, mutationHeaderBytes, MaxCommandBytes)
			if ok {
				total, ok = checkedAdd(total, uint64(len(mutation.Key)), MaxCommandBytes)
			}
			if ok {
				total, ok = checkedAdd(total, uint64(mutationWireValueBytes(*mutation)), MaxCommandBytes)
			}
			if !ok {
				return 0, ErrEnvelopeTooLarge
			}
		}
	}
	return int(total), nil
}

func validateCommandHeader(command Command) error {
	if !validCommandAuthorityClass(command.AuthorityClass) {
		return semantic("command authority class")
	}
	if command.Kind != CommandRetainedPrune && command.RetainedPrune != (RetainedPruneProof{}) {
		return semantic("unrelated retained prune proof")
	}
	if command.Kind != CommandSplitCaptureActivate && len(command.SplitCaptureActivation) != 0 {
		return semantic("unrelated split capture payload")
	}
	if (command.Kind == CommandRequestLedger) !=
		(command.AuthorityClass == CommandAuthorityRequestLedger) {
		return semantic("request ledger authority class")
	}
	if (command.Kind == CommandExecutionPin &&
		command.AuthorityClass != CommandAuthorityExecutionPin) ||
		(command.Kind != CommandExecutionPin &&
			command.AuthorityClass == CommandAuthorityExecutionPin &&
			!commandKindIsSessionLifecycle(command.Kind)) {
		return semantic("execution-pin authority class")
	}
	switch command.Kind {
	case CommandMutationBatch, CommandRetainedPrune:
		if len(command.Transaction) != 0 || len(command.RequestLedger) != 0 ||
			len(command.RouteGate) != 0 || len(command.ExecutionPin) != 0 {
			return semantic("ordinary command carries transaction control")
		}
		if err := validateRelationBatches(command.Batches); err != nil {
			return err
		}
		if command.Kind == CommandRetainedPrune {
			if command.AuthorityClass != CommandAuthorityTopology ||
				!command.RetainedPrune.Valid() ||
				command.RetainedPrune.BatchDigest != command.Fingerprint {
				return semantic("retained prune proof")
			}
			for batchIndex := range command.Batches {
				for mutationIndex := range command.Batches[batchIndex].Mutations {
					kind := command.Batches[batchIndex].Mutations[mutationIndex].Kind
					if kind != MutationDelete && kind != MutationDeleteDigestEqual {
						return semantic("retained prune carries non-delete mutation")
					}
				}
			}
		}
	case CommandSessionRetire, CommandSessionRelease, CommandSessionOpen,
		CommandSessionRenew, CommandSessionRevoke:
		if len(command.Batches) != 0 || len(command.Transaction) != 0 ||
			len(command.RequestLedger) != 0 || len(command.RouteGate) != 0 ||
			len(command.ExecutionPin) != 0 {
			return semantic("session lifecycle command carries payload")
		}
	case CommandTransaction:
		if len(command.RequestLedger) != 0 || len(command.RouteGate) != 0 ||
			len(command.ExecutionPin) != 0 {
			return semantic("transaction command carries unrelated control")
		}
		control, err := validatedTransactionControl(command.Transaction)
		if err != nil {
			return semantic("transaction control")
		}
		if err := validateTransactionClientIdentity(
			command.ClientID, command.ClientEpoch, command.ClientSequence, command.AckThrough, control,
		); err != nil {
			return err
		}
		if !transactionOperationCarriesRelationBatches(control.operation) {
			if len(command.Batches) != 0 {
				return semantic("transaction operation carries relation batches")
			}
		} else {
			if err := validateRelationBatches(command.Batches); err != nil {
				return err
			}
			digest, err := TransactionMutationDigest(command.Batches)
			if err != nil || digest != control.mutationDigest {
				return semantic("transaction mutation digest")
			}
		}
	case CommandRequestLedger:
		if len(command.Batches) != 0 || len(command.Transaction) != 0 ||
			len(command.RequestLedger) == 0 || len(command.RouteGate) != 0 ||
			len(command.ExecutionPin) != 0 {
			return semantic("request ledger command body")
		}
		if err := requestledger.ValidateCommand(command.RequestLedger); err != nil {
			return semantic("request ledger command body")
		}
	case CommandRouteGate:
		gate, gateErr := routegate.OpenCommand(command.RouteGate)
		if len(command.Batches) != 0 || len(command.Transaction) != 0 ||
			len(command.RequestLedger) != 0 ||
			len(command.ExecutionPin) != 0 ||
			len(command.RouteGate) != routegate.CommandBytes || gateErr != nil ||
			!routeGateAuthorityMatches(command.AuthorityClass, gate.Operation) {
			return semantic("route-gate command")
		}
	case CommandExecutionPin:
		if command.AuthorityClass != CommandAuthorityExecutionPin || len(command.Batches) != 0 ||
			len(command.Transaction) != 0 || len(command.RequestLedger) != 0 ||
			len(command.RouteGate) != 0 ||
			len(command.ExecutionPin) != executionpin.CommandBytes {
			return semantic("execution-pin command payload or authority")
		}
		if _, err := executionpin.OpenCommand(command.ExecutionPin); err != nil {
			return semantic("execution-pin command")
		}
	case CommandSplitCaptureActivate:
		if command.AuthorityClass != CommandAuthorityTopology || len(command.Batches) != 0 ||
			len(command.Transaction) != 0 || len(command.RequestLedger) != 0 ||
			len(command.RouteGate) != 0 || len(command.ExecutionPin) != 0 ||
			len(command.SplitCaptureActivation) == 0 {
			return semantic("split capture command payload")
		}
		if _, err := splitcapture.OpenCommand(command.SplitCaptureActivation); err != nil {
			return semantic("split capture command")
		}
	default:
		return semantic("unknown command kind")
	}
	return validateCommandEnvelopeMetadata(command, true)
}

func commandKindIsSessionLifecycle(kind CommandKind) bool {
	switch kind {
	case CommandSessionRetire, CommandSessionRelease, CommandSessionOpen,
		CommandSessionRenew, CommandSessionRevoke:
		return true
	default:
		return false
	}
}

func validateCommandEnvelopeMetadata(command Command, requireFingerprint bool) error {
	if !nonzero128(command.ClusterID) || !nonzero128(command.ClusterIncarnation) ||
		!nonzero128(command.ShardIncarnation) || !nonzero128(command.GroupID) ||
		!nonzero128(command.ClientID) {
		return semantic("command contains a zero identity")
	}
	if command.TopologyRecoveryEpoch == 0 || command.AllocationGeneration == 0 ||
		command.ReplicaSetVersion == 0 || command.ActivePolicyGeneration == 0 ||
		command.ProtectionEpoch == 0 || command.OwnershipEpoch == 0 ||
		command.SchemaGeneration == 0 || command.RoutingVersion == 0 ||
		command.RouteGeneration == 0 {
		return semantic("command contains a zero generation, epoch, or sequence")
	}
	if err := validateCommandClientTuple(
		command.Kind, command.ClientEpoch, command.ClientSequence, command.AckThrough,
	); err != nil {
		return err
	}
	if err := validateCommandLease(
		command.Kind, command.ExpectedDeadlineUnixNano, command.NextDeadlineUnixNano,
	); err != nil {
		return err
	}
	if requireFingerprint && !nonzeroDigest(command.Fingerprint) {
		return semantic("command contains a zero request fingerprint")
	}
	if len(command.Tenant) == 0 || len(command.Tenant) > MaxIdentityBytes {
		return semantic("tenant identity length")
	}
	if err := validateTextIdentity("distribution", command.Distribution, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateTextIdentity("shard", command.Shard, MaxIdentityBytes); err != nil {
		return err
	}
	return nil
}

func transactionOperationCarriesRelationBatches(
	operation distributedtxn.ReplicatedOperation,
) bool {
	switch operation {
	case distributedtxn.ReplicatedStageParticipant,
		distributedtxn.ReplicatedBeginPrepareCoordinator,
		distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		distributedtxn.ReplicatedStagePrepareParticipant:
		return true
	default:
		return false
	}
}

func validateRelationBatches(batches []RelationMutationBatch) error {
	if len(batches) == 0 || len(batches) > MaxRelationBatches {
		return semantic("relation batch count")
	}
	mutations := 0
	var previous RelationID
	for index := range batches {
		batch := &batches[index]
		if batch.Relation == 0 || batch.Relation > MaxRelationID ||
			index != 0 && batch.Relation <= previous {
			return semantic("relation batch order or identity")
		}
		if len(batch.Mutations) == 0 ||
			len(batches) > 1 && len(batch.Mutations) > 1<<16-1 ||
			len(batch.Mutations) > MaxMutations-mutations {
			return semantic("mutation count")
		}
		mutations += len(batch.Mutations)
		previous = batch.Relation
	}
	return nil
}

// TransactionMutationDigest returns the domain-separated SHA-256 identity of
// canonical native relation batches. The fixed framing prefix binds the
// compact singleton relation ID and the batch/mutation counts, which otherwise
// live in the outer command header rather than singleton relation bytes.
func TransactionMutationDigest(
	batches []RelationMutationBatch,
) (distributedtxn.Digest, error) {
	if err := validateRelationBatches(batches); err != nil {
		return distributedtxn.Digest{}, err
	}
	var framing [8]byte
	binary.LittleEndian.PutUint16(framing[0:2], uint16(len(batches)))
	if len(batches) == 1 {
		binary.LittleEndian.PutUint16(framing[2:4], uint16(batches[0].Relation))
	}
	binary.LittleEndian.PutUint32(framing[4:8], uint32(commandMutationCount(Command{Batches: batches})))
	h := sha256.New()
	canonicalBytes := uint64(0)
	for batchIndex := range batches {
		batch := &batches[batchIndex]
		if len(batches) > 1 {
			var header [relationBatchHeaderBytes]byte
			binary.LittleEndian.PutUint16(header[0:2], uint16(batch.Relation))
			binary.LittleEndian.PutUint16(header[2:4], uint16(len(batch.Mutations)))
			payloadBytes := uint64(0)
			for mutationIndex := range batch.Mutations {
				mutation := batch.Mutations[mutationIndex]
				if err := validateMutation(mutation); err != nil {
					return distributedtxn.Digest{}, err
				}
				mutationBytes := uint64(mutationHeaderBytes + len(mutation.Key) + mutationWireValueBytes(mutation))
				var ok bool
				payloadBytes, ok = checkedAdd(payloadBytes, mutationBytes, MaxCommandBytes)
				if !ok {
					return distributedtxn.Digest{}, ErrEnvelopeTooLarge
				}
			}
			var ok bool
			canonicalBytes, ok = checkedAdd(
				canonicalBytes, uint64(relationBatchHeaderBytes)+payloadBytes, MaxCommandBytes,
			)
			if !ok {
				return distributedtxn.Digest{}, ErrEnvelopeTooLarge
			}
			binary.LittleEndian.PutUint32(header[4:8], uint32(payloadBytes))
			_, _ = h.Write(header[:])
		}
		for mutationIndex := range batch.Mutations {
			mutation := batch.Mutations[mutationIndex]
			if len(batches) == 1 {
				if err := validateMutation(mutation); err != nil {
					return distributedtxn.Digest{}, err
				}
				mutationBytes := uint64(mutationHeaderBytes + len(mutation.Key) + mutationWireValueBytes(mutation))
				var ok bool
				canonicalBytes, ok = checkedAdd(canonicalBytes, mutationBytes, MaxCommandBytes)
				if !ok {
					return distributedtxn.Digest{}, ErrEnvelopeTooLarge
				}
			}
			var header [mutationHeaderBytes]byte
			header[0] = byte(mutation.Kind)
			binary.LittleEndian.PutUint16(header[2:4], uint16(len(mutation.Key)))
			binary.LittleEndian.PutUint32(header[4:8], uint32(mutationWireValueBytes(mutation)))
			_, _ = h.Write(header[:])
			_, _ = h.Write(mutation.Key)
			if mutation.Kind == MutationDeleteDigestEqual || mutation.Kind == MutationPutDigestEqual {
				var compare [mutationDigestCompareBytes]byte
				binary.LittleEndian.PutUint64(compare[:8], mutation.ExpectedValueLength)
				copy(compare[8:], mutation.ExpectedValueDigest[:])
				_, _ = h.Write(compare[:])
			}
			if mutation.Kind != MutationDeleteDigestEqual {
				_, _ = h.Write(mutation.Value)
			}
		}
	}
	var canonicalDigest [sha256.Size]byte
	h.Sum(canonicalDigest[:0])
	return finishTransactionMutationDigest(framing, canonicalDigest), nil
}

// TransactionMutationDigester reuses one SHA-256 state across an ordered
// participant stream. It is not safe for concurrent calls. Reset is implicit
// in Digest, and the result is byte-identical to TransactionMutationDigest.
type TransactionMutationDigester struct {
	hash            hash.Hash
	framing         [8]byte
	relationHeader  [relationBatchHeaderBytes]byte
	mutationHeader  [mutationHeaderBytes]byte
	compare         [mutationDigestCompareBytes]byte
	canonicalDigest [sha256.Size]byte
}

// Digest returns the canonical native relation-batch identity while retaining
// only reusable fixed hash state between calls.
func (digester *TransactionMutationDigester) Digest(
	batches []RelationMutationBatch,
) (distributedtxn.Digest, error) {
	if err := validateRelationBatches(batches); err != nil {
		return distributedtxn.Digest{}, err
	}
	clear(digester.framing[:])
	binary.LittleEndian.PutUint16(digester.framing[0:2], uint16(len(batches)))
	if len(batches) == 1 {
		binary.LittleEndian.PutUint16(digester.framing[2:4], uint16(batches[0].Relation))
	}
	binary.LittleEndian.PutUint32(digester.framing[4:8], uint32(commandMutationCount(Command{Batches: batches})))
	if digester == nil {
		return distributedtxn.Digest{}, ErrEnvelopeSemantic
	}
	if digester.hash == nil {
		digester.hash = sha256.New()
	} else {
		digester.hash.Reset()
	}
	h := digester.hash
	canonicalBytes := uint64(0)
	for batchIndex := range batches {
		batch := &batches[batchIndex]
		if len(batches) > 1 {
			clear(digester.relationHeader[:])
			binary.LittleEndian.PutUint16(digester.relationHeader[0:2], uint16(batch.Relation))
			binary.LittleEndian.PutUint16(digester.relationHeader[2:4], uint16(len(batch.Mutations)))
			payloadBytes := uint64(0)
			for mutationIndex := range batch.Mutations {
				mutation := batch.Mutations[mutationIndex]
				if err := validateMutation(mutation); err != nil {
					return distributedtxn.Digest{}, err
				}
				mutationBytes := uint64(mutationHeaderBytes + len(mutation.Key) + mutationWireValueBytes(mutation))
				var ok bool
				payloadBytes, ok = checkedAdd(payloadBytes, mutationBytes, MaxCommandBytes)
				if !ok {
					return distributedtxn.Digest{}, ErrEnvelopeTooLarge
				}
			}
			var ok bool
			canonicalBytes, ok = checkedAdd(
				canonicalBytes, uint64(relationBatchHeaderBytes)+payloadBytes, MaxCommandBytes,
			)
			if !ok {
				return distributedtxn.Digest{}, ErrEnvelopeTooLarge
			}
			binary.LittleEndian.PutUint32(digester.relationHeader[4:8], uint32(payloadBytes))
			_, _ = h.Write(digester.relationHeader[:])
		}
		for mutationIndex := range batch.Mutations {
			mutation := batch.Mutations[mutationIndex]
			if len(batches) == 1 {
				if err := validateMutation(mutation); err != nil {
					return distributedtxn.Digest{}, err
				}
				mutationBytes := uint64(mutationHeaderBytes + len(mutation.Key) + mutationWireValueBytes(mutation))
				var ok bool
				canonicalBytes, ok = checkedAdd(canonicalBytes, mutationBytes, MaxCommandBytes)
				if !ok {
					return distributedtxn.Digest{}, ErrEnvelopeTooLarge
				}
			}
			clear(digester.mutationHeader[:])
			digester.mutationHeader[0] = byte(mutation.Kind)
			binary.LittleEndian.PutUint16(digester.mutationHeader[2:4], uint16(len(mutation.Key)))
			binary.LittleEndian.PutUint32(digester.mutationHeader[4:8], uint32(mutationWireValueBytes(mutation)))
			_, _ = h.Write(digester.mutationHeader[:])
			_, _ = h.Write(mutation.Key)
			if mutation.Kind == MutationDeleteDigestEqual || mutation.Kind == MutationPutDigestEqual {
				binary.LittleEndian.PutUint64(digester.compare[:8], mutation.ExpectedValueLength)
				copy(digester.compare[8:], mutation.ExpectedValueDigest[:])
				_, _ = h.Write(digester.compare[:])
			}
			if mutation.Kind != MutationDeleteDigestEqual {
				_, _ = h.Write(mutation.Value)
			}
		}
	}
	h.Sum(digester.canonicalDigest[:0])
	return finishTransactionMutationDigest(digester.framing, digester.canonicalDigest), nil
}

func transactionMutationDigestFromBytes(
	relationBytes []byte,
	totalMutations uint32,
	relationCount uint16,
	inlineRelationID RelationID,
) distributedtxn.Digest {
	var framing [8]byte
	binary.LittleEndian.PutUint16(framing[0:2], relationCount)
	binary.LittleEndian.PutUint16(framing[2:4], uint16(inlineRelationID))
	binary.LittleEndian.PutUint32(framing[4:8], totalMutations)
	canonicalDigest := sha256.Sum256(relationBytes)
	return finishTransactionMutationDigest(framing, canonicalDigest)
}

func finishTransactionMutationDigest(
	framing [8]byte,
	canonicalDigest [sha256.Size]byte,
) distributedtxn.Digest {
	var material [len(transactionMutationDigestDomain) + 8 + sha256.Size]byte
	cursor := copy(material[:], transactionMutationDigestDomain[:])
	cursor += copy(material[cursor:], framing[:])
	copy(material[cursor:], canonicalDigest[:])
	return distributedtxn.Digest(sha256.Sum256(material[:]))
}

type transactionControlMetadata struct {
	role             distributedtxn.ReplicatedRole
	operation        distributedtxn.ReplicatedOperation
	id               distributedtxn.ID
	expectedRevision uint64
	mutationDigest   distributedtxn.Digest
	manifestIndex    uint32
	recoveryPulse    uint8
}

func validatedTransactionControl(raw []byte) (transactionControlMetadata, error) {
	if err := distributedtxn.ValidateReplicatedCommand(raw); err != nil {
		return transactionControlMetadata{}, err
	}
	control := transactionControlMetadata{
		role:             distributedtxn.ReplicatedRole(raw[5]),
		operation:        distributedtxn.ReplicatedOperation(raw[6]),
		expectedRevision: binary.LittleEndian.Uint64(raw[24:32]),
		recoveryPulse:    raw[121],
	}
	copy(control.id[:], raw[32:48])
	copy(control.mutationDigest[:], raw[88:120])
	headerBytes := int(binary.LittleEndian.Uint16(raw[8:10]))
	if control.operation == distributedtxn.ReplicatedStageManifestSegment {
		// Validated VTRC metadata guarantees this operation has no scopes and a
		// canonical VTM1 payload beginning at the authenticated header boundary.
		control.manifestIndex = binary.LittleEndian.Uint32(raw[headerBytes+8 : headerBytes+12])
	} else if control.operation == distributedtxn.ReplicatedAppendManifestSegments {
		segments, openErr := distributedtxn.OpenManifestSegmentSequence(
			raw[headerBytes : len(raw)-4],
		)
		if openErr != nil {
			return transactionControlMetadata{}, openErr
		}
		control.manifestIndex = segments.FirstIndex()
	}
	return control, nil
}

// TransactionClientSequence derives the sole legal replicated retry sequence
// for an exact canonical transaction control body. Coordinator decisions use
// disjoint high-bit namespaces; participant CAS transitions at one revision
// intentionally share a sequence and therefore cannot alias as distinct work.
func TransactionClientSequence(control []byte) (uint64, error) {
	view, err := validatedTransactionControl(control)
	if err != nil {
		return 0, semantic("transaction control")
	}
	return transactionClientSequence(view)
}

func transactionClientSequence(control transactionControlMetadata) (uint64, error) {
	switch control.operation {
	case distributedtxn.ReplicatedStageCoordinator,
		distributedtxn.ReplicatedStageManifestCoordinator,
		distributedtxn.ReplicatedStageParticipant,
		distributedtxn.ReplicatedBeginPrepareCoordinator,
		distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		distributedtxn.ReplicatedStagePrepareParticipant:
		return 1, nil
	case distributedtxn.ReplicatedStageManifestSegment,
		distributedtxn.ReplicatedAppendManifestSegments:
		// Page zero follows the atomic coordinator begin at sequence two.
		return 2 + uint64(control.manifestIndex), nil
	case distributedtxn.ReplicatedCommitCoordinator,
		distributedtxn.ReplicatedAbortCoordinator:
		if control.expectedRevision >= transactionCoordinatorRevisionMax {
			return 0, semantic("transaction coordinator revision")
		}
		return transactionCoordinatorDecisionTag | control.expectedRevision, nil
	case distributedtxn.ReplicatedPulseCoordinator:
		return transactionCoordinatorPulseTag | uint64(control.recoveryPulse), nil
	case distributedtxn.ReplicatedRetireCoordinator:
		if control.expectedRevision >= transactionCoordinatorRevisionMax {
			return 0, semantic("transaction coordinator revision")
		}
		return transactionCoordinatorRetireTag | control.expectedRevision, nil
	case distributedtxn.ReplicatedPrepareParticipant,
		distributedtxn.ReplicatedApplyParticipant,
		distributedtxn.ReplicatedAbortParticipant,
		distributedtxn.ReplicatedReleaseParticipant,
		distributedtxn.ReplicatedApplyReleaseParticipant,
		distributedtxn.ReplicatedAbortReleaseParticipant:
		if control.expectedRevision == ^uint64(0) {
			return 0, semantic("transaction participant revision")
		}
		return control.expectedRevision + 1, nil
	default:
		return 0, semantic("transaction operation")
	}
}

func validateTransactionClientIdentity(
	clientID ID128,
	clientEpoch, clientSequence, ackThrough uint64,
	control transactionControlMetadata,
) error {
	wantEpoch := transactionCoordinatorEpoch
	if control.role == distributedtxn.ReplicatedRoleParticipant {
		wantEpoch = transactionParticipantEpoch
	}
	wantSequence, err := transactionClientSequence(control)
	if err != nil {
		return err
	}
	if clientID != ID128(control.id) || clientEpoch != wantEpoch ||
		clientSequence != wantSequence || ackThrough != 0 {
		return semantic("transaction client identity")
	}
	return nil
}

func commandMutationCount(command Command) int {
	count := 0
	for index := range command.Batches {
		count += len(command.Batches[index].Mutations)
	}
	return count
}

func validateTextIdentity(field, value string, limit int) error {
	if value == "" || len(value) > limit {
		return semantic(field + " identity length")
	}
	if !utf8.ValidString(value) {
		return semantic(field + " identity is not valid UTF-8")
	}
	return nil
}

func validateMutation(mutation Mutation) error {
	if len(mutation.Key) == 0 || len(mutation.Key) > MaxMutationKeyBytes {
		return semantic("mutation key length")
	}
	zeroExpected := mutation.ExpectedValueLength == 0 &&
		mutation.ExpectedValueDigest == (Digest{})
	switch mutation.Kind {
	case MutationPut, MutationPutAbsentOrEqual, MutationPutAbsent, MutationPutPresent:
		if len(mutation.Value) == 0 || len(mutation.Value) > MaxMutationValueBytes {
			return semantic("put value length")
		}
		if !zeroExpected {
			return semantic("put carries delete compare")
		}
	case MutationDelete:
		if len(mutation.Value) != 0 || !zeroExpected {
			return semantic("delete carries value bytes")
		}
	case MutationDeleteDigestEqual:
		if len(mutation.Value) != 0 || mutation.ExpectedValueLength == 0 ||
			mutation.ExpectedValueLength > MaxMutationValueBytes ||
			mutation.ExpectedValueDigest == (Digest{}) {
			return semantic("delete compare value identity")
		}
	case MutationPutDigestEqual:
		if len(mutation.Value) == 0 || len(mutation.Value) > MaxMutationValueBytes ||
			mutation.ExpectedValueLength == 0 ||
			mutation.ExpectedValueLength > MaxMutationValueBytes ||
			mutation.ExpectedValueDigest == (Digest{}) {
			return semantic("put compare value identity")
		}
	default:
		return semantic("unknown mutation kind")
	}
	return nil
}

func mutationWireValueBytes(mutation Mutation) int {
	if mutation.Kind == MutationDeleteDigestEqual {
		return mutationDigestCompareBytes
	}
	if mutation.Kind == MutationPutDigestEqual {
		return mutationDigestCompareBytes + len(mutation.Value)
	}
	return len(mutation.Value)
}

// OpenCommand validates one exact command envelope and returns a borrowed
// view. It performs no allocation on valid input.
func OpenCommand(src []byte) (CommandView, error) {
	if len(src) < commandHeaderBytes+envelopeChecksumBytes {
		return CommandView{}, corrupt("short command")
	}
	if len(src) > MaxCommandBytes {
		return CommandView{}, ErrEnvelopeTooLarge
	}
	if !bytes.Equal(src[:8], commandMagic[:]) {
		return CommandView{}, corrupt("command magic")
	}
	sentinel := binary.LittleEndian.Uint16(src[8:10])
	if sentinel != commandCodecSentinel {
		return CommandView{}, unsupportedCommandSentinel(sentinel)
	}
	if binary.LittleEndian.Uint16(src[12:14]) != commandHeaderBytes {
		return CommandView{}, corrupt("command header size")
	}
	total := binary.LittleEndian.Uint32(src[16:20])
	bodyBytes := binary.LittleEndian.Uint32(src[20:24])
	if uint64(total) != uint64(len(src)) ||
		uint64(bodyBytes)+commandHeaderBytes+envelopeChecksumBytes != uint64(total) {
		return CommandView{}, corrupt("command total or body length")
	}
	if err := verifyEnvelopeChecksum(src); err != nil {
		return CommandView{}, err
	}
	kind, ok := openCommandKind(src[10])
	authorityClass := CommandAuthorityClass(src[11])
	if !ok || !validCommandAuthorityClass(authorityClass) ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 ||
		binary.LittleEndian.Uint16(src[246:248]) != 0 {
		return CommandView{}, semantic("command kind, flags, or reserved bytes")
	}
	if (kind == CommandRequestLedger) !=
		(authorityClass == CommandAuthorityRequestLedger) {
		return CommandView{}, semantic("request ledger authority class")
	}
	if (kind == CommandExecutionPin && authorityClass != CommandAuthorityExecutionPin) ||
		(kind != CommandExecutionPin && authorityClass == CommandAuthorityExecutionPin &&
			!commandKindIsSessionLifecycle(kind)) {
		return CommandView{}, semantic("execution-pin authority class")
	}
	count := binary.LittleEndian.Uint32(src[24:28])
	relationCount := binary.LittleEndian.Uint16(src[28:30])
	inlineRelationID := RelationID(binary.LittleEndian.Uint16(src[30:32]))
	switch kind {
	case CommandMutationBatch, CommandRetainedPrune:
		if count == 0 || uint64(count) > MaxMutations || relationCount == 0 ||
			relationCount > MaxRelationBatches ||
			(relationCount == 1) != (inlineRelationID != 0) ||
			inlineRelationID > MaxRelationID {
			return CommandView{}, semantic("mutation or relation batch count")
		}
	case CommandSessionRetire, CommandSessionRelease, CommandSessionOpen,
		CommandSessionRenew, CommandSessionRevoke:
		if count != 0 || relationCount != 0 || inlineRelationID != 0 {
			return CommandView{}, semantic("session lifecycle command carries relation batches")
		}
	case CommandTransaction:
		if count == 0 && relationCount == 0 && inlineRelationID == 0 {
			break
		}
		if count == 0 || relationCount == 0 || uint64(count) > MaxMutations ||
			relationCount > MaxRelationBatches ||
			(relationCount == 1) != (inlineRelationID != 0) ||
			inlineRelationID > MaxRelationID {
			return CommandView{}, semantic("transaction mutation or relation batch count")
		}
	case CommandRequestLedger:
		if count != 0 || relationCount != 0 || inlineRelationID != 0 {
			return CommandView{}, semantic("request ledger command carries relation batches")
		}
	case CommandRouteGate:
		if count != 0 || relationCount != 0 ||
			inlineRelationID != 0 {
			return CommandView{}, semantic("route-gate command header")
		}
	case CommandExecutionPin:
		if authorityClass != CommandAuthorityExecutionPin || count != 0 || relationCount != 0 ||
			inlineRelationID != 0 {
			return CommandView{}, semantic("execution-pin command header")
		}
	case CommandSplitCaptureActivate:
		if authorityClass != CommandAuthorityTopology || count != 0 || relationCount != 0 || inlineRelationID != 0 {
			return CommandView{}, semantic("split capture command header")
		}
	}

	view := CommandView{kind: kind, AuthorityClass: authorityClass}
	copy(view.ClusterID[:], src[32:48])
	copy(view.ClusterIncarnation[:], src[48:64])
	view.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(src[64:72])
	copy(view.ShardIncarnation[:], src[72:88])
	copy(view.GroupID[:], src[88:104])
	view.AllocationGeneration = binary.LittleEndian.Uint64(src[104:112])
	view.ReplicaSetVersion = binary.LittleEndian.Uint64(src[112:120])
	view.ActivePolicyGeneration = binary.LittleEndian.Uint64(src[120:128])
	view.ProtectionEpoch = binary.LittleEndian.Uint64(src[128:136])
	view.OwnershipEpoch = binary.LittleEndian.Uint64(src[136:144])
	view.SchemaGeneration = binary.LittleEndian.Uint64(src[144:152])
	view.RoutingVersion = binary.LittleEndian.Uint64(src[152:160])
	view.RouteGeneration = binary.LittleEndian.Uint64(src[160:168])
	copy(view.ClientID[:], src[168:184])
	view.ClientEpoch = binary.LittleEndian.Uint64(src[184:192])
	view.ClientSequence = binary.LittleEndian.Uint64(src[192:200])
	copy(view.Fingerprint[:], src[200:232])
	copy(view.RetryHome[:], src[232:240])
	view.AckThrough = binary.LittleEndian.Uint64(src[248:256])

	tenantLen := int(binary.LittleEndian.Uint16(src[240:242]))
	distributionLen := int(binary.LittleEndian.Uint16(src[242:244]))
	shardLen := int(binary.LittleEndian.Uint16(src[244:246]))
	identityBytes := tenantLen + distributionLen + shardLen
	bodyEnd := len(src) - envelopeChecksumBytes
	if tenantLen == 0 || tenantLen > MaxIdentityBytes ||
		distributionLen == 0 || distributionLen > MaxIdentityBytes ||
		shardLen == 0 || shardLen > MaxIdentityBytes ||
		identityBytes < 0 || identityBytes > bodyEnd-commandHeaderBytes {
		return CommandView{}, semantic("command identity lengths")
	}
	cursor := commandHeaderBytes
	end := cursor + tenantLen
	view.Tenant = src[cursor:end:end]
	cursor = end
	end = cursor + distributionLen
	view.Distribution = src[cursor:end:end]
	cursor = end
	end = cursor + shardLen
	view.Shard = src[cursor:end:end]
	cursor = end
	if !utf8.Valid(view.Distribution) || !utf8.Valid(view.Shard) {
		return CommandView{}, semantic("command text identity is not valid UTF-8")
	}
	if err := validateDecodedCommandScalars(view); err != nil {
		return CommandView{}, err
	}
	payload := src[cursor:bodyEnd:bodyEnd]
	switch kind {
	case CommandMutationBatch, CommandRetainedPrune:
		if kind == CommandRetainedPrune {
			if authorityClass != CommandAuthorityTopology || len(payload) < retainedPruneProofBytes {
				return CommandView{}, semantic("retained prune proof body")
			}
			proof, proofOK := openRetainedPruneProof(payload[:retainedPruneProofBytes])
			if !proofOK || proof.BatchDigest != view.Fingerprint {
				return CommandView{}, semantic("retained prune proof")
			}
			view.retainedPrune = proof
			payload = payload[retainedPruneProofBytes:]
		}
		if err := validateRelationBytes(
			payload, count, relationCount, inlineRelationID,
		); err != nil {
			return CommandView{}, err
		}
		view.relationBytes = payload
		if kind == CommandRetainedPrune {
			view.mutationCount = count
			view.relationCount = relationCount
			view.inlineRelationID = inlineRelationID
			batches := view.RelationBatches()
			for batches.Next() {
				mutations := batches.Batch().Mutations()
				for mutations.Next() {
					mutationKind := mutations.Mutation().Kind
					if mutationKind != MutationDelete && mutationKind != MutationDeleteDigestEqual {
						return CommandView{}, semantic("retained prune carries non-delete mutation")
					}
				}
			}
		}
	case CommandSessionOpen, CommandSessionRenew, CommandSessionRevoke:
		if len(payload) != sessionLeaseBodyBytes {
			return CommandView{}, semantic("session lease body length")
		}
		view.ExpectedDeadlineUnixNano = int64(binary.LittleEndian.Uint64(payload[0:8]))
		view.NextDeadlineUnixNano = int64(binary.LittleEndian.Uint64(payload[8:16]))
		if err := validateCommandLease(
			kind, view.ExpectedDeadlineUnixNano, view.NextDeadlineUnixNano,
		); err != nil {
			return CommandView{}, err
		}
	case CommandSessionRetire, CommandSessionRelease:
		if len(payload) != 0 {
			return CommandView{}, semantic("session lifecycle body length")
		}
	case CommandTransaction:
		if len(payload) < transactionLengthBytes {
			return CommandView{}, corrupt("transaction control length")
		}
		controlBytes64 := uint64(binary.LittleEndian.Uint32(payload[:transactionLengthBytes]))
		if controlBytes64 == 0 || controlBytes64 > distributedtxn.MaxReplicatedCommandBytes ||
			controlBytes64 > uint64(len(payload)-transactionLengthBytes) {
			return CommandView{}, corrupt("transaction control overruns command body")
		}
		controlEnd := transactionLengthBytes + int(controlBytes64)
		controlBytes := payload[transactionLengthBytes:controlEnd:controlEnd]
		control, err := validatedTransactionControl(controlBytes)
		if err != nil {
			return CommandView{}, corrupt("transaction control")
		}
		if err := validateTransactionClientIdentity(
			view.ClientID, view.ClientEpoch, view.ClientSequence, view.AckThrough, control,
		); err != nil {
			return CommandView{}, err
		}
		relationBytes := payload[controlEnd:len(payload):len(payload)]
		if transactionOperationCarriesRelationBatches(control.operation) {
			if count == 0 || relationCount == 0 {
				return CommandView{}, semantic("transaction prepare has no relation batches")
			}
			if err := validateRelationBytes(
				relationBytes, count, relationCount, inlineRelationID,
			); err != nil {
				return CommandView{}, err
			}
			if transactionMutationDigestFromBytes(
				relationBytes, count, relationCount, inlineRelationID,
			) != control.mutationDigest {
				return CommandView{}, semantic("transaction mutation digest")
			}
			view.relationBytes = relationBytes
		} else if count != 0 || relationCount != 0 || inlineRelationID != 0 || len(relationBytes) != 0 {
			return CommandView{}, semantic("transaction operation carries relation batches")
		}
		view.transactionBytes = controlBytes
		view.transactionRole = control.role
		view.transactionOp = control.operation
	case CommandRequestLedger:
		if len(payload) == 0 {
			return CommandView{}, semantic("request ledger body length")
		}
		if err := requestledger.ValidateCommand(payload); err != nil {
			return CommandView{}, corrupt("request ledger command")
		}
		view.requestLedgerBytes = payload
	case CommandRouteGate:
		gate, gateErr := routegate.OpenCommand(payload)
		if len(payload) != routegate.CommandBytes || gateErr != nil ||
			!routeGateAuthorityMatches(authorityClass, gate.Operation) {
			return CommandView{}, semantic("route-gate command body")
		}
		view.routeGateBytes = payload
	case CommandExecutionPin:
		if len(payload) != executionpin.CommandBytes {
			return CommandView{}, semantic("execution-pin body length")
		}
		if _, err := executionpin.OpenCommand(payload); err != nil {
			return CommandView{}, semantic("execution-pin command")
		}
		view.executionPinBytes = payload
	case CommandSplitCaptureActivate:
		if _, err := splitcapture.OpenCommand(payload); err != nil {
			return CommandView{}, semantic("split capture command body")
		}
		view.splitCaptureBytes = payload
	}
	view.raw = src[:len(src):len(src)]
	view.mutationCount = count
	view.relationCount = relationCount
	view.inlineRelationID = inlineRelationID
	return view, nil
}

func routeGateAuthorityMatches(class CommandAuthorityClass, operation routegate.Operation) bool {
	switch operation {
	case routegate.OperationAcquireShared, routegate.OperationReleaseShared:
		return class == CommandAuthorityData
	case routegate.OperationBeginExclusive, routegate.OperationReleaseExclusive,
		routegate.OperationCompactReleased:
		return class == CommandAuthorityTopology
	default:
		return false
	}
}

func openCommandKind(wire uint8) (CommandKind, bool) {
	switch wire {
	case commandWireMutationBatch:
		return CommandMutationBatch, true
	case commandWireSessionRetire:
		return CommandSessionRetire, true
	case commandWireSessionRelease:
		return CommandSessionRelease, true
	case commandWireSessionOpen:
		return CommandSessionOpen, true
	case commandWireSessionRenew:
		return CommandSessionRenew, true
	case commandWireSessionRevoke:
		return CommandSessionRevoke, true
	case commandWireTransaction:
		return CommandTransaction, true
	case commandWireRequestLedger:
		return CommandRequestLedger, true
	case commandWireRouteGate:
		return CommandRouteGate, true
	case commandWireExecutionPin:
		return CommandExecutionPin, true
	case commandWireRetainedPrune:
		return CommandRetainedPrune, true
	case commandWireSplitCaptureActivate:
		return CommandSplitCaptureActivate, true
	default:
		return 0, false
	}
}

func validateDecodedCommandScalars(view CommandView) error {
	if !nonzero128(view.ClusterID) || !nonzero128(view.ClusterIncarnation) ||
		!nonzero128(view.ShardIncarnation) || !nonzero128(view.GroupID) ||
		!nonzero128(view.ClientID) || !nonzeroDigest(view.Fingerprint) {
		return semantic("command contains a zero identity or fingerprint")
	}
	if view.TopologyRecoveryEpoch == 0 || view.AllocationGeneration == 0 ||
		view.ReplicaSetVersion == 0 || view.ActivePolicyGeneration == 0 ||
		view.ProtectionEpoch == 0 || view.OwnershipEpoch == 0 ||
		view.SchemaGeneration == 0 || view.RoutingVersion == 0 ||
		view.RouteGeneration == 0 {
		return semantic("command contains a zero generation, epoch, or sequence")
	}
	return validateCommandClientTuple(
		view.kind, view.ClientEpoch, view.ClientSequence, view.AckThrough,
	)
}

func validateCommandClientTuple(kind CommandKind, epoch, sequence, ackThrough uint64) error {
	if kind == CommandSessionOpen {
		if epoch != 0 || sequence != 1 || ackThrough != 0 {
			return semantic("session open client tuple")
		}
		return nil
	}
	if epoch == 0 || sequence == 0 {
		return semantic("command contains a zero client epoch or sequence")
	}
	if ackThrough >= sequence {
		return semantic("command acknowledgement does not precede client sequence")
	}
	return nil
}

func commandCarriesLeaseBody(kind CommandKind) bool {
	switch kind {
	case CommandSessionOpen, CommandSessionRenew, CommandSessionRevoke:
		return true
	default:
		return false
	}
}

func validateCommandLease(kind CommandKind, expected, next int64) error {
	switch kind {
	case CommandSessionOpen:
		if expected != 0 || next <= 0 {
			return semantic("session open lease deadlines")
		}
	case CommandSessionRenew:
		if expected <= 0 || next <= expected {
			return semantic("session renew lease deadlines")
		}
	case CommandSessionRevoke:
		if expected <= 0 || next != 0 {
			return semantic("session revoke lease deadlines")
		}
	default:
		if expected != 0 || next != 0 {
			return semantic("non-lease command carries lease deadlines")
		}
	}
	return nil
}

func validateMutationBytes(src []byte, count uint32) error {
	cursor := 0
	for index := uint32(0); index < count; index++ {
		if len(src)-cursor < mutationHeaderBytes {
			return corrupt("mutation header overruns command body")
		}
		header := src[cursor : cursor+mutationHeaderBytes]
		kind := MutationKind(header[0])
		if header[1] != 0 {
			return semantic("mutation reserved byte")
		}
		keyLen := int(binary.LittleEndian.Uint16(header[2:4]))
		valueLen64 := uint64(binary.LittleEndian.Uint32(header[4:8]))
		maxValueLen64 := uint64(MaxMutationValueBytes)
		switch kind {
		case MutationDelete:
			maxValueLen64 = 0
		case MutationDeleteDigestEqual:
			maxValueLen64 = mutationDigestCompareBytes
		case MutationPutDigestEqual:
			maxValueLen64 += mutationDigestCompareBytes
		}
		if keyLen == 0 || keyLen > MaxMutationKeyBytes ||
			valueLen64 > maxValueLen64 {
			return semantic("mutation key or value length")
		}
		payload, ok := checkedAdd(uint64(keyLen), valueLen64, uint64(len(src)-cursor-mutationHeaderBytes))
		if !ok {
			return corrupt("mutation payload overruns command body")
		}
		cursor += mutationHeaderBytes
		end := cursor + int(payload)
		value := src[cursor+keyLen : end]
		switch kind {
		case MutationPut, MutationPutAbsentOrEqual, MutationPutAbsent, MutationPutPresent:
			if len(value) == 0 || len(value) > MaxMutationValueBytes {
				return semantic("put value length")
			}
		case MutationDelete:
			if len(value) != 0 {
				return semantic("delete carries value bytes")
			}
		case MutationDeleteDigestEqual:
			if len(value) != mutationDigestCompareBytes ||
				binary.LittleEndian.Uint64(value[:8]) == 0 ||
				binary.LittleEndian.Uint64(value[:8]) > MaxMutationValueBytes ||
				allZero(value[8:]) {
				return semantic("delete compare value identity")
			}
		case MutationPutDigestEqual:
			if len(value) <= mutationDigestCompareBytes ||
				len(value)-mutationDigestCompareBytes > MaxMutationValueBytes ||
				binary.LittleEndian.Uint64(value[:8]) == 0 ||
				binary.LittleEndian.Uint64(value[:8]) > MaxMutationValueBytes ||
				allZero(value[8:mutationDigestCompareBytes]) {
				return semantic("put compare value identity")
			}
		default:
			return semantic("unknown mutation kind")
		}
		cursor = end
	}
	if cursor != len(src) {
		return corrupt("command body has trailing mutation bytes")
	}
	return nil
}

func validateRelationBytes(
	src []byte,
	totalMutations uint32,
	relationCount uint16,
	inlineRelationID RelationID,
) error {
	if relationCount == 1 {
		if inlineRelationID == 0 || inlineRelationID > MaxRelationID {
			return semantic("inline relation identity")
		}
		return validateMutationBytes(src, totalMutations)
	}
	if relationCount < 2 || relationCount > MaxRelationBatches || inlineRelationID != 0 {
		return semantic("relation batch count")
	}
	cursor := 0
	mutations := uint32(0)
	var previous RelationID
	for ordinal := uint16(0); ordinal < relationCount; ordinal++ {
		if len(src)-cursor < relationBatchHeaderBytes {
			return corrupt("relation batch header overruns command body")
		}
		header := src[cursor : cursor+relationBatchHeaderBytes]
		relation := RelationID(binary.LittleEndian.Uint16(header[0:2]))
		count := binary.LittleEndian.Uint16(header[2:4])
		payloadBytes64 := uint64(binary.LittleEndian.Uint32(header[4:8]))
		if relation == 0 || relation > MaxRelationID ||
			ordinal != 0 && relation <= previous || count == 0 {
			return semantic("relation batch order, identity, or mutation count")
		}
		remaining := uint64(len(src) - cursor - relationBatchHeaderBytes)
		if payloadBytes64 > remaining {
			return corrupt("relation batch payload overruns command body")
		}
		cursor += relationBatchHeaderBytes
		end := cursor + int(payloadBytes64)
		if err := validateMutationBytes(src[cursor:end], uint32(count)); err != nil {
			return err
		}
		if mutations > uint32(MaxMutations)-uint32(count) {
			return semantic("mutation count")
		}
		mutations += uint32(count)
		cursor = end
		previous = relation
	}
	if cursor != len(src) {
		return corrupt("command body has trailing relation bytes")
	}
	if mutations != totalMutations {
		return semantic("relation mutation count disagrees with command header")
	}
	return nil
}

func allZero(src []byte) bool {
	var aggregate byte
	for _, value := range src {
		aggregate |= value
	}
	return aggregate == 0
}
