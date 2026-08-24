package replication

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"
)

var commandMagic = [8]byte{'V', 'D', 'B', 'C', 'M', 'D', 0, 0}

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
	Kind CommandKind

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

	Batches []RelationMutationBatch
}

// CommandView is a checksum- and semantics-validated borrowed command. Its
// byte slices alias the OpenCommand input and must be treated as immutable.
// Copying or retaining the view retains the complete bounded input envelope.
type CommandView struct {
	kind CommandKind

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

	raw              []byte
	relationBytes    []byte
	mutationCount    uint32
	relationCount    uint16
	inlineRelationID RelationID
}

// Bytes returns the exact validated envelope. The result aliases the decoder
// input and is read-only for the view's lifetime.
func (v CommandView) Bytes() []byte {
	return v.raw[:len(v.raw):len(v.raw)]
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

// MutationView is one borrowed, validated mutation. Key and Value are
// capacity-clamped, read-only aliases into the command envelope.
type MutationView struct {
	Kind  MutationKind
	Key   []byte
	Value []byte
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
			frame[cursor] = byte(mutation.Kind)
			appendU16(frame, cursor+2, uint16(len(mutation.Key)))
			appendU32(frame, cursor+4, uint32(len(mutation.Value)))
			cursor += mutationHeaderBytes
			cursor += copy(frame[cursor:], mutation.Key)
			cursor += copy(frame[cursor:], mutation.Value)
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

func commandOverlapsAppendRegion(dst []byte, total int, command Command) bool {
	region := writableAppendRegion(dst, total)
	if len(region) == 0 {
		return false
	}
	if byteSlicesOverlap(region, command.Tenant) ||
		byteSliceStringOverlap(region, command.Distribution) ||
		byteSliceStringOverlap(region, command.Shard) ||
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
	default:
		panic("replication: validated command kind has no wire encoding")
	}
}

func measureCommand(command Command) (int, error) {
	if err := validateCommandHeader(command); err != nil {
		return 0, err
	}
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
				total, ok = checkedAdd(total, uint64(len(mutation.Value)), MaxCommandBytes)
			}
			if !ok {
				return 0, ErrEnvelopeTooLarge
			}
		}
	}
	return int(total), nil
}

func validateCommandHeader(command Command) error {
	switch command.Kind {
	case CommandMutationBatch:
		if len(command.Batches) == 0 || len(command.Batches) > MaxRelationBatches {
			return semantic("relation batch count")
		}
		mutations := 0
		var previous RelationID
		for index := range command.Batches {
			batch := &command.Batches[index]
			if batch.Relation == 0 || batch.Relation > MaxRelationID ||
				index != 0 && batch.Relation <= previous {
				return semantic("relation batch order or identity")
			}
			if len(batch.Mutations) == 0 ||
				len(command.Batches) > 1 && len(batch.Mutations) > 1<<16-1 ||
				len(batch.Mutations) > MaxMutations-mutations {
				return semantic("mutation count")
			}
			mutations += len(batch.Mutations)
			previous = batch.Relation
		}
	case CommandSessionRetire, CommandSessionRelease, CommandSessionOpen,
		CommandSessionRenew, CommandSessionRevoke:
		if len(command.Batches) != 0 {
			return semantic("session lifecycle command carries relation batches")
		}
	default:
		return semantic("unknown command kind")
	}
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
	if !nonzeroDigest(command.Fingerprint) {
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
	switch mutation.Kind {
	case MutationPut:
		if len(mutation.Value) == 0 || len(mutation.Value) > MaxMutationValueBytes {
			return semantic("put value length")
		}
	case MutationDelete:
		if len(mutation.Value) != 0 {
			return semantic("delete carries value bytes")
		}
	default:
		return semantic("unknown mutation kind")
	}
	return nil
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
	if !ok || src[11] != 0 ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 ||
		binary.LittleEndian.Uint16(src[246:248]) != 0 {
		return CommandView{}, semantic("command kind, flags, or reserved bytes")
	}
	count := binary.LittleEndian.Uint32(src[24:28])
	relationCount := binary.LittleEndian.Uint16(src[28:30])
	inlineRelationID := RelationID(binary.LittleEndian.Uint16(src[30:32]))
	switch kind {
	case CommandMutationBatch:
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
	}

	view := CommandView{kind: kind}
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
	case CommandMutationBatch:
		if err := validateRelationBytes(
			payload, count, relationCount, inlineRelationID,
		); err != nil {
			return CommandView{}, err
		}
		view.relationBytes = payload
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
	}
	view.raw = src[:len(src):len(src)]
	view.mutationCount = count
	view.relationCount = relationCount
	view.inlineRelationID = inlineRelationID
	return view, nil
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
		if keyLen == 0 || keyLen > MaxMutationKeyBytes ||
			valueLen64 > MaxMutationValueBytes {
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
		case MutationPut:
			if len(value) == 0 {
				return semantic("put value length")
			}
		case MutationDelete:
			if len(value) != 0 {
				return semantic("delete carries value bytes")
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
