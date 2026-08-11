package replication

import (
	"bytes"
	"encoding/binary"
	"unicode/utf8"
)

var commandMagic = [8]byte{'V', 'D', 'B', 'C', 'M', 'D', 0, 0}

// CommandV1 is one deterministic, single-collection state-machine mutation.
// It contains no Raft term, index, physical root generation, deadline, SQL text,
// or serialized execution plan. Fingerprint is the already-canonical request
// fingerprint minted by the request layer; this codec treats it as opaque.
// Mutation order, including duplicate keys, is semantic and preserved exactly;
// upstream fingerprinting must bind every mutation ordinal.
type CommandV1 struct {
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
	Fingerprint    Digest
	RetryHome      RetryHome

	Collection string
	Mutations  []Mutation
}

// CommandViewV1 is a checksum- and semantics-validated borrowed command. Its
// byte slices alias the OpenCommandV1 input and must be treated as immutable.
// Copying or retaining the view retains the complete bounded input envelope.
type CommandViewV1 struct {
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

	Tenant         []byte
	ClientID       ID128
	ClientEpoch    uint64
	ClientSequence uint64
	Fingerprint    Digest
	RetryHome      RetryHome
	Collection     []byte

	raw           []byte
	mutationBytes []byte
	mutationCount uint32
}

// Bytes returns the exact validated envelope. The result aliases the decoder
// input and is read-only for the view's lifetime.
func (v CommandViewV1) Bytes() []byte {
	return v.raw[:len(v.raw):len(v.raw)]
}

// MutationCount reports the number of mutations the iterator will produce.
func (v CommandViewV1) MutationCount() int { return int(v.mutationCount) }

// MutationViewV1 is one borrowed, validated mutation. Key and Value are
// capacity-clamped, read-only aliases into the command envelope.
type MutationViewV1 struct {
	Kind  MutationKind
	Key   []byte
	Value []byte
}

// MutationIteratorV1 walks a validated command without materializing a slice.
type MutationIteratorV1 struct {
	remaining uint32
	b         []byte
	current   MutationViewV1
}

// Mutations returns a fresh iterator positioned before the first mutation.
func (v CommandViewV1) Mutations() MutationIteratorV1 {
	b := v.mutationBytes
	return MutationIteratorV1{
		remaining: v.mutationCount,
		b:         b[:len(b):len(b)],
	}
}

// Next advances and reports whether one mutation is available.
func (i *MutationIteratorV1) Next() bool {
	if i == nil || i.remaining == 0 {
		return false
	}
	kind := MutationKind(i.b[0])
	keyLen := int(binary.LittleEndian.Uint16(i.b[2:4]))
	valueLen := int(binary.LittleEndian.Uint32(i.b[4:8]))
	keyStart := mutationHeaderBytes
	valueStart := keyStart + keyLen
	end := valueStart + valueLen
	i.current = MutationViewV1{
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
func (i *MutationIteratorV1) Mutation() MutationViewV1 {
	if i == nil {
		return MutationViewV1{}
	}
	return i.current
}

// AppendCommandV1 appends one deterministic command. On validation failure it
// returns the original dst unchanged. A correctly pre-sized dst incurs no heap
// allocation. Input slices must not overlap the writable append region in dst's
// current backing array; such aliases are rejected before dst is modified.
func AppendCommandV1(dst []byte, command CommandV1) ([]byte, error) {
	total, err := measureCommandV1(command)
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
	appendU16(frame, 8, CommandFormatV1)
	frame[10] = commandKindMutationBatch
	appendU16(frame, 12, commandHeaderBytes)
	appendU32(frame, 16, uint32(total))
	appendU32(frame, 20, uint32(total-commandHeaderBytes-envelopeChecksumBytes))
	appendU32(frame, 24, uint32(len(command.Mutations)))
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
	appendU16(frame, 246, uint16(len(command.Collection)))

	cursor := commandHeaderBytes
	cursor += copy(frame[cursor:], command.Tenant)
	cursor += copy(frame[cursor:], command.Distribution)
	cursor += copy(frame[cursor:], command.Shard)
	cursor += copy(frame[cursor:], command.Collection)
	for index := range command.Mutations {
		mutation := &command.Mutations[index]
		frame[cursor] = byte(mutation.Kind)
		appendU16(frame, cursor+2, uint16(len(mutation.Key)))
		appendU32(frame, cursor+4, uint32(len(mutation.Value)))
		cursor += mutationHeaderBytes
		cursor += copy(frame[cursor:], mutation.Key)
		cursor += copy(frame[cursor:], mutation.Value)
	}
	if cursor != total-envelopeChecksumBytes {
		panic("replication: measured command size diverged during encode")
	}
	sealEnvelope(frame)
	return dst, nil
}

func commandOverlapsAppendRegion(dst []byte, total int, command CommandV1) bool {
	region := writableAppendRegion(dst, total)
	if len(region) == 0 {
		return false
	}
	if byteSlicesOverlap(region, command.Tenant) ||
		byteSliceStringOverlap(region, command.Distribution) ||
		byteSliceStringOverlap(region, command.Shard) ||
		byteSliceStringOverlap(region, command.Collection) {
		return true
	}
	for index := range command.Mutations {
		if byteSlicesOverlap(region, command.Mutations[index].Key) ||
			byteSlicesOverlap(region, command.Mutations[index].Value) {
			return true
		}
	}
	return false
}

func measureCommandV1(command CommandV1) (int, error) {
	if err := validateCommandHeaderV1(command); err != nil {
		return 0, err
	}
	total := uint64(commandHeaderBytes + envelopeChecksumBytes)
	for _, size := range [...]int{
		len(command.Tenant), len(command.Distribution),
		len(command.Shard), len(command.Collection),
	} {
		var ok bool
		total, ok = checkedAdd(total, uint64(size), MaxCommandBytes)
		if !ok {
			return 0, ErrEnvelopeTooLarge
		}
	}
	for index := range command.Mutations {
		mutation := &command.Mutations[index]
		if err := validateMutationV1(*mutation); err != nil {
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
	return int(total), nil
}

func validateCommandHeaderV1(command CommandV1) error {
	if !nonzero128(command.ClusterID) || !nonzero128(command.ClusterIncarnation) ||
		!nonzero128(command.ShardIncarnation) || !nonzero128(command.GroupID) ||
		!nonzero128(command.ClientID) {
		return semantic("command contains a zero identity")
	}
	if command.TopologyRecoveryEpoch == 0 || command.AllocationGeneration == 0 ||
		command.ReplicaSetVersion == 0 || command.ActivePolicyGeneration == 0 ||
		command.ProtectionEpoch == 0 || command.OwnershipEpoch == 0 ||
		command.SchemaGeneration == 0 || command.RoutingVersion == 0 ||
		command.RouteGeneration == 0 || command.ClientEpoch == 0 ||
		command.ClientSequence == 0 {
		return semantic("command contains a zero generation, epoch, or sequence")
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
	if err := validateTextIdentity("collection", command.Collection, MaxCollectionBytes); err != nil {
		return err
	}
	if len(command.Mutations) == 0 || len(command.Mutations) > MaxMutations {
		return semantic("mutation count")
	}
	return nil
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

func validateMutationV1(mutation Mutation) error {
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

// OpenCommandV1 validates one exact command envelope and returns a borrowed
// view. It performs no allocation on valid input.
func OpenCommandV1(src []byte) (CommandViewV1, error) {
	if len(src) < commandHeaderBytes+envelopeChecksumBytes {
		return CommandViewV1{}, corrupt("short command")
	}
	if len(src) > MaxCommandBytes {
		return CommandViewV1{}, ErrEnvelopeTooLarge
	}
	if !bytes.Equal(src[:8], commandMagic[:]) {
		return CommandViewV1{}, corrupt("command magic")
	}
	version := binary.LittleEndian.Uint16(src[8:10])
	if version != CommandFormatV1 {
		return CommandViewV1{}, unsupported("command", version)
	}
	if binary.LittleEndian.Uint16(src[12:14]) != commandHeaderBytes {
		return CommandViewV1{}, corrupt("command header size")
	}
	total := binary.LittleEndian.Uint32(src[16:20])
	bodyBytes := binary.LittleEndian.Uint32(src[20:24])
	if uint64(total) != uint64(len(src)) ||
		uint64(bodyBytes)+commandHeaderBytes+envelopeChecksumBytes != uint64(total) {
		return CommandViewV1{}, corrupt("command total or body length")
	}
	if err := verifyEnvelopeChecksum(src); err != nil {
		return CommandViewV1{}, err
	}
	if src[10] != commandKindMutationBatch || src[11] != 0 ||
		binary.LittleEndian.Uint16(src[14:16]) != 0 ||
		binary.LittleEndian.Uint32(src[28:32]) != 0 ||
		!allZero(src[248:256]) {
		return CommandViewV1{}, semantic("command kind, flags, or reserved bytes")
	}
	count := binary.LittleEndian.Uint32(src[24:28])
	if count == 0 || uint64(count) > MaxMutations {
		return CommandViewV1{}, semantic("mutation count")
	}

	var view CommandViewV1
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

	tenantLen := int(binary.LittleEndian.Uint16(src[240:242]))
	distributionLen := int(binary.LittleEndian.Uint16(src[242:244]))
	shardLen := int(binary.LittleEndian.Uint16(src[244:246]))
	collectionLen := int(binary.LittleEndian.Uint16(src[246:248]))
	identityBytes := tenantLen + distributionLen + shardLen + collectionLen
	bodyEnd := len(src) - envelopeChecksumBytes
	if tenantLen == 0 || tenantLen > MaxIdentityBytes ||
		distributionLen == 0 || distributionLen > MaxIdentityBytes ||
		shardLen == 0 || shardLen > MaxIdentityBytes || collectionLen == 0 ||
		identityBytes < 0 || identityBytes > bodyEnd-commandHeaderBytes {
		return CommandViewV1{}, semantic("command identity lengths")
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
	end = cursor + collectionLen
	view.Collection = src[cursor:end:end]
	cursor = end
	if !utf8.Valid(view.Distribution) || !utf8.Valid(view.Shard) ||
		!utf8.Valid(view.Collection) {
		return CommandViewV1{}, semantic("command text identity is not valid UTF-8")
	}
	if err := validateDecodedCommandScalars(view); err != nil {
		return CommandViewV1{}, err
	}
	mutations := src[cursor:bodyEnd:bodyEnd]
	if err := validateMutationBytesV1(mutations, count); err != nil {
		return CommandViewV1{}, err
	}
	view.raw = src[:len(src):len(src)]
	view.mutationBytes = mutations
	view.mutationCount = count
	return view, nil
}

func validateDecodedCommandScalars(view CommandViewV1) error {
	if !nonzero128(view.ClusterID) || !nonzero128(view.ClusterIncarnation) ||
		!nonzero128(view.ShardIncarnation) || !nonzero128(view.GroupID) ||
		!nonzero128(view.ClientID) || !nonzeroDigest(view.Fingerprint) {
		return semantic("command contains a zero identity or fingerprint")
	}
	if view.TopologyRecoveryEpoch == 0 || view.AllocationGeneration == 0 ||
		view.ReplicaSetVersion == 0 || view.ActivePolicyGeneration == 0 ||
		view.ProtectionEpoch == 0 || view.OwnershipEpoch == 0 ||
		view.SchemaGeneration == 0 || view.RoutingVersion == 0 ||
		view.RouteGeneration == 0 || view.ClientEpoch == 0 ||
		view.ClientSequence == 0 {
		return semantic("command contains a zero generation, epoch, or sequence")
	}
	return nil
}

func validateMutationBytesV1(src []byte, count uint32) error {
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

func allZero(src []byte) bool {
	var aggregate byte
	for _, value := range src {
		aggregate |= value
	}
	return aggregate == 0
}
