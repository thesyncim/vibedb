package routeforward

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash/crc32"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

const (
	groupBytes          = 72
	commandFenceBytes   = 88
	routeAuthorityBytes = groupBytes + 8 + commandFenceBytes
	targetRouteBytes    = routeAuthorityBytes + 32

	EntryBytes           = 560
	ClearanceBytes       = 192
	commandHeaderBytes   = 104
	commandChecksumBytes = 4
	PublishCommandBytes  = commandHeaderBytes + EntryBytes + commandChecksumBytes
	ActivateCommandBytes = commandHeaderBytes + commandChecksumBytes
	PruneCommandBytes    = commandHeaderBytes + ClearanceBytes + commandChecksumBytes
	CompactCommandBytes  = ActivateCommandBytes
	OutcomeBytes         = 108
)

var (
	entryMagic     = [8]byte{'V', 'R', 'F', 'W', 'E', 'N', 'T', 0}
	clearanceMagic = [8]byte{'V', 'R', 'F', 'W', 'C', 'L', 'R', 0}
	commandMagic   = [8]byte{'V', 'R', 'F', 'W', 'C', 'M', 'D', 0}
	outcomeMagic   = [8]byte{'V', 'R', 'F', 'W', 'O', 'U', 'T', 0}
	castagnoli     = crc32.MakeTable(crc32.Castagnoli)
)

const (
	entryDigestDomain      = "vibedb/route-forward/entry\x00"
	entryKeyDomain         = "vibedb/route-forward/key\x00"
	entryCertificateDomain = "vibedb/route-forward/certificate\x00"
	clearanceDigestDomain  = "vibedb/route-forward/clearance\x00"
	tombstoneDigestDomain  = "vibedb/route-forward/tombstone\x00"
	compactKeyDomain       = "vibedb/route-forward/compact\x00"
)

// AppendEntry appends one canonical fixed entry.
func AppendEntry(dst []byte, entry Entry) ([]byte, error) {
	if !validEntry(entry) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, EntryBytes)...)
	frame := dst[start:]
	copy(frame[:8], entryMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], EntryBytes)
	frame[10] = byte(entry.Kind)
	putRouteAuthority(frame[16:184], entry.Old)
	copy(frame[184:216], entry.CommandFingerprint[:])
	copy(frame[216:248], entry.CommandDigest[:])
	copy(frame[248:280], entry.PlanDigest[:])
	putTargetRoute(frame[280:480], entry.Target)
	putValidity(frame[480:528], entry.Validity)
	digest := domainDigest(entryDigestDomain, frame[:528])
	copy(frame[528:560], digest[:])
	return dst, nil
}

// OpenEntry validates and opens one exact fixed entry.
func OpenEntry(raw []byte) (Entry, error) {
	if len(raw) != EntryBytes || !bytes.Equal(raw[:8], entryMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != EntryBytes || !allZero(raw[11:16]) ||
		domainDigest(entryDigestDomain, raw[:528]) != Digest(raw[528:560]) {
		return Entry{}, ErrCorrupt
	}
	entry := Entry{Kind: TopologyKind(raw[10])}
	entry.Old = openRouteAuthority(raw[16:184])
	copy(entry.CommandFingerprint[:], raw[184:216])
	copy(entry.CommandDigest[:], raw[216:248])
	copy(entry.PlanDigest[:], raw[248:280])
	entry.Target = openTargetRoute(raw[280:480])
	entry.Validity = openValidity(raw[480:528])
	if !validEntry(entry) {
		return Entry{}, ErrCorrupt
	}
	return entry, nil
}

// EntryKey is the central uniqueness identity for one exact old command. The
// target is deliberately excluded: the same old bytes can never map twice.
func EntryKey(entry Entry) Digest {
	if !validEntry(entry) {
		return Digest{}
	}
	var material [routeAuthorityBytes + 32 + 32]byte
	putRouteAuthority(material[:routeAuthorityBytes], entry.Old)
	copy(material[routeAuthorityBytes:routeAuthorityBytes+32], entry.CommandFingerprint[:])
	copy(material[routeAuthorityBytes+32:], entry.CommandDigest[:])
	return domainDigest(entryKeyDomain, material[:])
}

// AppendClearance appends one fixed prune attestation.
func AppendClearance(dst []byte, clearance Clearance) ([]byte, error) {
	if !validClearance(clearance) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, ClearanceBytes)...)
	frame := dst[start:]
	copy(frame[:8], clearanceMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], ClearanceBytes)
	copy(frame[16:48], clearance.Key[:])
	binary.LittleEndian.PutUint64(frame[48:56], clearance.CatalogGeneration)
	binary.LittleEndian.PutUint64(frame[56:64], clearance.RouteGateEpoch)
	binary.LittleEndian.PutUint64(frame[64:72], clearance.RouteGateRevision)
	binary.LittleEndian.PutUint64(frame[72:80], clearance.ActivePins)
	binary.LittleEndian.PutUint64(frame[80:88], clearance.OldestRetryApplied)
	binary.LittleEndian.PutUint64(frame[88:96], clearance.AuthorityRevision)
	copy(frame[96:128], clearance.GateCertificate[:])
	copy(frame[128:160], clearance.RetryCertificate[:])
	digest := domainDigest(clearanceDigestDomain, frame[:160])
	copy(frame[160:192], digest[:])
	return dst, nil
}

// OpenClearance validates one exact fixed prune witness.
func OpenClearance(raw []byte) (Clearance, error) {
	if len(raw) != ClearanceBytes || !bytes.Equal(raw[:8], clearanceMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != ClearanceBytes || !allZero(raw[10:16]) ||
		domainDigest(clearanceDigestDomain, raw[:160]) != Digest(raw[160:192]) {
		return Clearance{}, ErrCorrupt
	}
	clearance := Clearance{
		CatalogGeneration:  binary.LittleEndian.Uint64(raw[48:56]),
		RouteGateEpoch:     binary.LittleEndian.Uint64(raw[56:64]),
		RouteGateRevision:  binary.LittleEndian.Uint64(raw[64:72]),
		ActivePins:         binary.LittleEndian.Uint64(raw[72:80]),
		OldestRetryApplied: binary.LittleEndian.Uint64(raw[80:88]),
		AuthorityRevision:  binary.LittleEndian.Uint64(raw[88:96]),
	}
	copy(clearance.Key[:], raw[16:48])
	copy(clearance.GateCertificate[:], raw[96:128])
	copy(clearance.RetryCertificate[:], raw[128:160])
	if !validClearance(clearance) {
		return Clearance{}, ErrCorrupt
	}
	return clearance, nil
}

// AppendCommand appends one canonical operation-specific command.
func AppendCommand(dst []byte, command Command) ([]byte, error) {
	if !validCommand(command) {
		return dst, ErrCorrupt
	}
	total := ActivateCommandBytes
	switch command.Operation {
	case OperationPublish:
		total = PublishCommandBytes
	case OperationPrune:
		total = PruneCommandBytes
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[:8], commandMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], uint16(total))
	frame[10] = byte(command.Operation)
	binary.LittleEndian.PutUint64(frame[16:24], command.AuthorityEpoch)
	binary.LittleEndian.PutUint64(frame[24:32], command.ExpectedRevision)
	binary.LittleEndian.PutUint64(frame[32:40], command.NextAuthorityEpoch)
	copy(frame[40:72], command.Authority[:])
	copy(frame[72:104], command.Key[:])
	var err error
	switch command.Operation {
	case OperationPublish:
		_, err = AppendEntry(frame[104:104], command.Entry)
	case OperationPrune:
		_, err = AppendClearance(frame[104:104], command.Clearance)
	}
	if err != nil {
		return dst[:start], err
	}
	binary.LittleEndian.PutUint32(frame[total-4:], crc32.Checksum(frame[:total-4], castagnoli))
	return dst, nil
}

// OpenCommand validates one exact operation-specific command.
func OpenCommand(raw []byte) (Command, error) {
	if len(raw) < ActivateCommandBytes || !bytes.Equal(raw[:8], commandMagic[:]) ||
		int(binary.LittleEndian.Uint16(raw[8:10])) != len(raw) || !allZero(raw[11:16]) ||
		binary.LittleEndian.Uint32(raw[len(raw)-4:]) != crc32.Checksum(raw[:len(raw)-4], castagnoli) {
		return Command{}, ErrCorrupt
	}
	command := Command{
		Operation: Operation(raw[10]), AuthorityEpoch: binary.LittleEndian.Uint64(raw[16:24]),
		ExpectedRevision:   binary.LittleEndian.Uint64(raw[24:32]),
		NextAuthorityEpoch: binary.LittleEndian.Uint64(raw[32:40]),
	}
	copy(command.Authority[:], raw[40:72])
	copy(command.Key[:], raw[72:104])
	var err error
	switch command.Operation {
	case OperationPublish:
		if len(raw) != PublishCommandBytes {
			return Command{}, ErrCorrupt
		}
		command.Entry, err = OpenEntry(raw[104 : 104+EntryBytes])
	case OperationActivate:
		if len(raw) != ActivateCommandBytes {
			return Command{}, ErrCorrupt
		}
	case OperationPrune:
		if len(raw) != PruneCommandBytes {
			return Command{}, ErrCorrupt
		}
		command.Clearance, err = OpenClearance(raw[104 : 104+ClearanceBytes])
	case OperationCompactRetired:
		if len(raw) != CompactCommandBytes {
			return Command{}, ErrCorrupt
		}
	default:
		return Command{}, ErrCorrupt
	}
	if err != nil || !validCommand(command) {
		return Command{}, ErrCorrupt
	}
	return command, nil
}

// AppendOutcome appends one fixed settlement record.
func AppendOutcome(dst []byte, outcome Outcome) ([]byte, error) {
	if !validOutcome(outcome) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, OutcomeBytes)...)
	frame := dst[start:]
	copy(frame[:8], outcomeMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], OutcomeBytes)
	frame[10], frame[12] = byte(outcome.Reason), byte(outcome.State)
	if outcome.Mutated {
		frame[11] = 1
	}
	binary.LittleEndian.PutUint64(frame[16:24], outcome.Revision)
	binary.LittleEndian.PutUint64(frame[24:32], outcome.Live)
	binary.LittleEndian.PutUint64(frame[32:40], outcome.Tombstones)
	copy(frame[40:72], outcome.Key[:])
	copy(frame[72:104], outcome.Certificate[:])
	binary.LittleEndian.PutUint32(frame[104:108], crc32.Checksum(frame[:104], castagnoli))
	return dst, nil
}

// OpenOutcome validates one fixed settlement record.
func OpenOutcome(raw []byte) (Outcome, error) {
	if len(raw) != OutcomeBytes || !bytes.Equal(raw[:8], outcomeMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != OutcomeBytes || raw[11] > 1 ||
		!allZero(raw[13:16]) || binary.LittleEndian.Uint32(raw[104:108]) !=
		crc32.Checksum(raw[:104], castagnoli) {
		return Outcome{}, ErrCorrupt
	}
	outcome := Outcome{
		Reason: Reason(raw[10]), Mutated: raw[11] == 1, State: EntryState(raw[12]),
		Revision:   binary.LittleEndian.Uint64(raw[16:24]),
		Live:       binary.LittleEndian.Uint64(raw[24:32]),
		Tombstones: binary.LittleEndian.Uint64(raw[32:40]),
	}
	copy(outcome.Key[:], raw[40:72])
	copy(outcome.Certificate[:], raw[72:104])
	if !validOutcome(outcome) {
		return Outcome{}, ErrCorrupt
	}
	return outcome, nil
}

func validCommand(command Command) bool {
	if command.Authority == (Digest{}) || command.AuthorityEpoch == 0 || command.Key == (Digest{}) {
		return false
	}
	switch command.Operation {
	case OperationPublish:
		return command.NextAuthorityEpoch == 0 && validEntry(command.Entry) &&
			command.Key == EntryKey(command.Entry) &&
			command.Clearance == (Clearance{})
	case OperationActivate:
		return command.NextAuthorityEpoch == 0 && command.Entry == (Entry{}) &&
			command.Clearance == (Clearance{})
	case OperationPrune:
		return command.NextAuthorityEpoch == 0 && command.Entry == (Entry{}) &&
			validClearance(command.Clearance) &&
			command.Key == command.Clearance.Key
	case OperationCompactRetired:
		return command.NextAuthorityEpoch > command.AuthorityEpoch &&
			command.Key == compactKey(command.Authority, command.NextAuthorityEpoch) &&
			command.Entry == (Entry{}) && command.Clearance == (Clearance{})
	default:
		return false
	}
}

func validClearance(clearance Clearance) bool {
	return clearance.Key != (Digest{}) && clearance.CatalogGeneration != 0 &&
		clearance.RouteGateEpoch != 0 && clearance.RouteGateRevision != 0 &&
		clearance.OldestRetryApplied != 0 && clearance.AuthorityRevision != 0 &&
		clearance.GateCertificate != (Digest{}) && clearance.RetryCertificate != (Digest{})
}

func validOutcome(outcome Outcome) bool {
	if outcome.Reason <= ReasonInvalid || outcome.Reason > ReasonCompacted ||
		outcome.Revision == 0 || outcome.Key == (Digest{}) || outcome.Certificate == (Digest{}) ||
		outcome.State > EntryActive {
		return false
	}
	mutated := outcome.Reason == ReasonPublished || outcome.Reason == ReasonActivated ||
		outcome.Reason == ReasonPruned || outcome.Reason == ReasonCompacted
	if outcome.Mutated != mutated {
		return false
	}
	switch outcome.Reason {
	case ReasonPublished:
		return outcome.State == EntryPrepared
	case ReasonActivated:
		return outcome.State == EntryActive
	case ReasonPruned, ReasonCompacted:
		return outcome.State == EntryInvalid
	default:
		return true
	}
}

func compactKey(authority Digest, nextEpoch uint64) Digest {
	var material [40]byte
	copy(material[:32], authority[:])
	binary.LittleEndian.PutUint64(material[32:], nextEpoch)
	return domainDigest(compactKeyDomain, material[:])
}

func putRouteAuthority(dst []byte, authority RouteAuthority) {
	putGroup(dst[:groupBytes], authority.Group)
	binary.LittleEndian.PutUint64(dst[groupBytes:groupBytes+8], authority.AllocationGeneration)
	putCommandFence(dst[groupBytes+8:routeAuthorityBytes], authority.Command)
}

func openRouteAuthority(raw []byte) RouteAuthority {
	return RouteAuthority{
		Group:                openGroup(raw[:groupBytes]),
		AllocationGeneration: binary.LittleEndian.Uint64(raw[groupBytes : groupBytes+8]),
		Command:              openCommandFence(raw[groupBytes+8 : routeAuthorityBytes]),
	}
}

func putGroup(dst []byte, group raftmember.GroupKey) {
	copy(dst[0:16], group.ClusterID[:])
	copy(dst[16:32], group.ClusterIncarnation[:])
	binary.LittleEndian.PutUint64(dst[32:40], group.TopologyRecoveryEpoch)
	copy(dst[40:56], group.ShardIncarnation[:])
	copy(dst[56:72], group.GroupID[:])
}

func openGroup(raw []byte) raftmember.GroupKey {
	var group raftmember.GroupKey
	copy(group.ClusterID[:], raw[0:16])
	copy(group.ClusterIncarnation[:], raw[16:32])
	group.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(raw[32:40])
	copy(group.ShardIncarnation[:], raw[40:56])
	copy(group.GroupID[:], raw[56:72])
	return group
}

func putCommandFence(dst []byte, fence raftservice.CommandFence) {
	for index, value := range [...]uint64{
		fence.ReplicaSetVersion, fence.ActivePolicyGeneration,
		fence.ProtectionEpoch, fence.OwnershipEpoch, fence.SchemaGeneration,
		fence.RoutingVersion, fence.RouteGeneration,
	} {
		binary.LittleEndian.PutUint64(dst[index*8:index*8+8], value)
	}
	copy(dst[56:88], fence.RelationManifestDigest[:])
}

func openCommandFence(raw []byte) raftservice.CommandFence {
	fence := raftservice.CommandFence{
		ReplicaSetVersion:      binary.LittleEndian.Uint64(raw[0:8]),
		ActivePolicyGeneration: binary.LittleEndian.Uint64(raw[8:16]),
		ProtectionEpoch:        binary.LittleEndian.Uint64(raw[16:24]),
		OwnershipEpoch:         binary.LittleEndian.Uint64(raw[24:32]),
		SchemaGeneration:       binary.LittleEndian.Uint64(raw[32:40]),
		RoutingVersion:         binary.LittleEndian.Uint64(raw[40:48]),
		RouteGeneration:        binary.LittleEndian.Uint64(raw[48:56]),
	}
	copy(fence.RelationManifestDigest[:], raw[56:88])
	return fence
}

func putTargetRoute(dst []byte, target TargetRoute) {
	putRouteAuthority(dst[:routeAuthorityBytes], target.Authority)
	copy(dst[168:200], target.RouteSetDigest[:])
}

func openTargetRoute(raw []byte) TargetRoute {
	target := TargetRoute{
		Authority: openRouteAuthority(raw[:routeAuthorityBytes]),
	}
	copy(target.RouteSetDigest[:], raw[168:200])
	return target
}

func putValidity(dst []byte, validity Validity) {
	for index, value := range [...]uint64{
		validity.SourceAppliedFloor, validity.TargetAppliedFloor,
		validity.ValidFromCatalog, validity.RetainThroughCatalog,
		validity.ExpiresAfterCatalog, validity.GateEpoch,
	} {
		binary.LittleEndian.PutUint64(dst[index*8:index*8+8], value)
	}
}

func openValidity(raw []byte) Validity {
	return Validity{
		SourceAppliedFloor:   binary.LittleEndian.Uint64(raw[0:8]),
		TargetAppliedFloor:   binary.LittleEndian.Uint64(raw[8:16]),
		ValidFromCatalog:     binary.LittleEndian.Uint64(raw[16:24]),
		RetainThroughCatalog: binary.LittleEndian.Uint64(raw[24:32]),
		ExpiresAfterCatalog:  binary.LittleEndian.Uint64(raw[32:40]),
		GateEpoch:            binary.LittleEndian.Uint64(raw[40:48]),
	}
}

func domainDigest(domain string, raw []byte) Digest {
	h := sha256.New()
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write(raw)
	var result Digest
	_ = h.Sum(result[:0])
	return result
}

func allZero(raw []byte) bool {
	var combined byte
	for _, value := range raw {
		combined |= value
	}
	return combined == 0
}
