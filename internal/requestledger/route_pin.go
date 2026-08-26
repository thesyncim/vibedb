package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	MaxRouteGatePinCommandBytes    = 1113
	MaxRouteGatePinCompletionBytes = 1185
	routePinHeaderBytes            = 416
	MaxRoutePinRecordBytes         = routePinHeaderBytes + MaxRouteGatePinCommandBytes + MaxRouteGatePinCompletionBytes + checksumBytes
)

var (
	routePinMagic        = [4]byte{'V', 'R', 'L', 'R'}
	routePinDigestDomain = []byte("vibedb/request-ledger/route-pin\x00")
)

type RoutePinPhase uint8

const (
	RoutePinInvalid RoutePinPhase = iota
	RoutePinAcquiring
	RoutePinAcquired
	RoutePinReleasing
	RoutePinReleased
)

type RoutePinRecord struct {
	KeyDigest, RequestDigest, PlanRoot, PriorContinuationDigest                              Digest
	PinID                                                                                    PinID
	BindingDigest, PhysicalWitnessDigest                                                     Digest
	CommandDigest, CompletionDigest, PriorRecordDigest, RecordDigest, AcquiredEvidenceDigest Digest
	Revision, WaveOrdinal                                                                    uint64
	Phase                                                                                    RoutePinPhase
	Command, Completion                                                                      []byte
}

func NewRoutePinAcquiring(head HeadRecord, pinID PinID, binding, physical Digest, command []byte) (RoutePinRecord, error) {
	if err := validateHead(head); err != nil || head.Phase != PhaseSealed || nonzeroDigest(head.OutstandingRoutePinDigest) || pinID == (PinID{}) || !nonzeroDigest(binding) || !nonzeroDigest(physical) || len(command) == 0 || len(command) > MaxRouteGatePinCommandBytes {
		return RoutePinRecord{}, ErrInvalidState
	}
	r := RoutePinRecord{KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot, PriorContinuationDigest: head.ContinuationDigest, PinID: pinID, BindingDigest: binding, PhysicalWitnessDigest: physical, Revision: 1, WaveOrdinal: head.NextStepOrdinal, Phase: RoutePinAcquiring, Command: command}
	r.CommandDigest = digestBytes([]byte("vibedb/request-ledger/route-command\x00"), command)
	r.RecordDigest = routePinDigest(r)
	return r, validateRoutePin(r)
}
func RecordVerifiedRoutePinAcquired(r RoutePinRecord, revision uint64, completion []byte) (RoutePinRecord, error) {
	if err := validateRoutePin(r); err != nil || r.Phase != RoutePinAcquiring || !nextRevision(r.Revision, revision) || len(completion) == 0 || len(completion) > MaxRouteGatePinCompletionBytes {
		return RoutePinRecord{}, ErrInvalidState
	}
	r.PriorRecordDigest = r.RecordDigest
	r.Revision = revision
	r.Phase = RoutePinAcquired
	r.Completion = completion
	r.CompletionDigest = digestBytes([]byte("vibedb/request-ledger/route-completion\x00"), completion)
	r.AcquiredEvidenceDigest = routePinAcquiredEvidence(r)
	r.RecordDigest = routePinDigest(r)
	return r, validateRoutePin(r)
}
func BeginRoutePinRelease(r RoutePinRecord, revision uint64, command []byte) (RoutePinRecord, error) {
	if err := validateRoutePin(r); err != nil || r.Phase != RoutePinAcquired || !nextRevision(r.Revision, revision) || len(command) == 0 || len(command) > MaxRouteGatePinCommandBytes {
		return RoutePinRecord{}, ErrInvalidState
	}
	r.PriorRecordDigest = r.RecordDigest
	r.Revision = revision
	r.Phase = RoutePinReleasing
	r.Command = command
	r.CommandDigest = digestBytes([]byte("vibedb/request-ledger/route-command\x00"), command)
	r.Completion = nil
	r.CompletionDigest = Digest{}
	r.RecordDigest = routePinDigest(r)
	return r, validateRoutePin(r)
}
func RecordVerifiedRoutePinReleased(r RoutePinRecord, revision uint64, completion []byte) (RoutePinRecord, error) {
	if err := validateRoutePin(r); err != nil || r.Phase != RoutePinReleasing || !nextRevision(r.Revision, revision) || len(completion) == 0 || len(completion) > MaxRouteGatePinCompletionBytes {
		return RoutePinRecord{}, ErrInvalidState
	}
	r.PriorRecordDigest = r.RecordDigest
	r.Revision = revision
	r.Phase = RoutePinReleased
	r.Completion = completion
	r.CompletionDigest = digestBytes([]byte("vibedb/request-ledger/route-completion\x00"), completion)
	r.RecordDigest = routePinDigest(r)
	return r, validateRoutePin(r)
}

// AdvanceHeadRoutePin is the sole head-CAS transition for route acquire
// intent, verified acquire completion, and release intent. RoutePinRecord has
// its own per-wave revision 1..4; headRevision is independently monotone for
// the request lifecycle. Verified release completion uses MarkRoutePinReleased
// because it also clears the outstanding physical-route fence.
func AdvanceHeadRoutePin(
	head HeadRecord,
	prior RoutePinRecord,
	current RoutePinRecord,
	headRevision uint64,
) (HeadRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validateRoutePin(current)) != nil ||
		head.Phase != PhaseSealed || !nextRevision(head.Revision, headRevision) ||
		current.KeyDigest != head.KeyDigest || current.RequestDigest != head.RequestDigest ||
		current.PlanRoot != head.PlanRoot {
		return HeadRecord{}, ErrInvalidState
	}
	switch current.Phase {
	case RoutePinAcquiring:
		if prior.Phase != RoutePinInvalid || current.Revision != 1 ||
			current.PriorContinuationDigest != head.ContinuationDigest ||
			current.WaveOrdinal != head.NextStepOrdinal || nonzeroDigest(head.OutstandingRoutePinDigest) {
			return HeadRecord{}, ErrInvalidState
		}
	case RoutePinAcquired:
		if err := validateRoutePin(prior); err != nil || prior.Phase != RoutePinAcquiring ||
			current.PriorRecordDigest != prior.RecordDigest ||
			!nextRevision(prior.Revision, current.Revision) ||
			prior.KeyDigest != current.KeyDigest || prior.PlanRoot != current.PlanRoot ||
			prior.WaveOrdinal != current.WaveOrdinal || current.WaveOrdinal != head.NextStepOrdinal ||
			current.PriorContinuationDigest != head.ContinuationDigest ||
			nonzeroDigest(head.OutstandingRoutePinDigest) {
			return HeadRecord{}, ErrInvalidState
		}
	case RoutePinReleasing:
		if err := validateRoutePin(prior); err != nil || prior.Phase != RoutePinAcquired ||
			current.PriorRecordDigest != prior.RecordDigest ||
			!nextRevision(prior.Revision, current.Revision) ||
			prior.AcquiredEvidenceDigest != head.OutstandingRoutePinDigest ||
			current.AcquiredEvidenceDigest != prior.AcquiredEvidenceDigest ||
			current.WaveOrdinal == ^uint64(0) || current.WaveOrdinal+1 != head.NextStepOrdinal {
			return HeadRecord{}, ErrInvalidState
		}
	default:
		return HeadRecord{}, ErrInvalidState
	}
	head.Revision = headRevision
	return head, validateHead(head)
}
func AppendRoutePin(dst []byte, r RoutePinRecord) ([]byte, error) {
	if err := validateRoutePin(r); err != nil {
		return dst, err
	}
	start := len(dst)
	dst = append(dst, make([]byte, routePinHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], routePinMagic[:])
	out[8] = byte(r.Phase)
	binary.LittleEndian.PutUint64(out[16:24], r.Revision)
	binary.LittleEndian.PutUint64(out[24:32], r.WaveOrdinal)
	putDigest(out[32:64], r.KeyDigest)
	putDigest(out[64:96], r.RequestDigest)
	putDigest(out[96:128], r.PlanRoot)
	putDigest(out[128:160], r.PriorContinuationDigest)
	copy(out[160:176], r.PinID[:])
	putDigest(out[176:208], r.BindingDigest)
	putDigest(out[208:240], r.PhysicalWitnessDigest)
	putDigest(out[240:272], r.CommandDigest)
	putDigest(out[272:304], r.CompletionDigest)
	putDigest(out[304:336], r.PriorRecordDigest)
	putDigest(out[336:368], r.RecordDigest)
	putDigest(out[368:400], r.AcquiredEvidenceDigest)
	binary.LittleEndian.PutUint32(out[400:404], uint32(len(r.Command)))
	binary.LittleEndian.PutUint32(out[404:408], uint32(len(r.Completion)))
	dst = append(dst, r.Command...)
	dst = append(dst, r.Completion...)
	dst = appendChecksum(dst, start)
	return dst, nil
}
func OpenRoutePin(raw []byte) (RoutePinRecord, error) {
	if len(raw) < routePinHeaderBytes+1+checksumBytes || len(raw) > MaxRoutePinRecordBytes || !magicOK(raw, routePinMagic) || !zeroBytes(raw[4:8]) || !zeroBytes(raw[9:16]) || !zeroBytes(raw[408:416]) || !checksumOK(raw) {
		return RoutePinRecord{}, ErrCorrupt
	}
	commandBytes := binary.LittleEndian.Uint32(raw[400:404])
	completionBytes := binary.LittleEndian.Uint32(raw[404:408])
	want, ok := exactLength(routePinHeaderBytes+checksumBytes, uint64(commandBytes), uint64(completionBytes))
	if !ok || want != len(raw) {
		return RoutePinRecord{}, ErrCorrupt
	}
	commandEnd := routePinHeaderBytes + int(commandBytes)
	r := RoutePinRecord{Phase: RoutePinPhase(raw[8]), Revision: binary.LittleEndian.Uint64(raw[16:24]), WaveOrdinal: binary.LittleEndian.Uint64(raw[24:32]), KeyDigest: readDigest(raw[32:64]), RequestDigest: readDigest(raw[64:96]), PlanRoot: readDigest(raw[96:128]), PriorContinuationDigest: readDigest(raw[128:160]), BindingDigest: readDigest(raw[176:208]), PhysicalWitnessDigest: readDigest(raw[208:240]), CommandDigest: readDigest(raw[240:272]), CompletionDigest: readDigest(raw[272:304]), PriorRecordDigest: readDigest(raw[304:336]), RecordDigest: readDigest(raw[336:368]), AcquiredEvidenceDigest: readDigest(raw[368:400]), Command: raw[routePinHeaderBytes:commandEnd:commandEnd], Completion: raw[commandEnd : len(raw)-checksumBytes : len(raw)-checksumBytes]}
	copy(r.PinID[:], raw[160:176])
	if err := validateRoutePin(r); err != nil {
		return RoutePinRecord{}, ErrCorrupt
	}
	return r, nil
}
func validateRoutePin(r RoutePinRecord) error {
	if !nonzeroDigest(r.KeyDigest) || !nonzeroDigest(r.RequestDigest) || !nonzeroDigest(r.PlanRoot) || r.PinID == (PinID{}) || !nonzeroDigest(r.BindingDigest) || !nonzeroDigest(r.PhysicalWitnessDigest) || r.Revision == 0 || (r.WaveOrdinal == 0) != !nonzeroDigest(r.PriorContinuationDigest) || r.Phase < RoutePinAcquiring || r.Phase > RoutePinReleased || len(r.Command) == 0 || len(r.Command) > MaxRouteGatePinCommandBytes || r.CommandDigest != digestBytes([]byte("vibedb/request-ledger/route-command\x00"), r.Command) || (r.Phase == RoutePinAcquiring && (len(r.Completion) != 0 || nonzeroDigest(r.CompletionDigest) || nonzeroDigest(r.PriorRecordDigest) || nonzeroDigest(r.AcquiredEvidenceDigest))) || (r.Phase != RoutePinAcquiring && (!nonzeroDigest(r.PriorRecordDigest) || !nonzeroDigest(r.AcquiredEvidenceDigest))) || ((r.Phase == RoutePinAcquired || r.Phase == RoutePinReleased) && (len(r.Completion) == 0 || len(r.Completion) > MaxRouteGatePinCompletionBytes || r.CompletionDigest != digestBytes([]byte("vibedb/request-ledger/route-completion\x00"), r.Completion))) || (r.Phase == RoutePinAcquired && r.AcquiredEvidenceDigest != routePinAcquiredEvidence(r)) || (r.Phase == RoutePinReleasing && (len(r.Completion) != 0 || nonzeroDigest(r.CompletionDigest))) || r.RecordDigest != routePinDigest(r) {
		return ErrCorrupt
	}
	return nil
}
func routePinDigest(r RoutePinRecord) Digest {
	const domain = "vibedb/request-ledger/route-pin\x00"
	var framed [len(domain) + 10*sha256.Size + 16 + 24]byte
	at := copy(framed[:], routePinDigestDomain)
	framed[at] = byte(r.Phase)
	at += 8
	binary.LittleEndian.PutUint64(framed[at:at+8], r.Revision)
	binary.LittleEndian.PutUint64(framed[at+8:at+16], r.WaveOrdinal)
	at += 16
	for _, d := range [...]Digest{r.KeyDigest, r.RequestDigest, r.PlanRoot, r.PriorContinuationDigest, r.BindingDigest, r.PhysicalWitnessDigest, r.CommandDigest, r.CompletionDigest, r.PriorRecordDigest, r.AcquiredEvidenceDigest} {
		at += copy(framed[at:], d[:])
	}
	copy(framed[at:], r.PinID[:])
	return Digest(sha256.Sum256(framed[:]))
}
func routePinAcquiredEvidence(r RoutePinRecord) Digest {
	const domain = "vibedb/request-ledger/route-pin-acquired\x00"
	var framed [len(domain) + 5*sha256.Size + 16]byte
	at := copy(framed[:], domain)
	for _, d := range [...]Digest{r.KeyDigest, r.PlanRoot, r.BindingDigest, r.CommandDigest, r.CompletionDigest} {
		at += copy(framed[at:], d[:])
	}
	copy(framed[at:], r.PinID[:])
	return Digest(sha256.Sum256(framed[:]))
}
