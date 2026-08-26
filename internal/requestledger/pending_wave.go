package requestledger

import (
	"crypto/sha256"
	"encoding/binary"
)

const (
	MaxPendingWaveSteps       = 256
	pendingWaveHeaderBytes    = 288
	stepRefBytes              = 104
	MaxPendingWaveRecordBytes = pendingWaveHeaderBytes + MaxPendingWaveSteps*stepRefBytes + checksumBytes
)

var (
	pendingWaveMagic        = [4]byte{'V', 'R', 'L', 'W'}
	pendingWaveDigestDomain = []byte("vibedb/request-ledger/pending-wave\x00")
)

// StepRef names exact target and command bytes inside the immutable sealed
// recipe. A physical wave is bounded for one proposal; wider transactions use
// arbitrarily many monotone waves and have no participant-count policy ceiling.
type StepRef struct {
	TargetSource  PayloadSource
	CommandSource PayloadSource
	TargetOffset  uint64
	TargetLength  uint64
	CommandOffset uint64
	CommandLength uint64
	TargetDigest  Digest
	CommandDigest Digest
}

type PayloadSource uint8

const (
	PayloadSourceInvalid PayloadSource = iota
	PayloadSourcePlan
	PayloadSourceDynamic
)

type PendingWaveRecord struct {
	KeyDigest               Digest
	RequestDigest           Digest
	PlanRoot                Digest
	PriorContinuationDigest Digest
	WaveDigest              Digest
	PayloadBuildDigest      Digest
	RoutePinDigest          Digest
	ForwardingWitnessDigest Digest
	Revision                uint64
	WaveOrdinal             uint64
	Steps                   []StepRef
}

func NewPendingWaveWithRoutePin(
	head HeadRecord,
	build PayloadBuildRecord,
	revision uint64,
	routePin RoutePinRecord,
	steps []StepRef,
) (PendingWaveRecord, error) {
	if err := validateHead(head); err != nil || errOrNil(validateRoutePin(routePin)) != nil ||
		head.Phase != PhaseSealed || routePin.Phase != RoutePinAcquired ||
		routePin.KeyDigest != head.KeyDigest || routePin.RequestDigest != head.RequestDigest ||
		routePin.PlanRoot != head.PlanRoot || routePin.PriorContinuationDigest != head.ContinuationDigest ||
		routePin.WaveOrdinal != head.NextStepOrdinal ||
		!nextRevision(head.Revision, revision) || len(steps) == 0 ||
		len(steps) > MaxPendingWaveSteps {
		return PendingWaveRecord{}, ErrInvalidState
	}
	record := PendingWaveRecord{
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest, PlanRoot: head.PlanRoot,
		PriorContinuationDigest: head.ContinuationDigest,
		Revision:                revision, WaveOrdinal: head.NextStepOrdinal, Steps: steps,
		RoutePinDigest:          routePin.AcquiredEvidenceDigest,
		ForwardingWitnessDigest: routePin.PhysicalWitnessDigest,
	}
	if build != (PayloadBuildRecord{}) {
		if err := validatePayloadBuild(build); err != nil || build.Phase != PayloadBuildSealed ||
			build.KeyDigest != head.KeyDigest || build.RequestDigest != head.RequestDigest ||
			build.PlanRoot != head.PlanRoot || build.PriorContinuationDigest != head.ContinuationDigest ||
			build.WaveOrdinal != head.NextStepOrdinal {
			return PendingWaveRecord{}, ErrInvalidState
		}
		record.PayloadBuildDigest = build.BuildDigest
	}
	record.WaveDigest = pendingWaveDigest(record)
	if err := validatePendingWave(record, head.TotalPlanBytes); err != nil {
		return PendingWaveRecord{}, err
	}
	if uint64(pendingWaveHeaderBytes+len(steps)*stepRefBytes+checksumBytes) > head.MaxPendingWaveBytes {
		return PendingWaveRecord{}, ErrTooLarge
	}
	if nonzeroDigest(record.PayloadBuildDigest) {
		for i := range record.Steps {
			step := &record.Steps[i]
			if step.TargetSource == PayloadSourceDynamic &&
				(step.TargetOffset >= build.TotalBytes || step.TargetLength > build.TotalBytes-step.TargetOffset) ||
				step.CommandSource == PayloadSourceDynamic &&
					(step.CommandOffset >= build.TotalBytes || step.CommandLength > build.TotalBytes-step.CommandOffset) {
				return PendingWaveRecord{}, ErrCorrupt
			}
		}
	}
	return record, nil
}

func AppendPendingWave(dst []byte, record PendingWaveRecord) ([]byte, error) {
	if err := validatePendingWave(record, MaxPlanBytes); err != nil {
		return dst, err
	}
	total := pendingWaveHeaderBytes + len(record.Steps)*stepRefBytes + checksumBytes
	if total > MaxCommandBytes {
		return dst, ErrTooLarge
	}
	start := len(dst)
	dst = append(dst, make([]byte, pendingWaveHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], pendingWaveMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], record.Revision)
	binary.LittleEndian.PutUint64(out[16:24], record.WaveOrdinal)
	binary.LittleEndian.PutUint64(out[24:32], uint64(len(record.Steps)))
	putDigest(out[32:64], record.KeyDigest)
	putDigest(out[64:96], record.RequestDigest)
	putDigest(out[96:128], record.PlanRoot)
	putDigest(out[128:160], record.PriorContinuationDigest)
	putDigest(out[160:192], record.WaveDigest)
	putDigest(out[192:224], record.PayloadBuildDigest)
	putDigest(out[224:256], record.RoutePinDigest)
	putDigest(out[256:288], record.ForwardingWitnessDigest)
	for i := range record.Steps {
		step := &record.Steps[i]
		dst = append(dst, byte(step.TargetSource), byte(step.CommandSource), 0, 0, 0, 0, 0, 0)
		dst = appendU64(dst, step.TargetOffset)
		dst = appendU64(dst, step.TargetLength)
		dst = appendU64(dst, step.CommandOffset)
		dst = appendU64(dst, step.CommandLength)
		dst = append(dst, step.TargetDigest[:]...)
		dst = append(dst, step.CommandDigest[:]...)
	}
	dst = appendChecksum(dst, start)
	return dst, nil
}

type PendingWaveView struct {
	raw    []byte
	record PendingWaveRecord
}

func (view PendingWaveView) Bytes() []byte    { return view.raw[:len(view.raw):len(view.raw)] }
func (view PendingWaveView) Key() Digest      { return view.record.KeyDigest }
func (view PendingWaveView) Request() Digest  { return view.record.RequestDigest }
func (view PendingWaveView) Root() Digest     { return view.record.PlanRoot }
func (view PendingWaveView) Prior() Digest    { return view.record.PriorContinuationDigest }
func (view PendingWaveView) Digest() Digest   { return view.record.WaveDigest }
func (view PendingWaveView) Revision() uint64 { return view.record.Revision }
func (view PendingWaveView) Ordinal() uint64  { return view.record.WaveOrdinal }
func (view PendingWaveView) Count() uint64    { return uint64(len(view.record.Steps)) }
func (view PendingWaveView) Steps() []StepRef {
	return view.record.Steps[:len(view.record.Steps):len(view.record.Steps)]
}
func (view PendingWaveView) Record() PendingWaveRecord { return view.record }

// OpenPendingWaveInto performs every length/count/checksum admission check
// before writing caller scratch. No attacker-controlled allocation occurs.
func OpenPendingWaveInto(raw []byte, scratch []StepRef) (PendingWaveView, error) {
	if len(raw) < pendingWaveHeaderBytes+stepRefBytes+checksumBytes || len(raw) > MaxCommandBytes ||
		!magicOK(raw, pendingWaveMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return PendingWaveView{}, ErrCorrupt
	}
	count := binary.LittleEndian.Uint64(raw[24:32])
	if count == 0 || count > MaxPendingWaveSteps || count > uint64(len(scratch)) ||
		count > uint64((len(raw)-pendingWaveHeaderBytes-checksumBytes)/stepRefBytes) ||
		pendingWaveHeaderBytes+int(count)*stepRefBytes+checksumBytes != len(raw) {
		return PendingWaveView{}, ErrCorrupt
	}
	steps := scratch[:int(count)]
	at := pendingWaveHeaderBytes
	for i := range steps {
		entry := raw[at : at+stepRefBytes]
		steps[i] = StepRef{
			TargetSource:  PayloadSource(entry[0]),
			CommandSource: PayloadSource(entry[1]),
			TargetOffset:  binary.LittleEndian.Uint64(entry[8:16]),
			TargetLength:  binary.LittleEndian.Uint64(entry[16:24]),
			CommandOffset: binary.LittleEndian.Uint64(entry[24:32]),
			CommandLength: binary.LittleEndian.Uint64(entry[32:40]),
			TargetDigest:  readDigest(entry[40:72]), CommandDigest: readDigest(entry[72:104]),
		}
		at += stepRefBytes
	}
	record := PendingWaveRecord{
		Revision:    binary.LittleEndian.Uint64(raw[8:16]),
		WaveOrdinal: binary.LittleEndian.Uint64(raw[16:24]),
		KeyDigest:   readDigest(raw[32:64]), RequestDigest: readDigest(raw[64:96]),
		PlanRoot: readDigest(raw[96:128]), PriorContinuationDigest: readDigest(raw[128:160]),
		WaveDigest: readDigest(raw[160:192]), PayloadBuildDigest: readDigest(raw[192:224]),
		RoutePinDigest: readDigest(raw[224:256]), ForwardingWitnessDigest: readDigest(raw[256:288]), Steps: steps,
	}
	if err := validatePendingWave(record, MaxPlanBytes); err != nil {
		return PendingWaveView{}, ErrCorrupt
	}
	return PendingWaveView{raw: raw[:len(raw):len(raw)], record: record}, nil
}

// ValidatePendingWaveBytes performs full canonical validation without
// materializing StepRefs. Outer replication envelopes use this seam to remain
// allocation-free; apply callers reopen into their own fixed scratch.
func ValidatePendingWaveBytes(raw []byte) error {
	if len(raw) < pendingWaveHeaderBytes+stepRefBytes+checksumBytes || len(raw) > MaxCommandBytes ||
		!magicOK(raw, pendingWaveMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return ErrCorrupt
	}
	count := binary.LittleEndian.Uint64(raw[24:32])
	if count == 0 || count > MaxPendingWaveSteps ||
		count > uint64((len(raw)-pendingWaveHeaderBytes-checksumBytes)/stepRefBytes) ||
		pendingWaveHeaderBytes+int(count)*stepRefBytes+checksumBytes != len(raw) {
		return ErrCorrupt
	}
	record := PendingWaveRecord{Revision: binary.LittleEndian.Uint64(raw[8:16]),
		WaveOrdinal: binary.LittleEndian.Uint64(raw[16:24]), KeyDigest: readDigest(raw[32:64]),
		RequestDigest: readDigest(raw[64:96]), PlanRoot: readDigest(raw[96:128]),
		PriorContinuationDigest: readDigest(raw[128:160]), WaveDigest: readDigest(raw[160:192]),
		PayloadBuildDigest: readDigest(raw[192:224])}
	record.RoutePinDigest = readDigest(raw[224:256])
	record.ForwardingWitnessDigest = readDigest(raw[256:288])
	if !nonzeroDigest(record.KeyDigest) || !nonzeroDigest(record.RequestDigest) ||
		!nonzeroDigest(record.PlanRoot) || !nonzeroDigest(record.RoutePinDigest) ||
		!nonzeroDigest(record.ForwardingWitnessDigest) || record.Revision == 0 ||
		(record.WaveOrdinal == 0) != !nonzeroDigest(record.PriorContinuationDigest) {
		return ErrCorrupt
	}
	chain := pendingWaveSeed(record, count)
	var framed [sha256.Size + stepRefBytes]byte
	hasDynamic := false
	for index := uint64(0); index < count; index++ {
		at := pendingWaveHeaderBytes + int(index)*stepRefBytes
		entry := raw[at : at+stepRefBytes]
		if !zeroBytes(entry[2:8]) {
			return ErrCorrupt
		}
		targetSource, commandSource := PayloadSource(entry[0]), PayloadSource(entry[1])
		targetOffset, targetLength := binary.LittleEndian.Uint64(entry[8:16]), binary.LittleEndian.Uint64(entry[16:24])
		commandOffset, commandLength := binary.LittleEndian.Uint64(entry[24:32]), binary.LittleEndian.Uint64(entry[32:40])
		if (targetSource != PayloadSourcePlan && targetSource != PayloadSourceDynamic) ||
			(commandSource != PayloadSourcePlan && commandSource != PayloadSourceDynamic) ||
			targetLength == 0 || targetLength > MaxTargetBytes || commandLength == 0 || commandLength > MaxCommandBytes ||
			!validPayloadRef(targetSource, targetOffset, targetLength, MaxPlanBytes) ||
			!validPayloadRef(commandSource, commandOffset, commandLength, MaxPlanBytes) ||
			!nonzeroDigest(readDigest(entry[40:72])) || !nonzeroDigest(readDigest(entry[72:104])) {
			return ErrCorrupt
		}
		hasDynamic = hasDynamic || targetSource == PayloadSourceDynamic || commandSource == PayloadSourceDynamic
		copy(framed[:sha256.Size], chain[:])
		copy(framed[sha256.Size:], entry)
		chain = Digest(sha256.Sum256(framed[:]))
	}
	if hasDynamic != nonzeroDigest(record.PayloadBuildDigest) || chain != record.WaveDigest {
		return ErrCorrupt
	}
	return nil
}

func validatePendingWave(record PendingWaveRecord, totalPlanBytes uint64) error {
	if !nonzeroDigest(record.KeyDigest) || !nonzeroDigest(record.RequestDigest) ||
		!nonzeroDigest(record.PlanRoot) || !nonzeroDigest(record.RoutePinDigest) ||
		!nonzeroDigest(record.ForwardingWitnessDigest) || record.Revision == 0 ||
		len(record.Steps) == 0 || len(record.Steps) > MaxPendingWaveSteps ||
		(record.WaveOrdinal == 0) != !nonzeroDigest(record.PriorContinuationDigest) {
		return ErrCorrupt
	}
	hasDynamic := false
	for i := range record.Steps {
		step := &record.Steps[i]
		hasDynamic = hasDynamic || step.TargetSource == PayloadSourceDynamic || step.CommandSource == PayloadSourceDynamic
		if (step.TargetSource != PayloadSourcePlan && step.TargetSource != PayloadSourceDynamic) ||
			(step.CommandSource != PayloadSourcePlan && step.CommandSource != PayloadSourceDynamic) ||
			step.TargetLength == 0 || step.TargetLength > MaxTargetBytes ||
			step.CommandLength == 0 || step.CommandLength > MaxCommandBytes ||
			!validPayloadRef(step.TargetSource, step.TargetOffset, step.TargetLength, totalPlanBytes) ||
			!validPayloadRef(step.CommandSource, step.CommandOffset, step.CommandLength, totalPlanBytes) ||
			!nonzeroDigest(step.TargetDigest) || !nonzeroDigest(step.CommandDigest) {
			return ErrCorrupt
		}
	}
	if hasDynamic != nonzeroDigest(record.PayloadBuildDigest) {
		return ErrCorrupt
	}
	if record.WaveDigest != pendingWaveDigest(record) {
		return ErrCorrupt
	}
	return nil
}

func pendingWaveDigest(record PendingWaveRecord) Digest {
	chain := pendingWaveSeed(record, uint64(len(record.Steps)))
	var framed [sha256.Size + stepRefBytes]byte
	for i := range record.Steps {
		step := &record.Steps[i]
		copy(framed[:sha256.Size], chain[:])
		entry := framed[sha256.Size:]
		clear(entry)
		entry[0], entry[1] = byte(step.TargetSource), byte(step.CommandSource)
		binary.LittleEndian.PutUint64(entry[8:16], step.TargetOffset)
		binary.LittleEndian.PutUint64(entry[16:24], step.TargetLength)
		binary.LittleEndian.PutUint64(entry[24:32], step.CommandOffset)
		binary.LittleEndian.PutUint64(entry[32:40], step.CommandLength)
		copy(entry[40:72], step.TargetDigest[:])
		copy(entry[72:104], step.CommandDigest[:])
		chain = Digest(sha256.Sum256(framed[:]))
	}
	return chain
}

func pendingWaveSeed(record PendingWaveRecord, count uint64) Digest {
	const domain = "vibedb/request-ledger/pending-wave\x00"
	var seed [len(domain) + 24 + 7*sha256.Size]byte
	at := copy(seed[:], pendingWaveDigestDomain)
	binary.LittleEndian.PutUint64(seed[at:at+8], record.Revision)
	binary.LittleEndian.PutUint64(seed[at+8:at+16], record.WaveOrdinal)
	binary.LittleEndian.PutUint64(seed[at+16:at+24], count)
	at += 24
	for _, digest := range [...]Digest{
		record.KeyDigest, record.RequestDigest, record.PlanRoot, record.PriorContinuationDigest,
		record.PayloadBuildDigest,
		record.RoutePinDigest, record.ForwardingWitnessDigest,
	} {
		at += copy(seed[at:], digest[:])
	}
	return Digest(sha256.Sum256(seed[:]))
}

func validPayloadRef(source PayloadSource, offset, length, planBytes uint64) bool {
	if source == PayloadSourceDynamic {
		return offset < MaxDynamicWavePayloadBytes && length <= MaxDynamicWavePayloadBytes-offset
	}
	return offset < planBytes && length <= planBytes-offset
}

func InstallPendingWave(head HeadRecord, pending PendingWaveRecord, build PayloadBuildRecord, routePin RoutePinRecord) (HeadRecord, error) {
	if err := validateHead(head); err != nil ||
		errOrNil(validatePendingWave(pending, head.TotalPlanBytes)) != nil ||
		errOrNil(validateRoutePin(routePin)) != nil || routePin.Phase != RoutePinAcquired ||
		head.Phase != PhaseSealed || pending.KeyDigest != head.KeyDigest ||
		pending.RequestDigest != head.RequestDigest || pending.PlanRoot != head.PlanRoot ||
		pending.PriorContinuationDigest != head.ContinuationDigest ||
		pending.WaveOrdinal != head.NextStepOrdinal || !nextRevision(head.Revision, pending.Revision) {
		return HeadRecord{}, ErrInvalidState
	}
	if pending.RoutePinDigest != routePin.AcquiredEvidenceDigest ||
		pending.ForwardingWitnessDigest != routePin.PhysicalWitnessDigest ||
		routePin.KeyDigest != head.KeyDigest || routePin.RequestDigest != head.RequestDigest ||
		routePin.PlanRoot != head.PlanRoot || routePin.PriorContinuationDigest != head.ContinuationDigest ||
		routePin.WaveOrdinal != head.NextStepOrdinal {
		return HeadRecord{}, ErrInvalidState
	}
	if nonzeroDigest(pending.PayloadBuildDigest) {
		if err := validatePayloadBuild(build); err != nil || build.Phase != PayloadBuildSealed ||
			pending.PayloadBuildDigest != build.BuildDigest || build.KeyDigest != head.KeyDigest ||
			build.RequestDigest != head.RequestDigest || build.PlanRoot != head.PlanRoot ||
			build.PriorContinuationDigest != head.ContinuationDigest || build.WaveOrdinal != head.NextStepOrdinal {
			return HeadRecord{}, ErrInvalidState
		}
	} else if build != (PayloadBuildRecord{}) {
		return HeadRecord{}, ErrInvalidState
	}
	head.Revision = pending.Revision
	return head, nil
}
