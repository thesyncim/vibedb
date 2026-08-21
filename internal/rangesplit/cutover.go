package rangesplit

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"math"

	"github.com/thesyncim/vibedb/autosplit"
)

var ErrCutoverCertificate = errors.New("rangesplit: invalid cutover certificate")

const (
	cutoverCertificateFormat = uint16(1)
	cutoverCertificateBytes  = 544
	cutoverCertificateBody   = 512
)

var (
	cutoverCertificateMagic  = [8]byte{'V', 'D', 'B', 'S', 'P', 'C', 'T', 0}
	cutoverCertificateDomain = []byte("vibedb/range-split/cutover-certificate\x00")
)

// CutoverCertificate is immutable evidence that every non-retained child has
// durably consumed the same terminal ownership-fence entry. It is deliberately
// not topology authority: a catalog publisher must still perform its own
// generation-fenced compare-and-swap before exposing the desired manifest.
type CutoverCertificate struct {
	childCount  uint8
	retained    uint8
	plan        [sha256.Size]byte
	placement   [sha256.Size]byte
	cut         ChildArtifactSourceCut
	coordinates TailSourceCoordinates
	childBases  [autosplit.MaxSplitChildren][sha256.Size]byte
	sealBatches [autosplit.MaxSplitChildren][sha256.Size]byte
	childImages [autosplit.MaxSplitChildren][sha256.Size]byte
	digest      [sha256.Size]byte
}

// CutoverWorkspace retains source-decode, tail-translation, and certificate
// hash state. Reuse it serially.
type CutoverWorkspace struct {
	capture SourceCaptureWorkspace
	tail    TailWorkspace
	verify  CutoverVerifyWorkspace
	stages  [autosplit.MaxSplitChildren]*ChildStageCursor
	batches [autosplit.MaxSplitChildren][sha256.Size]byte
}

// CutoverVerifyWorkspace retains only the fixed hash state needed to validate
// one certificate. It avoids attaching the larger capture and tail workspaces
// to long-lived child stages.
type CutoverVerifyWorkspace struct {
	hasher hash.Hash
	digest [sha256.Size]byte
	body   [cutoverCertificateBody]byte
}

// SourceCut returns the terminal source publication bound to the certificate.
func (c CutoverCertificate) SourceCut() ChildArtifactSourceCut { return c.cut }

// SourceCoordinates returns the terminal ownership and routing fences.
func (c CutoverCertificate) SourceCoordinates() TailSourceCoordinates {
	return c.coordinates
}

// PlanDigest returns the exact split-plan identity.
func (c CutoverCertificate) PlanDigest() [sha256.Size]byte { return c.plan }

// PlacementDigest returns the exact compiled placement identity.
func (c CutoverCertificate) PlacementDigest() [sha256.Size]byte { return c.placement }

// Digest returns the semantic certificate checksum.
func (c CutoverCertificate) Digest() [sha256.Size]byte { return c.digest }

// ChildBaseDigest returns one child's artifact or retained-base identity.
func (c CutoverCertificate) ChildBaseDigest(child int) ([sha256.Size]byte, bool) {
	if child < 0 || child >= int(c.childCount) {
		return [sha256.Size]byte{}, false
	}
	return c.childBases[child], true
}

// SealBatchDigest returns one child's terminal empty-batch identity.
func (c CutoverCertificate) SealBatchDigest(child int) ([sha256.Size]byte, bool) {
	if child < 0 || child >= int(c.childCount) {
		return [sha256.Size]byte{}, false
	}
	return c.sealBatches[child], true
}

// ChildImageDigest returns the terminal ordered image identity for one
// non-retained child. The retained child is certified after source pruning.
func (c CutoverCertificate) ChildImageDigest(child int) ([sha256.Size]byte, bool) {
	if child < 0 || child >= int(c.childCount) || child == int(c.retained) {
		return [sha256.Size]byte{}, false
	}
	return c.childImages[child], true
}

// CertifyCutover reconstructs the terminal capture record, recomputes every
// child seal batch, and requires each non-retained stage to have durably
// persisted that exact batch at the exact terminal source cut.
func (p *Partitioner) CertifyCutover(
	capture *SourceCapture,
	cursor TailCursor,
	stages []ChildStageCursor,
	workspace *CutoverWorkspace,
) (CutoverCertificate, error) {
	if p == nil || capture == nil || workspace == nil || !cursor.sealed ||
		!p.validTailCursor(cursor) || len(stages) != int(p.childCount)-1 ||
		capture.partitioner.digest != p.digest || capture.placement != p.program.Digest() {
		return CutoverCertificate{}, ErrCutoverCertificate
	}
	clear(workspace.stages[:])
	clear(workspace.batches[:])
	for index := range stages {
		stage := &stages[index]
		child := int(stage.child)
		if child < 0 || child >= int(p.childCount) || child == int(p.retained) ||
			workspace.stages[child] != nil || stage.phase != ChildStageSealed ||
			stage.planDigest != p.digest || stage.placementDigest != p.program.Digest() ||
			stage.artifactDigest != cursor.childBaseDigests[child] ||
			stage.SourceCut() != cursor.SourceCut() ||
			stage.lastBatchDigest == ([sha256.Size]byte{}) ||
			stage.imageDigest == ([sha256.Size]byte{}) {
			return CutoverCertificate{}, ErrCutoverCertificate
		}
		workspace.stages[child] = stage
	}

	capture.mu.Lock()
	defer capture.mu.Unlock()
	if !capture.begun.Load() || capture.pending != 0 ||
		capture.head.Load() != cursor.applied ||
		capture.current != publicationFromTailCursor(cursor) ||
		capture.base.BaseDigest != cursor.baseDigest ||
		capture.base.Applied >= cursor.applied ||
		capture.base.RouteGeneration == math.MaxUint64 ||
		cursor.routeGeneration != capture.base.RouteGeneration+1 {
		return CutoverCertificate{}, ErrCutoverCertificate
	}
	pre := cursor
	pre.applied--
	pre.term = cursor.term
	pre.logicalDigest = [sha256.Size]byte{}
	pre.entryDigest = [sha256.Size]byte{}
	pre.ownershipEpoch = 0
	pre.routingVersion = 0
	pre.routeGeneration = 0
	pre.sealed = false
	binary.BigEndian.PutUint64(workspace.capture.key[:], cursor.applied)
	raw, found, err := capture.target.Collection.AppendRaw(
		workspace.capture.raw[:0], workspace.capture.key[:],
	)
	if err != nil || !found {
		return CutoverCertificate{}, errors.Join(ErrCutoverCertificate, err)
	}
	workspace.capture.raw = raw
	record, err := capture.decodeEntry(raw, &workspace.capture)
	if err != nil || record.Applied != cursor.applied {
		return CutoverCertificate{}, errors.Join(ErrCutoverCertificate, err)
	}
	pre.logicalDigest = record.BeforeLogicalDigest
	pre.entryDigest = record.PreviousEntryDigest
	pre.ownershipEpoch = record.BeforeOwnershipEpoch
	pre.routingVersion = record.BeforeRoutingVersion
	pre.routeGeneration = record.BeforeRouteGeneration
	entry := tailEntryFromCaptureRecord(record)

	var sinks [autosplit.MaxSplitChildren]TailSink
	for child := 0; child < int(p.childCount); child++ {
		child := child
		sinks[child] = func(batch TailBatch) error {
			workspace.batches[child] = batch.Digest
			if child == int(p.retained) {
				return nil
			}
			stage := workspace.stages[child]
			if stage == nil || stage.lastBatchDigest != batch.Digest {
				return ErrCutoverCertificate
			}
			return nil
		}
	}
	next, stats, err := p.TranslateTailEntry(
		pre, entry, sinks[:p.childCount], &workspace.tail,
	)
	if err != nil || next != cursor {
		return CutoverCertificate{}, errors.Join(ErrCutoverCertificate, err)
	}
	for child := 0; child < int(p.childCount); child++ {
		if workspace.batches[child] == ([sha256.Size]byte{}) ||
			workspace.batches[child] != stats.ChildDigests[child] {
			return CutoverCertificate{}, ErrCutoverCertificate
		}
	}
	certificate := CutoverCertificate{
		childCount: p.childCount, retained: p.retained,
		plan: p.digest, placement: p.program.Digest(), cut: cursor.SourceCut(),
		coordinates: cursor.coordinates(), childBases: cursor.childBaseDigests,
		sealBatches: workspace.batches,
	}
	for child := 0; child < int(p.childCount); child++ {
		if child != int(p.retained) {
			certificate.childImages[child] = workspace.stages[child].imageDigest
		}
	}
	certificate.digest = cutoverDigest(&certificate, &workspace.verify)
	return certificate, nil
}

// VerifyCutoverCertificate checks immutable plan and geometry bindings. It
// does not replace the live SourceCapture check performed by CertifyCutover.
func (p *Partitioner) VerifyCutoverCertificate(certificate CutoverCertificate) error {
	var workspace CutoverVerifyWorkspace
	return p.VerifyCutoverCertificateWithWorkspace(certificate, &workspace)
}

// VerifyCutoverCertificateWithWorkspace is VerifyCutoverCertificate with
// reusable hash state for allocation-free repeated checks.
func (p *Partitioner) VerifyCutoverCertificateWithWorkspace(
	certificate CutoverCertificate,
	workspace *CutoverVerifyWorkspace,
) error {
	if p == nil || !validCutoverCertificate(&certificate) ||
		workspace == nil ||
		certificate.plan != p.digest ||
		certificate.placement != p.program.Digest() ||
		certificate.childCount != p.childCount || certificate.retained != p.retained ||
		certificate.coordinates.OwnershipEpoch !=
			uint64(p.children[p.retained].OwnershipEpoch) ||
		certificate.coordinates.RoutingVersion != uint64(p.target) {
		return ErrCutoverCertificate
	}
	if cutoverDigest(&certificate, workspace) != certificate.digest {
		return ErrCutoverCertificate
	}
	return nil
}

// AppendCutoverCertificate appends one deterministic fixed-size certificate.
func AppendCutoverCertificate(
	dst []byte,
	certificate *CutoverCertificate,
) ([]byte, error) {
	return AppendCutoverCertificateWithWorkspace(dst, certificate, &CutoverWorkspace{})
}

// AppendCutoverCertificateWithWorkspace is AppendCutoverCertificate with
// reusable SHA state.
func AppendCutoverCertificateWithWorkspace(
	dst []byte,
	certificate *CutoverCertificate,
	workspace *CutoverWorkspace,
) ([]byte, error) {
	if workspace == nil || !validCutoverCertificate(certificate) ||
		cutoverDigest(certificate, &workspace.verify) != certificate.digest {
		return dst, ErrCutoverCertificate
	}
	start := len(dst)
	dst = append(dst, make([]byte, cutoverCertificateBytes)...)
	appendCutoverBody(dst[start:start], certificate)
	copy(dst[start+cutoverCertificateBody:start+cutoverCertificateBytes], certificate.digest[:])
	return dst, nil
}

// OpenCutoverCertificate strictly decodes one complete certificate.
func OpenCutoverCertificate(raw []byte) (*CutoverCertificate, error) {
	if len(raw) != cutoverCertificateBytes ||
		!bytes.Equal(raw[0:8], cutoverCertificateMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != cutoverCertificateFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != cutoverCertificateBytes ||
		binary.LittleEndian.Uint32(raw[12:16]) != cutoverCertificateBytes ||
		!allChildArtifactZero(raw[18:24]) {
		return nil, ErrCutoverCertificate
	}
	certificate := &CutoverCertificate{
		childCount: raw[16], retained: raw[17],
		cut: ChildArtifactSourceCut{
			Applied:         binary.LittleEndian.Uint64(raw[24:32]),
			Term:            binary.LittleEndian.Uint64(raw[32:40]),
			RouteGeneration: binary.LittleEndian.Uint64(raw[56:64]),
		},
		coordinates: TailSourceCoordinates{
			OwnershipEpoch:  binary.LittleEndian.Uint64(raw[40:48]),
			RoutingVersion:  binary.LittleEndian.Uint64(raw[48:56]),
			RouteGeneration: binary.LittleEndian.Uint64(raw[56:64]),
		},
	}
	copy(certificate.plan[:], raw[64:96])
	copy(certificate.placement[:], raw[96:128])
	copy(certificate.cut.LogicalDigest[:], raw[128:160])
	copy(certificate.cut.BaseDigest[:], raw[160:192])
	copy(certificate.cut.EntryDigest[:], raw[192:224])
	for child := 0; child < autosplit.MaxSplitChildren; child++ {
		copy(certificate.childBases[child][:], raw[224+child*32:256+child*32])
		copy(certificate.sealBatches[child][:], raw[320+child*32:352+child*32])
		copy(certificate.childImages[child][:], raw[416+child*32:448+child*32])
	}
	copy(certificate.digest[:], raw[cutoverCertificateBody:cutoverCertificateBytes])
	var workspace CutoverVerifyWorkspace
	if !validCutoverCertificate(certificate) ||
		cutoverDigest(certificate, &workspace) != certificate.digest {
		return nil, ErrCutoverCertificate
	}
	return certificate, nil
}

func appendCutoverBody(dst []byte, certificate *CutoverCertificate) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, cutoverCertificateBody)...)
	frame := dst[start:]
	copy(frame[0:8], cutoverCertificateMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], cutoverCertificateFormat)
	binary.LittleEndian.PutUint16(frame[10:12], cutoverCertificateBytes)
	binary.LittleEndian.PutUint32(frame[12:16], cutoverCertificateBytes)
	frame[16], frame[17] = certificate.childCount, certificate.retained
	binary.LittleEndian.PutUint64(frame[24:32], certificate.cut.Applied)
	binary.LittleEndian.PutUint64(frame[32:40], certificate.cut.Term)
	binary.LittleEndian.PutUint64(frame[40:48], certificate.coordinates.OwnershipEpoch)
	binary.LittleEndian.PutUint64(frame[48:56], certificate.coordinates.RoutingVersion)
	binary.LittleEndian.PutUint64(frame[56:64], certificate.coordinates.RouteGeneration)
	copy(frame[64:96], certificate.plan[:])
	copy(frame[96:128], certificate.placement[:])
	copy(frame[128:160], certificate.cut.LogicalDigest[:])
	copy(frame[160:192], certificate.cut.BaseDigest[:])
	copy(frame[192:224], certificate.cut.EntryDigest[:])
	for child := 0; child < autosplit.MaxSplitChildren; child++ {
		copy(frame[224+child*32:256+child*32], certificate.childBases[child][:])
		copy(frame[320+child*32:352+child*32], certificate.sealBatches[child][:])
		copy(frame[416+child*32:448+child*32], certificate.childImages[child][:])
	}
	return dst
}

func cutoverDigest(
	certificate *CutoverCertificate,
	workspace *CutoverVerifyWorkspace,
) [sha256.Size]byte {
	if workspace.hasher == nil {
		workspace.hasher = sha256.New()
	}
	h := workspace.hasher
	h.Reset()
	_, _ = h.Write(cutoverCertificateDomain)
	encoded := appendCutoverBody(workspace.body[:0], certificate)
	_, _ = h.Write(encoded)
	_ = h.Sum(workspace.digest[:0])
	return workspace.digest
}

func validCutoverCertificate(certificate *CutoverCertificate) bool {
	if certificate == nil || certificate.childCount < 2 ||
		certificate.childCount > autosplit.MaxSplitChildren ||
		certificate.retained >= certificate.childCount ||
		certificate.plan == ([sha256.Size]byte{}) ||
		certificate.placement == ([sha256.Size]byte{}) ||
		certificate.cut.Applied == 0 || certificate.cut.Applied == math.MaxUint64 ||
		certificate.cut.Term == 0 || certificate.cut.Term == math.MaxUint64 ||
		certificate.cut.RouteGeneration == 0 ||
		certificate.cut.LogicalDigest == ([sha256.Size]byte{}) ||
		certificate.cut.BaseDigest == ([sha256.Size]byte{}) ||
		certificate.cut.EntryDigest == ([sha256.Size]byte{}) ||
		certificate.coordinates.OwnershipEpoch == 0 ||
		certificate.coordinates.RoutingVersion == 0 ||
		certificate.coordinates.RouteGeneration != certificate.cut.RouteGeneration ||
		certificate.digest == ([sha256.Size]byte{}) {
		return false
	}
	for child := 0; child < autosplit.MaxSplitChildren; child++ {
		used := child < int(certificate.childCount)
		if used != (certificate.childBases[child] != ([sha256.Size]byte{})) ||
			used != (certificate.sealBatches[child] != ([sha256.Size]byte{})) ||
			(used && child != int(certificate.retained)) !=
				(certificate.childImages[child] != ([sha256.Size]byte{})) {
			return false
		}
	}
	return true
}

func publicationFromTailCursor(cursor TailCursor) sourceCapturePublication {
	return sourceCapturePublication{
		applied: cursor.applied, term: cursor.term,
		ownershipEpoch: cursor.ownershipEpoch, routingVersion: cursor.routingVersion,
		routeGeneration: cursor.routeGeneration,
		entryDigest:     cursor.entryDigest, logicalDigest: cursor.logicalDigest,
	}
}

func tailEntryFromCaptureRecord(record sourceCaptureEntry) TailEntry {
	return TailEntry{
		Applied: record.Applied, Term: record.Term,
		BeforeOwnershipEpoch:  record.BeforeOwnershipEpoch,
		AfterOwnershipEpoch:   record.AfterOwnershipEpoch,
		BeforeRoutingVersion:  record.BeforeRoutingVersion,
		AfterRoutingVersion:   record.AfterRoutingVersion,
		BeforeRouteGeneration: record.BeforeRouteGeneration,
		AfterRouteGeneration:  record.AfterRouteGeneration,
		PreviousEntryDigest:   record.PreviousEntryDigest,
		EntryDigest:           record.EntryDigest,
		BeforeLogicalDigest:   record.BeforeLogicalDigest,
		AfterLogicalDigest:    record.AfterLogicalDigest,
		Transitions:           record.Transitions,
	}
}
