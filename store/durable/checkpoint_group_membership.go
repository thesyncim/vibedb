package durable

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
)

// A membership transition is deliberately a separate, two-slot certificate.
// checkpoint.vgc remains the sole authority for the serving membership until
// the catalog has published the target generation.  This file only proves that
// the target was prepared from an exact, fully checkpointed source cut.
const (
	checkpointMembershipFilename              = "checkpoint-membership.vgm"
	checkpointMembershipFormat         uint16 = 0
	checkpointMembershipSlotBytes             = 8192
	checkpointMembershipSlots                 = 2
	checkpointMembershipFileBytes             = checkpointMembershipSlotBytes * checkpointMembershipSlots
	checkpointMembershipHeaderBytes           = 192
	checkpointMembershipMemberBytes           = 104
	checkpointMembershipChecksumOffset        = checkpointMembershipSlotBytes - sha256.Size
	checkpointMembershipMaxMembers            = (checkpointMembershipChecksumOffset - checkpointMembershipHeaderBytes) / checkpointMembershipMemberBytes
)

var checkpointMembershipMagic = [8]byte{'V', 'I', 'B', 'E', 'C', 'P', 'M', 0}
var checkpointMembershipDigestDomain = []byte("vibedb/checkpoint-membership/format-0\x00")
var checkpointMembershipSourceDomain = []byte("vibedb/checkpoint-membership/source/format-0\x00")
var checkpointMembershipTargetDomain = []byte("vibedb/checkpoint-membership/target/format-0\x00")

// ErrCheckpointMembershipTransition reports a stale, substituted, torn, or
// otherwise noncanonical membership transition. It never grants serving
// authority; checkpoint.vgc and the caller's catalog publication still do.
var ErrCheckpointMembershipTransition = errors.New("vibedb: invalid checkpoint membership transition")

// CheckpointMembershipWitness is the compact authenticated receipt returned by
// PrepareMembershipTransition. It is safe to persist in a catalog operation.
type CheckpointMembershipWitness struct {
	Sequence uint64
	Source   [sha256.Size]byte
	Target   [sha256.Size]byte
}

type checkpointMembershipMember struct {
	nameDigest [sha256.Size]byte
	pathDigest [sha256.Size]byte
	storeID    [16]byte
	journalID  [16]byte
	generation uint64
}

type checkpointMembershipCertificate struct {
	sequence       uint64
	sourceSequence uint64
	applied        uint64
	txnHighWater   uint64
	markerEpoch    uint64
	markerID       [16]byte
	source         [sha256.Size]byte
	target         [sha256.Size]byte
	authorization  [sha256.Size]byte
	members        []checkpointMembershipMember
}

// ObserveMembershipTransition settles an outcome-unknown prepare against the
// exact current checkpoint owner and rollout authorization. It does not
// activate the target membership.
func (g *CheckpointGroup) ObserveMembershipTransition(
	witness CheckpointMembershipWitness,
	authorization [sha256.Size]byte,
) error {
	if g == nil || witness.Sequence == 0 || witness.Source == ([sha256.Size]byte{}) ||
		witness.Target == ([sha256.Size]byte{}) || authorization == ([sha256.Size]byte{}) {
		return ErrCheckpointMembershipTransition
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	record, err := openCheckpointMembershipCertificate(g.log)
	if err != nil || checkpointMembershipWitness(record) != witness ||
		record.authorization != authorization ||
		record.source != checkpointMembershipSourceDigest(g.certificateLocked()) {
		return errors.Join(ErrCheckpointMembershipTransition, err)
	}
	return nil
}

// PrepareMembershipTransition durably stages one exact replacement membership
// from the current fully folded cut. authorization must be the caller's
// authenticated catalog-rollout digest. The target collections are borrowed;
// they remain non-serving and outside the CheckpointGroup after this returns.
//
// The cold path performs one checkpoint as needed, an empty marker rollover,
// and one transition-file fsync. Retry of the same target is device-silent.
func (g *CheckpointGroup) PrepareMembershipTransition(
	target []NamedCollection,
	authorization [sha256.Size]byte,
) (CheckpointMembershipWitness, error) {
	if g == nil || authorization == ([sha256.Size]byte{}) {
		return CheckpointMembershipWitness{}, ErrCheckpointMembershipTransition
	}
	ordered, err := checkpointGroupMembers(target)
	if err != nil || len(ordered) > checkpointMembershipMaxMembers {
		return CheckpointMembershipWitness{}, errors.Join(ErrCheckpointMembershipTransition, err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return CheckpointMembershipWitness{}, err
	}
	if g.sequence >= math.MaxUint64-1 || g.markerEpoch == math.MaxUint64 {
		return CheckpointMembershipWitness{}, ErrCheckpointGroupSequence
	}
	order := checkpointMembershipLockOrder(ordered)
	lockCheckpointMembershipCollections(order)
	fast, fastErr := buildCheckpointMembershipCertificate(
		g, g.certificateLocked(), ordered, authorization,
	)
	if fastErr == nil {
		if prior, openErr := openCheckpointMembershipCertificate(g.log); openErr == nil &&
			prior.source == fast.source && prior.target == fast.target &&
			prior.authorization == fast.authorization {
			unlockCheckpointMembershipCollections(order)
			return checkpointMembershipWitness(prior), nil
		}
	}
	unlockCheckpointMembershipCollections(order)
	if err := g.checkpointLocked(); err != nil {
		return CheckpointMembershipWitness{}, err
	}
	// An empty marker is essential: after a catalog-selected target reopen there
	// must be no retained decision whose participants name the old stores.
	if err := g.recycleMarkerLocked(); err != nil {
		return CheckpointMembershipWitness{}, err
	}
	lockCheckpointMembershipCollections(order)
	defer unlockCheckpointMembershipCollections(order)
	cert := g.certificateLocked()
	record, err := buildCheckpointMembershipCertificate(g, cert, ordered, authorization)
	if err != nil {
		return CheckpointMembershipWitness{}, err
	}
	return writeCheckpointMembershipCertificate(g.log, record)
}

func checkpointMembershipLockOrder(members []checkpointGroupMember) []*Collection {
	order := make([]*Collection, len(members))
	for i := range members {
		order[i] = members[i].collection
	}
	sortCollectionSnapshotOrder(order)
	return order
}

func lockCheckpointMembershipCollections(order []*Collection) {
	for _, collection := range order {
		collection.writer.Lock()
	}
}

func unlockCheckpointMembershipCollections(order []*Collection) {
	for i := len(order) - 1; i >= 0; i-- {
		order[i].writer.Unlock()
	}
}

func buildCheckpointMembershipCertificate(
	owner *CheckpointGroup,
	source checkpointGroupCertificate,
	target []checkpointGroupMember,
	authorization [sha256.Size]byte,
) (checkpointMembershipCertificate, error) {
	if len(target) == 0 || len(target) > checkpointMembershipMaxMembers ||
		authorization == ([sha256.Size]byte{}) {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	record := checkpointMembershipCertificate{
		sequence: 1, sourceSequence: source.sequence, applied: source.applied,
		txnHighWater: source.txnHighWater, markerEpoch: source.markerEpoch,
		markerID: source.markerID, source: checkpointMembershipSourceDigest(source),
		authorization: authorization,
		members:       make([]checkpointMembershipMember, len(target)),
	}
	for i, member := range target {
		if member.collection == nil || member.collection.file == nil ||
			member.collection.closed || member.collection.Generation() == 0 ||
			member.collection.Generation() != member.collection.DurableGeneration() ||
			member.collection.journal == nil || member.collection.journal.Cursor() != 0 ||
			(member.collection.checkpointGroup.Load() != nil &&
				member.collection.checkpointGroup.Load() != owner) ||
			member.collection.checkpointGroupRetired.Load() {
			return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
		}
		record.members[i] = checkpointMembershipMember{
			nameDigest: member.nameDigest,
			pathDigest: sha256.Sum256([]byte(filepath.Base(member.collection.file.Name()))),
			storeID:    member.storeID, journalID: member.journalID,
			generation: member.collection.DurableGeneration(),
		}
	}
	record.target = checkpointMembershipTargetDigest(record.members)
	return record, nil
}

func checkpointMembershipSourceDigest(c checkpointGroupCertificate) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(checkpointMembershipSourceDomain)
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], c.applied)
	_, _ = h.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], c.txnHighWater)
	_, _ = h.Write(scalar[:])
	_, _ = h.Write([]byte{c.maxApplySpan})
	binary.LittleEndian.PutUint64(scalar[:], c.maxSpanTxn)
	_, _ = h.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], c.maxSpanFirst)
	_, _ = h.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], c.maxSpanLast)
	_, _ = h.Write(scalar[:])
	_, _ = h.Write(c.markerID[:])
	binary.LittleEndian.PutUint64(scalar[:], c.seedApplied)
	_, _ = h.Write(scalar[:])
	_, _ = h.Write(c.seedState[:])
	_, _ = h.Write(c.seedMember[:])
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(c.members)))
	_, _ = h.Write(scalar[:])
	for _, member := range c.members {
		_, _ = h.Write(member.nameDigest[:])
		_, _ = h.Write(member.storeID[:])
		_, _ = h.Write(member.journalID[:])
	}
	var out [sha256.Size]byte
	_ = h.Sum(out[:0])
	return out
}

func checkpointMembershipTargetDigest(members []checkpointMembershipMember) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(checkpointMembershipTargetDomain)
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], uint64(len(members)))
	_, _ = h.Write(scalar[:])
	for _, member := range members {
		_, _ = h.Write(member.nameDigest[:])
		_, _ = h.Write(member.pathDigest[:])
		_, _ = h.Write(member.storeID[:])
		_, _ = h.Write(member.journalID[:])
		binary.LittleEndian.PutUint64(scalar[:], member.generation)
		_, _ = h.Write(scalar[:])
	}
	var out [sha256.Size]byte
	_ = h.Sum(out[:0])
	return out
}

func encodeCheckpointMembershipCertificate(c checkpointMembershipCertificate) ([]byte, error) {
	if c.sequence == 0 || c.sourceSequence == 0 || c.markerEpoch == 0 ||
		c.markerID == ([16]byte{}) || c.source == ([sha256.Size]byte{}) ||
		c.authorization == ([sha256.Size]byte{}) || len(c.members) == 0 ||
		len(c.members) > checkpointMembershipMaxMembers ||
		c.target != checkpointMembershipTargetDigest(c.members) {
		return nil, ErrCheckpointMembershipTransition
	}
	buf := make([]byte, checkpointMembershipSlotBytes)
	copy(buf[:8], checkpointMembershipMagic[:])
	binary.LittleEndian.PutUint16(buf[8:10], checkpointMembershipFormat)
	binary.LittleEndian.PutUint16(buf[10:12], checkpointMembershipHeaderBytes)
	binary.LittleEndian.PutUint16(buf[12:14], uint16(len(c.members)))
	binary.LittleEndian.PutUint64(buf[16:24], c.sequence)
	binary.LittleEndian.PutUint64(buf[24:32], c.sourceSequence)
	binary.LittleEndian.PutUint64(buf[32:40], c.applied)
	binary.LittleEndian.PutUint64(buf[40:48], c.txnHighWater)
	binary.LittleEndian.PutUint64(buf[48:56], c.markerEpoch)
	copy(buf[56:72], c.markerID[:])
	copy(buf[72:104], c.source[:])
	copy(buf[104:136], c.target[:])
	copy(buf[136:168], c.authorization[:])
	for i, member := range c.members {
		off := checkpointMembershipHeaderBytes + i*checkpointMembershipMemberBytes
		copy(buf[off:off+32], member.nameDigest[:])
		copy(buf[off+32:off+64], member.pathDigest[:])
		copy(buf[off+64:off+80], member.storeID[:])
		copy(buf[off+80:off+96], member.journalID[:])
		binary.LittleEndian.PutUint64(buf[off+96:off+104], member.generation)
	}
	h := sha256.New()
	_, _ = h.Write(checkpointMembershipDigestDomain)
	_, _ = h.Write(buf[:checkpointMembershipChecksumOffset])
	copy(buf[checkpointMembershipChecksumOffset:], h.Sum(nil))
	return buf, nil
}

func decodeCheckpointMembershipCertificate(buf []byte) (checkpointMembershipCertificate, error) {
	if len(buf) != checkpointMembershipSlotBytes ||
		!slices.Equal(buf[:8], checkpointMembershipMagic[:]) ||
		binary.LittleEndian.Uint16(buf[8:10]) != checkpointMembershipFormat ||
		binary.LittleEndian.Uint16(buf[10:12]) != checkpointMembershipHeaderBytes {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	h := sha256.New()
	_, _ = h.Write(checkpointMembershipDigestDomain)
	_, _ = h.Write(buf[:checkpointMembershipChecksumOffset])
	if !slices.Equal(buf[checkpointMembershipChecksumOffset:], h.Sum(nil)) {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	count := int(binary.LittleEndian.Uint16(buf[12:14]))
	if count == 0 || count > checkpointMembershipMaxMembers {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	c := checkpointMembershipCertificate{
		sequence:       binary.LittleEndian.Uint64(buf[16:24]),
		sourceSequence: binary.LittleEndian.Uint64(buf[24:32]),
		applied:        binary.LittleEndian.Uint64(buf[32:40]),
		txnHighWater:   binary.LittleEndian.Uint64(buf[40:48]),
		markerEpoch:    binary.LittleEndian.Uint64(buf[48:56]),
		members:        make([]checkpointMembershipMember, count),
	}
	copy(c.markerID[:], buf[56:72])
	copy(c.source[:], buf[72:104])
	copy(c.target[:], buf[104:136])
	copy(c.authorization[:], buf[136:168])
	for i := range c.members {
		off := checkpointMembershipHeaderBytes + i*checkpointMembershipMemberBytes
		copy(c.members[i].nameDigest[:], buf[off:off+32])
		copy(c.members[i].pathDigest[:], buf[off+32:off+64])
		copy(c.members[i].storeID[:], buf[off+64:off+80])
		copy(c.members[i].journalID[:], buf[off+80:off+96])
		c.members[i].generation = binary.LittleEndian.Uint64(buf[off+96 : off+104])
	}
	canonical, err := encodeCheckpointMembershipCertificate(c)
	if err != nil || !slices.Equal(canonical, buf) {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	return c, nil
}

func writeCheckpointMembershipCertificate(
	log *TxnLog, c checkpointMembershipCertificate,
) (CheckpointMembershipWitness, error) {
	if log == nil || log.root == nil {
		return CheckpointMembershipWitness{}, ErrCheckpointMembershipTransition
	}
	path := checkpointMembershipFilename
	info, statErr := log.root.Lstat(path)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return CheckpointMembershipWitness{}, statErr
	}
	flags := os.O_RDWR
	if created {
		flags |= os.O_CREATE | os.O_EXCL
	}
	file, err := log.root.OpenFile(path, flags, 0o600)
	if err != nil {
		return CheckpointMembershipWitness{}, err
	}
	defer file.Close()
	if opened, statErr := file.Stat(); statErr != nil {
		return CheckpointMembershipWitness{}, statErr
	} else if !opened.Mode().IsRegular() || (!created && !os.SameFile(info, opened)) {
		return CheckpointMembershipWitness{}, ErrCheckpointMembershipTransition
	} else if opened.Size() != 0 && opened.Size() != checkpointMembershipFileBytes {
		return CheckpointMembershipWitness{}, ErrCheckpointMembershipTransition
	}
	if created {
		if err := file.Truncate(checkpointMembershipFileBytes); err != nil {
			return CheckpointMembershipWitness{}, err
		}
	}
	if !created {
		prior, openErr := readCheckpointMembershipFile(file)
		if openErr != nil {
			return CheckpointMembershipWitness{}, openErr
		}
		if prior.source == c.source && prior.target == c.target &&
			prior.authorization == c.authorization {
			return checkpointMembershipWitness(prior), nil
		}
		c.sequence = prior.sequence + 1
		if c.sequence == 0 {
			return CheckpointMembershipWitness{}, ErrCheckpointGroupSequence
		}
	}
	encoded, err := encodeCheckpointMembershipCertificate(c)
	if err != nil {
		return CheckpointMembershipWitness{}, err
	}
	if _, err := file.WriteAt(encoded, int64(c.sequence%checkpointMembershipSlots)*checkpointMembershipSlotBytes); err != nil {
		return CheckpointMembershipWitness{}, journalCommitOutcomeUnknown(err)
	}
	if checkpointGroupFaultHook != nil {
		if err := checkpointGroupFaultHook(checkpointGroupAfterMembershipWrite); err != nil {
			return CheckpointMembershipWitness{}, err
		}
	}
	if err := file.Sync(); err != nil {
		return CheckpointMembershipWitness{}, journalCommitOutcomeUnknown(err)
	}
	if checkpointGroupFaultHook != nil {
		if err := checkpointGroupFaultHook(checkpointGroupAfterMembershipSync); err != nil {
			return CheckpointMembershipWitness{}, err
		}
	}
	if created {
		if err := syncTxnLogDirectory(log.root); err != nil {
			return CheckpointMembershipWitness{}, journalCommitOutcomeUnknown(err)
		}
		if checkpointGroupFaultHook != nil {
			if err := checkpointGroupFaultHook(checkpointGroupAfterMembershipDirectorySync); err != nil {
				return CheckpointMembershipWitness{}, err
			}
		}
	}
	return checkpointMembershipWitness(c), nil
}

func openCheckpointMembershipCertificate(log *TxnLog) (checkpointMembershipCertificate, error) {
	if log == nil || log.root == nil {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	info, err := log.root.Lstat(checkpointMembershipFilename)
	if err != nil {
		return checkpointMembershipCertificate{}, err
	}
	if !info.Mode().IsRegular() || info.Size() != checkpointMembershipFileBytes {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	file, err := log.root.OpenFile(checkpointMembershipFilename, os.O_RDONLY, 0)
	if err != nil {
		return checkpointMembershipCertificate{}, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	return readCheckpointMembershipFile(file)
}

func checkpointMembershipWitness(c checkpointMembershipCertificate) CheckpointMembershipWitness {
	return CheckpointMembershipWitness{Sequence: c.sequence, Source: c.source, Target: c.target}
}

func readCheckpointMembershipFile(file *os.File) (checkpointMembershipCertificate, error) {
	valid := make([]checkpointMembershipCertificate, 0, checkpointMembershipSlots)
	for slot := 0; slot < checkpointMembershipSlots; slot++ {
		buf := make([]byte, checkpointMembershipSlotBytes)
		_, err := file.ReadAt(buf, int64(slot*checkpointMembershipSlotBytes))
		if err != nil && !errors.Is(err, io.EOF) {
			return checkpointMembershipCertificate{}, err
		}
		candidate, err := decodeCheckpointMembershipCertificate(buf)
		if err != nil {
			if checkpointMembershipChecksumValid(buf) {
				return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
			}
			continue
		}
		if int(candidate.sequence%checkpointMembershipSlots) != slot {
			return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
		}
		valid = append(valid, candidate)
	}
	if len(valid) == 0 {
		return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
	}
	slices.SortFunc(valid, func(a, b checkpointMembershipCertificate) int {
		if a.sequence < b.sequence {
			return -1
		}
		if a.sequence > b.sequence {
			return 1
		}
		return 0
	})
	if len(valid) == 2 {
		previous, selected := valid[0], valid[1]
		if previous.sequence == math.MaxUint64 || previous.sequence+1 != selected.sequence ||
			selected.sourceSequence < previous.sourceSequence ||
			selected.markerEpoch < previous.markerEpoch ||
			selected.applied < previous.applied ||
			selected.txnHighWater < previous.txnHighWater {
			return checkpointMembershipCertificate{}, ErrCheckpointMembershipTransition
		}
	}
	return valid[len(valid)-1], nil
}

func checkpointMembershipChecksumValid(buf []byte) bool {
	if len(buf) != checkpointMembershipSlotBytes {
		return false
	}
	h := sha256.New()
	_, _ = h.Write(checkpointMembershipDigestDomain)
	_, _ = h.Write(buf[:checkpointMembershipChecksumOffset])
	return slices.Equal(buf[checkpointMembershipChecksumOffset:], h.Sum(nil))
}
