package durable

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
)

// FinalizeMembershipTransition binds an already-prepared replacement to its
// sole committed control transition. The exclusive checkpoint owner must first
// verify the exact persisted command (including term/index) and permanently
// fence the source machine. commandDigest binds that verified command; this
// storage layer does not interpret or independently authorize its semantics.
//
// Only one applied index and one physical transaction may separate preparation
// from finalization. Every other source-digest field must still match. Fresh
// target images remain immutable; only shared source members acquire their
// newly checkpointed generations. The original witness remains the recovery
// authority, so finalization never requires rewriting a committed Raft entry.
func (g *CheckpointGroup) FinalizeMembershipTransition(
	witness CheckpointMembershipWitness,
	authorization [sha256.Size]byte,
	committedApplied uint64,
	commandDigest [sha256.Size]byte,
) error {
	return g.finalizeMembershipTransition(witness, authorization, 0, committedApplied, commandDigest)
}

// FinalizeMembershipTransitionAfterEmptySuffix permits leader no-op entries
// between preparation and the sole semantic transition. The schema owner must
// have authenticated the complete WAL suffix as empty normal entries before
// persisting the activation bound. This layer additionally proves that the
// checkpoint transaction and applied deltas are identical and that resetting
// those counters reconstructs the exact prepared source certificate.
func (g *CheckpointGroup) FinalizeMembershipTransitionAfterEmptySuffix(
	witness CheckpointMembershipWitness, authorization [sha256.Size]byte,
	preparedApplied, committedApplied uint64, commandDigest [sha256.Size]byte,
) error {
	if preparedApplied == 0 || committedApplied <= preparedApplied+1 {
		return ErrCheckpointMembershipTransition
	}
	return g.finalizeMembershipTransition(
		witness, authorization, preparedApplied, committedApplied, commandDigest,
	)
}

func (g *CheckpointGroup) finalizeMembershipTransition(
	witness CheckpointMembershipWitness, authorization [sha256.Size]byte,
	preparedApplied, committedApplied uint64, commandDigest [sha256.Size]byte,
) error {
	if g == nil || authorization == ([sha256.Size]byte{}) || committedApplied == 0 ||
		commandDigest == ([sha256.Size]byte{}) {
		return ErrCheckpointMembershipTransition
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	prior, err := openCheckpointMembershipCertificate(g.log)
	if err != nil || checkpointMembershipWitness(prior) != witness || prior.authorization != authorization {
		return errors.Join(ErrCheckpointMembershipTransition, err)
	}
	current := g.certificateLocked()
	if !checkpointMembershipCommittedSourceMatchesAt(
		prior, current, preparedApplied, committedApplied, commandDigest,
	) {
		return ErrCheckpointMembershipTransition
	}
	if prior.prepared.Sequence != 0 {
		return g.ensureMembershipDurableLocked(prior)
	}
	if err := g.ensureMembershipDurableLocked(prior); err != nil {
		return err
	}
	// Fold the sole committed transition before certifying shared member roots.
	// A crash before the final record is written can retry from the same exact
	// source digest; checkpoint/marker rollover does not change that digest.
	if err := g.checkpointLocked(); err != nil {
		return err
	}
	if err := g.recycleMarkerLocked(); err != nil {
		return err
	}
	current = g.certificateLocked()
	final := prior
	final.sequence, final.prepared, final.commandDigest = prior.sequence+1, witness, commandDigest
	final.sourceSequence, final.applied, final.txnHighWater = current.sequence, current.applied, current.txnHighWater
	final.markerEpoch, final.markerID = current.markerEpoch, current.markerID
	final.source = checkpointMembershipSourceDigest(current)
	final.members = slices.Clone(prior.members)
	for i := range final.members {
		member := &final.members[i]
		for _, shared := range g.members {
			if member.storeID != shared.storeID {
				continue
			}
			if member.nameDigest != shared.nameDigest || member.journalID != shared.journalID ||
				member.pathDigest != sha256.Sum256([]byte(filepath.Base(shared.collection.file.Name()))) ||
				shared.collection.Generation() != shared.collection.DurableGeneration() ||
				shared.collection.DurableGeneration() < member.generation {
				return ErrCheckpointMembershipTransition
			}
			member.generation = shared.collection.DurableGeneration()
			break
		}
	}
	final.target = checkpointMembershipTargetDigest(final.members)
	_, err = writeCheckpointMembershipCertificate(g.log, final)
	if err != nil {
		// A readable record is not proof that a failed write/Sync became
		// durable. Do not let a device-silent retry certify uncertain bytes.
		return g.poisonLocked(journalCommitOutcomeUnknown(err))
	}
	g.membershipDurableSequence = final.sequence
	return nil
}

func checkpointMembershipCommittedSourceMatchesAt(
	record checkpointMembershipCertificate, current checkpointGroupCertificate,
	preparedApplied, committedApplied uint64, commandDigest [sha256.Size]byte,
) bool {
	if preparedApplied == 0 {
		return checkpointMembershipCommittedSourceMatches(record, current, committedApplied, commandDigest)
	}
	if current.applied != committedApplied || record.prepared.Sequence != 0 ||
		record.applied != preparedApplied || committedApplied <= preparedApplied ||
		current.txnHighWater < record.txnHighWater ||
		current.txnHighWater-record.txnHighWater != committedApplied-preparedApplied {
		return false
	}
	before := current
	before.applied, before.txnHighWater = record.applied, record.txnHighWater
	return checkpointMembershipSourceDigest(before) == record.source
}

// ObserveCommittedSourceMembershipTransition verifies the exact source-side
// recovery cut after one committed schema entry. The caller must first verify
// that entry's complete command, term/index, and source schema contract; the
// prepared membership record predates the command and cannot prove its bytes.
// A finalized record additionally binds commandDigest to the original witness.
//
// This read-only check never finalizes membership, selects the target, syncs
// files, or grants serving authority. The reopened source must remain fenced.
// It accepts neither an uncheckpointed live apply nor an already-selected
// target, even if the target is at the same applied index as the source cut.
func (g *CheckpointGroup) ObserveCommittedSourceMembershipTransition(
	witness CheckpointMembershipWitness,
	authorization [sha256.Size]byte,
	committedApplied uint64,
	commandDigest [sha256.Size]byte,
) error {
	if g == nil || witness.Sequence == 0 || authorization == ([sha256.Size]byte{}) ||
		committedApplied == 0 || commandDigest == ([sha256.Size]byte{}) {
		return ErrCheckpointMembershipTransition
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	record, err := openCheckpointMembershipCertificate(g.log)
	if err != nil || checkpointMembershipWitness(record) != witness || record.authorization != authorization {
		return errors.Join(ErrCheckpointMembershipTransition, err)
	}
	current := g.certificateLocked()
	if current.applied != g.certApplied.Load() || current.txnHighWater != g.certTxn.Load() ||
		!checkpointMembershipCommittedSourceMatches(record, current, committedApplied, commandDigest) {
		return ErrCheckpointMembershipTransition
	}
	return nil
}

// ObserveCommittedSourceMembershipTransitionAfterEmptySuffix is the read-only
// counterpart of FinalizeMembershipTransitionAfterEmptySuffix. The caller has
// already authenticated the WAL suffix; this layer proves that its applied and
// transaction deltas reconstruct the exact prepared checkpoint certificate.
func (g *CheckpointGroup) ObserveCommittedSourceMembershipTransitionAfterEmptySuffix(
	witness CheckpointMembershipWitness,
	authorization [sha256.Size]byte,
	preparedApplied, committedApplied uint64,
	commandDigest [sha256.Size]byte,
) error {
	if g == nil || witness.Sequence == 0 || authorization == ([sha256.Size]byte{}) ||
		preparedApplied == 0 || committedApplied <= preparedApplied+1 ||
		commandDigest == ([sha256.Size]byte{}) {
		return ErrCheckpointMembershipTransition
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.checkUsableLocked(); err != nil {
		return err
	}
	record, err := openCheckpointMembershipCertificate(g.log)
	if err != nil || checkpointMembershipWitness(record) != witness || record.authorization != authorization {
		return errors.Join(ErrCheckpointMembershipTransition, err)
	}
	current := g.certificateLocked()
	if current.applied != g.certApplied.Load() || current.txnHighWater != g.certTxn.Load() ||
		!checkpointMembershipCommittedSourceMatchesAt(record, current, preparedApplied, committedApplied, commandDigest) {
		return ErrCheckpointMembershipTransition
	}
	return nil
}

func checkpointMembershipCommittedSourceMatches(
	record checkpointMembershipCertificate, current checkpointGroupCertificate,
	committedApplied uint64, commandDigest [sha256.Size]byte,
) bool {
	if current.applied != committedApplied {
		return false
	}
	if record.prepared.Sequence != 0 {
		return record.applied == committedApplied && record.commandDigest == commandDigest &&
			record.source == checkpointMembershipSourceDigest(current)
	}
	if record.sequence == math.MaxUint64 || record.applied == math.MaxUint64 || record.txnHighWater == math.MaxUint64 ||
		current.applied != record.applied+1 || current.txnHighWater != record.txnHighWater+1 {
		return false
	}
	before := current
	before.applied, before.txnHighWater = record.applied, record.txnHighWater
	return checkpointMembershipSourceDigest(before) == record.source
}

func (g *CheckpointGroup) ensureMembershipDurableLocked(record checkpointMembershipCertificate) error {
	if g.membershipDurableSequence == record.sequence {
		return nil
	}
	if err := syncCheckpointMembershipCertificate(g.log, record); err != nil {
		return g.poisonLocked(journalCommitOutcomeUnknown(err))
	}
	g.membershipDurableSequence = record.sequence
	return nil
}

// Recovery may inherit a readable but unsynced publication from another
// process. Its first acknowledgement fences both the exact record and name.
func syncCheckpointMembershipCertificate(log *TxnLog, record checkpointMembershipCertificate) (resultErr error) {
	info, err := log.root.Lstat(checkpointMembershipFilename)
	if err != nil || !info.Mode().IsRegular() {
		return errors.Join(ErrCheckpointMembershipTransition, err)
	}
	file, err := log.root.OpenFile(checkpointMembershipFilename, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return errors.Join(ErrCheckpointMembershipTransition, err)
	}
	current, err := readCheckpointMembershipFile(file)
	if err != nil {
		return err
	}
	want, err := encodeCheckpointMembershipCertificate(record)
	if err != nil {
		return err
	}
	got, err := encodeCheckpointMembershipCertificate(current)
	if err != nil || !slices.Equal(want, got) {
		return errors.Join(ErrCheckpointMembershipTransition, err)
	}
	if err := file.Sync(); err != nil {
		return journalCommitOutcomeUnknown(err)
	}
	if checkpointGroupFaultHook != nil {
		if err := checkpointGroupFaultHook(checkpointGroupAfterMembershipSync); err != nil {
			return journalCommitOutcomeUnknown(err)
		}
	}
	if err := syncTxnLogDirectory(log.root); err != nil {
		return journalCommitOutcomeUnknown(err)
	}
	return nil
}

// ValidateFinalizedCheckpointMembershipTransition checks the bounded durable
// finalization receipt before a catalog-authorized namespace move. It neither
// opens collections nor grants serving authority. The current certificate must
// still be the exact finalized source or an already-selected target; recovery
// additionally verifies every selected target member's durable generation.
func ValidateFinalizedCheckpointMembershipTransition(
	dir string,
	witness CheckpointMembershipWitness,
	authorization [sha256.Size]byte,
	committedApplied uint64,
	commandDigest [sha256.Size]byte,
) error {
	return validateFinalizedCheckpointMembershipTransition(dir, witness, authorization, committedApplied, commandDigest, false)
}

// ValidateSelectedCheckpointMembershipTransition additionally proves that the
// current checkpoint certificate names exactly the finalized target stores.
// A published SQL catalog alone cannot authorize retiring the source images.
// Later data checkpoints are allowed only within that exact target membership.
func ValidateSelectedCheckpointMembershipTransition(
	dir string,
	witness CheckpointMembershipWitness,
	authorization [sha256.Size]byte,
	committedApplied uint64,
	commandDigest [sha256.Size]byte,
) error {
	return validateFinalizedCheckpointMembershipTransition(dir, witness, authorization, committedApplied, commandDigest, true)
}

func validateFinalizedCheckpointMembershipTransition(
	dir string, witness CheckpointMembershipWitness, authorization [sha256.Size]byte,
	committedApplied uint64, commandDigest [sha256.Size]byte, selectedRequired bool,
) (resultErr error) {
	if !filepath.IsAbs(dir) || filepath.Clean(dir) != dir || dir == string(filepath.Separator) ||
		witness.Sequence == 0 || authorization == ([sha256.Size]byte{}) || committedApplied == 0 || commandDigest == ([sha256.Size]byte{}) {
		return ErrCheckpointMembershipTransition
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrCheckpointMembershipTransition, err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	log := &TxnLog{root: root}
	record, err := openCheckpointMembershipCertificate(log)
	if err != nil {
		return fmt.Errorf("open finalized membership receipt: %w", err)
	}
	if record.prepared != witness || record.authorization != authorization || record.applied != committedApplied || record.commandDigest != commandDigest {
		return fmt.Errorf("%w: finalized receipt witness=%t authorization=%t applied=%d/%d command=%t",
			ErrCheckpointMembershipTransition, record.prepared == witness,
			record.authorization == authorization, record.applied, committedApplied,
			record.commandDigest == commandDigest)
	}
	file, selected, err := openCheckpointGroupCertificateFlags(log, os.O_RDONLY)
	if err != nil {
		return fmt.Errorf("open selected checkpoint certificate: %w", err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	if !selectedRequired && checkpointMembershipSourceDigest(selected) == record.source {
		return nil
	}
	if selected.applied < record.applied || selected.txnHighWater < record.txnHighWater ||
		selected.markerID != record.markerID || len(selected.members) != len(record.members) {
		return fmt.Errorf("%w: selected checkpoint applied=%d/%d txn=%d/%d marker=%t members=%d/%d",
			ErrCheckpointMembershipTransition, selected.applied, record.applied,
			selected.txnHighWater, record.txnHighWater, selected.markerID == record.markerID,
			len(selected.members), len(record.members))
	}
	for i, member := range record.members {
		current := selected.members[i]
		if member.nameDigest != current.nameDigest || member.storeID != current.storeID || member.journalID != current.journalID {
			return fmt.Errorf("%w: selected checkpoint member %d name=%t store=%t journal=%t",
				ErrCheckpointMembershipTransition, i, member.nameDigest == current.nameDigest,
				member.storeID == current.storeID, member.journalID == current.journalID)
		}
	}
	return nil
}

func checkpointMembershipFinalizedMembers(before, after []checkpointMembershipMember) bool {
	if len(before) != len(after) {
		return false
	}
	for i, member := range before {
		generation := after[i].generation
		member.generation = generation
		if generation < before[i].generation || member != after[i] {
			return false
		}
	}
	return true
}
