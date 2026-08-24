package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"
)

func TestCheckpointGroupRetentionSuccessorCanonicalGrammar(t *testing.T) {
	_, _, _, group := newCheckpointGroupTestStore(t, 8)
	base := group.certificateLocked()

	ordinary := base
	ordinary.sequence++
	ordinary.applied++
	ordinary.txnHighWater++
	if !validCheckpointGroupCertificateSuccessor(base, ordinary) {
		t.Fatal("ordinary zero-seal successor was rejected")
	}

	sealed := ordinary
	sealed.sequence++
	sealed.retentionApplied = sealed.applied
	sealed.retentionCommitment = checkpointRetentionSealCommitment(
		sealed,
		sealed.retentionApplied,
	)
	if !validCheckpointGroupCertificateSuccessor(ordinary, sealed) {
		t.Fatal("first exact retention seal was rejected")
	}

	mirrored := sealed
	mirrored.sequence++
	if !validCheckpointGroupCertificateSuccessor(sealed, mirrored) {
		t.Fatal("exact retention mirror was rejected")
	}

	later := mirrored
	later.sequence++
	later.applied++
	later.txnHighWater++
	if !validCheckpointGroupCertificateSuccessor(mirrored, later) {
		t.Fatal("ordinary checkpoint carrying an exact seal was rejected")
	}

	higher := later
	higher.sequence++
	higher.retentionApplied = higher.applied
	higher.retentionCommitment = checkpointRetentionSealCommitment(
		higher,
		higher.retentionApplied,
	)
	if !validCheckpointGroupCertificateSuccessor(later, higher) {
		t.Fatal("higher exact retention seal was rejected")
	}

	rollover := higher
	rollover.sequence++
	rollover.markerEpoch++
	rollover.txnBase = higher.txnHighWater
	if !validCheckpointGroupCertificateSuccessor(higher, rollover) {
		t.Fatal("marker rollover carrying an exact seal was rejected")
	}

	widened := rollover
	widened.sequence++
	widened.txnHighWater++
	widened.applied += 2
	checkpointGroupTestSetMaxSpan(
		&widened,
		2,
		widened.txnHighWater,
		widened.applied,
	)
	if !validCheckpointGroupCertificateSuccessor(rollover, widened) {
		t.Fatal("proven max-span widening carrying an exact seal was rejected")
	}

	changedCommitment := sealed
	changedCommitment.retentionCommitment[0] ^= 0xff
	changedCommitment.sequence = ordinary.sequence + 1
	ordinaryDuplicate := ordinary
	ordinaryDuplicate.sequence++
	sameFloorChanged := mirrored
	sameFloorChanged.retentionCommitment[0] ^= 0xff
	ordinarySealChange := later
	ordinarySealChange.retentionCommitment[0] ^= 0xff
	rolloverSealChange := rollover
	rolloverSealChange.retentionCommitment[0] ^= 0xff
	regressed := higher
	regressed.sequence++
	regressed.retentionApplied = sealed.retentionApplied
	regressed.retentionCommitment = checkpointRetentionSealCommitment(
		regressed,
		regressed.retentionApplied,
	)
	combinedTransition := later
	combinedTransition.sequence++
	combinedTransition.retentionApplied = combinedTransition.applied
	combinedTransition.retentionCommitment = checkpointRetentionSealCommitment(
		combinedTransition,
		combinedTransition.retentionApplied,
	)
	checkpointGroupTestSetMaxSpan(
		&combinedTransition,
		2,
		combinedTransition.txnHighWater,
		combinedTransition.applied,
	)

	for _, test := range []struct {
		name     string
		previous checkpointGroupCertificate
		selected checkpointGroupCertificate
	}{
		{name: "zero-seal duplicate", previous: ordinary, selected: ordinaryDuplicate},
		{name: "arbitrary first commitment", previous: ordinary, selected: changedCommitment},
		{name: "same-floor changed commitment", previous: sealed, selected: sameFloorChanged},
		{name: "ordinary checkpoint seal change", previous: mirrored, selected: ordinarySealChange},
		{name: "rollover seal change", previous: higher, selected: rolloverSealChange},
		{name: "retention regression", previous: higher, selected: regressed},
		{name: "same-cut seal and span change", previous: later, selected: combinedTransition},
	} {
		t.Run(test.name, func(t *testing.T) {
			if validCheckpointGroupCertificateSuccessor(test.previous, test.selected) {
				t.Fatal("invalid combined certificate transition was accepted")
			}
		})
	}
}

func TestCheckpointGroupSealRetentionFloorCannotLaunderForgedCurrentSeal(
	t *testing.T,
) {
	for _, test := range []struct {
		name      string
		canonical bool
		dirty     bool
	}{
		{name: "arbitrary-clean"},
		{name: "arbitrary-dirty-suffix", dirty: true},
		{name: "canonical-clean", canonical: true},
		{name: "canonical-dirty-suffix", canonical: true, dirty: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members, "one")
			if err := group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			if test.dirty {
				checkpointGroupPut(t, group, 2, members, "dirty-suffix")
			}
			before := group.Stats()
			wantApplied := uint64(1)
			if test.dirty {
				wantApplied = 2
			}
			if before.CheckpointAppliedIndex != 1 ||
				before.AppliedIndex != wantApplied {
				t.Fatalf("forged-seal cut precondition = %+v", before)
			}

			group.mu.Lock()
			currentSlot := int(group.sequence % checkpointGroupSlots)
			otherSlot := 1 - currentSlot
			var forged [checkpointGroupSlotBytes]byte
			var previousRaw [checkpointGroupSlotBytes]byte
			_, readErr := group.file.ReadAt(
				forged[:],
				int64(currentSlot*checkpointGroupSlotBytes),
			)
			if readErr == nil {
				_, readErr = group.file.ReadAt(
					previousRaw[:],
					int64(otherSlot*checkpointGroupSlotBytes),
				)
			}
			current, decodeErr := decodeCheckpointGroupCertificate(forged[:])
			previous, previousErr := decodeCheckpointGroupCertificate(previousRaw[:])
			if readErr != nil || decodeErr != nil || previousErr != nil {
				group.mu.Unlock()
				t.Fatalf("read forge source = %v, %v, %v", readErr, decodeErr, previousErr)
			}
			current.retentionApplied = current.applied
			if test.canonical {
				current.retentionCommitment = checkpointRetentionSealCommitment(
					current,
					current.retentionApplied,
				)
				encoded, encodeErr := encodeCheckpointGroupCertificate(current)
				if encodeErr != nil {
					group.mu.Unlock()
					t.Fatal(encodeErr)
				}
				copy(forged[:], encoded)
			} else {
				binary.LittleEndian.PutUint64(
					forged[checkpointGroupRetentionAppliedOffset:checkpointGroupRetentionCommitOffset],
					current.retentionApplied,
				)
				copy(
					forged[checkpointGroupRetentionCommitOffset:checkpointGroupRetentionEndOffset],
					bytes.Repeat(
						[]byte{0x5a},
						checkpointGroupRetentionEndOffset-checkpointGroupRetentionCommitOffset,
					),
				)
				h := sha256.New()
				_, _ = h.Write(checkpointGroupDigestDomain)
				_, _ = h.Write(forged[:checkpointGroupChecksumOffset])
				copy(forged[checkpointGroupChecksumOffset:], h.Sum(nil))
			}
			decoded, forgedDecodeErr := decodeCheckpointGroupCertificate(forged[:])
			if test.canonical {
				if forgedDecodeErr != nil {
					group.mu.Unlock()
					t.Fatalf("canonical forged slot did not decode: %v", forgedDecodeErr)
				}
				if validCheckpointGroupCertificateSuccessor(previous, decoded) {
					group.mu.Unlock()
					t.Fatal("canonical forged slot passed adjacent successor validation")
				}
			} else if !errors.Is(forgedDecodeErr, ErrCheckpointGroupCorrupt) {
				group.mu.Unlock()
				t.Fatalf("arbitrary forged slot decoded as canonical: %v", forgedDecodeErr)
			}
			_, writeErr := group.file.WriteAt(
				forged[:],
				int64(currentSlot*checkpointGroupSlotBytes),
			)
			if writeErr == nil {
				writeErr = group.file.Sync()
			}
			group.mu.Unlock()
			if writeErr != nil {
				t.Fatal(writeErr)
			}
			if !checkpointGroupCertificateChecksumValid(forged[:]) {
				t.Fatal("forged regression did not retain a valid checksum")
			}

			witness, err := group.SealRetentionFloor(before.AppliedIndex)
			if !witness.IsZero() || !errors.Is(err, ErrCheckpointGroupCorrupt) {
				t.Fatalf("forged current seal was laundered: witness %+v err %v", witness, err)
			}
			after := group.Stats()
			if after.CertificateSyncs != before.CertificateSyncs {
				t.Fatalf("forged seal triggered a certificate write: before %+v after %+v", before, after)
			}
		})
	}
}

func TestCheckpointGroupSealRetentionFloorIsExactAndIdempotent(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")

	witness, err := group.SealRetentionFloor(1)
	if err != nil {
		t.Fatal(err)
	}
	if witness.IsZero() || !witness.BindsAppliedIndex(1) ||
		witness.Commitment() == ([32]byte{}) {
		t.Fatalf("retention witness = %+v", witness)
	}
	if size := unsafe.Sizeof(witness); size != 32 {
		t.Fatalf("retention witness width = %d, want 32", size)
	}
	if err := group.ValidateRetentionWitness(witness); err != nil {
		t.Fatalf("ValidateRetentionWitness: %v", err)
	}
	sealedStats := group.Stats()
	if sealedStats.CheckpointAppliedIndex != 1 || sealedStats.CheckpointTransactions != 1 ||
		sealedStats.Checkpoints != 1 || sealedStats.CertificateSyncs != 3 ||
		sealedStats.JournalSyncs != uint64(len(members)) ||
		sealedStats.BarrierSyncs != uint64(len(members)+1) {
		t.Fatalf("sealed stats = %+v", sealedStats)
	}

	repeated, err := group.SealRetentionFloor(1)
	if err != nil || repeated != witness {
		t.Fatalf("idempotent seal = %+v, %v; want %+v", repeated, err, witness)
	}
	if got := group.Stats(); got != sealedStats {
		t.Fatalf("idempotent seal changed stats: got %+v, want %+v", got, sealedStats)
	}
	for _, stale := range []uint64{0, 2} {
		if _, err := group.SealRetentionFloor(stale); !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf("SealRetentionFloor(%d) = %v", stale, err)
		}
	}

	checkpointGroupPut(t, group, 2, members, "two")
	if err := group.ValidateRetentionWitness(witness); err != nil {
		t.Fatalf("uncertified suffix invalidated sealed floor: %v", err)
	}
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := group.ValidateRetentionWitness(witness); err != nil {
		t.Fatalf("monotonic checkpoint invalidated floor: %v", err)
	}
	checkpointImage := copyCheckpointGroupDirectory(t, dir)
	_, _, checkpointReopened := openCheckpointGroupTestCopy(t, checkpointImage)
	if err := checkpointReopened.ValidateRetentionWitness(witness); err != nil {
		t.Fatalf("ordinary checkpoint reopen invalidated floor: %v", err)
	}
	next, err := group.SealRetentionFloor(2)
	if err != nil || next == witness || !next.BindsAppliedIndex(2) {
		t.Fatalf("next seal = %+v, %v", next, err)
	}
	if err := group.ValidateRetentionWitness(next); err != nil {
		t.Fatalf("validate next witness: %v", err)
	}
	higherImage := copyCheckpointGroupDirectory(t, dir)
	_, _, higherReopened := openCheckpointGroupTestCopy(t, higherImage)
	if err := higherReopened.ValidateRetentionWitness(next); err != nil {
		t.Fatalf("higher seal reopen invalidated floor: %v", err)
	}
	if err := group.ValidateRetentionWitness(witness); !errors.Is(err, ErrCheckpointRetentionWitness) {
		t.Fatalf("higher sealed floor retained old authority: %v", err)
	}
	forged := next
	forged.commitment[0] ^= 0xff
	if err := group.ValidateRetentionWitness(forged); !errors.Is(err, ErrCheckpointRetentionWitness) {
		t.Fatalf("forged witness validation = %v", err)
	}
	if err := group.ValidateRetentionWitness(next); err != nil {
		t.Fatalf("forged witness poisoned owner: %v", err)
	}
}

func TestCheckpointGroupSealRetentionFloorAfterFoldedCheckpoint(t *testing.T) {
	_, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	before := group.Stats()
	witness, err := group.SealRetentionFloor(1)
	if err != nil {
		t.Fatal(err)
	}
	after := group.Stats()
	if after.CertificateSyncs-before.CertificateSyncs != 2 ||
		after.PhysicalCheckpoints != before.PhysicalCheckpoints ||
		after.BarrierSyncs != before.BarrierSyncs {
		t.Fatalf("folded seal stats: before=%+v after=%+v", before, after)
	}
	if err := group.ValidateRetentionWitness(witness); err != nil {
		t.Fatal(err)
	}
	if checkpointGroupRetentionAppliedOffset != 4008 ||
		checkpointGroupRetentionCommitOffset != 4016 ||
		checkpointGroupRetentionEndOffset != 4040 {
		t.Fatalf(
			"retention seal layout = [%d,%d,%d)",
			checkpointGroupRetentionAppliedOffset,
			checkpointGroupRetentionCommitOffset,
			checkpointGroupRetentionEndOffset,
		)
	}
	group.mu.Lock()
	for slot := 0; slot < checkpointGroupSlots; slot++ {
		var raw [checkpointGroupSlotBytes]byte
		if _, err := group.file.ReadAt(raw[:], int64(slot*checkpointGroupSlotBytes)); err != nil {
			group.mu.Unlock()
			t.Fatal(err)
		}
		certificate, err := decodeCheckpointGroupCertificate(raw[:])
		if err != nil || certificate.retentionApplied != 1 ||
			certificate.retentionCommitment != witness.commitment ||
			binary.LittleEndian.Uint64(
				raw[checkpointGroupRetentionAppliedOffset:checkpointGroupRetentionCommitOffset],
			) != 1 ||
			!bytes.Equal(
				raw[checkpointGroupRetentionCommitOffset:checkpointGroupRetentionEndOffset],
				witness.commitment[:],
			) ||
			!bytes.Equal(
				raw[checkpointGroupRetentionEndOffset:checkpointGroupChecksumOffset],
				make([]byte, checkpointGroupChecksumOffset-checkpointGroupRetentionEndOffset),
			) {
			group.mu.Unlock()
			t.Fatalf("slot %d retention tail = %+v, %v", slot, certificate, err)
		}
	}
	group.mu.Unlock()
}

func TestCheckpointGroupRetentionWitnessIsBoundToExactOwner(t *testing.T) {
	_, firstMembers, _, first := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, first, 1, firstMembers, "one")
	witness, err := first.SealRetentionFloor(1)
	if err != nil {
		t.Fatal(err)
	}

	_, secondMembers, _, second := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, second, 1, secondMembers, "one")
	if _, err := second.SealRetentionFloor(1); err != nil {
		t.Fatal(err)
	}
	if err := second.ValidateRetentionWitness(witness); !errors.Is(err, ErrCheckpointRetentionWitness) {
		t.Fatalf("foreign witness validation = %v", err)
	}
	if _, err := second.SealRetentionFloor(1); err != nil {
		t.Fatalf("foreign witness poisoned owner: %v", err)
	}
}

func TestCheckpointGroupRetentionMirrorEveryShortWriteFailsClosed(t *testing.T) {
	_, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	if err := group.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	group.mu.Lock()
	oldSequence := group.sequence
	oldSlot := int((oldSequence + 1) % checkpointGroupSlots)
	oldBytes := make([]byte, checkpointGroupSlotBytes)
	if _, err := group.file.ReadAt(oldBytes, int64(oldSlot*checkpointGroupSlotBytes)); err != nil {
		group.mu.Unlock()
		t.Fatal(err)
	}
	candidate := group.certificateLocked()
	candidate.sequence++
	newBytes, err := encodeCheckpointGroupCertificate(candidate)
	group.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if oldSlot != int(candidate.sequence%checkpointGroupSlots) {
		t.Fatal("mirror does not target the older slot")
	}

	for cut := 0; cut <= checkpointGroupSlotBytes; cut++ {
		mixed := append([]byte(nil), oldBytes...)
		copy(mixed[:cut], newBytes[:cut])
		decoded, decodeErr := decodeCheckpointGroupCertificate(mixed)
		switch {
		case bytes.Equal(mixed, oldBytes):
			if decodeErr != nil || decoded.sequence != oldSequence-1 {
				t.Fatalf("unchanged mirror prefix %d = sequence %d, %v", cut, decoded.sequence, decodeErr)
			}
		case cut == checkpointGroupSlotBytes:
			if decodeErr != nil || decoded.sequence != candidate.sequence ||
				!equalCheckpointGroupCertificateBody(decoded, candidate) {
				t.Fatalf("complete mirror = sequence %d, %v", decoded.sequence, decodeErr)
			}
		default:
			if decodeErr == nil || checkpointGroupCertificateChecksumValid(mixed) {
				t.Fatalf("short mirror cut %d authenticated", cut)
			}
		}
	}
}

func TestCheckpointGroupRetentionSealCrashCutsResume(t *testing.T) {
	for _, test := range []struct {
		name       string
		faultPoint checkpointGroupFaultPoint
		occurrence int
	}{
		{name: "after-first-seal-write", faultPoint: checkpointGroupAfterCertificateWrite, occurrence: 1},
		{name: "after-first-seal-sync", faultPoint: checkpointGroupAfterCertificateSync, occurrence: 1},
		{name: "after-mirror-seal-sync", faultPoint: checkpointGroupAfterCertificateSync, occurrence: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members, "one")
			if err := group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			fault := errors.New("retention crash cut")
			seen := 0
			previous := checkpointGroupFaultHook
			checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
				if point == test.faultPoint {
					seen++
					if seen == test.occurrence {
						return fault
					}
				}
				return nil
			}
			failedWitness, sealErr := group.SealRetentionFloor(1)
			checkpointGroupFaultHook = previous
			if !failedWitness.IsZero() || !errors.Is(sealErr, fault) {
				t.Fatalf("seal crash = %+v, %v", failedWitness, sealErr)
			}

			crashImage := copyCheckpointGroupDirectory(t, dir)
			clearCheckpointGroupTestPoison(group)
			_, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
			if reopened.AppliedIndex() != 1 || reopened.CheckpointAppliedIndex() != 1 {
				t.Fatalf("reopened cuts = %d/%d", reopened.AppliedIndex(), reopened.CheckpointAppliedIndex())
			}
			witness, err := reopened.SealRetentionFloor(1)
			if err != nil || witness.IsZero() {
				t.Fatalf("resume seal = %+v, %v", witness, err)
			}
			if err := reopened.ValidateRetentionWitness(witness); err != nil {
				t.Fatalf("validate resumed witness: %v", err)
			}
		})
	}
}

func TestCheckpointGroupRetentionFloorSurvivesEitherTornSlot(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	original, err := group.SealRetentionFloor(1)
	if err != nil {
		t.Fatal(err)
	}

	for slot := 0; slot < checkpointGroupSlots; slot++ {
		t.Run(string(rune('0'+slot)), func(t *testing.T) {
			crashImage := copyCheckpointGroupDirectory(t, dir)
			path := filepath.Join(crashImage, checkpointGroupFilename)
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err == nil {
				_, err = file.WriteAt(
					bytes.Repeat([]byte{0xa5}, checkpointGroupSlotBytes/2),
					int64(slot*checkpointGroupSlotBytes),
				)
			}
			if file != nil {
				err = errors.Join(err, file.Sync(), file.Close())
			}
			if err != nil {
				t.Fatal(err)
			}
			_, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
			if reopened.AppliedIndex() != 1 || reopened.CheckpointAppliedIndex() != 1 {
				t.Fatalf("reopened cuts = %d/%d", reopened.AppliedIndex(), reopened.CheckpointAppliedIndex())
			}
			recovered, err := reopened.SealRetentionFloor(1)
			if err != nil || recovered != original {
				t.Fatalf("recover torn slot seal = %+v, %v", recovered, err)
			}
			if err := reopened.ValidateRetentionWitness(recovered); err != nil {
				t.Fatalf("recover torn slot validation: %v", err)
			}
			reopened.mu.Lock()
			if int(reopened.sequence%checkpointGroupSlots) == slot {
				if err := reopened.writeNextRetentionCertificateLocked(); err != nil {
					reopened.mu.Unlock()
					t.Fatal(err)
				}
			}
			if int(reopened.sequence%checkpointGroupSlots) == slot {
				reopened.mu.Unlock()
				t.Fatalf("slot %d remained selected before tear", slot)
			}
			_, err = reopened.file.WriteAt(
				bytes.Repeat([]byte{0xa5}, checkpointGroupSlotBytes/2),
				int64(slot*checkpointGroupSlotBytes),
			)
			if err == nil {
				err = reopened.file.Sync()
			}
			reopened.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if err := reopened.ValidateRetentionWitness(original); !errors.Is(err, ErrCheckpointRetentionWitness) {
				t.Fatalf("one-slot witness qualified before repair: %v", err)
			}
			witness, err := reopened.SealRetentionFloor(1)
			if err != nil {
				t.Fatalf("reseal: %v", err)
			}
			if err := reopened.ValidateRetentionWitness(witness); err != nil {
				t.Fatalf("validate reseal: %v", err)
			}
			if err := reopened.ValidateRetentionWitness(original); err != nil {
				t.Fatalf("reseal invalidated original floor: %v", err)
			}
		})
	}
}

func TestCheckpointGroupRetentionWitnessRejectsAuthenticatedRollback(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	lower, err := group.SealRetentionFloor(1)
	if err != nil {
		t.Fatal(err)
	}
	rollbackImage := copyCheckpointGroupDirectory(t, dir)

	checkpointGroupPut(t, group, 2, members, "two")
	higher, err := group.SealRetentionFloor(2)
	if err != nil {
		t.Fatal(err)
	}
	_, _, rolledBack := openCheckpointGroupTestCopy(t, rollbackImage)
	if err := rolledBack.ValidateRetentionWitness(higher); !errors.Is(err, ErrCheckpointRetentionWitness) {
		t.Fatalf("authenticated whole-store rollback validation = %v", err)
	}
	if err := rolledBack.ValidateRetentionWitness(lower); err != nil {
		t.Fatalf("rollback rejected its exact lower witness: %v", err)
	}
}

func TestCheckpointGroupRetentionSealSurvivesMarkerRecycleAndReopen(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	witness, err := group.SealRetentionFloor(1)
	if err != nil {
		t.Fatal(err)
	}
	group.mu.Lock()
	err = group.recycleMarkerLocked()
	group.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := group.ValidateRetentionWitness(witness); err != nil {
		t.Fatalf("marker recycle invalidated retention seal: %v", err)
	}

	crashImage := copyCheckpointGroupDirectory(t, dir)
	_, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if err := reopened.ValidateRetentionWitness(witness); err != nil {
		t.Fatalf("reopen invalidated retention seal: %v", err)
	}
	resumed, err := reopened.SealRetentionFloor(1)
	if err != nil || resumed != witness {
		t.Fatalf("reopened idempotent seal = %+v, %v want %+v", resumed, err, witness)
	}
}

func TestCheckpointGroupRetentionSealSurvivesSustainedPeriodicCheckpoints(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	witness, err := group.SealRetentionFloor(1)
	if err != nil {
		t.Fatal(err)
	}
	before := group.Stats()
	for applied := uint64(2); applied <= 42; applied++ {
		checkpointGroupPut(t, group, applied, members, "churn")
		if err := group.ValidateRetentionWitness(witness); err != nil {
			t.Fatalf("periodic checkpoint at applied %d invalidated seal: %v", applied, err)
		}
	}
	after := group.Stats()
	if after.Checkpoints-before.Checkpoints < 5 ||
		after.CertificateSyncs-before.CertificateSyncs < 5 {
		t.Fatalf("periodic checkpoint churn: before=%+v after=%+v", before, after)
	}

	crashImage := copyCheckpointGroupDirectory(t, dir)
	_, _, reopened := openCheckpointGroupTestCopy(t, crashImage)
	if err := reopened.ValidateRetentionWitness(witness); err != nil {
		t.Fatalf("periodic checkpoint reopen invalidated seal: %v", err)
	}
}

func TestCheckpointGroupRetentionSealSerializesConcurrentTransition(t *testing.T) {
	_, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")

	mirrorReached := make(chan struct{})
	releaseMirror := make(chan struct{})
	previous := checkpointGroupFaultHook
	syncs := 0
	checkpointGroupFaultHook = func(point checkpointGroupFaultPoint) error {
		if point == checkpointGroupAfterCertificateSync {
			syncs++
			if syncs == 2 {
				close(mirrorReached)
				<-releaseMirror
			}
		}
		return nil
	}
	t.Cleanup(func() { checkpointGroupFaultHook = previous })

	type sealResult struct {
		witness CheckpointRetentionWitness
		err     error
	}
	sealed := make(chan sealResult, 1)
	go func() {
		witness, err := group.SealRetentionFloor(1)
		sealed <- sealResult{witness: witness, err: err}
	}()
	select {
	case <-mirrorReached:
	case <-time.After(5 * time.Second):
		t.Fatal("retention mirror did not reach Sync")
	}

	updateStarted := make(chan struct{})
	updated := make(chan error, 1)
	go func() {
		close(updateStarted)
		updated <- group.Update(2, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			for _, member := range members {
				write, err := batch.Collection(member.Name)
				if err != nil {
					return err
				}
				if err := write.Put([]byte("two"), []byte(`{"n":2}`)); err != nil {
					return err
				}
			}
			return nil
		})
	}()
	<-updateStarted
	select {
	case err := <-updated:
		t.Fatalf("transition crossed retention seal: %v", err)
	default:
	}
	close(releaseMirror)
	result := <-sealed
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := <-updated; err != nil {
		t.Fatal(err)
	}
	checkpointGroupFaultHook = previous
	if err := group.ValidateRetentionWitness(result.witness); err != nil {
		t.Fatalf("uncertified concurrent suffix invalidated sealed floor: %v", err)
	}
}
