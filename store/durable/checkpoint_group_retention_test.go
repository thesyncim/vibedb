package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/storeio"
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
		case bytes.Equal(mixed, newBytes):
			// A physical short write can still produce the complete target image
			// when every unwritten suffix byte already equals its replacement. That
			// is not a torn certificate and must remain accepted; checksum bytes in
			// particular have a legitimate 1/256 last-byte collision probability.
			if decodeErr != nil || decoded.sequence != candidate.sequence ||
				!equalCheckpointGroupCertificateBody(decoded, candidate) {
				t.Fatalf("complete mirror at cut %d = sequence %d, %v",
					cut, decoded.sequence, decodeErr)
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

func TestCheckpointGroupRetentionSealReservesTerminalSequenceBudget(t *testing.T) {
	for _, test := range []struct {
		name     string
		sequence uint64
		dirty    bool
	}{
		{name: "checkpoint-plus-two-slots", sequence: math.MaxUint64 - 2, dirty: true},
		{name: "one-sequence-left", sequence: math.MaxUint64 - 1},
		{name: "sequence-exhausted", sequence: math.MaxUint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, members, _, group := newCheckpointGroupTestStore(t, 8)
			checkpointGroupPut(t, group, 1, members, "one")
			if err := group.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			checkpointGroupTestRewriteCertificateSequences(
				t,
				group,
				test.sequence-1,
				test.sequence,
			)
			applied := uint64(1)
			if test.dirty {
				group.mu.Lock()
				originalTxn, originalApplied := group.txn, group.applied
				group.txn++
				group.applied++
				applied = group.applied
				group.mu.Unlock()
				t.Cleanup(func() {
					group.mu.Lock()
					group.txn = originalTxn
					group.applied = originalApplied
					group.poison = nil
					group.log.poison = nil
					group.mu.Unlock()
				})
			}

			beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
			beforeStats := group.Stats()
			group.mu.Lock()
			beforeOwner := group.certificateLocked()
			group.mu.Unlock()
			faults := 0
			previousHook := checkpointGroupFaultHook
			checkpointGroupFaultHook = func(checkpointGroupFaultPoint) error {
				faults++
				return nil
			}
			t.Cleanup(func() { checkpointGroupFaultHook = previousHook })

			witness, err := group.SealRetentionFloor(applied)
			checkpointGroupFaultHook = previousHook
			if !witness.IsZero() || !errors.Is(err, ErrCheckpointGroupSequence) {
				t.Fatalf("terminal seal = %+v, %v", witness, err)
			}
			if faults != 0 {
				t.Fatalf("terminal seal crossed %d write/Sync fault points", faults)
			}
			if afterStats := group.Stats(); afterStats != beforeStats {
				t.Fatalf("terminal seal stats: before=%+v after=%+v", beforeStats, afterStats)
			}
			requireCheckpointGroupDirectoryBytes(t, dir, beforeDirectory)
			group.mu.Lock()
			afterOwner := group.certificateLocked()
			ownerPoison := group.poison
			logPoison := group.log.poison
			group.mu.Unlock()
			if afterOwner.sequence != beforeOwner.sequence ||
				!equalCheckpointGroupCertificateBody(afterOwner, beforeOwner) ||
				ownerPoison != nil || logPoison != nil {
				t.Fatalf(
					"terminal seal mutated owner: before=%+v after=%+v poison=%v/%v",
					beforeOwner,
					afterOwner,
					ownerPoison,
					logPoison,
				)
			}
		})
	}
}

func TestCheckpointGroupTerminalSequenceExactLastSuccessors(t *testing.T) {
	t.Run("new-seal-consumes-two", func(t *testing.T) {
		dir, members, _, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members, "one")
		if err := group.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-3,
			math.MaxUint64-2,
		)
		before := group.Stats()

		witness, err := group.SealRetentionFloor(1)
		if err != nil {
			t.Fatalf("last two certificate successors: %v", err)
		}
		after := group.Stats()
		if after.CertificateSyncs-before.CertificateSyncs != 2 {
			t.Fatalf(
				"last seal certificate Syncs = %d, want 2",
				after.CertificateSyncs-before.CertificateSyncs,
			)
		}
		checkpointGroupTestRequireTerminalPair(t, group, witness, func(previous, current checkpointGroupCertificate) {
			if !equalCheckpointGroupCertificateBody(previous, current) {
				t.Fatal("terminal mirrored retention certificates differ")
			}
		})
		checkpointGroupTestRequireTerminalSuccessorRejected(t, dir, group)
		checkpointGroupTestRequireTerminalReopenSucceeds(t, dir)
	})

	t.Run("dirty-checkpoint-consumes-one", func(t *testing.T) {
		dir, members, _, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members, "one")
		witness, err := group.SealRetentionFloor(1)
		if err != nil {
			t.Fatal(err)
		}
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-2,
			math.MaxUint64-1,
		)
		checkpointGroupPut(t, group, 2, members, "two")
		before := group.Stats()
		if err := group.Checkpoint(); err != nil {
			t.Fatalf("last checkpoint successor: %v", err)
		}
		after := group.Stats()
		if after.CertificateSyncs-before.CertificateSyncs != 1 ||
			after.Checkpoints-before.Checkpoints != 1 {
			t.Fatalf("last checkpoint stats: before=%+v after=%+v", before, after)
		}
		checkpointGroupTestRequireTerminalPair(t, group, witness, func(previous, current checkpointGroupCertificate) {
			if previous.applied != 1 || current.applied != 2 ||
				previous.txnHighWater+1 != current.txnHighWater {
				t.Fatalf(
					"terminal dirty checkpoint pair = previous %+v current %+v",
					previous,
					current,
				)
			}
		})
		checkpointGroupTestRequireTerminalSuccessorRejected(t, dir, group)
		checkpointGroupTestRequireTerminalReopenSucceeds(t, dir)
	})

	t.Run("marker-recycle-consumes-one", func(t *testing.T) {
		dir, members, _, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members, "one")
		witness, err := group.SealRetentionFloor(1)
		if err != nil {
			t.Fatal(err)
		}
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-2,
			math.MaxUint64-1,
		)
		before := group.Stats()
		group.mu.Lock()
		err = group.recycleMarkerLocked()
		group.mu.Unlock()
		if err != nil {
			t.Fatalf("last marker-recycle successor: %v", err)
		}
		after := group.Stats()
		if after.CertificateSyncs-before.CertificateSyncs != 1 ||
			after.MarkerSyncs-before.MarkerSyncs != 1 {
			t.Fatalf("last marker-recycle stats: before=%+v after=%+v", before, after)
		}
		checkpointGroupTestRequireTerminalPair(t, group, witness, func(previous, current checkpointGroupCertificate) {
			if current.markerEpoch != previous.markerEpoch+1 ||
				current.txnBase != current.txnHighWater ||
				previous.applied != current.applied ||
				previous.txnHighWater != current.txnHighWater {
				t.Fatalf(
					"terminal marker successor = previous %+v current %+v",
					previous,
					current,
				)
			}
		})
		checkpointGroupTestRequireTerminalSuccessorRejected(t, dir, group)
		checkpointGroupTestRequireTerminalReopenSucceeds(t, dir)
	})
}

func TestCheckpointGroupTerminalSequenceFailsBeforeCheckpointAndMarkerMutation(t *testing.T) {
	t.Run("checkpoint", func(t *testing.T) {
		dir, members, _, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members, "one")
		witness, err := group.SealRetentionFloor(1)
		if err != nil {
			t.Fatal(err)
		}
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-1,
			math.MaxUint64,
		)

		group.mu.Lock()
		originalTxn, originalApplied := group.txn, group.applied
		group.txn++
		group.applied++
		dirtyOwner := group.certificateLocked()
		group.mu.Unlock()
		t.Cleanup(func() {
			group.mu.Lock()
			group.txn = originalTxn
			group.applied = originalApplied
			group.poison = nil
			group.log.poison = nil
			group.mu.Unlock()
		})
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		faults := 0
		previousHook := checkpointGroupFaultHook
		checkpointGroupFaultHook = func(checkpointGroupFaultPoint) error {
			faults++
			return nil
		}
		t.Cleanup(func() { checkpointGroupFaultHook = previousHook })

		err = group.Checkpoint()
		checkpointGroupFaultHook = previousHook
		if !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf("terminal checkpoint = %v", err)
		}
		if faults != 0 {
			t.Fatalf("terminal checkpoint crossed %d write/Sync fault points", faults)
		}
		if afterStats := group.Stats(); afterStats != beforeStats {
			t.Fatalf("terminal checkpoint stats: before=%+v after=%+v", beforeStats, afterStats)
		}
		requireCheckpointGroupDirectoryBytes(t, dir, beforeDirectory)
		group.mu.Lock()
		afterOwner := group.certificateLocked()
		ownerPoison := group.poison
		logPoison := group.log.poison
		group.mu.Unlock()
		if afterOwner.sequence != dirtyOwner.sequence ||
			!equalCheckpointGroupCertificateBody(afterOwner, dirtyOwner) ||
			ownerPoison != nil || logPoison != nil {
			t.Fatalf(
				"terminal checkpoint mutated owner: before=%+v after=%+v poison=%v/%v",
				dirtyOwner,
				afterOwner,
				ownerPoison,
				logPoison,
			)
		}
		if err := group.ValidateRetentionWitness(witness); err != nil {
			t.Fatalf("terminal checkpoint invalidated retained floor: %v", err)
		}
	})

	t.Run("marker-recycle", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members, "one")
		witness, err := group.SealRetentionFloor(1)
		if err != nil {
			t.Fatal(err)
		}
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-1,
			math.MaxUint64,
		)
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		beforeHeader := log.marker.Header()
		beforeCursor := log.marker.Cursor()
		group.mu.Lock()
		beforeOwner := group.certificateLocked()
		err = group.recycleMarkerLocked()
		afterOwner := group.certificateLocked()
		ownerPoison := group.poison
		logPoison := group.log.poison
		group.mu.Unlock()

		if !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf("terminal marker recycle = %v", err)
		}
		if afterStats := group.Stats(); afterStats != beforeStats {
			t.Fatalf("terminal recycle stats: before=%+v after=%+v", beforeStats, afterStats)
		}
		requireCheckpointGroupDirectoryBytes(t, dir, beforeDirectory)
		if afterHeader := log.marker.Header(); afterHeader != beforeHeader ||
			log.marker.Cursor() != beforeCursor {
			t.Fatalf(
				"terminal recycle mutated marker: before=%+v/%d after=%+v/%d",
				beforeHeader,
				beforeCursor,
				afterHeader,
				log.marker.Cursor(),
			)
		}
		if afterOwner.sequence != beforeOwner.sequence ||
			!equalCheckpointGroupCertificateBody(afterOwner, beforeOwner) ||
			ownerPoison != nil || logPoison != nil {
			t.Fatalf(
				"terminal recycle mutated owner: before=%+v after=%+v poison=%v/%v",
				beforeOwner,
				afterOwner,
				ownerPoison,
				logPoison,
			)
		}
		if err := group.ValidateRetentionWitness(witness); err != nil {
			t.Fatalf("terminal recycle invalidated retained floor: %v", err)
		}
	})
}

func TestCheckpointGroupTerminalSequenceRejectsMutationAdmission(t *testing.T) {
	t.Run("update", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		checkpointGroupPut(t, group, 1, members, "one")
		if _, err := group.SealRetentionFloor(1); err != nil {
			t.Fatal(err)
		}
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-1,
			math.MaxUint64,
		)
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		beforeHeader := log.marker.Header()
		beforeCursor := log.marker.Cursor()
		group.mu.Lock()
		beforeOwner := group.certificateLocked()
		group.mu.Unlock()
		called := false
		err := group.Update(2, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			called = true
			write, collectionErr := batch.Collection("system")
			if collectionErr != nil {
				return collectionErr
			}
			return write.Put([]byte("terminal"), []byte(`{"n":2}`))
		})
		if called || !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf("terminal update = called %v, err %v", called, err)
		}
		checkpointGroupTestRequireUnchangedOwner(
			t, dir, group, beforeDirectory, beforeStats, beforeHeader, beforeCursor, beforeOwner,
		)
		if _, found, err := members[0].Collection.AppendRaw(nil, []byte("terminal")); err != nil || found {
			t.Fatalf("terminal update row = found %v, err %v", found, err)
		}
	})

	t.Run("transaction-high-water", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 8)
		group.mu.Lock()
		group.txn = math.MaxUint64 - 1
		group.visibleTxn.Store(group.txn)
		group.mu.Unlock()
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		beforeHeader := log.marker.Header()
		beforeCursor := log.marker.Cursor()
		group.mu.Lock()
		beforeOwner := group.certificateLocked()
		group.mu.Unlock()
		called := false
		err := group.Update(1, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			called = true
			write, collectionErr := batch.Collection("system")
			if collectionErr != nil {
				return collectionErr
			}
			return write.Put([]byte("terminal-txn"), []byte(`{"n":1}`))
		})
		if called || !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf("terminal transaction update = called %v, err %v", called, err)
		}
		checkpointGroupTestRequireUnchangedOwner(
			t, dir, group, beforeDirectory, beforeStats, beforeHeader, beforeCursor, beforeOwner,
		)
	})

	t.Run("ordinary-small-marker-admits-sparse-declared-set", func(t *testing.T) {
		_, members, log, group := checkpointGroupTestStoreWithMarkerCapacity(
			t, 8, uint64(storeio.TxnMarkerMinSectorSize),
		)
		called := false
		err := group.Update(1, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			called = true
			write, collectionErr := batch.Collection("system")
			if collectionErr != nil {
				return collectionErr
			}
			return write.Put([]byte("sparse"), []byte(`{"n":1}`))
		})
		if err != nil || !called {
			t.Fatalf("ordinary sparse update = called %v, err %v", called, err)
		}
		if log.marker.Cursor() != log.marker.Header().Capacity {
			t.Fatalf(
				"ordinary sparse marker charge = %d/%d",
				log.marker.Cursor(),
				log.marker.Header().Capacity,
			)
		}
		if _, found, err := members[0].Collection.AppendRaw(nil, []byte("sparse")); err != nil || !found {
			t.Fatalf("ordinary sparse row = found %v, err %v", found, err)
		}
	})

	t.Run("marker-full-reserves-rollover-and-future-certificate", func(t *testing.T) {
		dir, members, log, group := checkpointGroupTestStoreWithMarkerCapacity(
			t, 8, uint64(storeio.TxnMarkerMinSectorSize),
		)
		checkpointGroupPut(t, group, 1, members, "one")
		if log.marker.Cursor() != log.marker.Header().Capacity {
			t.Fatalf(
				"marker-full fixture cursor = %d/%d",
				log.marker.Cursor(),
				log.marker.Header().Capacity,
			)
		}
		if err := group.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-2,
			math.MaxUint64-1,
		)
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		beforeHeader := log.marker.Header()
		beforeCursor := log.marker.Cursor()
		group.mu.Lock()
		beforeOwner := group.certificateLocked()
		group.mu.Unlock()
		called := false
		err := group.Update(2, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			called = true
			write, collectionErr := batch.Collection("system")
			if collectionErr != nil {
				return collectionErr
			}
			return write.Put([]byte("terminal"), []byte(`{"n":2}`))
		})
		if called || !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf("last-generation marker-full admission = called %v, err %v", called, err)
		}
		checkpointGroupTestRequireUnchangedOwner(
			t, dir, group, beforeDirectory, beforeStats, beforeHeader, beforeCursor, beforeOwner,
		)
	})

	t.Run("empty-small-marker-admits-sparse-last-update", func(t *testing.T) {
		_, members, log, group := checkpointGroupTestStoreWithMarkerCapacity(
			t, 8, uint64(storeio.TxnMarkerMinSectorSize),
		)
		group.mu.Lock()
		err := group.recycleMarkerLocked()
		group.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-2,
			math.MaxUint64-1,
		)
		called := false
		err = group.Update(1, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			called = true
			write, collectionErr := batch.Collection("system")
			if collectionErr != nil {
				return collectionErr
			}
			return write.Put([]byte("sparse"), []byte(`{"n":1}`))
		})
		if err != nil || !called {
			t.Fatalf("sparse last-generation update = called %v, err %v", called, err)
		}
		if log.marker.Cursor() != log.marker.Header().Capacity {
			t.Fatalf(
				"sparse marker charge = %d/%d",
				log.marker.Cursor(),
				log.marker.Header().Capacity,
			)
		}
		before := group.Stats()
		if err := group.Checkpoint(); err != nil {
			t.Fatalf("sparse last certificate: %v", err)
		}
		after := group.Stats()
		if after.CertificateSyncs-before.CertificateSyncs != 1 ||
			after.Checkpoints-before.Checkpoints != 1 {
			t.Fatalf("sparse last certificate stats: before=%+v after=%+v", before, after)
		}
		group.mu.Lock()
		slots, currentSlot, err := group.retentionSlotsLocked()
		group.mu.Unlock()
		if err != nil || !slots[1-currentSlot].valid || !slots[currentSlot].valid ||
			slots[1-currentSlot].certificate.sequence != math.MaxUint64-1 ||
			slots[currentSlot].certificate.sequence != math.MaxUint64 ||
			!validCheckpointGroupCertificateSuccessor(
				slots[1-currentSlot].certificate,
				slots[currentSlot].certificate,
			) {
			t.Fatalf("sparse terminal pair = slots %+v current %d err %v", slots, currentSlot, err)
		}
		if _, found, err := members[0].Collection.AppendRaw(nil, []byte("sparse")); err != nil || !found {
			t.Fatalf("sparse terminal row = found %v, err %v", found, err)
		}
	})

	t.Run("periodic-admission-reserves-future-certificate", func(t *testing.T) {
		dir, members, log, group := newCheckpointGroupTestStore(t, 1)
		checkpointGroupPut(t, group, 1, members, "one")
		if err := group.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		checkpointGroupPut(t, group, 2, members, "two")
		checkpointGroupTestRewriteCertificateSequences(
			t,
			group,
			math.MaxUint64-2,
			math.MaxUint64-1,
		)
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		beforeHeader := log.marker.Header()
		beforeCursor := log.marker.Cursor()
		group.mu.Lock()
		beforeOwner := group.certificateLocked()
		group.mu.Unlock()
		called := false
		err := group.Update(3, members, defaultTxnLimits(), func(batch *DatabaseBatch) error {
			called = true
			write, collectionErr := batch.Collection("system")
			if collectionErr != nil {
				return collectionErr
			}
			return write.Put([]byte("terminal"), []byte(`{"n":3}`))
		})
		if called || !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf("last-generation periodic admission = called %v, err %v", called, err)
		}
		checkpointGroupTestRequireUnchangedOwner(
			t, dir, group, beforeDirectory, beforeStats, beforeHeader, beforeCursor, beforeOwner,
		)
	})

	t.Run("seed", func(t *testing.T) {
		dir, members, log := newCheckpointGroupTestResources(t, "system", "user")
		if _, err := members[1].Collection.Put(
			[]byte("row"), []byte(`{"value":"staged"}`),
		); err != nil {
			t.Fatal(err)
		}
		seed := CheckpointGroupSeed{
			Applied: 9, Member: "system", Envelope: []byte(`{"state":"imported"}`),
		}
		seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
		group, err := NewSeededCheckpointGroup(
			log, members, seed, CheckpointGroupOptions{CheckpointEvery: 8},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = group.Close() })
		checkpointGroupTestRewriteSingleCertificateSequence(t, group, math.MaxUint64)
		beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
		beforeStats := group.Stats()
		beforeHeader := log.marker.Header()
		beforeCursor := log.marker.Cursor()
		group.mu.Lock()
		beforeOwner := group.certificateLocked()
		group.mu.Unlock()
		err = group.Seed(seed, members[0], defaultTxnLimits(), []byte("state"))
		if !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf("terminal seed = %v", err)
		}
		checkpointGroupTestRequireUnchangedOwner(
			t, dir, group, beforeDirectory, beforeStats, beforeHeader, beforeCursor, beforeOwner,
		)
		if _, found, err := members[0].Collection.AppendRaw(nil, []byte("state")); err != nil || found {
			t.Fatalf("terminal seed row = found %v, err %v", found, err)
		}
	})
}

func TestCheckpointGroupTerminalMarkerStateReopensReadOnly(t *testing.T) {
	for _, test := range []struct {
		name              string
		mutateCertificate func(*checkpointGroupCertificate)
		mutateHeader      func(*storeio.TxnMarkerHeader)
	}{
		{
			name: "epoch",
			mutateCertificate: func(certificate *checkpointGroupCertificate) {
				certificate.markerEpoch = math.MaxUint64
			},
			mutateHeader: func(header *storeio.TxnMarkerHeader) {
				header.Epoch = math.MaxUint64
			},
		},
		{
			name: "recycle-count",
			mutateHeader: func(header *storeio.TxnMarkerHeader) {
				header.RecycleCount = math.MaxUint64
			},
		},
		{
			name: "zero-dcsn-successor",
			mutateCertificate: func(certificate *checkpointGroupCertificate) {
				certificate.txnBase = math.MaxUint64
				certificate.txnHighWater = math.MaxUint64
			},
			mutateHeader: func(header *storeio.TxnMarkerHeader) {
				header.BaseSequence = math.MaxUint64
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, _, _, _ := newCheckpointGroupTestStore(t, 8)
			crashImage := copyCheckpointGroupDirectory(t, dir)
			if test.mutateCertificate != nil {
				checkpointGroupTestRewriteSingleDiskCertificate(
					t, crashImage, test.mutateCertificate,
				)
			}
			checkpointGroupTestRewriteMarkerHeader(
				t, crashImage, test.mutateHeader,
			)
			checkpointGroupTestRequireTerminalReopenSucceeds(t, crashImage)
		})
	}
}

func TestCheckpointGroupTerminalMarkerTransitionalBoundary(t *testing.T) {
	for _, test := range []struct {
		name              string
		mutateCertificate func(*checkpointGroupCertificate)
		mutateHeader      func(*storeio.TxnMarkerHeader)
	}{
		{
			name: "recycle-count",
			mutateHeader: func(header *storeio.TxnMarkerHeader) {
				header.Epoch++
				header.RecycleCount = math.MaxUint64
			},
		},
		{
			name: "epoch",
			mutateCertificate: func(certificate *checkpointGroupCertificate) {
				certificate.markerEpoch = math.MaxUint64 - 1
			},
			mutateHeader: func(header *storeio.TxnMarkerHeader) {
				header.Epoch = math.MaxUint64
				header.RecycleCount++
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir, _, _, _ := checkpointGroupTestStoreWithMarkerCapacity(
				t, 8, uint64(storeio.TxnMarkerMinSectorSize),
			)
			crashImage := copyCheckpointGroupDirectory(t, dir)
			if test.mutateCertificate != nil {
				checkpointGroupTestRewriteSingleDiskCertificate(
					t, crashImage, test.mutateCertificate,
				)
			}
			checkpointGroupTestRewriteMarkerHeader(
				t, crashImage, test.mutateHeader,
			)
			collections, log, group := openCheckpointGroupTestCopy(t, crashImage)
			reopenedDir := log.dir
			named := []NamedCollection{
				{Name: "system", Collection: collections[0]},
				{Name: "user", Collection: collections[1]},
			}
			checkpointGroupTestRequireRejectedParticipantUpdate(
				t, reopenedDir, group, log, named[:1], group.AppliedIndex(),
			)
			closeCheckpointGroupTestHandles(t, collections, log, group)
			checkpointGroupTestRequireTerminalReopenSucceeds(t, reopenedDir)
		})
	}
}

func TestCheckpointGroupTerminalTransitionalRecoveryDoesNotWrap(t *testing.T) {
	dir, members, _, group := newCheckpointGroupTestStore(t, 8)
	checkpointGroupPut(t, group, 1, members, "one")
	if _, err := group.SealRetentionFloor(1); err != nil {
		t.Fatal(err)
	}
	group.mu.Lock()
	err := group.recycleMarkerLocked()
	group.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	checkpointGroupTestRewriteCertificateSequences(
		t,
		group,
		math.MaxUint64-1,
		math.MaxUint64,
	)

	crashImage := copyCheckpointGroupDirectory(t, dir)
	marker, _, err := storeio.OpenTxnMarker(
		filepath.Join(crashImage, txnMarkerFilename),
		storeio.TxnMarkerOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	markerCursor := marker.Cursor()
	if markerCursor != 0 {
		_ = marker.Close()
		t.Fatalf("terminal transition fixture marker cursor = %d", markerCursor)
	}
	if err := marker.Recycle(header.Epoch + 1); err != nil {
		_ = marker.Close()
		t.Fatal(err)
	}
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	beforeDirectory := checkpointGroupDirectoryBytes(t, crashImage)
	requests, files := checkpointGroupTestOpenRequests(t, crashImage)
	collections, log, recovered, err := OpenCollectionsWithCheckpointGroup(
		crashImage,
		TxnLogOptions{},
		requests,
		[]string{"system", "user"},
		CheckpointGroupOptions{CheckpointEvery: 8},
	)
	for _, file := range files {
		_ = file.Close()
	}
	if collections != nil || log != nil || recovered != nil ||
		!errors.Is(err, ErrCheckpointGroupSequence) {
		t.Fatalf(
			"terminal transitional recovery = collections %v log %v group %v err %v",
			collections,
			log,
			recovered,
			err,
		)
	}
	requireCheckpointGroupDirectoryBytes(t, crashImage, beforeDirectory)
}

func checkpointGroupTestRewriteCertificateSequences(
	t testing.TB,
	group *CheckpointGroup,
	previousSequence uint64,
	currentSequence uint64,
) {
	t.Helper()
	group.mu.Lock()
	defer group.mu.Unlock()
	slots, currentSlot, err := group.retentionSlotsLocked()
	if err != nil {
		t.Fatal(err)
	}
	if !slots[1-currentSlot].valid || previousSequence == math.MaxUint64 ||
		previousSequence+1 != currentSequence {
		t.Fatalf(
			"terminal sequence fixture = previous %d current %d slots %+v",
			previousSequence,
			currentSequence,
			slots,
		)
	}
	previous := slots[1-currentSlot].certificate
	current := slots[currentSlot].certificate
	previous.sequence = previousSequence
	current.sequence = currentSequence
	if !validCheckpointGroupCertificateSuccessor(previous, current) {
		t.Fatal("terminal sequence fixture is not a canonical successor pair")
	}
	previousRaw, err := encodeCheckpointGroupCertificate(previous)
	if err != nil {
		t.Fatal(err)
	}
	currentRaw, err := encodeCheckpointGroupCertificate(current)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := group.file.WriteAt(
		previousRaw,
		int64((previous.sequence%checkpointGroupSlots)*checkpointGroupSlotBytes),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := group.file.WriteAt(
		currentRaw,
		int64((current.sequence%checkpointGroupSlots)*checkpointGroupSlotBytes),
	); err != nil {
		t.Fatal(err)
	}
	if err := group.file.Sync(); err != nil {
		t.Fatal(err)
	}
	group.sequence = currentSequence
	if _, _, err := group.retentionSlotsLocked(); err != nil {
		t.Fatalf("terminal sequence fixture did not qualify: %v", err)
	}
}

func checkpointGroupTestRewriteSingleDiskCertificate(
	t testing.TB,
	dir string,
	mutate func(*checkpointGroupCertificate),
) {
	t.Helper()
	path := filepath.Join(dir, checkpointGroupFilename)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	valid := 0
	selectedSlot := -1
	var selected checkpointGroupCertificate
	for slot := 0; slot < checkpointGroupSlots; slot++ {
		start := slot * checkpointGroupSlotBytes
		candidate, decodeErr := decodeCheckpointGroupCertificate(
			raw[start : start+checkpointGroupSlotBytes],
		)
		if decodeErr != nil {
			continue
		}
		valid++
		selectedSlot = slot
		selected = candidate
	}
	if valid != 1 || selectedSlot < 0 {
		t.Fatalf("single-certificate fixture has %d valid slots", valid)
	}
	mutate(&selected)
	encoded, err := encodeCheckpointGroupCertificate(selected)
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	targetSlot := int(selected.sequence % checkpointGroupSlots)
	copy(raw[targetSlot*checkpointGroupSlotBytes:(targetSlot+1)*checkpointGroupSlotBytes], encoded)
	if n, writeErr := file.WriteAt(raw, 0); writeErr != nil || n != len(raw) {
		_ = file.Close()
		t.Fatalf("rewrite certificate = %d,%v", n, writeErr)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func checkpointGroupTestRewriteMarkerHeader(
	t testing.TB,
	dir string,
	mutate func(*storeio.TxnMarkerHeader),
) {
	t.Helper()
	path := filepath.Join(dir, txnMarkerFilename)
	marker, _, err := storeio.OpenTxnMarker(path, storeio.TxnMarkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	header := marker.Header()
	if err := marker.Close(); err != nil {
		t.Fatal(err)
	}
	mutate(&header)
	encoded := make([]byte, storeio.TxnMarkerHeaderSize)
	if _, err := storeio.EncodeTxnMarkerHeader(encoded, header); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	for slot := 0; slot < 2; slot++ {
		offset := int64(slot * storeio.TxnMarkerHeaderSize)
		if n, writeErr := file.WriteAt(encoded, offset); writeErr != nil || n != len(encoded) {
			_ = file.Close()
			t.Fatalf("rewrite marker slot %d = %d,%v", slot, n, writeErr)
		}
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func checkpointGroupTestStoreWithMarkerCapacity(
	t testing.TB,
	checkpointEvery uint64,
	markerCapacity uint64,
) (string, []NamedCollection, *TxnLog, *CheckpointGroup) {
	t.Helper()
	dir := t.TempDir()
	members := make([]NamedCollection, 0, 2)
	for _, name := range []string{"system", "user"} {
		members = append(members, openTxnNamedCollection(t, dir, name, txnTestOptions()))
	}
	log, err := NewTxnLog(dir, TxnLogOptions{Capacity: markerCapacity})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	group, err := NewCheckpointGroup(
		log, members, CheckpointGroupOptions{CheckpointEvery: checkpointEvery},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = group.Close() })
	return dir, members, log, group
}

func checkpointGroupTestRewriteSingleCertificateSequence(
	t testing.TB,
	group *CheckpointGroup,
	sequence uint64,
) {
	t.Helper()
	group.mu.Lock()
	defer group.mu.Unlock()
	slots, currentSlot, err := group.retentionSlotsLocked()
	if err != nil {
		t.Fatal(err)
	}
	if !slots[currentSlot].valid || sequence%checkpointGroupSlots != uint64(currentSlot) {
		t.Fatalf("single terminal sequence fixture = sequence %d slots %+v", sequence, slots)
	}
	certificate := slots[currentSlot].certificate
	certificate.sequence = sequence
	raw, err := encodeCheckpointGroupCertificate(certificate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := group.file.WriteAt(
		raw,
		int64(currentSlot*checkpointGroupSlotBytes),
	); err != nil {
		t.Fatal(err)
	}
	if err := group.file.Sync(); err != nil {
		t.Fatal(err)
	}
	group.sequence = sequence
}

func checkpointGroupTestRequireTerminalPair(
	t testing.TB,
	group *CheckpointGroup,
	witness CheckpointRetentionWitness,
	check func(checkpointGroupCertificate, checkpointGroupCertificate),
) {
	t.Helper()
	group.mu.Lock()
	defer group.mu.Unlock()
	slots, currentSlot, err := group.retentionSlotsLocked()
	if err != nil {
		t.Fatal(err)
	}
	previous := slots[1-currentSlot]
	current := slots[currentSlot]
	if !previous.valid || !current.valid ||
		previous.certificate.sequence != math.MaxUint64-1 ||
		current.certificate.sequence != math.MaxUint64 ||
		!validCheckpointGroupCertificateSuccessor(previous.certificate, current.certificate) ||
		!checkpointRetentionSealMatches(previous.certificate, witness) ||
		!checkpointRetentionSealMatches(current.certificate, witness) {
		t.Fatalf("terminal certificate pair = previous %+v current %+v", previous, current)
	}
	check(previous.certificate, current.certificate)
}

func checkpointGroupTestRequireTerminalSuccessorRejected(
	t testing.TB,
	dir string,
	group *CheckpointGroup,
) {
	t.Helper()
	beforeDirectory := checkpointGroupDirectoryBytes(t, dir)
	beforeStats := group.Stats()
	beforeHeader := group.log.marker.Header()
	beforeCursor := group.log.marker.Cursor()
	group.mu.Lock()
	beforeOwner := group.certificateLocked()
	err := group.recycleMarkerLocked()
	afterOwner := group.certificateLocked()
	ownerPoison := group.poison
	logPoison := group.log.poison
	group.mu.Unlock()
	if !errors.Is(err, ErrCheckpointGroupSequence) {
		t.Fatalf("successor after terminal sequence = %v", err)
	}
	if afterStats := group.Stats(); afterStats != beforeStats {
		t.Fatalf("terminal successor stats: before=%+v after=%+v", beforeStats, afterStats)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, beforeDirectory)
	if afterHeader := group.log.marker.Header(); afterHeader != beforeHeader ||
		group.log.marker.Cursor() != beforeCursor {
		t.Fatalf(
			"terminal successor mutated marker: before=%+v/%d after=%+v/%d",
			beforeHeader,
			beforeCursor,
			afterHeader,
			group.log.marker.Cursor(),
		)
	}
	if afterOwner.sequence != beforeOwner.sequence ||
		!equalCheckpointGroupCertificateBody(afterOwner, beforeOwner) ||
		ownerPoison != nil || logPoison != nil {
		t.Fatalf(
			"terminal successor mutated owner: before=%+v after=%+v poison=%v/%v",
			beforeOwner,
			afterOwner,
			ownerPoison,
			logPoison,
		)
	}
}

func checkpointGroupTestRequireUnchangedOwner(
	t testing.TB,
	dir string,
	group *CheckpointGroup,
	beforeDirectory map[string][]byte,
	beforeStats CheckpointGroupStats,
	beforeHeader storeio.TxnMarkerHeader,
	beforeCursor uint64,
	beforeOwner checkpointGroupCertificate,
) {
	t.Helper()
	if afterStats := group.Stats(); afterStats != beforeStats {
		t.Fatalf("terminal admission stats: before=%+v after=%+v", beforeStats, afterStats)
	}
	requireCheckpointGroupDirectoryBytes(t, dir, beforeDirectory)
	if afterHeader := group.log.marker.Header(); afterHeader != beforeHeader ||
		group.log.marker.Cursor() != beforeCursor {
		t.Fatalf(
			"terminal admission mutated marker: before=%+v/%d after=%+v/%d",
			beforeHeader,
			beforeCursor,
			afterHeader,
			group.log.marker.Cursor(),
		)
	}
	group.mu.Lock()
	afterOwner := group.certificateLocked()
	ownerPoison := group.poison
	logPoison := group.log.poison
	group.mu.Unlock()
	if afterOwner.sequence != beforeOwner.sequence ||
		!equalCheckpointGroupCertificateBody(afterOwner, beforeOwner) ||
		ownerPoison != nil || logPoison != nil {
		t.Fatalf(
			"terminal admission mutated owner: before=%+v after=%+v poison=%v/%v",
			beforeOwner,
			afterOwner,
			ownerPoison,
			logPoison,
		)
	}
}

func checkpointGroupTestRequireTerminalReopenSucceeds(t *testing.T, dir string) {
	t.Helper()
	crashImage := copyCheckpointGroupDirectory(t, dir)
	beforeDirectory := checkpointGroupDirectoryBytes(t, crashImage)
	for attempt := 0; attempt < 2; attempt++ {
		requests, files := checkpointGroupTestOpenRequests(t, crashImage)
		collections, log, group, err := OpenCollectionsWithCheckpointGroup(
			crashImage,
			TxnLogOptions{},
			requests,
			[]string{"system", "user"},
			CheckpointGroupOptions{CheckpointEvery: 8},
		)
		if err != nil || len(collections) != 2 || log == nil || group == nil {
			t.Fatalf(
				"terminal reopen %d = collections %v log %v group %v err %v",
				attempt, collections, log, group, err,
			)
		}
		requireCheckpointGroupDirectoryBytes(t, crashImage, beforeDirectory)
		applied := group.AppliedIndex()
		if applied == math.MaxUint64 {
			t.Fatal("terminal reopen fixture has no legal next applied index")
		}
		named := []NamedCollection{
			{Name: "system", Collection: collections[0]},
			{Name: "user", Collection: collections[1]},
		}
		called := false
		err = group.Update(
			applied+1, named[:1], defaultTxnLimits(),
			func(*DatabaseBatch) error {
				called = true
				return nil
			},
		)
		if called || !errors.Is(err, ErrCheckpointGroupSequence) {
			t.Fatalf(
				"terminal reopen %d update = called %v, err %v",
				attempt, called, err,
			)
		}
		closeCheckpointGroupTestHandles(t, collections, log, group)
		for _, file := range files {
			_ = file.Close()
		}
		requireCheckpointGroupDirectoryBytes(t, crashImage, beforeDirectory)
	}
}
