package raftstore

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
)

const (
	familyManifestBytes = 4096
	familySlotBytes     = 512
	familySlotCount     = 2
	familySlotTagOffset = familySlotBytes - sha256.Size
)

var (
	familySlotMagic = [8]byte{'V', 'D', 'B', 'R', 'F', 'A', 'M', 0}
	familyKeyDomain = []byte("vibedb/raft-wal/family-manifest-key/fixed\x00")
)

type familyPhase uint8

const (
	familyPhaseSource    familyPhase = 1
	familyPhaseSelecting familyPhase = 2
	familyPhaseActive    familyPhase = 3
)

type familyState struct {
	slotGeneration      uint64
	phase               familyPhase
	familyID            [16]byte
	identityDigest      [sha256.Size]byte
	activeGeneration    uint64
	activeFileID        [16]byte
	activeHeaderDigest  [sha256.Size]byte
	activeBindingDigest [sha256.Size]byte
	parentBindingDigest [sha256.Size]byte
	sourceFileID        [16]byte
	sourceCutDigest     [sha256.Size]byte
	snapshotBaseDigest  [sha256.Size]byte
	retentionCommitment [sha256.Size]byte
}

type decodedFamilySlot struct {
	state  familyState
	absent bool
}

func familyManifestKey(
	key Key,
	familyID [16]byte,
	identityDigest [sha256.Size]byte,
) [sha256.Size]byte {
	mac := hmac.New(sha256.New, key.Material[:])
	_, _ = mac.Write(familyKeyDomain)
	_, _ = mac.Write(familyID[:])
	_, _ = mac.Write(identityDigest[:])
	var keyIDBytes [8]byte
	binary.LittleEndian.PutUint64(keyIDBytes[:], uint64(len(key.ID)))
	_, _ = mac.Write(keyIDBytes[:])
	_, _ = mac.Write([]byte(key.ID))
	var result [sha256.Size]byte
	_ = mac.Sum(result[:0])
	return result
}

func marshalFamilySlot(
	state familyState,
	slot uint8,
	key [sha256.Size]byte,
) ([familySlotBytes]byte, error) {
	if slot >= familySlotCount || !validFamilyState(state) {
		return [familySlotBytes]byte{}, ErrInvalid
	}
	var result [familySlotBytes]byte
	copy(result[0:8], familySlotMagic[:])
	binary.LittleEndian.PutUint16(result[8:10], codecVersion)
	result[10] = slot
	result[11] = byte(state.phase)
	binary.LittleEndian.PutUint64(result[16:24], state.slotGeneration)
	copy(result[24:40], state.familyID[:])
	copy(result[40:72], state.identityDigest[:])
	binary.LittleEndian.PutUint64(result[72:80], state.activeGeneration)
	copy(result[80:96], state.activeFileID[:])
	copy(result[96:128], state.activeHeaderDigest[:])
	copy(result[128:160], state.activeBindingDigest[:])
	copy(result[160:192], state.parentBindingDigest[:])
	copy(result[192:208], state.sourceFileID[:])
	copy(result[208:240], state.sourceCutDigest[:])
	copy(result[240:272], state.snapshotBaseDigest[:])
	copy(result[272:304], state.retentionCommitment[:])
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(result[:familySlotTagOffset])
	_ = mac.Sum(result[familySlotTagOffset:familySlotTagOffset])
	return result, nil
}

func unmarshalFamilySlot(
	src []byte,
	wantSlot uint8,
	wantFamily [16]byte,
	wantIdentity [sha256.Size]byte,
	key [sha256.Size]byte,
) (decodedFamilySlot, error) {
	if len(src) != familySlotBytes || wantSlot >= familySlotCount {
		return decodedFamilySlot{}, ErrCorrupt
	}
	if allZero(src) {
		return decodedFamilySlot{absent: true}, nil
	}
	if !bytes.Equal(src[0:8], familySlotMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != codecVersion ||
		src[10] != wantSlot || !allZero(src[12:16]) ||
		!allZero(src[304:familySlotTagOffset]) {
		return decodedFamilySlot{}, fmt.Errorf("%w: family slot envelope", ErrCorrupt)
	}
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write(src[:familySlotTagOffset])
	var tag [sha256.Size]byte
	_ = mac.Sum(tag[:0])
	if subtle.ConstantTimeCompare(tag[:], src[familySlotTagOffset:]) != 1 {
		return decodedFamilySlot{}, fmt.Errorf("%w: family slot authentication", ErrCorrupt)
	}
	state := familyState{
		slotGeneration:   binary.LittleEndian.Uint64(src[16:24]),
		phase:            familyPhase(src[11]),
		activeGeneration: binary.LittleEndian.Uint64(src[72:80]),
	}
	copy(state.familyID[:], src[24:40])
	copy(state.identityDigest[:], src[40:72])
	copy(state.activeFileID[:], src[80:96])
	copy(state.activeHeaderDigest[:], src[96:128])
	copy(state.activeBindingDigest[:], src[128:160])
	copy(state.parentBindingDigest[:], src[160:192])
	copy(state.sourceFileID[:], src[192:208])
	copy(state.sourceCutDigest[:], src[208:240])
	copy(state.snapshotBaseDigest[:], src[240:272])
	copy(state.retentionCommitment[:], src[272:304])
	if !validFamilyState(state) || state.familyID != wantFamily ||
		state.identityDigest != wantIdentity {
		return decodedFamilySlot{}, fmt.Errorf("%w: family slot state", ErrCorrupt)
	}
	return decodedFamilySlot{state: state}, nil
}

func validFamilyState(state familyState) bool {
	if state.slotGeneration == 0 || state.familyID == ([16]byte{}) ||
		state.identityDigest == ([sha256.Size]byte{}) ||
		state.activeFileID == ([16]byte{}) ||
		state.activeHeaderDigest == ([sha256.Size]byte{}) {
		return false
	}
	if state.phase == familyPhaseSource {
		return state.slotGeneration == 1 && state.activeGeneration == 0 &&
			state.activeBindingDigest == ([sha256.Size]byte{}) &&
			state.parentBindingDigest == ([sha256.Size]byte{}) &&
			state.sourceFileID == ([16]byte{}) &&
			state.sourceCutDigest == ([sha256.Size]byte{}) &&
			state.snapshotBaseDigest == ([sha256.Size]byte{}) &&
			state.retentionCommitment == ([sha256.Size]byte{})
	}
	validTransitionGeneration :=
		(state.phase == familyPhaseSelecting && state.slotGeneration >= 2 && state.slotGeneration%2 == 0) ||
			(state.phase == familyPhaseActive && state.slotGeneration >= 3 && state.slotGeneration%2 == 1)
	return validTransitionGeneration &&
		state.activeGeneration != 0 &&
		state.activeBindingDigest != ([sha256.Size]byte{}) &&
		((state.activeGeneration == FirstWALGeneration &&
			state.parentBindingDigest == ([sha256.Size]byte{})) ||
			(state.activeGeneration > FirstWALGeneration &&
				state.parentBindingDigest != ([sha256.Size]byte{}))) &&
		state.sourceFileID != ([16]byte{}) &&
		state.sourceCutDigest != ([sha256.Size]byte{}) &&
		state.snapshotBaseDigest != ([sha256.Size]byte{}) &&
		state.retentionCommitment != ([sha256.Size]byte{})
}

func selectFamilyState(slots [familySlotCount]decodedFamilySlot) (
	familyState,
	uint8,
	error,
) {
	if slots[0].absent && slots[1].absent {
		return familyState{}, 0, ErrCorrupt
	}
	if slots[0].absent {
		return slots[1].state, 1, nil
	}
	if slots[1].absent {
		return slots[0].state, 0, nil
	}
	left, right := slots[0].state, slots[1].state
	if left.slotGeneration == right.slotGeneration {
		if left != right || left.phase != familyPhaseSource || left.slotGeneration != 1 {
			return familyState{}, 0, fmt.Errorf("%w: divergent equal family slots", ErrCorrupt)
		}
		return right, 1, nil
	}
	if (left.slotGeneration > right.slotGeneration &&
		left.slotGeneration-right.slotGeneration != 1) ||
		(right.slotGeneration > left.slotGeneration &&
			right.slotGeneration-left.slotGeneration != 1) {
		return familyState{}, 0, fmt.Errorf("%w: family slot generations", ErrCorrupt)
	}
	if left.slotGeneration > right.slotGeneration {
		if !validFamilyTransition(right, left) {
			return familyState{}, 0, fmt.Errorf("%w: family slot transition", ErrCorrupt)
		}
		return left, 0, nil
	}
	if !validFamilyTransition(left, right) {
		return familyState{}, 0, fmt.Errorf("%w: family slot transition", ErrCorrupt)
	}
	return right, 1, nil
}

func validFamilyTransition(previous, next familyState) bool {
	if next.slotGeneration != previous.slotGeneration+1 ||
		next.familyID != previous.familyID || next.identityDigest != previous.identityDigest {
		return false
	}
	switch {
	case previous.phase == familyPhaseSource && next.phase == familyPhaseSelecting:
		return next.activeGeneration == FirstWALGeneration &&
			next.parentBindingDigest == ([sha256.Size]byte{}) &&
			next.sourceFileID == previous.activeFileID
	case previous.phase == familyPhaseSelecting && next.phase == familyPhaseActive:
		copy := next
		copy.slotGeneration = previous.slotGeneration
		copy.phase = previous.phase
		return copy == previous
	case previous.phase == familyPhaseActive && next.phase == familyPhaseSelecting:
		return previous.activeGeneration != ^uint64(0) &&
			next.activeGeneration == previous.activeGeneration+1 &&
			next.parentBindingDigest == previous.activeBindingDigest &&
			next.sourceFileID == previous.activeFileID
	default:
		return false
	}
}
