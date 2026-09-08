package seglog

import (
	"errors"
	"testing"
)

func checkpointRetirementTestRef(id byte) (fileID, [32]byte) {
	var file fileID
	file[0] = id
	var hash [32]byte
	hash[0] = id + 0x40
	return file, hash
}

func checkpointRetirementTestSlot(current, previous byte) metadataSlot {
	slot := metadataSlot{}
	if current != 0 {
		id, hash := checkpointRetirementTestRef(current)
		slot.CheckpointID, slot.CheckpointHash = [16]byte(id), hash
	}
	if previous != 0 {
		id, hash := checkpointRetirementTestRef(previous)
		slot.PreviousCheckpointID, slot.PreviousCheckpointHash = [16]byte(id), hash
	}
	return slot
}

func TestCarryCheckpointRetirementsUsesBothPostPublicationBanks(t *testing.T) {
	store := &metadataStore{
		slotIndex:  0,
		bankSlots:  [2]metadataSlot{checkpointRetirementTestSlot(1, 2), checkpointRetirementTestSlot(3, 4)},
		bankUsable: [2]bool{true, true},
	}
	next := checkpointRetirementTestSlot(5, 1)
	beforeBanks := store.bankSlots
	if err := store.carryCheckpointRetirements(&next); err != nil {
		t.Fatal(err)
	}
	if store.bankSlots != beforeBanks {
		t.Fatal("queue calculation mutated bank state")
	}
	if next.RetiredCheckpointCount != 2 {
		t.Fatalf("retired checkpoint count=%d, want 2", next.RetiredCheckpointCount)
	}
	for i, id := range []byte{3, 4} {
		file, hash := checkpointRetirementTestRef(id)
		got := next.RetiredCheckpoints[i]
		if got.ID != file || got.Hash != hash {
			t.Fatalf("retired[%d]=%+v, want id=%v hash=%x", i, got, file, hash)
		}
	}
}

func TestCarryCheckpointRetirementsCapacityIsTransactional(t *testing.T) {
	store := &metadataStore{
		slotIndex:  0,
		bankSlots:  [2]metadataSlot{checkpointRetirementTestSlot(1, 2), checkpointRetirementTestSlot(3, 4)},
		bankUsable: [2]bool{true, true},
	}
	next := checkpointRetirementTestSlot(5, 0)
	next.RetiredCheckpointCount = 3
	for i, id := range []byte{9, 10, 11} {
		file, hash := checkpointRetirementTestRef(id)
		next.RetiredCheckpoints[i] = retiredCheckpointDescriptor{ID: file, Hash: hash}
	}
	before := next
	err := store.carryCheckpointRetirements(&next)
	if !errors.Is(err, ErrBounds) || !errors.Is(err, errCheckpointRetirementCapacity) {
		t.Fatalf("capacity error=%v", err)
	}
	if next != before {
		t.Fatal("capacity failure partially mutated candidate")
	}
}

func TestCarryCheckpointRetirementsRejectsProtectedQueueEntry(t *testing.T) {
	store := &metadataStore{
		slotIndex:  0,
		bankSlots:  [2]metadataSlot{checkpointRetirementTestSlot(1, 2), checkpointRetirementTestSlot(3, 4)},
		bankUsable: [2]bool{true, true},
	}
	next := checkpointRetirementTestSlot(5, 1)
	file, hash := checkpointRetirementTestRef(2)
	next.RetiredCheckpointCount = 1
	next.RetiredCheckpoints[0] = retiredCheckpointDescriptor{ID: file, Hash: hash}
	if err := store.carryCheckpointRetirements(&next); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("protected queued checkpoint accepted: %v", err)
	}
}
