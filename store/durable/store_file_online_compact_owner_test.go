package durable

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestCompactOnlineSingleFlight(t *testing.T) {
	_, members, _ := newCheckpointGroupTestResources(t, "user")
	collection := members[0].Collection
	collection.onlineCompactionFlight.Store(true)
	_, err := collection.CompactOnline()
	collection.onlineCompactionFlight.Store(false)
	if !errors.Is(err, storeio.ErrQueueFull) {
		t.Fatalf("concurrent compaction = %v", err)
	}
}

func TestCheckpointGroupSeedRejectsActiveOnlineCompaction(t *testing.T) {
	_, members, log := newCheckpointGroupTestResources(t, "system", "user")
	collection := members[1].Collection
	collection.onlineCompactionFlight.Store(true)
	seed := CheckpointGroupSeed{
		Applied: 9, Member: "system", Envelope: []byte(`{"state":"imported"}`),
	}
	seed.Images = checkpointGroupSeedImagesForTest(members, seed.Member)
	_, err := NewSeededCheckpointGroup(log, members, seed, CheckpointGroupOptions{})
	collection.onlineCompactionFlight.Store(false)
	if !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("seed activation during unlocked compaction scan = %v", err)
	}
	for _, member := range members {
		if member.Collection.checkpointGroup.Load() != nil {
			t.Fatal("refused seed partially attached a member")
		}
	}
	group, err := NewSeededCheckpointGroup(log, members, seed, CheckpointGroupOptions{})
	if err != nil {
		t.Fatalf("seed activation after flight releases = %v", err)
	}
	t.Cleanup(func() { _ = group.Close() })
}

func TestOnlineCompactionPublicationHelpersRejectCheckpointGroupOwner(t *testing.T) {
	_, members, _, _ := newCheckpointGroupTestStore(t, 8)
	collection := members[0].Collection
	if _, err := collection.CompactOnline(); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("CompactOnline owner fence = %v", err)
	}
	collection.writer.Lock()
	_, _, err := collection.publishOnlineMigrationReservationLocked(4096, 1)
	collection.writer.Unlock()
	if !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("reservation owner fence = %v", err)
	}
	if _, _, err := collection.growOnlineMigrationStaging(nil, 4096, 1); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("growth owner fence = %v", err)
	}
	if err := collection.retireOnlineMigrationMetadata(nil); !errors.Is(err, ErrCheckpointGroupOwned) {
		t.Fatalf("retirement owner fence = %v", err)
	}
}
