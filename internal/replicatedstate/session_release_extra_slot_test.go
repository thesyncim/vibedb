package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/store/durable"
)

// TestSessionReleaseRejectsExtraSameDigestSlot pins the bounded-prefix proof at
// the release boundary. A header claiming N physical slots must not let an
// attacker hide slot N under the same session digest and have Release erase
// the evidence along with the canonical ring.
func TestSessionReleaseRejectsExtraSameDigestSlot(t *testing.T) {
	t.Run("admission", func(t *testing.T) {
		testSessionReleaseRejectsExtraSameDigestSlot(t, true)
	})
	t.Run("committed-apply", func(t *testing.T) {
		testSessionReleaseRejectsExtraSameDigestSlot(t, false)
	})
}

func testSessionReleaseRejectsExtraSameDigestSlot(t *testing.T, admit bool) {
	fixture := newSessionReleaseFixture(t, 1, 4)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	identity := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, identity)
	applySessionReleaseCommand(t, fixture.machine, 3, identity)
	retirement := sessionRetirement(commandValue(fixture.binding, 2))
	applySessionReleaseCommand(t, fixture.machine, 4, retirement)

	digest := SessionKey(retirement.AuthorityClass, retirement.Tenant, retirement.ClientID)
	headerKey := SessionStorageKey(digest)
	headerBytes, found := rawSessionReleaseRow(t, fixture.system.Collection, headerKey[:])
	if !found {
		t.Fatal("retired session header is missing")
	}
	header, err := OpenSessionRecord(headerBytes)
	if err != nil {
		t.Fatal(err)
	}
	extraKey, err := SessionSlotStorageKey(digest, header.PhysicalSlotCount)
	if err != nil {
		t.Fatal(err)
	}
	extraValue := []byte("same-digest-extra-slot")
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(extraKey[:], extraValue)
	}); err != nil {
		t.Fatalf("inject extra slot: %v", err)
	}

	want := make(map[string][]byte, int(header.PhysicalSlotCount)+4)
	want[string(stateKey)] = mustRawSystemRow(t, fixture.system.Collection, stateKey)
	authorityKey := AuthorityBindingStorageKey(AuthorityIdentityKey(identity.Tenant, identity.ClientID))
	want[string(authorityKey[:])] = mustRawSystemRow(t, fixture.system.Collection, authorityKey[:])
	want[string(headerKey[:])] = bytes.Clone(headerBytes)
	for slot := uint16(0); slot < header.PhysicalSlotCount; slot++ {
		key, keyErr := SessionSlotStorageKey(digest, slot)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		want[string(key[:])] = mustRawSystemRow(t, fixture.system.Collection, key[:])
	}
	want[string(extraKey[:])] = bytes.Clone(extraValue)

	releaseBytes := encodeCommand(t, sessionRelease(retirement))
	if admit {
		if err := fixture.machine.AdmitCommand(releaseBytes); !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("release admission with extra slot = %v, want ErrSessionCorrupt", err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(5), releaseBytes); !errors.Is(err, ErrApplyPoisoned) {
			t.Fatalf("apply after poisoned admission = %v, want ErrApplyPoisoned", err)
		}
	} else if _, err := fixture.machine.ApplyNormal(normalMeta(5), releaseBytes); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("release apply with extra slot = %v, want ErrSessionCorrupt", err)
	}
	if _, err := fixture.machine.SessionCapacityState(); !errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("machine after committed corrupt release = %v, want ErrApplyPoisoned", err)
	}
	if fixture.system.Collection.Len() != uint64(len(want)) {
		t.Fatalf("corrupt release changed row count = %d, want %d",
			fixture.system.Collection.Len(), len(want))
	}
	for encodedKey, wantValue := range want {
		got, present := rawSessionReleaseRow(
			t, fixture.system.Collection, []byte(encodedKey),
		)
		if !present || !bytes.Equal(got, wantValue) {
			t.Fatalf("corrupt release changed row %x = (%x,%t), want %x",
				[]byte(encodedKey), got, present, wantValue)
		}
	}
}

func mustRawSystemRow(
	t testing.TB,
	collection *durable.Collection,
	key []byte,
) []byte {
	t.Helper()
	value, found, err := collection.AppendRaw(nil, key)
	if err != nil || !found {
		t.Fatalf("system row %x = (%x,%t,%v)", key, value, found, err)
	}
	return value
}
