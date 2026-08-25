package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestSessionOpenAdmissionLookupParityPastRetryFloor(t *testing.T) {
	t.Run("active retained open", func(t *testing.T) {
		fixture := newSessionReleaseFixture(t, 4, 4)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		prototype := commandValue(fixture.binding, 1)
		_, openBytes, _ := applySessionOpen(t, fixture.machine, 2, prototype)
		prototype.AckThrough = 1
		applySessionReleaseCommand(t, fixture.machine, 3, prototype)

		if err := fixture.machine.AdmitCommand(openBytes); !errors.Is(err, ErrRetryRetired) {
			t.Fatalf("exact open admission = %v, want ErrRetryRetired", err)
		}
		if _, err := fixture.machine.LookupCompletion(openBytes); !errors.Is(err, ErrRetryRetired) {
			t.Fatalf("exact open lookup = %v, want ErrRetryRetired", err)
		}

		competing := sessionOpenFor(prototype)
		competing.Fingerprint[0] ^= 0xff
		competingBytes := encodeCommand(t, competing)
		if err := fixture.machine.AdmitCommand(competingBytes); !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("competing open admission = %v, want ErrRequestConflict", err)
		}
		lookup, err := fixture.machine.LookupCompletion(competingBytes)
		if !errors.Is(err, ErrRequestConflict) || len(lookup.Bytes) == 0 {
			t.Fatalf("competing open lookup = %d bytes, %v; want retained conflict", len(lookup.Bytes), err)
		}
	})

	t.Run("retired retained open", func(t *testing.T) {
		fixture := newSessionReleaseFixture(t, 4, 4)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		prototype := commandValue(fixture.binding, 1)
		_, openBytes, _ := applySessionOpen(t, fixture.machine, 2, prototype)
		applySessionReleaseCommand(t, fixture.machine, 3, sessionRetirement(prototype))

		if err := fixture.machine.AdmitCommand(openBytes); !errors.Is(err, ErrRetryRetired) {
			t.Fatalf("exact retired open admission = %v, want ErrRetryRetired", err)
		}
		if _, err := fixture.machine.LookupCompletion(openBytes); !errors.Is(err, ErrRetryRetired) {
			t.Fatalf("exact retired open lookup = %v, want ErrRetryRetired", err)
		}

		competing := sessionOpenFor(prototype)
		competing.Fingerprint[0] ^= 0xff
		competingBytes := encodeCommand(t, competing)
		if err := fixture.machine.AdmitCommand(competingBytes); !errors.Is(err, ErrSessionRetired) {
			t.Fatalf("competing retired open admission = %v, want ErrSessionRetired", err)
		}
		if _, err := fixture.machine.LookupCompletion(competingBytes); !errors.Is(err, ErrSessionRetired) {
			t.Fatalf("competing retired open lookup = %v, want ErrSessionRetired", err)
		}
	})
}

func TestSessionSlotBindsIssuedTokenToApplyIndex(t *testing.T) {
	open := sessionCodecSlot(t)
	open.Slot = 0
	open.ClientSequence = 1
	open.ResultCode = ResultSessionOpened
	open.AppliedSequence = open.ClientEpoch
	if _, err := AppendSessionSlot(nil, open); err != nil {
		t.Fatalf("valid open slot: %v", err)
	}
	wrongOpen := open
	wrongOpen.AppliedSequence++
	if _, err := AppendSessionSlot(nil, wrongOpen); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("open token/apply mismatch = %v, want ErrSessionCorrupt", err)
	}

	ordinary := sessionCodecSlot(t)
	ordinary.AppliedSequence = ordinary.ClientEpoch
	if _, err := AppendSessionSlot(nil, ordinary); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("ordinary result at open index = %v, want ErrSessionCorrupt", err)
	}
}

func TestSessionOpenTokenCorruptionFailsPointPathAndReopen(t *testing.T) {
	for _, path := range []string{"point", "reopen"} {
		t.Run(path, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			prototype := commandValue(fixture.binding, 1)
			_, openBytes, _ := applySessionOpen(t, fixture.machine, 2, prototype)
			if _, err := fixture.machine.ApplyNormal(normalMeta(3), nil); err != nil {
				t.Fatal(err)
			}
			digest := SessionKey(prototype.AuthorityClass, prototype.Tenant, prototype.ClientID)
			rewriteSessionSlot(t, fixture, digest, 0, func(raw []byte) {
				binary.LittleEndian.PutUint64(raw[68:76], 3)
			})

			if path == "point" {
				if _, err := fixture.machine.LookupCompletion(openBytes); !errors.Is(err, ErrSessionCorrupt) {
					t.Fatalf("corrupt open point lookup = %v, want ErrSessionCorrupt", err)
				}
				return
			}
			if _, err := Open(
				fixture.binding, fixture.bootstrap, fixture.system,
				UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
				fixture.machine.options,
			); !errors.Is(err, ErrSessionCorrupt) {
				t.Fatalf("corrupt open reopen = %v, want ErrSessionCorrupt", err)
			}
		})
	}
}

func TestReopenRejectsDuplicateIssuedSessionEpoch(t *testing.T) {
	fixture := newSessionReleaseFixture(t, 4, 4)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	first := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, first)
	second := first
	second.ClientID = replication.ID128{0x44}
	second.Fingerprint = replication.Digest{0x55}
	applySessionOpen(t, fixture.machine, 3, second)

	digest := SessionKey(second.AuthorityClass, second.Tenant, second.ClientID)
	headerKey := SessionStorageKey(digest)
	header, found, err := fixture.system.Collection.AppendRaw(nil, headerKey[:])
	if err != nil || !found {
		t.Fatalf("read second header = %v, %v", found, err)
	}
	binary.LittleEndian.PutUint64(header[80:88], 2)
	sealRecord(header, sessionRecordChecksumDomain)
	slotKey, err := SessionSlotStorageKey(digest, 0)
	if err != nil {
		t.Fatal(err)
	}
	slot, found, err := fixture.system.Collection.AppendRaw(nil, slotKey[:])
	if err != nil || !found {
		t.Fatalf("read second open slot = %v, %v", found, err)
	}
	binary.LittleEndian.PutUint64(slot[52:60], 2)
	binary.LittleEndian.PutUint64(slot[68:76], 2)
	sealRecord(slot, sessionSlotChecksumDomain)
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		if err := batch.Put(headerKey[:], header); err != nil {
			return err
		}
		return batch.Put(slotKey[:], slot)
	}); err != nil {
		t.Fatal(err)
	}

	_, err = Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if !errors.Is(err, ErrSessionCorrupt) || !bytes.Contains([]byte(err.Error()), []byte("duplicate session epoch")) {
		t.Fatalf("duplicate issued epoch reopen = %v, want duplicate ErrSessionCorrupt", err)
	}
}
