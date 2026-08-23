package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func newSessionReleaseFixture(
	t testing.TB,
	maxSessions uint64,
	retryWindow uint16,
) machineFixture {
	t.Helper()
	dir := t.TempDir()
	openCollection := func(name string, options durable.Options) CollectionTarget {
		file, err := os.OpenFile(
			filepath.Join(dir, name+".vdb"), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600,
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return targetOf(collection)
	}

	systemDocuments := int(retryWindow) + 2
	if systemDocuments < 3 {
		systemDocuments = 3
	}
	system := openCollection("system", durable.Options{
		OpaqueValues:      true,
		MaxBatchDocuments: systemDocuments,
	})
	system = systemTargetOf(system.Collection)
	user := openCollection("user", durable.Options{})
	log, err := durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	txnDocuments := user.Limits.MaxDistinctMutations + 3
	if systemDocuments > txnDocuments {
		txnDocuments = systemDocuments
	}
	binding := testBinding()
	bootstrap := testBootstrap()
	options := Options{
		TxnLimits: durable.TxnLimits{
			MaxCollections: 2,
			MaxDocuments:   txnDocuments,
			MaxBytes:       64 << 20,
		},
		MaxSessions: maxSessions,
		RetryWindow: retryWindow,
	}
	machine, err := Open(
		binding, bootstrap, system,
		UserCollection{Name: "docs", Target: user}, log, options,
	)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return machineFixture{
		machine: machine, binding: binding, bootstrap: bootstrap,
		system: system, user: user, log: log, dir: dir,
	}
}

func applySessionReleaseCommand(
	t testing.TB,
	machine *Machine,
	index uint64,
	command replication.Command,
) []byte {
	t.Helper()
	encoded := encodeCommand(t, command)
	publication, err := machine.ApplyNormal(normalMeta(index), encoded)
	if err != nil || publication.Applied != index {
		t.Fatalf("apply %d = %+v, %v", index, publication, err)
	}
	return encoded
}

func sessionRetirement(command replication.Command) replication.Command {
	command.Kind = replication.CommandSessionRetire
	command.AckThrough = command.ClientSequence - 1
	command.Mutations = nil
	return command
}

func sessionRelease(retirement replication.Command) replication.Command {
	retirement.Kind = replication.CommandSessionRelease
	return retirement
}

func rawSessionReleaseRow(
	t testing.TB,
	collection *durable.Collection,
	key []byte,
) ([]byte, bool) {
	t.Helper()
	raw, found, err := collection.AppendRaw(nil, key)
	if err != nil {
		t.Fatalf("read raw row %x: %v", key, err)
	}
	return raw, found
}

func TestSessionReleaseReclaimsCapacityAndFencesResurrection(t *testing.T) {
	fixture := newSessionReleaseFixture(t, 1, 4)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}

	first := commandValue(fixture.binding, 1)
	firstBytes := applySessionReleaseCommand(t, fixture.machine, 2, first)
	secondIdentity := commandValue(fixture.binding, 1)
	secondIdentity.ClientID = id128(99)
	secondIdentity.ClientEpoch = 2
	secondIdentity.Fingerprint = sha256.Sum256([]byte("second stable identity"))
	secondBytes := encodeCommand(t, secondIdentity)
	if err := fixture.machine.AdmitCommand(secondBytes); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("full-capacity admission = %v, want ErrAdmissionBound", err)
	}
	if publication, err := fixture.machine.ApplyNormal(normalMeta(3), secondBytes); err != nil ||
		publication.Applied != 3 {
		t.Fatalf("committed capacity refusal = %+v, %v", publication, err)
	}
	if fixture.machine.state.SessionCount != 1 ||
		fixture.machine.state.SessionEpochHighWater != 1 {
		t.Fatalf("capacity refusal changed session state: %+v", fixture.machine.state)
	}

	retirement := sessionRetirement(commandValue(fixture.binding, 2))
	applySessionReleaseCommand(t, fixture.machine, 4, retirement)
	release := sessionRelease(retirement)
	releaseBytes := encodeCommand(t, release)
	if err := fixture.machine.AdmitCommand(releaseBytes); err != nil {
		t.Fatalf("release admission: %v", err)
	}
	applySessionReleaseCommand(t, fixture.machine, 5, release)
	if fixture.machine.state.SessionCount != 0 ||
		fixture.machine.state.SessionSlotCount != 0 ||
		fixture.machine.state.SessionEpochHighWater != 1 ||
		fixture.system.Collection.Len() != 1 {
		t.Fatalf("release did not reclaim bounded image: state=%+v rows=%d",
			fixture.machine.state, fixture.system.Collection.Len())
	}

	if err := fixture.machine.AdmitCommand(firstBytes); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("released command admission = %v, want ErrRetryRetired", err)
	}
	if _, err := fixture.machine.LookupCompletion(firstBytes); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("released command lookup = %v, want ErrRetryRetired", err)
	}
	if publication, err := fixture.machine.ApplyNormal(normalMeta(6), firstBytes); err != nil ||
		publication.Applied != 6 {
		t.Fatalf("committed old retry refusal = %+v, %v", publication, err)
	}
	if fixture.machine.state.SessionCount != 0 ||
		fixture.machine.state.SessionSlotCount != 0 || fixture.system.Collection.Len() != 1 {
		t.Fatalf("old retry recreated released image: state=%+v rows=%d",
			fixture.machine.state, fixture.system.Collection.Len())
	}

	if err := fixture.machine.AdmitCommand(secondBytes); err != nil {
		t.Fatalf("reclaimed capacity admission: %v", err)
	}
	applySessionReleaseCommand(t, fixture.machine, 7, secondIdentity)
	if fixture.machine.state.SessionCount != 1 ||
		fixture.machine.state.SessionSlotCount != 1 ||
		fixture.machine.state.SessionEpochHighWater != 2 {
		t.Fatalf("reclaimed capacity was not reusable: %+v", fixture.machine.state)
	}
}

func TestSessionReleaseExactRetryIsIdempotent(t *testing.T) {
	fixture := newSessionReleaseFixture(t, 4, 4)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionReleaseCommand(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	retirement := sessionRetirement(commandValue(fixture.binding, 2))
	applySessionReleaseCommand(t, fixture.machine, 3, retirement)
	release := sessionRelease(retirement)
	releaseBytes := applySessionReleaseCommand(t, fixture.machine, 4, release)
	if _, err := fixture.machine.LookupCompletion(releaseBytes); !errors.Is(err, ErrSessionReleased) {
		t.Fatalf("first release lookup = %v, want ErrSessionReleased", err)
	}

	wantChain := fixture.machine.Published().DataChainDigest
	if err := fixture.machine.AdmitCommand(releaseBytes); err != nil {
		t.Fatalf("exact release retry admission: %v", err)
	}
	applySessionReleaseCommand(t, fixture.machine, 5, release)
	if _, err := fixture.machine.LookupCompletion(releaseBytes); !errors.Is(err, ErrSessionReleased) {
		t.Fatalf("retried release lookup = %v, want ErrSessionReleased", err)
	}
	if fixture.machine.state.SessionCount != 0 ||
		fixture.machine.state.SessionSlotCount != 0 ||
		fixture.machine.state.SessionEpochHighWater != 1 ||
		fixture.system.Collection.Len() != 1 ||
		fixture.machine.Published().DataChainDigest != wantChain {
		t.Fatalf("idempotent release changed durable postcondition: state=%+v rows=%d",
			fixture.machine.state, fixture.system.Collection.Len())
	}
}

func TestSessionReleaseCannotDeleteActiveOrNewerSession(t *testing.T) {
	t.Run("active", func(t *testing.T) {
		fixture := newSessionReleaseFixture(t, 4, 4)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		active := commandValue(fixture.binding, 1)
		applySessionReleaseCommand(t, fixture.machine, 2, active)
		release := sessionRelease(active)
		release.Mutations = nil
		releaseBytes := encodeCommand(t, release)
		if err := fixture.machine.AdmitCommand(releaseBytes); !errors.Is(err, ErrSessionActive) {
			t.Fatalf("active release admission = %v, want ErrSessionActive", err)
		}
		if publication, err := fixture.machine.ApplyNormal(normalMeta(3), releaseBytes); err != nil ||
			publication.Applied != 3 {
			t.Fatalf("committed active release refusal = %+v, %v", publication, err)
		}
		if _, err := fixture.machine.LookupCompletion(releaseBytes); !errors.Is(err, ErrSessionActive) {
			t.Fatalf("active release lookup = %v, want ErrSessionActive", err)
		}
		if fixture.machine.state.SessionCount != 1 ||
			fixture.machine.state.SessionSlotCount != 1 || fixture.system.Collection.Len() != 3 {
			t.Fatalf("active session was deleted: state=%+v rows=%d",
				fixture.machine.state, fixture.system.Collection.Len())
		}
	})

	t.Run("newer epoch", func(t *testing.T) {
		fixture := newSessionReleaseFixture(t, 4, 4)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		applySessionReleaseCommand(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		retirement := sessionRetirement(commandValue(fixture.binding, 2))
		applySessionReleaseCommand(t, fixture.machine, 3, retirement)
		newer := commandValue(fixture.binding, 1)
		newer.ClientEpoch = 2
		newerBytes := applySessionReleaseCommand(t, fixture.machine, 4, newer)

		oldRelease := sessionRelease(retirement)
		oldReleaseBytes := encodeCommand(t, oldRelease)
		if err := fixture.machine.AdmitCommand(oldReleaseBytes); err != nil {
			t.Fatalf("older release postcondition admission: %v", err)
		}
		applySessionReleaseCommand(t, fixture.machine, 5, oldRelease)
		if _, err := fixture.machine.LookupCompletion(oldReleaseBytes); !errors.Is(err, ErrSessionReleased) {
			t.Fatalf("older release lookup = %v, want ErrSessionReleased", err)
		}
		if _, err := fixture.machine.LookupCompletion(newerBytes); err != nil {
			t.Fatalf("newer completion disappeared: %v", err)
		}
		if fixture.machine.state.SessionCount != 1 ||
			fixture.machine.state.SessionSlotCount != 2 ||
			fixture.machine.state.SessionEpochHighWater != 2 ||
			fixture.system.Collection.Len() != 4 {
			t.Fatalf("older release deleted newer image: state=%+v rows=%d",
				fixture.machine.state, fixture.system.Collection.Len())
		}
	})
}

func TestSessionReleaseConflictsPreserveRetiredImage(t *testing.T) {
	fixture := newSessionReleaseFixture(t, 4, 4)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionReleaseCommand(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	retirement := sessionRetirement(commandValue(fixture.binding, 2))
	applySessionReleaseCommand(t, fixture.machine, 3, retirement)
	digest := SessionKey(retirement.Tenant, retirement.ClientID)
	headerKey := SessionStorageKey(digest)
	wantHeader, found := rawSessionReleaseRow(t, fixture.system.Collection, headerKey[:])
	if !found {
		t.Fatal("retired session header is missing")
	}

	wrongFingerprint := sessionRelease(retirement)
	wrongFingerprint.Fingerprint[0] ^= 0xff
	wrongFingerprintBytes := encodeCommand(t, wrongFingerprint)
	if err := fixture.machine.AdmitCommand(wrongFingerprintBytes); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("wrong-fingerprint release admission = %v, want ErrRequestConflict", err)
	}
	if publication, err := fixture.machine.ApplyNormal(normalMeta(4), wrongFingerprintBytes); err != nil ||
		publication.Applied != 4 {
		t.Fatalf("committed fingerprint conflict = %+v, %v", publication, err)
	}

	wrongHome := sessionRelease(retirement)
	wrongHome.RetryHome[0] = 1
	wrongHomeBytes := encodeCommand(t, wrongHome)
	if err := fixture.machine.AdmitCommand(wrongHomeBytes); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("wrong-home release admission = %v, want ErrRequestConflict", err)
	}
	if publication, err := fixture.machine.ApplyNormal(normalMeta(5), wrongHomeBytes); err != nil ||
		publication.Applied != 5 {
		t.Fatalf("committed retry-home conflict = %+v, %v", publication, err)
	}

	gotHeader, found := rawSessionReleaseRow(t, fixture.system.Collection, headerKey[:])
	if !found || !bytes.Equal(gotHeader, wantHeader) ||
		fixture.machine.state.SessionCount != 1 ||
		fixture.machine.state.SessionSlotCount != 2 || fixture.system.Collection.Len() != 4 {
		t.Fatalf("release conflict changed retired image: state=%+v rows=%d",
			fixture.machine.state, fixture.system.Collection.Len())
	}
	exactRelease := sessionRelease(retirement)
	applySessionReleaseCommand(t, fixture.machine, 6, exactRelease)
	if fixture.machine.state.SessionCount != 0 || fixture.machine.state.SessionSlotCount != 0 {
		t.Fatalf("exact release after conflicts failed: %+v", fixture.machine.state)
	}
}

func TestSessionReleaseReopensWithOnlyEpochFence(t *testing.T) {
	fixture := newSessionReleaseFixture(t, 4, 4)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	first := applySessionReleaseCommand(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	retirement := sessionRetirement(commandValue(fixture.binding, 2))
	applySessionReleaseCommand(t, fixture.machine, 3, retirement)
	release := sessionRelease(retirement)
	releaseBytes := applySessionReleaseCommand(t, fixture.machine, 4, release)

	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen released image: %v", err)
	}
	if reopened.state.SessionCount != 0 || reopened.state.SessionSlotCount != 0 ||
		reopened.state.SessionEpochHighWater != 1 || reopened.Applied() != 4 ||
		fixture.system.Collection.Len() != 1 {
		t.Fatalf("reopened release state = %+v rows=%d",
			reopened.state, fixture.system.Collection.Len())
	}
	if _, err := reopened.LookupCompletion(releaseBytes); !errors.Is(err, ErrSessionReleased) {
		t.Fatalf("reopened release lookup = %v, want ErrSessionReleased", err)
	}
	if _, err := reopened.LookupCompletion(first); !errors.Is(err, ErrRetryRetired) {
		t.Fatalf("reopened old retry lookup = %v, want ErrRetryRetired", err)
	}
}

func TestSessionReleaseFullRetryWindowIsBounded(t *testing.T) {
	fixture := newSessionReleaseFixture(t, 1, MaxSessionRetryWindow)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}

	retirement := sessionRetirement(commandValue(
		fixture.binding, uint64(MaxSessionRetryWindow)+1,
	))
	retirementBytes := encodeCommand(t, retirement)
	retirementView, err := replication.OpenCommand(retirementBytes)
	if err != nil {
		t.Fatal(err)
	}
	digest := SessionKey(retirement.Tenant, retirement.ClientID)
	header, err := AppendSessionRecord(nil, SessionRecord{
		Tenant: retirement.Tenant, ClientID: retirement.ClientID,
		ClientEpoch: retirement.ClientEpoch, RetryHome: retirement.RetryHome,
		AckThrough: retirement.AckThrough, HighSequence: retirement.ClientSequence,
		Status: SessionRetired, RetryWindow: MaxSessionRetryWindow,
		PhysicalSlotCount: MaxSessionRetryWindow,
	})
	if err != nil {
		t.Fatal(err)
	}
	state := fixture.machine.state
	state.Applied = uint64(MaxSessionRetryWindow) + 2
	state.LastTerm = 2
	state.LastKind = RecordNormal
	state.LastEntryType = normalMeta(state.Applied).Type
	state.LastEntryDigest = normalEntryDigest(normalMeta(state.Applied), retirementBytes)
	state.SessionCount = 1
	state.SessionSlotCount = MaxSessionRetryWindow
	state.SessionEpochHighWater = retirement.ClientEpoch
	stateEnvelope, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	headerKey := SessionStorageKey(digest)
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		if err := batch.Put(stateKey, stateEnvelope); err != nil {
			return err
		}
		if err := batch.Put(headerKey[:], header); err != nil {
			return err
		}
		for slot := uint16(0); slot < MaxSessionRetryWindow; slot++ {
			sequence := uint64(slot) + 1
			resultCode := ResultApplied
			fingerprint := sha256.Sum256([]byte{0x31, byte(slot), byte(slot >> 8)})
			logicalDigest := sha256.Sum256([]byte{0x73, byte(slot), byte(slot >> 8)})
			if slot == 0 {
				sequence = retirement.ClientSequence
				resultCode = ResultSessionRetired
				fingerprint = retirement.Fingerprint
				logicalDigest = LogicalCommandDigest(retirementView)
			}
			slotRecord, slotErr := AppendSessionSlot(nil, SessionSlot{
				Slot: slot, SessionDigest: digest, ClientEpoch: retirement.ClientEpoch,
				ClientSequence: sequence, AppliedSequence: sequence + 1,
				Fingerprint: fingerprint, LogicalCommandDigest: logicalDigest,
				ResultCode: resultCode, ReplicaSetVersion: 1,
				ActivePolicyGeneration: fixture.binding.ActivePolicyGeneration,
				ProtectionEpoch:        fixture.binding.ProtectionEpoch,
				RoutingVersion:         fixture.binding.RoutingVersion,
				RouteGeneration:        fixture.binding.RouteGeneration,
			})
			if slotErr != nil {
				return slotErr
			}
			key, keyErr := SessionSlotStorageKey(digest, slot)
			if keyErr != nil {
				return keyErr
			}
			if err := batch.Put(key[:], slotRecord); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("seed full retry ring: %v", err)
	}

	machine, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
		fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("open full retry ring: %v", err)
	}
	if machine.state.SessionSlotCount != MaxSessionRetryWindow ||
		fixture.system.Collection.Len() != uint64(MaxSessionRetryWindow)+2 {
		t.Fatalf("full retry ring state=%+v rows=%d",
			machine.state, fixture.system.Collection.Len())
	}
	release := sessionRelease(retirement)
	releaseBytes := encodeCommand(t, release)
	if err := machine.AdmitCommand(releaseBytes); err != nil {
		t.Fatalf("full-window release admission: %v", err)
	}
	publication, err := machine.ApplyNormal(normalMeta(state.Applied+1), releaseBytes)
	if err != nil || publication.Applied != state.Applied+1 {
		t.Fatalf("full-window release = %+v, %v", publication, err)
	}
	if machine.state.SessionCount != 0 || machine.state.SessionSlotCount != 0 ||
		machine.state.SessionEpochHighWater != 1 || fixture.system.Collection.Len() != 1 {
		t.Fatalf("full-window release left rows: state=%+v rows=%d",
			machine.state, fixture.system.Collection.Len())
	}
}

func TestSessionReleaseCorruptOrMissingSlotDeletesNothing(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(testing.TB, *durable.Collection, []byte)
	}{
		{
			name: "corrupt",
			mutate: func(t testing.TB, collection *durable.Collection, key []byte) {
				raw, found := rawSessionReleaseRow(t, collection, key)
				if !found {
					t.Fatal("slot to corrupt is missing")
				}
				raw[0] ^= 0xff
				if err := collection.Update(func(batch *durable.WriteBatch) error {
					return batch.Put(key, raw)
				}); err != nil {
					t.Fatalf("corrupt slot: %v", err)
				}
			},
		},
		{
			name: "missing",
			mutate: func(t testing.TB, collection *durable.Collection, key []byte) {
				if deleted, err := collection.Delete(key); err != nil || !deleted {
					t.Fatalf("delete slot = %v, %v", deleted, err)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSessionReleaseFixture(t, 4, 2)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			applySessionReleaseCommand(t, fixture.machine, 2, commandValue(fixture.binding, 1))
			second := commandValue(fixture.binding, 2)
			second.AckThrough = 1
			applySessionReleaseCommand(t, fixture.machine, 3, second)
			retirement := sessionRetirement(commandValue(fixture.binding, 3))
			applySessionReleaseCommand(t, fixture.machine, 4, retirement)

			digest := SessionKey(retirement.Tenant, retirement.ClientID)
			targetKey, err := SessionSlotStorageKey(digest, 1)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(t, fixture.system.Collection, targetKey[:])

			headerKey := SessionStorageKey(digest)
			retirementKey, err := SessionSlotStorageKey(digest, 0)
			if err != nil {
				t.Fatal(err)
			}
			keys := [][]byte{stateKey, headerKey[:], retirementKey[:], targetKey[:]}
			beforeRows := make([][]byte, len(keys))
			beforeFound := make([]bool, len(keys))
			for i := range keys {
				beforeRows[i], beforeFound[i] = rawSessionReleaseRow(
					t, fixture.system.Collection, keys[i],
				)
			}
			beforeLen := fixture.system.Collection.Len()
			beforeApplied := fixture.machine.Applied()
			releaseBytes := encodeCommand(t, sessionRelease(retirement))
			if _, err := fixture.machine.ApplyNormal(normalMeta(5), releaseBytes); !errors.Is(err, ErrSessionCorrupt) {
				t.Fatalf("release corrupt image error = %v, want ErrSessionCorrupt", err)
			}
			if fixture.machine.Applied() != beforeApplied ||
				fixture.system.Collection.Len() != beforeLen {
				t.Fatalf("failed release changed publication: applied=%d rows=%d",
					fixture.machine.Applied(), fixture.system.Collection.Len())
			}
			for i := range keys {
				after, found := rawSessionReleaseRow(t, fixture.system.Collection, keys[i])
				if found != beforeFound[i] || !bytes.Equal(after, beforeRows[i]) {
					t.Fatalf("failed release changed row %x: found=%v want=%v",
						keys[i], found, beforeFound[i])
				}
			}
			if _, err := fixture.machine.ApplyNormal(normalMeta(5), releaseBytes); !errors.Is(err, ErrApplyPoisoned) {
				t.Fatalf("post-corruption apply = %v, want ErrApplyPoisoned", err)
			}
		})
	}
}
