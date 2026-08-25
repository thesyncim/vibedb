package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestSessionAuthorityClassFencesStableIdentityBidirectionally(t *testing.T) {
	classes := []replication.CommandAuthorityClass{
		replication.CommandAuthorityData,
		replication.CommandAuthorityTopology,
	}
	for _, owner := range classes {
		foreign := replication.CommandAuthorityTopology
		if owner == replication.CommandAuthorityTopology {
			foreign = replication.CommandAuthorityData
		}
		t.Run(authorityClassName(owner), func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			prototype := commandValue(fixture.binding, 1)
			prototype.AuthorityClass = owner
			_, ownerOpen, epoch := applySessionOpen(t, fixture.machine, 2, prototype)

			foreignPrototype := prototype
			foreignPrototype.AuthorityClass = foreign
			foreignOpen := encodeCommand(t, sessionOpenFor(foreignPrototype))
			assertSessionAuthorityConflict(t, fixture.machine.AdmitCommand(foreignOpen))
			if publication, err := fixture.machine.ApplyNormal(normalMeta(3), foreignOpen); err != nil ||
				publication.Applied != 3 {
				t.Fatalf("committed foreign open = %+v, %v", publication, err)
			}
			if _, err := fixture.machine.LookupCompletion(foreignOpen); err == nil {
				t.Fatal("foreign open resolved the owner's retained completion")
			} else {
				assertSessionAuthorityConflict(t, err)
			}

			foreignMutation := foreignPrototype
			foreignMutation.ClientEpoch = epoch
			foreignCompare := foreignMutation
			foreignCompare.Batches = []replication.RelationMutationBatch{{
				Relation: 1,
				Mutations: []replication.Mutation{{
					Kind: replication.MutationPutDigestEqual, Key: []byte("compare"),
					Value: []byte(`{"n":2}`), ExpectedValueLength: 7,
					ExpectedValueDigest: sha256.Sum256([]byte(`{"n":1}`)),
				}},
			}}
			foreignRenew := foreignMutation
			foreignRenew.Kind = replication.CommandSessionRenew
			foreignRenew.Batches = nil
			foreignRenew.ExpectedDeadlineUnixNano = testSessionLeaseDeadlineUnixNano
			foreignRenew.NextDeadlineUnixNano = testSessionLeaseDeadlineUnixNano + 1
			foreignRevoke := foreignRenew
			foreignRevoke.Kind = replication.CommandSessionRevoke
			foreignRevoke.NextDeadlineUnixNano = 0
			foreignRetire := foreignMutation
			foreignRetire.Kind = replication.CommandSessionRetire
			foreignRetire.Batches = nil
			foreignRetire.AckThrough = 1
			foreignRelease := sessionRelease(foreignRetire)
			foreignCommands := []struct {
				name    string
				command replication.Command
			}{
				{"mutation", foreignMutation},
				{"compare_mutation", foreignCompare},
				{"renew", foreignRenew},
				{"revoke", foreignRevoke},
				{"retire", foreignRetire},
				{"release", foreignRelease},
			}
			for _, test := range foreignCommands {
				bytes := encodeCommand(t, test.command)
				assertSessionAuthorityConflict(t, fixture.machine.AdmitCommand(bytes))
				if _, err := fixture.machine.LookupCompletion(bytes); err == nil {
					t.Fatalf("%s resolved the owner's retained session", test.name)
				} else {
					assertSessionAuthorityConflict(t, err)
				}
			}

			reopened, err := Open(
				fixture.binding, fixture.bootstrap, fixture.system,
				UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
				fixture.machine.options,
			)
			if err != nil {
				t.Fatalf("reopen authority-bound session: %v", err)
			}
			for _, test := range foreignCommands {
				bytes := encodeCommand(t, test.command)
				assertSessionAuthorityConflict(t, reopened.AdmitCommand(bytes))
				if _, err := reopened.LookupCompletion(bytes); err == nil {
					t.Fatalf("reopened %s resolved the owner's retained session", test.name)
				} else {
					assertSessionAuthorityConflict(t, err)
				}
			}

			ownerMutation := prototype
			ownerMutation.ClientEpoch = epoch
			ownerBytes := encodeCommand(t, ownerMutation)
			if err := reopened.AdmitCommand(ownerBytes); err != nil {
				t.Fatalf("owner command rejected after foreign attempts: %v", err)
			}
			if publication, err := reopened.ApplyNormal(normalMeta(4), ownerBytes); err != nil ||
				publication.Applied != 4 {
				t.Fatalf("owner command = %+v, %v", publication, err)
			}
			if _, err := reopened.LookupCompletion(ownerOpen); err != nil {
				t.Fatalf("owner open no longer retryable: %v", err)
			}
		})
	}
}

func TestSessionAuthorityBatchSerializesStableIdentityAcrossClasses(t *testing.T) {
	fixture := newNormalBatchFixtureWithSystemDocuments(t, MaxDistinctMutations, 8, 8)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	data := commandValue(fixture.binding, 1)
	topology := data
	topology.AuthorityClass = replication.CommandAuthorityTopology
	entries := []raftmodel.NormalApply{
		{Meta: normalMeta(2), Data: encodeCommand(t, sessionOpenFor(data))},
		{Meta: normalMeta(3), Data: encodeCommand(t, sessionOpenFor(topology))},
	}
	applied, publication, err := fixture.machine.ApplyNormalBatch(entries, normalBatchWitnesses(entries))
	if err != nil || applied != 1 || publication.Applied != 2 ||
		fixture.machine.state.AuthorityBindingCount != 1 || fixture.machine.state.SessionCount != 1 {
		t.Fatalf("cross-class batch applied=%d publication=%+v state=%+v err=%v",
			applied, publication, fixture.machine.state, err)
	}
	assertSessionAuthorityConflict(t, fixture.machine.AdmitCommand(entries[1].Data))
}

func TestSessionAuthorityBindingCorruptionAndMissingRowFailReopen(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, machineFixture, []byte, [33]byte)
	}{
		{"corrupt", func(t *testing.T, fixture machineFixture, raw []byte, key [33]byte) {
			raw = bytes.Clone(raw)
			raw[len(raw)-1] ^= 1
			if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
				return batch.Put(key[:], raw)
			}); err != nil {
				t.Fatal(err)
			}
		}},
		{"missing", func(t *testing.T, fixture machineFixture, _ []byte, key [33]byte) {
			if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
				return batch.Delete(key[:])
			}); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			identity := commandValue(fixture.binding, 1)
			applySessionOpen(t, fixture.machine, 2, identity)
			digest := AuthorityIdentityKey(identity.Tenant, identity.ClientID)
			key := AuthorityBindingStorageKey(digest)
			raw := mustRawSystemRow(t, fixture.system.Collection, key[:])
			test.mutate(t, fixture, raw, key)
			if snapshot, err := fixture.machine.Snapshot(); err == nil {
				_ = snapshot.Close()
				t.Fatalf("snapshot accepted authority %s", test.name)
			} else if !errors.Is(err, ErrSessionCorrupt) && !errors.Is(err, ErrStateCorrupt) {
				t.Fatalf("snapshot authority %s err=%v, want corruption", test.name, err)
			}
			if _, err := Open(
				fixture.binding, fixture.bootstrap, fixture.system,
				UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
				fixture.machine.options,
			); !errors.Is(err, ErrSessionCorrupt) && !errors.Is(err, ErrStateCorrupt) {
				t.Fatalf("reopen authority %s err=%v, want corruption", test.name, err)
			}
		})
	}
}

func TestSessionAuthorityClassSurvivesReleaseAndSameClassReopen(t *testing.T) {
	for _, owner := range []replication.CommandAuthorityClass{
		replication.CommandAuthorityData, replication.CommandAuthorityTopology,
	} {
		foreign := replication.CommandAuthorityTopology
		if owner == replication.CommandAuthorityTopology {
			foreign = replication.CommandAuthorityData
		}
		t.Run(authorityClassName(owner), func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			prototype := commandValue(fixture.binding, 1)
			prototype.AuthorityClass = owner
			applySessionOpen(t, fixture.machine, 2, prototype)
			retirement := sessionRetirement(commandValue(fixture.binding, 1))
			retirement.AuthorityClass = owner
			applySessionReleaseCommand(t, fixture.machine, 3, retirement)
			release := sessionRelease(retirement)
			applySessionReleaseCommand(t, fixture.machine, 4, release)
			if fixture.machine.state.AuthorityBindingCount != 1 ||
				fixture.machine.state.SessionCount != 0 || fixture.system.Collection.Len() != 2 {
				t.Fatalf("released authority state=%+v rows=%d",
					fixture.machine.state, fixture.system.Collection.Len())
			}

			reopened, err := Open(
				fixture.binding, fixture.bootstrap, fixture.system,
				UserCollection{Name: "docs", Target: fixture.user}, fixture.log,
				fixture.machine.options,
			)
			if err != nil {
				t.Fatal(err)
			}
			foreignRelease := release
			foreignRelease.AuthorityClass = foreign
			foreignReleaseBytes := encodeCommand(t, foreignRelease)
			wantConflictKey := AuthorityIdentityKey(prototype.Tenant, prototype.ClientID)
			assertSessionAuthorityConflictKey(
				t, reopened.AdmitCommand(foreignReleaseBytes), wantConflictKey,
			)
			if _, err := reopened.LookupCompletion(foreignReleaseBytes); err == nil {
				t.Fatal("foreign release resolved the retained authority tombstone")
			} else {
				assertSessionAuthorityConflict(t, err)
			}
			foreignOpen := prototype
			foreignOpen.AuthorityClass = foreign
			foreignOpenBytes := encodeCommand(t, sessionOpenFor(foreignOpen))
			assertSessionAuthorityConflictKey(
				t, reopened.AdmitCommand(foreignOpenBytes), wantConflictKey,
			)
			if _, err := reopened.LookupSessionLease(
				foreign, prototype.Tenant, prototype.ClientID, 2,
			); err == nil {
				t.Fatal("foreign lease lookup crossed retained authority")
			} else {
				assertSessionAuthorityConflict(t, err)
			}
			if _, err := reopened.LookupSessionLease(
				owner, prototype.Tenant, prototype.ClientID, 2,
			); !errors.Is(err, ErrSessionReleased) {
				t.Fatalf("owner released lease lookup=%v", err)
			}

			sameOpen := encodeCommand(t, sessionOpenFor(prototype))
			if err := reopened.AdmitCommand(sameOpen); err != nil {
				t.Fatalf("same-class reopen admission: %v", err)
			}
			if publication, err := reopened.ApplyNormal(normalMeta(5), sameOpen); err != nil ||
				publication.Applied != 5 || reopened.state.SessionCount != 1 ||
				reopened.state.AuthorityBindingCount != 1 {
				t.Fatalf("same-class reopen=%+v state=%+v err=%v",
					publication, reopened.state, err)
			}
		})
	}
}

func assertSessionAuthorityConflict(t testing.TB, err error) {
	t.Helper()
	var conflict *RequestConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("authority mismatch error = %v, want RequestConflictError", err)
	}
}

func assertSessionAuthorityConflictKey(t testing.TB, err error, want [sha256.Size]byte) {
	t.Helper()
	var conflict *RequestConflictError
	if !errors.As(err, &conflict) || conflict.Key != want {
		t.Fatalf("authority mismatch error=%v key=%x, want %x", err, conflictKey(conflict), want)
	}
}

func conflictKey(conflict *RequestConflictError) [sha256.Size]byte {
	if conflict == nil {
		return [sha256.Size]byte{}
	}
	return conflict.Key
}

func authorityClassName(class replication.CommandAuthorityClass) string {
	if class == replication.CommandAuthorityTopology {
		return "topology_owner"
	}
	return "data_owner"
}
