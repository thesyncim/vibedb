package replicatedstate

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
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

func assertSessionAuthorityConflict(t testing.TB, err error) {
	t.Helper()
	var conflict *RequestConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("authority mismatch error = %v, want RequestConflictError", err)
	}
}

func authorityClassName(class replication.CommandAuthorityClass) string {
	if class == replication.CommandAuthorityTopology {
		return "topology_owner"
	}
	return "data_owner"
}
