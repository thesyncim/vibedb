//go:build linux

package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Restart the physical node log and SQL roots at both sides of the commit
// boundary. Recovery must not publish an uncommitted preparation, and must
// finish a committed publication even when the manifest still names its source.
func TestRF3NodeSchemaStartupCommitBoundary(t *testing.T) {
	for _, state := range []struct {
		name                    string
		committed, commitMarker bool
	}{
		{"prepared-before-commit", false, false},
		{"committed-before-publication", true, true},
		{"applied-before-commit-marker", true, false},
	} {
		committed := state.committed
		t.Run(state.name, func(t *testing.T) {
			f := newRF3NodeRecoveryFixture(t)
			group, _ := f.store.GroupByID(f.boots[0].Descriptor.GroupID)
			_, _, db, claim, err := openRF3RetainedApply(f.paths[0], group, f.bases[0], f.applies[0])
			if err != nil {
				t.Fatal(err)
			}
			member := &rf3testfixture.PreparedMember{SQLPath: f.paths[0], Base: f.bases[0], ApplyIdentity: f.applies[0], Apply: claim}
			raw := prepareSchemaStartupTarget(t, member, rf3testfixture.MemberOptions{Table: "docs", CreateTable: `CREATE TABLE docs (PRIMARY KEY (id))`, Apply: f.applyOptions})
			proof, err := claim.PrepareReplicatedSchemaTarget(raw, claim.Applied(), [32]byte{41})
			if err != nil {
				t.Fatal(err)
			}
			cas, err := claim.ReplicatedSchemaCatalogCASDigest(proof, [32]byte{41}, [32]byte{42})
			if err != nil {
				t.Fatal(err)
			}
			command, err := claim.AppendReplicatedSchemaTransition(nil, proof, sqldriver.ReplicatedSchemaTransitionAuthority{RequestDigest: [32]byte{41}, AuthorizationDigest: [32]byte{42}, CatalogCASDigest: cas})
			if err != nil {
				t.Fatal(err)
			}
			if err := claim.PersistReplicatedSchemaTransition(command); err != nil {
				t.Fatal(err)
			}
			if committed {
				descriptor, err := group.Descriptor()
				if err != nil {
					t.Fatal(err)
				}
				incarnations, err := f.store.BeginIncarnations([]uint64{descriptor.LogKey})
				if err != nil {
					t.Fatal(err)
				}
				index, term, typ := proof.SourceApplied+1, uint64(2), pb.EntryNormal
				durableCommit := index
				if !state.commitMarker {
					durableCommit = proof.SourceApplied
				}
				if err := group.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnations[0].Incarnation, ReadyID: 1, MustSync: true,
					HardState: &pb.HardState{Term: &term, Commit: &durableCommit}, Entries: []*pb.Entry{{Index: &index, Term: &term, Type: &typ, Data: command}}}); err != nil {
					t.Fatal(err)
				}
				if _, err := claim.ApplyNormal(raftmodel.ApplyMeta{Index: index, Term: term, Type: typ}, command); err != nil {
					t.Fatal(err)
				}
			}
			if err := errors.Join(claim.Close(), db.Close()); err != nil {
				t.Fatal(err)
			}
			wantSchema, wantApplied := uint64(1), proof.SourceApplied
			if committed {
				wantSchema++
				wantApplied++
			}
			for restart := 0; restart < 2; restart++ {
				f.reopen(t)
				group, _ = f.store.GroupByID(f.boots[0].Descriptor.GroupID)
				base, id, db, claim, err := openRF3RetainedApply(f.paths[0], group, f.bases[0], f.applies[0])
				if err != nil {
					t.Fatalf("restart %d: %v", restart, err)
				}
				if uint64(base.Binding.Authority.SchemaGeneration) != wantSchema || claim.Applied() != wantApplied {
					t.Fatalf("restart %d selected schema/applied %d/%d, want %d/%d", restart, base.Binding.Authority.SchemaGeneration, claim.Applied(), wantSchema, wantApplied)
				}
				persisted, found, err := sqldriver.ObservePersistedReplicatedSchemaTransition(f.paths[0])
				if err != nil || !found || !bytes.Equal(persisted.Bytes(), command) {
					t.Fatalf("retained witness changed: %v", err)
				}
				if committed {
					if _, err := claim.SchemaApplyContractDigest(); err != nil {
						t.Fatal(err)
					}
					if id.ValidationDigest == f.applies[0].ValidationDigest {
						t.Fatal("target kept source validation profile")
					}
				} else {
					if !base.Equal(f.bases[0]) || id != f.applies[0] {
						t.Fatal("uncommitted source identity changed")
					}
					if _, published, err := sqldriver.ObservePublishedReplicatedSchemaTransition(f.paths[0]); err != nil || published {
						t.Fatalf("uncommitted target published: %v", err)
					}
				}
				if err := errors.Join(claim.Close(), db.Close()); err != nil {
					t.Fatal(err)
				}
				// The neighboring range remains independently recoverable after publication.
				neighbor, _ := f.store.GroupByID(f.boots[1].Descriptor.GroupID)
				_, _, db, claim, err = openRF3RetainedApply(f.paths[1], neighbor, f.bases[1], f.applies[1])
				if err != nil {
					t.Fatal(err)
				}
				if claim.Applied() != 1 {
					t.Fatal("schema publication advanced another range")
				}
				if err := errors.Join(claim.Close(), db.Close()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
