//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Three independent physical members qualify retained SQL/WAL recovery across
// consecutive schema generations. This is not a network quorum qualification.
func TestRF3SchemaLineageRepeatedCommittedSourceRecovery(t *testing.T) {
	testRF3SchemaLineageRepeatedCommittedSourceRecovery(t, false)
}

func TestRF3SchemaShadowLineageRepeatedCommittedSourceRecovery(t *testing.T) {
	testRF3SchemaLineageRepeatedCommittedSourceRecovery(t, true)
}

func testRF3SchemaLineageRepeatedCommittedSourceRecovery(t *testing.T, online bool) {
	for replica := 1; replica <= 3; replica++ {
		t.Run(fmt.Sprintf("member-%d", replica), func(t *testing.T) {
			options := rf3testfixture.DurableGatewayMemberProfiles()[rf3testfixture.DurableGatewayDataAGroup]
			options.SchemaStatements, options.GlobalIndexes = nil, nil
			options.Root = t.TempDir()
			options.Table = "employees"
			options.CreateTable = "CREATE TABLE employees (id TEXT PRIMARY KEY, name TEXT NOT NULL, team TEXT NOT NULL, city TEXT, score INTEGER NOT NULL, active BOOLEAN NOT NULL)"
			options.Identity = raftstore.Identity{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}, Distribution: "employees", Shard: "source", AllocationGeneration: 1, MemberID: uint64(replica), StoreID: [16]byte{byte(replica + 4)}}
			options.Bootstrap = rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
			options.WAL, options.Key = rf3testfixture.DurableGatewayWALOptions(), raftstore.Key{ID: "lineage-test", Wrapped: []byte("wrapped")}
			options.Key.Material[0] = 9
			member, err := rf3testfixture.PrepareMember(options)
			if err != nil {
				t.Fatal(err)
			}
			wal, database, apply := member.WAL, member.Database, member.Apply
			base, applyID := member.Base, member.ApplyIdentity
			t.Cleanup(func() { _ = apply.Close(); _ = database.Close(); _ = wal.Close() })
			seedIncarnation, err := wal.BeginIncarnation()
			if err != nil {
				t.Fatal(err)
			}
			var seedReady uint64
			seedSchemaBuildReplica(t, member, func(index uint64, raw []byte) {
				seedReady++
				term, kind := uint64(2), pb.EntryNormal
				if err := wal.Persist(raftmodel.PersistBatch{NodeIncarnation: seedIncarnation, ReadyID: seedReady, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &index}, Entries: []*pb.Entry{{Index: &index, Term: &term, Type: &kind, Data: raw}}}); err != nil {
					t.Fatal(err)
				}
			})
			manifest := rf3Manifest{SQL: rf3ManifestSQL{Path: member.SQLPath, IdentityPath: filepath.Join(options.Root, "original-sql.json"), ApplyIdentityPath: filepath.Join(options.Root, "original-apply.json")}}
			baseBytes, err := base.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			applyBytes, err := applyID.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifest.SQL.IdentityPath, baseBytes, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(manifest.SQL.ApplyIdentityPath, applyBytes, 0600); err != nil {
				t.Fatal(err)
			}
			for step, sql := range []string{"CREATE INDEX by_city ON employees (city)", "DROP INDEX by_city", "TRUNCATE TABLE employees"} {
				before := apply.Applied()
				request, authorization := [32]byte{byte(step + 1), 41}, [32]byte{byte(step + 1), 42}
				var proof sqldriver.ReplicatedSchemaTargetProof
				if online {
					shadow, buildErr := apply.BuildReplicatedSchemaDDLShadow(t.Context(), request, sql, 100, 1<<20)
					if buildErr != nil {
						t.Fatal(buildErr)
					}
					verified, auditErr := apply.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, before)
					if auditErr != nil {
						t.Fatal(auditErr)
					}
					proof, err = verified.Prepare(t.Context(), request)
					err = errors.Join(err, verified.Close())
				} else {
					target, buildErr := apply.BuildJournaledReplicatedSchemaDDLTarget(t.Context(), [32]byte{byte(step + 1)}, before, sql)
					if buildErr != nil {
						t.Fatal(buildErr)
					}
					proof, err = apply.PrepareReplicatedSchemaTarget(target.Catalog, before, request)
				}
				if err != nil {
					t.Fatal(err)
				}
				cas, err := apply.ReplicatedSchemaCatalogCASDigest(proof, request, authorization)
				if err != nil {
					t.Fatal(err)
				}
				command, err := apply.AppendReplicatedSchemaTransition(nil, proof, sqldriver.ReplicatedSchemaTransitionAuthority{RequestDigest: request, AuthorizationDigest: authorization, CatalogCASDigest: cas})
				if err != nil {
					t.Fatal(err)
				}
				if err := apply.PersistReplicatedSchemaTransition(command); err != nil {
					t.Fatal(err)
				}
				incarnation, ready := seedIncarnation, seedReady+1
				if step > 0 {
					incarnation, err = wal.BeginIncarnation()
					if err != nil {
						t.Fatal(err)
					}
					ready = 1
				}
				index, term, entryType := before+1, uint64(step+2), pb.EntryNormal
				if err := wal.Persist(raftmodel.PersistBatch{NodeIncarnation: incarnation, ReadyID: ready, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &index}, Entries: []*pb.Entry{{Index: &index, Term: &term, Type: &entryType, Data: command}}}); err != nil {
					t.Fatal(err)
				}
				if _, err := apply.ApplyNormal(raftmodel.ApplyMeta{Index: index, Term: term, Type: entryType}, command); err != nil {
					t.Fatal(err)
				}
				// Crash after the source's exact committed command, before SQL
				// catalog publication. Never refresh the original identity files.
				if err := errors.Join(apply.Close(), database.Close(), wal.Close()); err != nil {
					t.Fatal(err)
				}
				base, applyID, err = loadRF3RetainedIdentities(manifest)
				if err != nil {
					t.Fatal(err)
				}
				wal, err = raftstore.Open(member.WALPath, options.Identity, member.Base.Binding.TopologyRecoveryEpoch, options.Key, options.WAL)
				if err != nil {
					t.Fatal(err)
				}
				base, applyID, database, apply, err = openRF3RetainedApply(member.SQLPath, wal, base, applyID)
				if err != nil {
					t.Fatal(err)
				}
				if base.Binding.Authority.SchemaGeneration != member.Base.Binding.Authority.SchemaGeneration+uint64(step)+1 || apply.Applied() != index {
					t.Fatal("restart lost generation/applied cut")
				}
				var cut replicatedstate.DataReadCut
				if err := apply.DataReadCutInto(nil, index, &cut); err != nil {
					t.Fatal(err)
				}
				relation, _ := cut.Relation(1)
				rows := 0
				err = relation.RangeRaw(func(_, _ []byte) error { rows++; return nil })
				err = errors.Join(err, cut.Close())
				want := 1000
				if step == 2 {
					want = 0
				}
				if err != nil || rows != want {
					t.Fatalf("rows=%d want=%d: %v", rows, want, err)
				}
				if drained, err := sqldriver.DrainPublishedReplicatedSchemaSource(member.SQLPath, command); err != nil || !drained {
					t.Fatalf("drain=%v: %v", drained, err)
				}
				for {
					done, err := apply.ReclaimReplicatedSchemaCapture(t.Context(), command, 1)
					if err != nil {
						t.Fatal(err)
					}
					if done {
						break
					}
				}
				if done, err := apply.ObserveReclaimedReplicatedSchemaCapture(t.Context(), command); err != nil || !done {
					t.Fatalf("capture drain=%v: %v", done, err)
				}
				runtime, err := raftmember.AdoptRuntime(wal, database, apply)
				if err != nil {
					t.Fatal(err)
				}
				if err := runtime.Close(); err != nil {
					t.Fatal(err)
				}
				base, applyID, err = loadRF3RetainedIdentities(manifest)
				if err != nil {
					t.Fatal(err)
				}
				wal, err = raftstore.Open(member.WALPath, options.Identity, member.Base.Binding.TopologyRecoveryEpoch, options.Key, options.WAL)
				if err != nil {
					t.Fatal(err)
				}
				base, applyID, database, apply, err = openRF3RetainedApply(member.SQLPath, wal, base, applyID)
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
