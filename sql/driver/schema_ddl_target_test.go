package driver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func TestReplicatedSchemaDDLTargetBuildsIndexedAndTruncatedImages(t *testing.T) {
	for _, test := range []struct {
		name, statement string
		rows, indexes   int
		initialIndex    bool
	}{
		{"create-index", "CREATE INDEX by_city ON docs (city)", 1000, 1, false},
		{"compound-index", "CREATE INDEX by_city_score ON docs (city, score)", 1000, 1, false},
		{"truncate", "TRUNCATE TABLE docs", 0, 0, false},
		{"truncate-indexed", "TRUNCATE TABLE docs", 0, 1, true},
		{"drop-index", "DROP INDEX by_city", 1000, 0, true},
		{"alter-add-column", "ALTER TABLE docs ADD COLUMN department TEXT", 1000, 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			path, db, binding, _ := prepareReplicatedTestRoot(t, test.name, false)
			defer db.Close()
			if test.initialIndex {
				session, err := db.NewSession(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				err = testRuntimeExec(session, "CREATE INDEX by_city ON docs (city)", nil)
				if err = errors.Join(err, session.Close()); err != nil {
					t.Fatal(err)
				}
			}
			base := requireReplicatedShardStoreBind(t, db, binding, "docs")
			claim, beforeApply, err := db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer claim.Close()
			if _, err = claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
				t.Fatal(err)
			}
			epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
			for first, sequence := 0, uint64(2); first < 1000; first, sequence = first+64, sequence+1 {
				var mutations []replication.Mutation
				for i := first; i < min(first+64, 1000); i++ {
					document := []byte(fmt.Sprintf(`{"id":"employee-%04d","city":"Lisbon","score":%d}`, i, i))
					mutations = append(mutations, replication.Mutation{Kind: replication.MutationPut,
						Key: testReplicatedApplyKey(t, db, document), Value: document})
				}
				command, err := replication.AppendCommand(nil, testReplicatedApplyCommandValue(base, epoch, sequence, mutations))
				if err != nil {
					t.Fatal(err)
				}
				if _, err = claim.ApplyNormal(testReplicatedApplyMeta(sequence+1), command); err != nil {
					t.Fatal(err)
				}
				if result := completionResultCode(t, claim, command); result != replicatedstate.ResultApplied {
					t.Fatalf("seed result=%d", result)
				}
			}
			beforeCatalog, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			before := claim.Applied()
			target, err := claim.BuildReplicatedSchemaDDLTarget(t.Context(), before, test.statement)
			if err != nil {
				t.Fatal(err)
			}
			if target.NoOp || target.Proof.SourceApplied != before || target.Proof.Relations.TotalRows != uint64(test.rows) || target.Proof.Membership.Sequence != 0 {
				t.Fatalf("target = %+v", target.Proof)
			}
			catalog, _, err := openReplicatedSchemaCatalogImage(target.Catalog)
			if err != nil {
				t.Fatal(err)
			}
			identity := catalog.ReplicatedShardStore
			if identity.LogID != base.LogID || identity.Binding.StoreID != base.Binding.StoreID ||
				identity.Binding.GroupID != base.Binding.GroupID || identity.UserStorage == base.UserStorage ||
				identity.RelationSchemaGeneration != base.RelationSchemaGeneration+1 ||
				catalog.ReplicatedApply.Storage != beforeApply.Storage || catalog.ReplicatedApply.CaptureStorage != beforeApply.CaptureStorage {
				t.Fatal("DDL changed durable request or logical shard identity")
			}
			if test.name == "alter-add-column" {
				columns := tableInfoFromMeta("docs", catalog.Tables["docs"]).Columns
				if len(columns) != 2 || columns[1].Path != "/department" || columns[1].Required {
					t.Fatalf("ALTER target columns = %+v", columns)
				}
			}
			afterCatalog, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(beforeCatalog, afterCatalog) || claim.Applied() != before {
				t.Fatalf("builder mutated serving state: %v", err)
			}
			var source replicatedstate.DataReadCut
			if err = claim.DataReadCutInto(nil, before, &source); err != nil {
				t.Fatal(err)
			}
			snapshot, _ := source.Relation(1)
			count := 0
			err = snapshot.RangeRaw(func(_, _ []byte) error { count++; return nil })
			if err = errors.Join(err, source.Close()); err != nil || count != 1000 {
				t.Fatalf("source rows=%d err=%v", count, err)
			}
			directory, err := claim.ReplicatedSchemaTargetDirectory()
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(filepath.Join(directory, identity.UserStorage+".vjc"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			collection, err := durable.Open(file, durableOptions(&table{meta: catalog.Tables["docs"]}))
			if err != nil {
				file.Close()
				t.Fatal(err)
			}
			fresh, err := collection.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			count = 0
			err = fresh.RangeRaw(func(_, _ []byte) error { count++; return nil })
			if err != nil || count != test.rows || len(fresh.AppendIndexes(nil)) != test.indexes {
				t.Fatalf("target rows=%d indexes=%v err=%v", count, fresh.AppendIndexes(nil), err)
			}
			if test.indexes != 0 {
				indexes := fresh.AppendIndexes(nil)
				needles := []string{`"Lisbon"`}
				want := test.rows
				if indexes[0].ColumnCount == 2 {
					needles = append(needles, "37")
					want = 1
				}
				values := make([]vibejson.Index, len(needles))
				for i, needle := range needles {
					needed, err := vibejson.RequiredIndexEntries([]byte(needle))
					if err != nil {
						t.Fatal(err)
					}
					values[i], err = vibejson.BuildIndex([]byte(needle), make([]vibejson.IndexEntry, needed))
					if err != nil {
						t.Fatal(err)
					}
				}
				masks, err := fresh.AppendIndexMasks(nil, indexes[0].Name, values...)
				found := 0
				for _, mask := range masks {
					found += bits.OnesCount64(mask.Bits)
				}
				if err != nil || found != want {
					t.Fatalf("actual index postings=%d want=%d err=%v", found, want, err)
				}
			}
			if err = errors.Join(fresh.Close(), collection.Close(), file.Close()); err != nil {
				t.Fatal(err)
			}
			// Exercise the production certification/membership boundary, not just
			// metadata inspection. The build itself did not prepare membership.
			proof, err := claim.PrepareReplicatedSchemaTarget(target.Catalog, before, [32]byte{0x37})
			if err != nil || proof.Membership.Sequence == 0 {
				t.Fatalf("prepare=%+v err=%v", proof, err)
			}
			cas, err := claim.ReplicatedSchemaCatalogCASDigest(proof, [32]byte{0x37}, [32]byte{0x38})
			if err != nil {
				t.Fatal(err)
			}
			command, err := claim.AppendReplicatedSchemaTransition(nil, proof, ReplicatedSchemaTransitionAuthority{
				RequestDigest: [32]byte{0x37}, AuthorizationDigest: [32]byte{0x38}, CatalogCASDigest: cas,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err = claim.PersistReplicatedSchemaTransition(command); err != nil {
				t.Fatal(err)
			}
			if _, err = claim.ApplyNormal(testReplicatedApplyMeta(before+1), command); err != nil {
				t.Fatal(err)
			}
			testPublishSchemaCatalogFence(t, claim, db.connector.db)
			if published, err := claim.PublishReplicatedSchemaCatalog(); err != nil || !published {
				t.Fatalf("publish=%t err=%v", published, err)
			}
			if err = errors.Join(claim.Close(), db.Close()); err != nil {
				t.Fatal(err)
			}
			applyID := catalog.ReplicatedApply.identity()
			testSchemaTargetSelectionFence(t, path, base, *identity, applyID)
			activated, err := OpenReplicatedShardStoreWithApply(path, *identity, applyID)
			if err != nil {
				t.Fatal(err)
			}
			defer activated.Close()
			active, _, err := activated.OpenReplicatedApply(*identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer active.Close()
			var live replicatedstate.DataReadCut
			if err = active.DataReadCutInto(nil, before+1, &live); err != nil {
				t.Fatal(err)
			}
			selected, _ := live.Relation(1)
			count = 0
			err = selected.RangeRaw(func(_, _ []byte) error { count++; return nil })
			selectedIndexes := len(selected.AppendIndexes(nil))
			if err = errors.Join(err, live.Close()); err != nil || count != test.rows || selectedIndexes != test.indexes {
				t.Fatalf("activated rows=%d indexes=%d err=%v", count, selectedIndexes, err)
			}
		})
	}
}

func TestReplicatedSchemaDDLTargetRefusesStaleAndCanceledCuts(t *testing.T) {
	_, db, base := bindReplicatedApplyTestRoot(t, "ddl-stale")
	defer db.Close()
	claim, _, err := db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	if _, err = claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	_, err = claim.BuildReplicatedSchemaDDLTarget(t.Context(), claim.Applied()+1, "TRUNCATE docs")
	if !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("stale=%v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = claim.BuildReplicatedSchemaDDLTarget(ctx, claim.Applied(), "TRUNCATE docs")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled=%v", err)
	}
	_, err = claim.BuildReplicatedSchemaDDLTarget(t.Context(), claim.Applied(), "TRUNCATE other")
	if !errors.Is(err, ErrTableNotFound) {
		t.Fatalf("foreign table=%v", err)
	}
}

func TestReplicatedSchemaDDLTargetNoOpAndIndexErrors(t *testing.T) {
	_, db, binding, _ := prepareReplicatedTestRoot(t, "ddl-no-op", false)
	defer db.Close()
	session, err := db.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	err = testRuntimeExec(session, "CREATE INDEX by_city ON docs (city)", nil)
	if err = errors.Join(err, session.Close()); err != nil {
		t.Fatal(err)
	}
	base := requireReplicatedShardStoreBind(t, db, binding, "docs")
	claim, _, err := db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	if _, err = claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{"CREATE INDEX IF NOT EXISTS by_city ON docs (city)", "DROP INDEX IF EXISTS absent"} {
		target, err := claim.BuildReplicatedSchemaDDLTarget(t.Context(), claim.Applied(), sql)
		if err != nil || !target.NoOp || len(target.Catalog) != 0 {
			t.Fatalf("%s: %+v %v", sql, target, err)
		}
	}
	for _, test := range []struct {
		sql  string
		want error
	}{
		{"CREATE INDEX by_city ON docs (city)", ErrIndexExists},
		{"DROP INDEX absent", ErrIndexNotFound},
		{"DROP INDEX by_city ON other", ErrTableNotFound},
		{"DROP TABLE docs", ErrReplicatedSchemaCatalogImage},
	} {
		if _, err := claim.BuildReplicatedSchemaDDLTarget(t.Context(), claim.Applied(), test.sql); !errors.Is(err, test.want) {
			t.Fatalf("%s: %v", test.sql, err)
		}
	}
	if _, err := os.Stat(filepath.Join(db.connector.db.dataDir, replicatedSchemaTargetsDirectory)); !os.IsNotExist(err) {
		t.Fatalf("no-op/refused DDL created target storage: %v", err)
	}
}

// This context triggers a deterministic interleaving at the first row-copy
// checkpoint, outside the serving lock. No sleep or process-wide fault hook is
// needed to prove cancellation and a concurrent committed publication.
type schemaDDLInterleaveContext struct {
	context.Context
	calls int
	apply func()
}

func (c *schemaDDLInterleaveContext) Err() error {
	c.calls++
	if c.calls == 3 {
		c.apply()
	}
	return c.Context.Err()
}

func TestReplicatedSchemaDDLTargetCleansFailedBuildAndRejectsConcurrentApply(t *testing.T) {
	for _, cancelBuild := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel=%t", cancelBuild), func(t *testing.T) {
			_, db, base := bindReplicatedApplyTestRoot(t, "ddl-interleaved")
			defer db.Close()
			claim, _, err := db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer claim.Close()
			if _, err = claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
				t.Fatal(err)
			}
			epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
			document := []byte(`{"id":"one","city":"Lisbon"}`)
			key := testReplicatedApplyKey(t, db, document)
			command, err := replication.AppendCommand(nil, testReplicatedApplyCommandValue(base, epoch, 2,
				[]replication.Mutation{{Kind: replication.MutationPut, Key: key, Value: document}}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = claim.ApplyNormal(testReplicatedApplyMeta(3), command); err != nil {
				t.Fatal(err)
			}
			seeded, err := claim.PointReadInto(1, key, 3, base.UserLimits.MaxDocumentBytes, nil)
			if err != nil || !seeded.Found {
				t.Fatalf("seed=%+v err=%v", seeded, err)
			}
			expected := bytes.Clone(seeded.Value)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			interleave := &schemaDDLInterleaveContext{Context: ctx, apply: func() {
				if cancelBuild {
					cancel()
					return
				}
				if _, err := claim.ApplyNormal(testReplicatedApplyMeta(4), nil); err != nil {
					t.Fatal(err)
				}
			}}
			_, err = claim.BuildReplicatedSchemaDDLTarget(interleave, 3, "CREATE INDEX by_city ON docs (city)")
			want := ErrTransactionConflict
			if cancelBuild {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatalf("build=%v want=%v checkpoints=%d", err, want, interleave.calls)
			}
			directory, err := claim.ReplicatedSchemaTargetDirectory()
			if err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("failed target leaked files: %v %v", entries, err)
			}
			row, err := claim.PointReadInto(1, key, claim.Applied(), base.UserLimits.MaxDocumentBytes, nil)
			if err != nil || !row.Found || !bytes.Equal(row.Value, expected) {
				t.Fatalf("source altered by failed build: %+v %v", row, err)
			}
		})
	}
}
