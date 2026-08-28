package driver

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestVerifiedSchemaTargetPreparesThousandRowsWithoutRescanning(t *testing.T) {
	_, db, base, _, claim := schemaDDLJournalFixture(t)
	applied := uint64(4)
	for first := 1; first < 1000; first += 64 {
		var docs []string
		for i := first; i < min(first+64, 1000); i++ {
			docs = append(docs, fmt.Sprintf(`{"id":"employee-%04d","city":"Lisbon"}`, i))
		}
		schemaShadowApply(t, claim, db, base, applied, docs...)
		applied++
	}
	op := [32]byte{121}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, applied-1)
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Close()
	before := verified.opened[0].collection.Stats().SnapshotFullScanCalls
	if before == 0 || verified.proof.Relations.TotalRows != 1000 {
		t.Fatalf("missing preflight audit: scans=%d rows=%d", before, verified.proof.Relations.TotalRows)
	}
	proof, err := verified.Prepare(t.Context(), op)
	if err != nil || proof.SourceApplied != applied-1 || proof.Membership.Sequence == 0 || proof.Relations.TotalRows != 1000 {
		t.Fatalf("prepare=%+v err=%v", proof, err)
	}
	if after := verified.opened[0].collection.Stats().SnapshotFullScanCalls; after != before {
		t.Fatalf("prepare rescanned target: before=%d after=%d", before, after)
	}
	if repeated, err := verified.Prepare(t.Context(), op); err != nil || repeated != proof {
		t.Fatalf("idempotent prepare changed proof: %v", err)
	}
	if after := verified.opened[0].collection.Stats().SnapshotFullScanCalls; after != before {
		t.Fatal("prepare retry rescanned target")
	}
	if err := verified.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := verified.Prepare(t.Context(), op); err == nil {
		t.Fatal("closed image handle authorized preparation")
	}
	if recovered, err := verified.ResumePrepared(t.Context(), claim, shadow.Catalog, op); err != nil || recovered != proof {
		t.Fatalf("closed audit could not resume exact preparation: %v", err)
	}
	if _, err := claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 1); err == nil {
		t.Fatal("prepared target was still mutable")
	}
}

func TestPreparedSchemaShadowRecoveryAfterSourceReopen(t *testing.T) {
	path, db, base, applyID, claim := schemaDDLJournalFixture(t)
	op := [32]byte{128}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.RecoverPreparedReplicatedSchemaTarget(shadow.Catalog, op); err == nil {
		t.Fatal("unprepared mutable shadow recovered as immutable")
	}
	v, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	proof, err := v.Prepare(t.Context(), op)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.ResumePrepared(t.Context(), claim, shadow.Catalog, op); err == nil {
		t.Fatal("open handle admitted closed preparation resume")
	}
	if err := errors.Join(v.Close(), claim.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, applyID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	active, _, err := reopened.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	// No process-local audit is supplied: recovery must re-audit the exact
	// durably prepared shadow, not blanket-refuse the retained build journal.
	got, err := active.RecoverPreparedReplicatedSchemaTarget(shadow.Catalog, op)
	if err != nil || got != proof {
		t.Fatalf("prepared shadow recovery: %v", err)
	}
	if _, err := active.RecoverPreparedReplicatedSchemaTarget(shadow.Catalog, [32]byte{129}); err == nil {
		t.Fatal("foreign operation recovered prepared shadow")
	}
	if _, err := v.ResumePrepared(t.Context(), active, shadow.Catalog, [32]byte{129}); err == nil {
		t.Fatal("foreign operation reused closed audit")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := v.ResumePrepared(canceled, active, shadow.Catalog, op); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled resume: %v", err)
	}
	// Recovery did not move or expose the target, and writes remain fenced
	// from the prepared image even after process replacement.
	if _, err := active.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 1); err == nil {
		t.Fatal("reopened preparation allowed replay")
	}
}

func TestPreparedSchemaShadowResumeRejectsAdvancedSourceAndChangedCatalog(t *testing.T) {
	_, _, _, _, claim := schemaDDLJournalFixture(t)
	op := [32]byte{130}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	v, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if _, err := v.Prepare(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := v.ResumePrepared(t.Context(), claim, append(shadow.Catalog[:len(shadow.Catalog):len(shadow.Catalog)], ' '), op); err == nil {
		t.Fatal("changed catalog reused closed audit")
	}
	if _, err := (&VerifiedReplicatedSchemaTarget{}).ResumePrepared(t.Context(), claim, shadow.Catalog, op); err == nil {
		t.Fatal("zero audit supplied a prepared proof")
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(4), nil); err != nil {
		t.Fatal(err)
	}
	if _, err := v.ResumePrepared(t.Context(), claim, shadow.Catalog, op); !errors.Is(err, ErrTransactionConflict) {
		t.Fatalf("advanced source resumed stale proof: %v", err)
	}
	if _, err := claim.RecoverPreparedReplicatedSchemaTarget(shadow.Catalog, op); err == nil {
		t.Fatal("cold recovery substituted the current source cut")
	}
}

func TestVerifiedSchemaTargetRefusesSourceAdvanceAndChangedTarget(t *testing.T) {
	for _, mutateTarget := range []bool{false, true} {
		t.Run(fmt.Sprint(mutateTarget), func(t *testing.T) {
			_, db, base, _, claim := schemaDDLJournalFixture(t)
			op := [32]byte{122}
			shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			verified, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 3)
			if err != nil {
				t.Fatal(err)
			}
			defer verified.Close()
			if mutateTarget {
				key := testReplicatedApplyKey(t, db, []byte(`{"id":"one"}`))
				if _, err := verified.opened[0].collection.Put(key, []byte(`{"id":"one","city":"Madrid"}`)); err != nil {
					t.Fatal(err)
				}
			} else {
				// Holding an audited target must not hold the source write lock.
				schemaShadowApply(t, claim, db, base, 4, `{"id":"two","city":"Porto"}`)
			}
			if _, err := verified.Prepare(t.Context(), op); err == nil {
				t.Fatal("stale image/cut authorized preparation")
			}
			if _, found, err := readReplicatedSchemaStageMarker(claim.database.dataDir); err != nil || found {
				t.Fatalf("failed preflight selected membership: %v", err)
			}
			if err := verified.Close(); err != nil {
				t.Fatal(err)
			}
			if !mutateTarget {
				if _, err := claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10); err != nil {
					t.Fatal(err)
				}
				fresh, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 4)
				if err != nil {
					t.Fatal(err)
				}
				defer fresh.Close()
				if _, err := fresh.Prepare(t.Context(), op); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestVerifiedSchemaTargetKeepsExactOperationAndCut(t *testing.T) {
	_, _, _, _, claim := schemaDDLJournalFixture(t)
	op := [32]byte{123}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 4); err == nil {
		t.Fatal("wrong source cut accepted")
	}
	verified, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Close()
	if _, err := verified.Prepare(t.Context(), [32]byte{124}); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("changed operation accepted: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := verified.Prepare(canceled, op); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled prepare published: %v", err)
	}
	var empty VerifiedReplicatedSchemaTarget
	if _, err := empty.Prepare(t.Context(), op); err == nil {
		t.Fatal("zero value authorized preparation")
	}
}

func TestVerifiedSchemaTargetOwnsDDLFilesUntilClose(t *testing.T) {
	_, db, base, _, claim := schemaDDLJournalFixture(t)
	op := [32]byte{127}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := claim.PreflightReplicatedSchemaTarget(canceled, shadow.Catalog, 3); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preflight: %v", err)
	}
	verified, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Close()
	if other, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 3); err == nil {
		other.Close()
		t.Fatal("second preflight acquired owned target files")
	}
	if _, err := claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 1); err == nil {
		t.Fatal("replay acquired audited target files")
	}
	// DDL ownership is not a source write lock.
	schemaShadowApply(t, claim, db, base, 4, `{"id":"two","city":"Porto"}`)
	if err := verified.Close(); err != nil {
		t.Fatal(err)
	}
	if err := verified.Close(); err != nil {
		t.Fatalf("repeated close: %v", err)
	}
	if _, err := claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 1); err != nil {
		t.Fatalf("close did not release DDL ownership: %v", err)
	}
	fresh, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestVerifiedSchemaTargetPublishesCapturedShadowAndReopens(t *testing.T) {
	path, db, base, _, claim := schemaDDLJournalFixture(t)
	op, authorization := [32]byte{125}, [32]byte{126}
	shadow, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), op, "CREATE INDEX by_city ON docs (city)", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	schemaShadowApply(t, claim, db, base, 4, `{"id":"two","city":"Porto"}`)
	shadow, err = claim.ReplayReplicatedSchemaDDLShadow(t.Context(), op, 10)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := claim.PreflightReplicatedSchemaTarget(t.Context(), shadow.Catalog, 4)
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Close()
	proof, err := verified.Prepare(t.Context(), op)
	if err != nil {
		t.Fatal(err)
	}
	if err := verified.Close(); err != nil {
		t.Fatal(err)
	}
	proof, err = verified.ResumePrepared(t.Context(), claim, shadow.Catalog, op)
	if err != nil {
		t.Fatal(err)
	}
	cas, err := claim.ReplicatedSchemaCatalogCASDigest(proof, op, authorization)
	if err != nil {
		t.Fatal(err)
	}
	command, err := claim.AppendReplicatedSchemaTransition(nil, proof, ReplicatedSchemaTransitionAuthority{
		RequestDigest: op, AuthorizationDigest: authorization, CatalogCASDigest: cas,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.PersistReplicatedSchemaTransition(command); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(5), command); err != nil {
		t.Fatal(err)
	}
	if err := verified.Close(); err != nil {
		t.Fatal(err)
	}
	testPublishSchemaCatalogFence(t, claim, db.connector.db)
	if published, err := claim.PublishReplicatedSchemaCatalog(); err != nil || !published {
		t.Fatalf("publish=%v err=%v", published, err)
	}
	catalog, _, err := openReplicatedSchemaCatalogImage(shadow.Catalog)
	if err != nil {
		t.Fatal(err)
	}
	target, applyID := *catalog.ReplicatedShardStore, catalog.ReplicatedApply.identity()
	if err := errors.Join(claim.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	testSchemaTargetSelectionFence(t, path, base, target, applyID)
	reopened, err := OpenReplicatedShardStoreWithApply(path, target, applyID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	before := reopened.connector.db.tables[target.UserTable].collection.Stats().SnapshotFullScanCalls
	active, _, err := verified.OpenActivatedApply(reopened, target, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	if after := reopened.connector.db.tables[target.UserTable].collection.Stats().SnapshotFullScanCalls; after != before {
		t.Fatalf("activation rescanned audited target: before=%d after=%d", before, after)
	}
	var cut replicatedstate.DataReadCut
	if err := active.DataReadCutInto(nil, 5, &cut); err != nil {
		t.Fatal(err)
	}
	defer cut.Close()
	rows, _ := cut.Relation(1)
	if rows.Len() != 2 || len(rows.AppendIndexes(nil)) != 1 {
		t.Fatalf("activated rows=%d indexes=%v", rows.Len(), rows.AppendIndexes(nil))
	}
}
