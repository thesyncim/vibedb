package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// Activate a real captured shadow, retaining the old source until the test
// explicitly drains it. No fabricated lineage or capture identity is used.
func schemaCaptureReclaimFixture(t *testing.T) (string, *Database, *ReplicatedApply, []byte) {
	t.Helper()
	path, db, base, _, claim := schemaDDLJournalFixture(t)
	op, authorization := [32]byte{131}, [32]byte{132}
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
	db, err = OpenReplicatedShardStoreWithApply(path, target, applyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	claim, _, err = db.OpenReplicatedApply(target, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = claim.Close() })
	return path, db, claim, command
}

func TestReplicatedSchemaCaptureReclaimBoundedRestartAndNextDDL(t *testing.T) {
	path, db, claim, command := schemaCaptureReclaimFixture(t)
	collection := claim.database.replicatedCaptureCollection
	initial := collection.Len()
	if initial < 3 {
		t.Fatalf("missing retained stream: %d rows", initial)
	}
	var key [8]byte
	header, found, err := collection.AppendRaw(nil, key[:])
	if err != nil || !found {
		t.Fatalf("header: %v", err)
	}
	if done, err := claim.ReclaimReplicatedSchemaCapture(t.Context(), command, 1); done || !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("reclaimed undrained source: done=%v err=%v", done, err)
	}
	if collection.Len() != initial {
		t.Fatal("undrained refusal mutated capture")
	}
	if drained, err := DrainPublishedReplicatedSchemaSource(path, command); err != nil || !drained {
		t.Fatalf("drain=%v err=%v", drained, err)
	}
	if done, err := claim.ObserveReclaimedReplicatedSchemaCapture(t.Context(), command); done || err != nil {
		t.Fatalf("lineage alone reported completed cleanup: %v %v", done, err)
	}
	identity, err := claim.Identity()
	if err != nil {
		t.Fatal(err)
	}
	publication := claim.Published()
	profile, err := claim.CapacityQualificationProfile()
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{0, -1, 1025} {
		if done, err := claim.ReclaimReplicatedSchemaCapture(t.Context(), command, limit); done || err == nil {
			t.Fatalf("invalid limit %d accepted", limit)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if done, err := claim.ReclaimReplicatedSchemaCapture(ctx, command, 1); done || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cleanup: %v %v", done, err)
	}
	foreign := bytes.Clone(command)
	foreign[0] ^= 1
	if done, err := claim.ReclaimReplicatedSchemaCapture(t.Context(), foreign, 1); done || err == nil {
		t.Fatal("foreign command reclaimed capture")
	}
	wrongHeader := sha256.Sum256(header)
	wrongHeader[0] ^= 1
	if done, err := claim.machine.ReclaimRetiredTransitionCapture(wrongHeader, 1, 1); done || err == nil {
		t.Fatal("foreign header reclaimed capture")
	}
	if collection.Len() != initial {
		t.Fatal("refused cleanup mutated capture")
	}

	// Restart after every bounded batch, including after deleting the header.
	// A partial stream is valid only because the exact drained lineage exists.
	for remaining := initial; remaining > 0; remaining-- {
		done, err := claim.ReclaimReplicatedSchemaCapture(t.Context(), command, 1)
		if err != nil || done != (remaining == 1) || collection.Len() != remaining-1 {
			t.Fatalf("remaining=%d done=%v rows=%d err=%v", remaining, done, collection.Len(), err)
		}
		if done {
			stats, err := claim.DurabilityStats()
			if err != nil || stats.TransactionHighWater != stats.CheckpointTransactions {
				t.Fatalf("cleanup acknowledged uncertified deletion: %+v err=%v", stats, err)
			}
		}
		got, found, err := collection.AppendRaw(nil, key[:])
		if err != nil || found != (remaining > 1) || found && !bytes.Equal(got, header) {
			t.Fatalf("header removed before prefix: remaining=%d found=%v err=%v", remaining, found, err)
		}
		base := claim.database.catalog.ReplicatedShardStore.Clone()
		if err := errors.Join(claim.Close(), db.Close()); err != nil {
			t.Fatal(err)
		}
		db, err = OpenReplicatedShardStoreWithApply(path, base, identity)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		claim, _, err = db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
		if err != nil {
			t.Fatalf("partial cleanup restart: %v", err)
		}
		t.Cleanup(func() { _ = claim.Close() })
		collection = claim.database.replicatedCaptureCollection
		if got, err := claim.Identity(); err != nil || got != identity {
			t.Fatalf("identity changed: %v", err)
		}
		if !reflect.DeepEqual(claim.Published(), publication) {
			t.Fatal("cleanup changed replicated publication")
		}
		gotProfile, err := claim.CapacityQualificationProfile()
		if err != nil || !reflect.DeepEqual(profile, gotProfile) {
			t.Fatalf("session state changed: %v", err)
		}
		if observed, err := claim.ObserveReclaimedReplicatedSchemaCapture(t.Context(), command); err != nil || observed != done {
			t.Fatalf("observed=%v want=%v err=%v", observed, done, err)
		}
	}
	if _, err := os.Stat(filepath.Join(path+".tables", schemaDDLShadowName)); !os.IsNotExist(err) {
		t.Fatalf("completed shadow journal retained: %v", err)
	}
	if done, err := claim.ReclaimReplicatedSchemaCapture(t.Context(), command, 1024); err != nil || !done {
		t.Fatalf("completed retry: %v %v", done, err)
	}
	var cut replicatedstate.DataReadCut
	if err := claim.DataReadCutInto(nil, 5, &cut); err != nil {
		t.Fatal(err)
	}
	rows, _ := cut.Relation(1)
	if rows.Len() != 2 || len(rows.AppendIndexes(nil)) != 1 {
		t.Fatal("serving rows/indexes changed")
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), [32]byte{133}, "DROP INDEX by_city", 100, 1<<20); err != nil {
		t.Fatalf("next DDL could not start: %v", err)
	}
	newHeader, found, err := collection.AppendRaw(nil, key[:])
	if err != nil || !found || bytes.Equal(newHeader, header) {
		t.Fatalf("next capture header: %v", err)
	}
	if done, err := claim.ReclaimReplicatedSchemaCapture(t.Context(), command, 1024); done || !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
		t.Fatalf("old cleanup accepted new capture: %v %v", done, err)
	}
	got, found, err := collection.AppendRaw(nil, key[:])
	if err != nil || !found || !bytes.Equal(got, newHeader) {
		t.Fatal("old cleanup changed next capture")
	}
}

func TestReplicatedSchemaCaptureReclaimJournalInterruption(t *testing.T) {
	for _, stage := range []string{"reclaim-journal", "reclaim-journal-removed"} {
		t.Run(stage, func(t *testing.T) {
			path, db, claim, command := schemaCaptureReclaimFixture(t)
			if drained, err := DrainPublishedReplicatedSchemaSource(path, command); err != nil || !drained {
				t.Fatalf("drain=%v err=%v", drained, err)
			}
			injected := errors.New("interrupted completed capture cleanup")
			schemaDDLShadowFaultHook = func(at string) error {
				if at == stage {
					return injected
				}
				return nil
			}
			t.Cleanup(func() { schemaDDLShadowFaultHook = nil })
			for i := uint64(0); i < 10; i++ {
				done, err := claim.ReclaimReplicatedSchemaCapture(t.Context(), command, 1)
				if errors.Is(err, injected) {
					break
				}
				if err != nil || done || i == 9 {
					t.Fatalf("missing interruption: done=%v err=%v", done, err)
				}
			}
			schemaDDLShadowFaultHook = nil
			if claim.database.replicatedCaptureCollection.Len() != 0 {
				t.Fatal("header not deleted before journal interruption")
			}
			base := claim.database.catalog.ReplicatedShardStore.Clone()
			identity, err := claim.Identity()
			if err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(claim.Close(), db.Close()); err != nil {
				t.Fatal(err)
			}
			db, err = OpenReplicatedShardStoreWithApply(path, base, identity)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			claim, _, err = db.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer claim.Close()
			if done, err := claim.ObserveReclaimedReplicatedSchemaCapture(t.Context(), command); err != nil || done != (stage == "reclaim-journal-removed") {
				t.Fatalf("interrupted journal observation: done=%v err=%v", done, err)
			}
			if done, err := claim.ReclaimReplicatedSchemaCapture(t.Context(), command, 1); err != nil || !done {
				t.Fatalf("resume journal cleanup: done=%v err=%v", done, err)
			}
			if _, err := claim.BuildReplicatedSchemaDDLShadow(t.Context(), [32]byte{134}, "TRUNCATE docs", 100, 1<<20); err != nil {
				t.Fatalf("next DDL after journal interruption: %v", err)
			}
		})
	}
}
