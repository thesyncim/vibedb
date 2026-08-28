package driver

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

// This is a safety gate for the existing exact-cut builder, not qualification
// of online DDL: foreground work must run while materialization is paused, and
// the now-stale build must fail rather than omit an acknowledged mutation.
func TestReplicatedSchemaDDLBuildDoesNotHoldServingLock(t *testing.T) {
	for _, statement := range []string{
		"CREATE INDEX by_score ON docs (score)",
		"DROP INDEX by_city",
		"TRUNCATE TABLE docs",
	} {
		t.Run(statement, func(t *testing.T) {
			_, db, binding, _ := prepareReplicatedTestRoot(t, "ddl-concurrent", false)
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
			epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
			one := []byte(`{"id":"one","city":"Lisbon","score":1}`)
			two := []byte(`{"id":"two","city":"Porto","score":2}`)
			three := []byte(`{"id":"three","city":"Faro","score":3}`)
			keys := [][]byte{testReplicatedApplyKey(t, db, one), testReplicatedApplyKey(t, db, two), testReplicatedApplyKey(t, db, three)}
			seed, err := replication.AppendCommand(nil, testReplicatedApplyCommandValue(base, epoch, 2, []replication.Mutation{
				{Kind: replication.MutationPut, Key: keys[0], Value: one},
				{Kind: replication.MutationPut, Key: keys[1], Value: two},
			}))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = claim.ApplyNormal(testReplicatedApplyMeta(3), seed); err != nil {
				t.Fatal(err)
			}
			if result := completionResultCode(t, claim, seed); result != replicatedstate.ResultApplied {
				t.Fatalf("seed refused: %d", result)
			}
			var old replicatedstate.DataReadCut
			if err = claim.DataReadCutInto(nil, 3, &old); err != nil {
				t.Fatal(err)
			}
			defer old.Close()
			snapshot, _ := old.Relation(1)
			before, found, err := snapshot.AppendRaw(nil, keys[0])
			if err != nil || !found {
				t.Fatalf("old row: found=%t err=%v", found, err)
			}

			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			buildDone := make(chan error, 1)
			go func() {
				_, buildErr := claim.buildReplicatedSchemaDDLTarget(t.Context(), 3, statement, func(*catalogFile) error {
					close(entered)
					<-release // Exact snapshot pinned; no serving lock may be held.
					return nil
				})
				buildDone <- buildErr
				close(buildDone)
			}()
			defer func() {
				unblock()
				<-buildDone // Join before closing the claim, including failure paths.
			}()
			select {
			case <-entered:
			case err := <-buildDone:
				t.Fatalf("build ended before reservation: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("build did not reach off-lock materialization")
			}

			mutate, err := replication.AppendCommand(nil, testReplicatedApplyCommandValue(base, epoch, 3, []replication.Mutation{
				{Kind: replication.MutationPut, Key: keys[0], Value: []byte(`{"id":"one","city":"Braga","score":4}`)},
				{Kind: replication.MutationDelete, Key: keys[1]},
				{Kind: replication.MutationPut, Key: keys[2], Value: three},
			}))
			if err != nil {
				t.Fatal(err)
			}
			foregroundDone := make(chan error, 1)
			go func() {
				_, err := claim.ApplyNormal(testReplicatedApplyMeta(4), mutate)
				if err == nil {
					// A new read must observe all three changes while the builder is
					// still paused. The old reader must retain its original view.
					for i, key := range keys {
						row, readErr := claim.PointReadInto(1, key, 4, base.UserLimits.MaxDocumentBytes, nil)
						if readErr != nil || row.Found != (i != 1) || i == 0 && bytes.Equal(row.Value, before) {
							err = fmt.Errorf("foreground row %d: found=%t err=%v", i, row.Found, readErr)
							break
						}
					}
				}
				foregroundDone <- err
				close(foregroundDone)
			}()
			defer func() { unblock(); <-foregroundDone }()
			select {
			case err := <-foregroundDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("DDL materialization blocked foreground writes or reads")
			}
			if result := completionResultCode(t, claim, mutate); result != replicatedstate.ResultApplied {
				t.Fatalf("concurrent mutation refused: %d", result)
			}
			for i, key := range keys {
				row, found, err := snapshot.AppendRaw(nil, key)
				if err != nil || found != (i != 2) || i == 0 && !bytes.Equal(row, before) {
					t.Fatalf("old snapshot changed at row %d: found=%t err=%v", i, found, err)
				}
			}
			unblock()
			if err = <-buildDone; !errors.Is(err, ErrTransactionConflict) {
				t.Fatalf("stale build=%v, want conflict", err)
			}
			directory, err := claim.ReplicatedSchemaTargetDirectory()
			if err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 0 {
				t.Fatalf("stale build leaked files: %v %v", entries, err)
			}
			if claim.Applied() != 4 {
				t.Fatal("failed DDL altered the serving publication")
			}
		})
	}
}
