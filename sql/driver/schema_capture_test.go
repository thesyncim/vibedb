package driver

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/schemachange"
)

func TestReplicatedSchemaCaptureRestoresBeforeServingAndKeepsIdentity(t *testing.T) {
	for _, finish := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "sealed"}[finish], func(t *testing.T) {
			path, db, base, identity, claim := schemaDDLJournalFixture(t)
			op, plan := [32]byte{71}, [32]byte{72}
			start, err := claim.BeginReplicatedSchemaCapture(t.Context(), op, plan, 100, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			if start.Base.Publication.Applied != 3 || !reflect.DeepEqual(identity, claim.identity) {
				t.Fatal("capture changed frozen source identity")
			}
			if replay, err := claim.BeginReplicatedSchemaCapture(t.Context(), op, plan, 100, 1<<20); err != nil || replay != start {
				t.Fatal("capture retry changed the base")
			}
			if _, err := claim.BeginReplicatedSchemaCapture(t.Context(), [32]byte{73}, plan, 100, 1<<20); !errors.Is(err, ErrReplicatedSchemaDDLConflict) {
				t.Fatal("another operation replaced capture")
			}
			if _, err := claim.ApplyNormal(testReplicatedApplyMeta(4), nil); err != nil {
				t.Fatal(err)
			}
			if finish {
				if _, err := claim.FinishReplicatedSchemaCapture(t.Context(), op, 3); err == nil {
					t.Fatal("stale cut sealed")
				}
				if _, err := claim.FinishReplicatedSchemaCapture(t.Context(), op, 4); err != nil {
					t.Fatal(err)
				}
			}
			before, err := claim.ReplicatedSchemaCaptureDescriptor(op)
			if err != nil {
				t.Fatal(err)
			}
			if err := errors.Join(claim.Close(), db.Close()); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			active, reopenedIdentity, err := reopened.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
			if err != nil {
				t.Fatal(err)
			}
			defer active.Close()
			if !reflect.DeepEqual(identity, reopenedIdentity) {
				t.Fatal("reopen rewrote source/session identity")
			}
			after, err := active.ReplicatedSchemaCaptureDescriptor(op)
			if err != nil || after != before {
				t.Fatalf("capture not recovered before claim publication: %+v err=%v", after, err)
			}
			var workspace schemachange.CaptureWorkspace
			entry, found, err := active.ReadReplicatedSchemaCapture(t.Context(), op, start.Base, &workspace)
			if err != nil || !found || entry.After.Applied != 4 {
				t.Fatalf("retained tail found=%v err=%v", found, err)
			}
			if _, err := active.ApplyNormal(testReplicatedApplyMeta(5), nil); err != nil {
				t.Fatal(err)
			}
			after, err = active.ReplicatedSchemaCaptureDescriptor(op)
			want := uint64(5)
			if finish {
				want = 4
			}
			if err != nil || after.Head.Publication.Applied != want {
				t.Fatalf("post-reopen head=%d want=%d err=%v", after.Head.Publication.Applied, want, err)
			}
			if finish {
				if historical, err := active.FinishReplicatedSchemaCapture(t.Context(), op, 4); err != nil || historical != before {
					t.Fatal("historical seal changed after later writes")
				}
			}
		})
	}
}

func TestReplicatedSchemaCaptureAbortReopenKeepsInsertsWorking(t *testing.T) {
	path, db, base, identity, claim := schemaDDLJournalFixture(t)
	op := [32]byte{81}
	if _, err := claim.BeginReplicatedSchemaCapture(t.Context(), op, [32]byte{82}, 1, 1<<20); err != nil {
		t.Fatal(err)
	}
	doc := []byte(`{"id":"two","city":"Porto"}`)
	key := testReplicatedApplyKey(t, db, doc)
	command, err := replication.AppendCommand(nil, testReplicatedApplyCommandValue(base, 2, 3, []replication.Mutation{{Kind: replication.MutationPut, Key: key, Value: doc}}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(4), command); err != nil {
		t.Fatal(err)
	}
	d, err := claim.ReplicatedSchemaCaptureDescriptor(op)
	if err != nil || d.Abort != schemachange.AbortCapacity {
		t.Fatalf("missing abort %+v %v", d, err)
	}
	if err := errors.Join(claim.Close(), db.Close()); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	active, _, err := reopened.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	if _, err := active.ApplyNormal(testReplicatedApplyMeta(5), command); err != nil {
		t.Fatal(err)
	}
	row, err := active.PointReadInto(1, key, 5, base.UserLimits.MaxDocumentBytes, nil)
	if err != nil || !row.Found || !bytes.Equal(row.Value, []byte(`{"city":"Porto","id":"two"}`)) {
		t.Fatalf("acknowledged insert missing: %+v %v", row, err)
	}
	if got, err := active.ReplicatedSchemaCaptureDescriptor(op); err != nil || got != d {
		t.Fatal("aborted capture resumed or changed identity")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := active.BeginReplicatedSchemaCapture(canceled, op, [32]byte{82}, 1, 1<<20); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled capture control ignored")
	}
}
