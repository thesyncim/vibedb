package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestDataReadCutPinsPublicationAndReusesStorage(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte("document")
	before := []byte(`{"n":1}`)
	after := []byte(`{"n":2}`)
	put := func(index, sequence uint64, value []byte) {
		t.Helper()
		command := fixture.command(t, sequence, replication.RelationMutationBatch{
			Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: key, Value: value}},
		})
		if _, err := fixture.machine.ApplyNormal(normalMeta(index), command); err != nil {
			t.Fatal(err)
		}
		if code := bundleCompletionResult(t, fixture.machine, command); code != ResultApplied {
			t.Fatalf("put completion=%d", code)
		}
	}
	put(3, 1, before)
	var cut DataReadCut
	defer cut.Close()
	ids := []replication.RelationID{1, 2}
	if err := fixture.machine.DataReadCutInto(ids, 3, &cut); err != nil {
		t.Fatal(err)
	}
	if cut.Fence().Applied != 3 || cut.Fence().Binding != fixture.binding || !cut.FullOwnership() {
		t.Fatalf("cut fence=%+v", cut.Fence())
	}
	if err := fixture.machine.DataReadCutInto(ids, 3, &cut); !errors.Is(err, ErrDataReadOpen) {
		t.Fatalf("overwrite live cut=%v", err)
	}
	put(4, 2, after)
	snapshot, ok := cut.Relation(1)
	if !ok {
		t.Fatal("admitted relation missing")
	}
	got, found, err := snapshot.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(got, before) || cut.Fence().Applied != 3 {
		t.Fatalf("pinned row=%q found=%v err=%v", got, found, err)
	}
	for _, id := range []replication.RelationID{0, 3, 65535} {
		if _, ok := cut.Relation(id); ok || cut.OwnsKey(id, key) {
			t.Fatalf("unadmitted relation %d exposed", id)
		}
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cut.Close(); err != nil || cut.Fence() != (SnapshotFence{}) || cut.FullOwnership() {
		t.Fatalf("closed cut retained access: %v", err)
	}
	if _, ok := cut.Relation(1); ok || cut.OwnsKey(1, key) {
		t.Fatal("closed relation remains accessible")
	}
	if err := fixture.machine.DataReadCutInto(ids[:1], 4, &cut); err != nil {
		t.Fatal(err)
	}
	if _, ok := cut.Relation(2); ok {
		t.Fatal("reused cut exposed a previously admitted relation")
	}
	snapshot, _ = cut.Relation(1)
	got, found, err = snapshot.AppendRaw(nil, key)
	if err != nil || !found || !bytes.Equal(got, after) {
		t.Fatalf("new cut row=%q found=%v err=%v", got, found, err)
	}
}

func TestDataReadCutAdmission(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	for _, test := range []struct {
		ids   []replication.RelationID
		floor uint64
		want  error
	}{
		{[]replication.RelationID{}, 1, ErrInvalidCollection},
		{[]replication.RelationID{0}, 1, ErrInvalidCollection},
		{[]replication.RelationID{3}, 1, ErrInvalidCollection},
		{[]replication.RelationID{1, 1}, 1, ErrInvalidCollection},
		{[]replication.RelationID{1}, 0, ErrReadBehind},
		{[]replication.RelationID{1}, fixture.machine.Published().Applied + 1, ErrReadBehind},
	} {
		var cut DataReadCut
		err := fixture.machine.DataReadCutInto(test.ids, test.floor, &cut)
		if !errors.Is(err, test.want) || cut.Fence() != (SnapshotFence{}) || cut.open {
			t.Fatalf("ids=%v floor=%d err=%v want=%v", test.ids, test.floor, err, test.want)
		}
		if err := cut.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDataReadCutRefusesActiveIntentsUntilRelease(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(231)
	stage := transactionParticipantStageCommand(t, fixture, id, []replication.RelationMutationBatch{{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: []byte("pending"), Value: []byte(`{"n":1}`),
		}},
	}})
	applyTransactionCommand(t, fixture.machine, 3, stage)
	var cut DataReadCut
	defer cut.Close()
	for _, ids := range [][]replication.RelationID{{1}, {2, 1}, {2}} {
		if err := fixture.machine.DataReadCutInto(ids, 3, &cut); !errors.Is(err, ErrTransactionIntentActive) || cut.open {
			t.Fatalf("intersecting relations %v err=%v open=%v", ids, err, cut.open)
		}
	}
	for step, op := range []distributedtxn.ReplicatedOperation{
		distributedtxn.ReplicatedPrepareParticipant,
		distributedtxn.ReplicatedApplyParticipant,
		distributedtxn.ReplicatedReleaseParticipant,
	} {
		command := transactionParticipantTransitionCommand(t, fixture, id, op, uint64(step+1))
		applyTransactionCommand(t, fixture.machine, uint64(step+4), command)
		if step < 2 {
			if err := fixture.machine.DataReadCutInto([]replication.RelationID{1}, uint64(step+4), &cut); !errors.Is(err, ErrTransactionIntentActive) {
				t.Fatalf("read before release, step=%d err=%v", step, err)
			}
		}
	}
	if err := fixture.machine.DataReadCutInto([]replication.RelationID{1, 2}, 6, &cut); err != nil {
		t.Fatalf("released intent=%v", err)
	}
}

func TestDataReadCutPinsOwnershipAndFailsClosedWithoutValidator(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	m := fixture.machine
	// Use the same deterministic placement oracle as the point-read tests.
	m.relations[0].target.Validator = pointOwnershipValidator{m.relations[0].target.Validator}
	m.state.Binding.OwnedRange = distribution.KeyRange{
		End: distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x80}},
	}
	var cut DataReadCut
	defer cut.Close()
	if err := m.DataReadCutInto([]replication.RelationID{1}, 1, &cut); err != nil {
		t.Fatal(err)
	}
	m.state.Binding.OwnedRange = distribution.KeyRange{
		Start: distribution.KeyspacePoint{0x80}, End: distribution.KeyspaceEnd{Max: true},
	}
	if cut.FullOwnership() || !cut.OwnsKey(1, []byte{0x01}) || cut.OwnsKey(1, []byte{0x80}) || cut.OwnsKey(1, nil) {
		t.Fatal("cut did not preserve original half-open ownership")
	}
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if err := m.DataReadCutInto([]replication.RelationID{2}, 1, &cut); !errors.Is(err, ErrWrongBinding) {
		t.Fatalf("range-less validator accepted narrowed ownership: %v", err)
	}
}

func TestDataReadCutWarmedCaptureAllocatesNothing(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	var cut DataReadCut
	ids := []replication.RelationID{1, 2}
	run := func() {
		if err := fixture.machine.DataReadCutInto(ids, 1, &cut); err != nil {
			panic(err)
		}
		if err := cut.Close(); err != nil {
			panic(err)
		}
	}
	run()
	if allocations := testing.AllocsPerRun(1000, run); allocations != 0 {
		t.Fatalf("warmed capture allocations=%g, want zero", allocations)
	}
}
