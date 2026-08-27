package driver

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// This uses real durable rows and the replicated machine on every host. Only
// the SQL ownership wrapper is assembled directly: sealed SQL creation still
// belongs to the mandatory Linux external-process gate.
func TestReplicatedPointBatchReadUsesCoherentCutAndActivationFences(t *testing.T) {
	fixture := newReplicatedChildSourceFixture(t)
	core := &database{}
	apply := &ReplicatedApply{database: core, machine: fixture.machine}
	core.replicatedApplyClaim = apply
	missing, ok := orderedkey.AppendString(nil, []byte("batch-missing"), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode missing typed key")
	}
	packed, err := replicatedstate.AppendPointReadBatch(nil, []replicatedstate.PointRead{
		{Relation: 1, Key: fixture.key}, {Relation: 1, Key: missing}, {Relation: 1, Key: fixture.key},
	})
	if err != nil {
		t.Fatal(err)
	}
	floor := fixture.machine.Published().Applied
	read, err := apply.PointReadBatchInto(packed, floor, 4096, nil)
	want, wantErr := fixture.machine.PointReadBatchInto(packed, floor, 4096, nil)
	if err != nil || wantErr != nil || read.Fence != want.Fence || !bytes.Equal(read.Data, want.Data) || read.Fence.Applied != floor {
		t.Fatalf("wrapper read=%+v/%v machine=%+v/%v", read, err, want, wantErr)
	}
	value, err := replicatedstate.OpenPointReadBatchValue(read.Data)
	if err != nil || value.Count() != 3 {
		t.Fatalf("batch count=%d err=%v", value.Count(), err)
	}
	for index := range 3 {
		raw, found, ok := value.Lookup(index)
		if !ok || found != (index != 1) || found && !bytes.Equal(raw, fixture.document) || !found && len(raw) != 0 {
			t.Fatalf("position %d raw=%q found=%v ok=%v", index, raw, found, ok)
		}
	}
	assertFailure := func(t *testing.T, request []byte, applied uint64, maximum int, want error) {
		t.Helper()
		got, err := apply.PointReadBatchInto(request, applied, maximum, nil)
		if !errors.Is(err, want) || got.Data != nil || got.Fence != (replicatedstate.SnapshotFence{}) {
			t.Fatalf("failed batch leaked result=%+v err=%v want=%v", got, err, want)
		}
	}
	assertFailure(t, packed, floor+1, 4096, replicatedstate.ErrReadBehind)
	assertFailure(t, packed, floor, len(read.Data)-1, replicatedstate.ErrReadBufferBound)
	assertFailure(t, packed[:len(packed)-1], floor, 4096, replicatedstate.ErrPointReadBatch)
	unknown, err := replicatedstate.AppendPointReadBatch(nil, []replicatedstate.PointRead{{Relation: 2, Key: fixture.key}})
	if err != nil {
		t.Fatal(err)
	}
	assertFailure(t, unknown, floor, 4096, replicatedstate.ErrInvalidCollection)
	for _, test := range []struct {
		name  string
		set   func()
		reset func()
		want  error
	}{
		{"closed", func() { apply.closed = true }, func() { apply.closed = false }, ErrReplicatedApplyClosed},
		{"foreign-claim", func() { core.replicatedApplyClaim = nil }, func() { core.replicatedApplyClaim = apply }, ErrReplicatedApplyClosed},
		{"closed-database", func() { core.closed = true }, func() { core.closed = false }, ErrReplicatedApplyClosed},
		{"activation", func() { apply.activationBasePending[0] = 1 }, func() { apply.activationBasePending = [32]byte{} }, ErrReplicatedApplyBasePending},
		{"wal-active", func() { apply.walBaseSelectActive = true }, func() { apply.walBaseSelectActive = false }, ErrReplicatedApplyBusy},
		{"wal-pending", func() { apply.walBaseSelectPending = true }, func() { apply.walBaseSelectPending = false }, ErrReplicatedApplyBusy},
	} {
		t.Run(test.name, func(t *testing.T) { test.set(); defer test.reset(); assertFailure(t, packed, floor, 4096, test.want) })
	}
}

func TestReplicatedPointBatchReadRejectsAbsentOwner(t *testing.T) {
	for _, apply := range []*ReplicatedApply{nil, {}} {
		got, err := apply.PointReadBatchInto(nil, 1, 4096, nil)
		if !errors.Is(err, ErrReplicatedApplyClosed) || got.Data != nil || got.Fence != (replicatedstate.SnapshotFence{}) {
			t.Fatalf("absent owner read=%+v/%v", got, err)
		}
	}
}
