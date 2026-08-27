package driver

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestReplicatedExecutionPinReadRejectsAbsentOwner(t *testing.T) {
	for _, apply := range []*ReplicatedApply{nil, {}} {
		got, err := apply.ExecutionPinRead(executionpin.PinID{1}, 1)
		if !errors.Is(err, ErrReplicatedApplyClosed) || got != (replicatedstate.ExecutionPinReadResult{}) {
			t.Fatalf("absent owner read = %+v, %v", got, err)
		}
	}
}

func TestReplicatedExecutionPinReadUsesPublicationAndActivationFences(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "execution-pin-read")
	defer database.Close()
	apply, _, err := database.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer apply.Close()
	pin := executionpin.PinID{1}
	got, err := apply.ExecutionPinRead(pin, 1)
	if !errors.Is(err, replicatedstate.ErrWrongBinding) || got != (replicatedstate.ExecutionPinReadResult{}) {
		t.Fatalf("uninitialized owner read=%+v/%v", got, err)
	}
	if _, err = apply.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	got, err = apply.ExecutionPinRead(pin, 1)
	want, wantErr := apply.machine.ExecutionPinRead(pin, 1)
	if err != nil || wantErr != nil || got != want || got.Found {
		t.Fatalf("missing pin read=%+v/%v machine=%+v/%v", got, err, want, wantErr)
	}
	for _, test := range []struct {
		pin   executionpin.PinID
		floor uint64
		want  error
	}{
		{pin, 2, replicatedstate.ErrReadBehind},
		{pin, 0, replicatedstate.ErrExecutionPinStateCorrupt},
		{executionpin.PinID{}, 1, replicatedstate.ErrExecutionPinStateCorrupt},
	} {
		got, err = apply.ExecutionPinRead(test.pin, test.floor)
		if !errors.Is(err, test.want) || got != (replicatedstate.ExecutionPinReadResult{}) {
			t.Fatalf("invalid pin/floor read=%+v/%v want=%v", got, err, test.want)
		}
	}
	apply.activationBasePending[0] = 1
	got, err = apply.ExecutionPinRead(pin, 1)
	if err == nil || got != (replicatedstate.ExecutionPinReadResult{}) {
		t.Fatalf("uninstalled activation read=%+v/%v", got, err)
	}
	apply.activationBasePending = [32]byte{}
	for _, active := range []bool{true, false} {
		apply.walBaseSelectActive = active
		apply.walBaseSelectPending = !active
		got, err = apply.ExecutionPinRead(pin, 1)
		if !errors.Is(err, ErrReplicatedApplyBusy) || got != (replicatedstate.ExecutionPinReadResult{}) {
			t.Fatalf("WAL selection read=%+v/%v", got, err)
		}
	}
	apply.walBaseSelectActive, apply.walBaseSelectPending = false, false
	if err = apply.Close(); err != nil {
		t.Fatal(err)
	}
	got, err = apply.ExecutionPinRead(pin, 1)
	if !errors.Is(err, ErrReplicatedApplyClosed) || got != (replicatedstate.ExecutionPinReadResult{}) {
		t.Fatalf("closed owner read=%+v/%v", got, err)
	}
}
