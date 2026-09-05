package multiraft

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
)

func TestHostControlProposalReturnsCoreAdmission(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := newFakeRuntime(4)
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	if _, _, err := host.RunOne(); err != nil {
		t.Fatal(err)
	}
	group := runtime.identity.Group
	runtime.proposalErrs = []error{raftmodel.ErrReadyPending}
	if err := host.ProposeControl(group, []byte("ownership")); !errors.Is(err, raftmodel.ErrReadyPending) {
		t.Fatalf("core refusal = %v", err)
	}
	if len(runtime.proposals) != 1 || host.queueItems != 0 || host.queueBytes != 0 {
		t.Fatal("control command was queued instead of reporting core admission")
	}
	if err := host.ProposeControl(group, []byte("ownership")); err != nil {
		t.Fatal(err)
	}
	if len(runtime.proposals) != 2 || host.runnableLen() != 1 {
		t.Fatal("successful retry did not admit the command and wake Ready processing")
	}
	if err := host.ProposeControl(group, make([]byte, raftmodel.MaxProposalBytes+1)); !errors.Is(err, raftmodel.ErrAdmissionBound) {
		t.Fatalf("oversized control = %v", err)
	}
	if len(runtime.proposals) != 2 {
		t.Fatal("oversized command reached core admission")
	}
	state, err := host.lookup(group)
	if err != nil {
		t.Fatal(err)
	}
	for _, flag := range []*bool{&state.schemaTransitionFenced, &state.schemaQuiescing, &state.schemaQuiesced} {
		*flag = true
		if err := host.ProposeControl(group, []byte("ownership")); !errors.Is(err, ErrGroupBusy) {
			t.Fatalf("schema-fenced control = %v", err)
		}
		*flag = false
	}
	if len(runtime.proposals) != 2 {
		t.Fatal("schema-fenced command reached core admission")
	}
}
