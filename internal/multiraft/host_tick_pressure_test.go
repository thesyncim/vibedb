package multiraft

import (
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestHostRejectedTimerLeavesAdmittedTicksAndMessagesExact(t *testing.T) {
	limits := testHostLimits()
	limits.MaxGroupItems, limits.MaxPendingTicks = 2, 1
	host, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	member := newFakeRuntime(42)
	if err := host.addRuntime(member); err != nil {
		t.Fatal(err)
	}
	key := member.identity.Group
	message := hostMessage(99, member.identity.MemberID, "already-admitted")
	if err := host.AdoptMessage(key, message); err != nil {
		t.Fatal(err)
	}
	if err := host.RequestTick(key); err != nil {
		t.Fatal(err)
	}
	bytes := host.queueBytes
	for range 64 {
		if err := host.RequestTick(key); err != ErrQueueFull {
			t.Fatalf("timer refusal: %v", err)
		}
		group := host.groups[key]
		if host.queueItems != 2 || host.queueBytes != bytes || group.items != 2 || group.ticks != 1 ||
			group.messages.len() != 1 || group.messages.items[0].message != message {
			t.Fatal("rejected tick changed admitted queue")
		}
	}
	for turns := 0; turns < 16 && host.queueItems != 0; turns++ {
		if _, _, err := host.RunOne(); err != nil {
			t.Fatal(err)
		}
	}
	ticks := 0
	for _, kind := range member.inputs {
		if kind == ProgressTick {
			ticks++
		}
	}
	if host.queueItems != 0 || host.queueBytes != 0 || ticks != 1 || len(member.messages) != 1 || !proto.Equal(member.messages[0], message) {
		t.Fatal("bounded drain lost input or manufactured rejected ticks")
	}
	if err := host.RequestTick(key); err != nil {
		t.Fatal("drained timer capacity not reusable", err)
	}
	for turns := 0; turns < 16 && host.queueItems != 0; turns++ {
		if _, _, err := host.RunOne(); err != nil {
			t.Fatal(err)
		}
	}
	if host.queueItems != 0 {
		t.Fatal("fresh timer did not progress")
	}
	for _, quiesce := range []func(){func() { host.groups[key].schemaQuiesced = true }, func() { host.groups[key].schemaQuiesced = false; host.groups[key].retiring = true }} {
		quiesce()
		if err := host.RequestTick(key); err != ErrGroupBusy || host.queueItems != 0 || host.groups[key].ticks != 0 {
			t.Fatal("intentional quiesce admitted a timer", err)
		}
	}
}
