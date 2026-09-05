package multiraft

import (
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

type authorityIngressTestRuntime struct {
	*fakeRuntime
	seen     []raftauthority.Message
	produced bool
	outbound raftmember.OutboundMessage
	stepErr  error
}

func (runtime *authorityIngressTestRuntime) StepAuthorityMessage(
	message *raftauthority.Message,
) (raftmember.OutboundMessage, bool, error) {
	if message != nil {
		runtime.seen = append(runtime.seen, *message)
	}
	if runtime.stepErr != nil {
		return raftmember.OutboundMessage{}, false, runtime.stepErr
	}
	return runtime.outbound, runtime.produced, nil
}

func TestHostAuthorityIngressOwnsFixedRecordAndOutbound(t *testing.T) {
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := &authorityIngressTestRuntime{fakeRuntime: newFakeRuntime(90)}
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	group := runtime.identity.Group
	member := runtime.identity.MemberID
	input := hostAuthorityIngressMessage(group, raftauthority.MessageRequest, member, 0)
	output := hostAuthorityIngressMessage(group, raftauthority.MessageGrant, member+1, member)
	runtime.produced = true
	runtime.outbound = raftmember.OutboundMessage{
		Group: group, From: member, To: member + 1, Authority: &output,
	}
	if err := host.AdoptAuthorityMessage(group, &input); err != nil {
		t.Fatalf("AdoptAuthorityMessage: %v", err)
	}
	if host.queueItems != 1 || host.queueBytes != raftauthority.CanonicalMessageBytes ||
		host.groups[group].items != 1 || host.groups[group].bytes != raftauthority.CanonicalMessageBytes {
		t.Fatalf("queued authority accounting = items %d/%d bytes %d/%d",
			host.queueItems, host.groups[group].items, host.queueBytes, host.groups[group].bytes)
	}
	originalTerm := input.Request.Term
	input.Request.Term++

	progress, done, err := host.RunOne()
	if err != nil || !done || progress.Kind != ProgressMessage {
		t.Fatalf("RunOne = %+v, done=%t err=%v", progress, done, err)
	}
	if len(runtime.seen) != 1 || runtime.seen[0].Request.Term != originalTerm {
		t.Fatalf("runtime saw %d authority messages with term %d, want term %d",
			len(runtime.seen), runtime.seen[0].Request.Term, originalTerm)
	}
	if host.queueItems != 0 || host.queueBytes != 0 || host.groups[group].items != 0 || host.groups[group].bytes != 0 {
		t.Fatalf("queue accounting after step = global %d/%d group %d/%d",
			host.queueItems, host.queueBytes, host.groups[group].items, host.groups[group].bytes)
	}
	owned, ok := host.PopOutbound()
	if !ok || owned.Authority == nil {
		t.Fatalf("PopOutbound = %+v, ok=%t; want authority outbound", owned, ok)
	}
	if owned.Group != group || owned.From != member || owned.To != member+1 || *owned.Authority != output {
		t.Fatalf("owned outbound = %+v, want %+v", owned, runtime.outbound)
	}
	if _, ok := host.PopOutbound(); ok {
		t.Fatal("outbox retained an extra authority message")
	}
}

func TestHostAuthorityIngressRequiresOptInAndHonorsQueueBounds(t *testing.T) {
	groupHost, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	plain := newFakeRuntime(91)
	if err := groupHost.addRuntime(plain); err != nil {
		t.Fatal(err)
	}
	message := hostAuthorityIngressMessage(plain.identity.Group, raftauthority.MessageRequest, plain.identity.MemberID, 0)
	if err := groupHost.AdoptAuthorityMessage(plain.identity.Group, &message); !errors.Is(err, raftauthority.ErrPolicyDisabled) {
		t.Fatalf("unconfigured runtime error = %v, want ErrPolicyDisabled", err)
	}
	if groupHost.queueItems != 0 || groupHost.queueBytes != 0 || groupHost.groups[plain.identity.Group].items != 0 {
		t.Fatalf("unconfigured runtime was charged: queue %d/%d group %d",
			groupHost.queueItems, groupHost.queueBytes, groupHost.groups[plain.identity.Group].items)
	}
	if err := groupHost.AdoptAuthorityMessage(plain.identity.Group, nil); err == nil {
		t.Fatal("nil authority message was accepted")
	}

	limits := testHostLimits()
	limits.MaxQueueItems = 1
	limits.MaxGroupItems = 1
	limits.MaxPendingTicks = 1
	boundedHost, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	authorityRuntime := &authorityIngressTestRuntime{fakeRuntime: newFakeRuntime(92)}
	if err := boundedHost.addRuntime(authorityRuntime); err != nil {
		t.Fatal(err)
	}
	key := authorityRuntime.identity.Group
	first := hostAuthorityIngressMessage(key, raftauthority.MessageRequest, authorityRuntime.identity.MemberID, 0)
	second := hostAuthorityIngressMessage(key, raftauthority.MessageRequest, authorityRuntime.identity.MemberID, 0)
	if err := boundedHost.AdoptAuthorityMessage(key, &first); err != nil {
		t.Fatalf("first authority admission: %v", err)
	}
	if err := boundedHost.AdoptAuthorityMessage(key, &second); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("second authority admission = %v, want ErrQueueFull", err)
	}
	if boundedHost.queueItems != 1 || boundedHost.queueBytes != raftauthority.CanonicalMessageBytes ||
		boundedHost.groups[key].items != 1 {
		t.Fatalf("bounded queue accounting = items %d/%d bytes %d",
			boundedHost.queueItems, boundedHost.groups[key].items, boundedHost.queueBytes)
	}
	if _, done, err := boundedHost.RunOne(); err != nil || !done {
		t.Fatalf("drain authority = done %t err %v", done, err)
	}
	if boundedHost.queueItems != 0 || boundedHost.queueBytes != 0 || boundedHost.groups[key].items != 0 {
		t.Fatalf("bounded queue retained charge = items %d/%d bytes %d/%d",
			boundedHost.queueItems, boundedHost.groups[key].items, boundedHost.queueBytes, boundedHost.groups[key].bytes)
	}
}

func hostAuthorityIngressMessage(
	group raftmember.GroupKey,
	kind raftauthority.MessageKind,
	holder, voter uint64,
) raftauthority.Message {
	request := raftauthority.AuthorityRequest{
		Group: raftauthority.GroupIdentity{
			ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
			TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			ShardIncarnation:      group.ShardIncarnation, GroupID: group.GroupID,
		},
		Term: 5, Holder: holder, HolderIncarnation: 7,
		Config:        raftauthority.ConfigIdentity{AppliedVersion: 9, Digest: [32]byte{0x44}},
		PolicyVersion: 3, PolicyDigest: [32]byte{0x55}, Nonce: 11,
		StartAt: 12 * time.Millisecond,
	}
	message := raftauthority.Message{Kind: kind, Request: request}
	if kind == raftauthority.MessageGrant {
		message.Grant = raftauthority.AuthorityGrant{
			Request: request, Voter: voter, GrantedAt: 20 * time.Millisecond,
			PromiseUntil: time.Second,
		}
	}
	return message
}
