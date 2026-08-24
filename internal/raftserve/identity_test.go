package raftserve

import (
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

func testGroup(seed byte) raftmember.GroupKey {
	group := raftmember.GroupKey{TopologyRecoveryEpoch: uint64(seed) + 1}
	for index := range group.ClusterID {
		group.ClusterID[index] = seed + byte(index) + 1
		group.ClusterIncarnation[index] = seed + byte(index) + 21
		group.ShardIncarnation[index] = seed + byte(index) + 41
		group.GroupID[index] = seed + byte(index) + 61
	}
	return group
}

func testCommand(group raftmember.GroupKey, client byte, sequence uint64) replication.Command {
	fingerprint := sha256.Sum256([]byte{client, byte(sequence), 0x9d})
	return replication.Command{
		ClusterID:             replication.ID128(group.ClusterID),
		ClusterIncarnation:    replication.ID128(group.ClusterIncarnation),
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		Distribution:          "distribution", Shard: "shard", AllocationGeneration: 7,
		ShardIncarnation: replication.ID128(group.ShardIncarnation),
		GroupID:          replication.ID128(group.GroupID), ReplicaSetVersion: 2,
		ActivePolicyGeneration: 3, ProtectionEpoch: 4, OwnershipEpoch: 5,
		SchemaGeneration: 6, RoutingVersion: 7, RouteGeneration: 8,
		Tenant: []byte("tenant"), ClientID: replication.ID128{client},
		ClientEpoch: 9, ClientSequence: sequence, AckThrough: sequence - 1,
		Fingerprint: fingerprint, RetryHome: replication.RetryHome{client},
		Collection: "documents",
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte{0, client}, Value: []byte{1, byte(sequence)},
		}},
	}
}

func encodeTestCommand(t testing.TB, command replication.Command) []byte {
	t.Helper()
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testRegistry(t testing.TB, identities, attempts, waiters int) *Registry {
	t.Helper()
	registry, err := NewRegistry(Limits{
		MaxGroups:                  min(identities, multiraft.AbsoluteMaxGroups),
		MaxOutstandingIdentities:   identities,
		MaxOutstandingAttempts:     attempts,
		MaxWaiters:                 waiters,
		MaxAttemptsPerIdentity:     attempts,
		MaxRetainedCompletionBytes: int64(identities * completionSlotBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

type testProposalHost struct {
	registry *Registry
	tokens   []multiraft.ProposalToken
	data     [][]byte
	err      error
	admit    bool
}

func (host *testProposalHost) EnqueueTrackedProposal(
	group raftmember.GroupKey,
	data []byte,
	token multiraft.ProposalToken,
) error {
	if host.err != nil {
		return host.err
	}
	host.tokens = append(host.tokens, token)
	host.data = append(host.data, append([]byte(nil), data...))
	if host.admit {
		host.registry.settleProposalAdmission(multiraft.ProposalAdmission{
			Group: group, Token: token, Admitted: true,
		})
	}
	return nil
}

func TestAttemptDigestBindsEveryCommandViewFieldOnce(t *testing.T) {
	group := testGroup(3)
	base := testCommand(group, 4, 2)
	baseView, err := replication.OpenCommand(encodeTestCommand(t, base))
	if err != nil {
		t.Fatal(err)
	}
	baseLogical := replicatedstate.LogicalCommandDigest(baseView)
	baseAttempt := attemptDigest(baseView, baseLogical)
	tests := []struct {
		name   string
		mutate func(*replication.Command)
	}{
		{"cluster-id", func(c *replication.Command) { c.ClusterID[1]++ }},
		{"cluster-incarnation", func(c *replication.Command) { c.ClusterIncarnation[1]++ }},
		{"topology-recovery", func(c *replication.Command) { c.TopologyRecoveryEpoch++ }},
		{"distribution", func(c *replication.Command) { c.Distribution += "-next" }},
		{"shard", func(c *replication.Command) { c.Shard += "-next" }},
		{"allocation", func(c *replication.Command) { c.AllocationGeneration++ }},
		{"shard-incarnation", func(c *replication.Command) { c.ShardIncarnation[1]++ }},
		{"group-id", func(c *replication.Command) { c.GroupID[1]++ }},
		{"replica-set", func(c *replication.Command) { c.ReplicaSetVersion++ }},
		{"policy", func(c *replication.Command) { c.ActivePolicyGeneration++ }},
		{"protection", func(c *replication.Command) { c.ProtectionEpoch++ }},
		{"ownership", func(c *replication.Command) { c.OwnershipEpoch++ }},
		{"schema", func(c *replication.Command) { c.SchemaGeneration++ }},
		{"routing", func(c *replication.Command) { c.RoutingVersion++ }},
		{"route", func(c *replication.Command) { c.RouteGeneration++ }},
		{"command-kind", func(c *replication.Command) {
			c.Kind = replication.CommandSessionRetire
			c.Mutations = nil
		}},
		{"tenant", func(c *replication.Command) { c.Tenant = []byte("other-tenant") }},
		{"client-id", func(c *replication.Command) { c.ClientID[1]++ }},
		{"client-epoch", func(c *replication.Command) { c.ClientEpoch++ }},
		{"client-sequence", func(c *replication.Command) { c.ClientSequence++; c.AckThrough++ }},
		{"ack-through", func(c *replication.Command) { c.AckThrough = 0 }},
		{"fingerprint", func(c *replication.Command) { c.Fingerprint[1]++ }},
		{"retry-home", func(c *replication.Command) { c.RetryHome[1]++ }},
		{"collection", func(c *replication.Command) { c.Collection += "-next" }},
		{"mutation-kind", func(c *replication.Command) {
			c.Mutations[0] = replication.Mutation{Kind: replication.MutationDelete, Key: []byte{0, 4}}
		}},
		{"mutation-key", func(c *replication.Command) { c.Mutations[0].Key = []byte{0, 4, 1} }},
		{"mutation-value", func(c *replication.Command) { c.Mutations[0].Value = []byte{1, 2, 3} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			candidate.Tenant = append([]byte(nil), base.Tenant...)
			candidate.Mutations = append([]replication.Mutation(nil), base.Mutations...)
			test.mutate(&candidate)
			view, openErr := replication.OpenCommand(encodeTestCommand(t, candidate))
			if openErr != nil {
				t.Fatal(openErr)
			}
			logical := replicatedstate.LogicalCommandDigest(view)
			attempt := attemptDigest(view, logical)
			if logical == baseLogical && attempt == baseAttempt {
				t.Fatal("semantic field changed neither logical nor attempt digest")
			}
		})
	}
}

func TestAttemptDigestBindsLeaseDeadlineFields(t *testing.T) {
	group := testGroup(4)
	base := testCommand(group, 5, 2)
	base.Kind = replication.CommandSessionRenew
	base.Mutations = nil
	base.ExpectedDeadlineUnixNano = 100
	base.NextDeadlineUnixNano = 200
	baseView, err := replication.OpenCommand(encodeTestCommand(t, base))
	if err != nil {
		t.Fatal(err)
	}
	baseLogical := replicatedstate.LogicalCommandDigest(baseView)
	baseAttempt := attemptDigest(baseView, baseLogical)
	for _, test := range []struct {
		name   string
		mutate func(*replication.Command)
	}{
		{"expected-deadline", func(command *replication.Command) {
			command.ExpectedDeadlineUnixNano--
		}},
		{"next-deadline", func(command *replication.Command) {
			command.NextDeadlineUnixNano++
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			test.mutate(&candidate)
			view, openErr := replication.OpenCommand(encodeTestCommand(t, candidate))
			if openErr != nil {
				t.Fatal(openErr)
			}
			logical := replicatedstate.LogicalCommandDigest(view)
			if logical == baseLogical && attemptDigest(view, logical) == baseAttempt {
				t.Fatal("lease field changed neither logical nor attempt digest")
			}
		})
	}
}

func TestRegistryCoalescesExactAttemptsButEnqueuesChangedAck(t *testing.T) {
	registry := testRegistry(t, 2, 4, 4)
	host := &testProposalHost{registry: registry, admit: true}
	group := testGroup(9)
	command := testCommand(group, 8, 2)
	command.AckThrough = 0
	encoded := encodeTestCommand(t, command)
	first, err := registry.Enqueue(host, group, encoded)
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Enqueue(host, group, encoded)
	if err != nil {
		t.Fatal(err)
	}
	command.AckThrough = 1
	changed := encodeTestCommand(t, command)
	third, err := registry.Enqueue(host, group, changed)
	if err != nil {
		t.Fatal(err)
	}
	if len(host.tokens) != 2 || len(host.data) != 2 {
		t.Fatalf("Host enqueues = %d, want exact+changed only", len(host.tokens))
	}
	if first == (Waiter{}) || second == (Waiter{}) || third == (Waiter{}) {
		t.Fatal("missing waiter")
	}
	if got := registry.Stats(); got.OutstandingIdentities != 1 ||
		got.OutstandingAttempts != 2 || got.Waiters != 3 {
		t.Fatalf("stats = %+v", got)
	}
}

func TestRegistryExactTenantCheckSurvivesForcedHashAndSessionDigestCollision(t *testing.T) {
	registry := testRegistry(t, 2, 2, 2)
	registry.hashMask = 0
	group := testGroup(11)
	first, err := openCommandIdentity(group, encodeTestCommand(t, testCommand(group, 7, 2)))
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.tenant = []byte("collision-tenant")
	second.position.sessionDigest = first.position.sessionDigest
	registry.mu.Lock()
	_, _, _, err = registry.registerLocked(first)
	if err == nil {
		_, _, _, err = registry.registerLocked(second)
	}
	registry.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if got := registry.Stats(); got.OutstandingIdentities != 2 {
		t.Fatalf("collision identities = %+v", got)
	}
}

func TestRequestNamespacesSeparateReleaseOnly(t *testing.T) {
	group := testGroup(12)
	ordinary := testCommand(group, 7, 2)
	retire := ordinary
	retire.Kind = replication.CommandSessionRetire
	retire.Mutations = nil
	retire.AckThrough = 1
	release := retire
	release.Kind = replication.CommandSessionRelease
	ordinaryIdentity, err := openCommandIdentity(group, encodeTestCommand(t, ordinary))
	if err != nil {
		t.Fatal(err)
	}
	retireIdentity, err := openCommandIdentity(group, encodeTestCommand(t, retire))
	if err != nil {
		t.Fatal(err)
	}
	releaseIdentity, err := openCommandIdentity(group, encodeTestCommand(t, release))
	if err != nil {
		t.Fatal(err)
	}
	if ordinaryIdentity.position.namespace != retireIdentity.position.namespace ||
		ordinaryIdentity.position != retireIdentity.position {
		t.Fatal("ordinary sequenced commands were over-separated")
	}
	if releaseIdentity.position.namespace == retireIdentity.position.namespace ||
		releaseIdentity.position.sequence != retireIdentity.position.sequence {
		t.Fatal("retirement and release terminal sequence collided")
	}

	registry := testRegistry(t, 2, 2, 2)
	registry.mu.Lock()
	_, _, _, err = registry.registerLocked(retireIdentity)
	if err == nil {
		_, _, _, err = registry.registerLocked(ordinaryIdentity)
		if !errors.Is(err, replicatedstate.ErrRequestConflict) {
			registry.mu.Unlock()
			t.Fatalf("ordinary terminal reuse = %v", err)
		}
		_, _, _, err = registry.registerLocked(releaseIdentity)
	}
	registry.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegistryRollsBackOnlyNewReservationOnHostRefusal(t *testing.T) {
	registry := testRegistry(t, 1, 1, 1)
	want := errors.New("queue refused")
	host := &testProposalHost{registry: registry, err: want}
	group := testGroup(15)
	if waiter, err := registry.Enqueue(
		host, group, encodeTestCommand(t, testCommand(group, 4, 2)),
	); waiter != (Waiter{}) || !errors.Is(err, want) {
		t.Fatalf("enqueue = %+v, %v", waiter, err)
	}
	if got := registry.Stats(); got.OutstandingIdentities != 0 ||
		got.OutstandingAttempts != 0 || got.Waiters != 0 {
		t.Fatalf("retained refused reservation = %+v", got)
	}
}
