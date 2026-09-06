package gateway

import (
	"bytes"
	"context"
	"errors"
	"github.com/thesyncim/vibedb/distribution"
	"testing"
)

func testGroupTransitionIntent(t *testing.T, current *Snapshot, source ReplicatedShardDescriptor, target ReplicatedReplicaDescriptor, retiring uint64) GroupTransitionIntent {
	t.Helper()
	manifest, ok := current.Manifest(source.Distribution)
	if !ok {
		t.Fatal("source manifest missing")
	}
	ordinal, metadata := manifestShardOrdinal(manifest, source.Shard)
	if ordinal < 0 {
		t.Fatal("source shard missing")
	}
	route := make([]distribution.EndpointID, metadata.LeaderCount)
	for i := range route {
		route[i], _ = manifest.ShardLeaderAt(ordinal, i)
	}
	digest, err := CatalogSnapshotDigest(current)
	if err != nil {
		t.Fatal(err)
	}
	key := GroupTransitionKey{OperationID: [32]byte{0x91}, Distribution: source.Distribution, Shard: source.Shard, Group: source.Group,
		SourceAllocationGeneration: uint64(metadata.AllocationGeneration), SourceDescriptorDigest: DigestReplicatedShardDescriptor(source), SourceCommandFenceDigest: DigestCommandFence(source.Command)}
	intent := GroupTransitionIntent{Key: key, SourceMember: retiring, TargetMember: target.Member, SourceHeadGeneration: current.Generation(), SourceHeadDigest: digest,
		SourceDistributionVersion: manifest.Version(), SourceGroupDigest: key.SourceDescriptorDigest, SourceRosterDigest: DigestReplicaRoster(source.Replicas), SourceRouteDigest: DigestRoute(manifest, source.Shard),
		SourceCommandFenceDigest: key.SourceCommandFenceDigest, SourceDescriptor: source, SourceRoute: route, Replacement: target, TargetDistributionVersion: manifest.Version() + 1}
	if !intent.Valid() {
		t.Fatal("invalid fixture transition")
	}
	return intent
}

func TestGroupTransitionReceiptAtomicRecoveryAndOwnerFence(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixtureWithDescriptor(t, func(source *ReplicatedShardDescriptor) { source.LogicalSchemaDigest = [32]byte{0x93} })
	observer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x92)
	source := current.ReplicatedShardDescriptors()[0]
	grant, _, target, command := testCertifiedReplicaReplacement(t, current, source)
	intent := testGroupTransitionIntent(t, current, source, target, grant.SourceMember)
	ctx := context.Background()
	if err := authority.PublishMembershipGrant(ctx, grant); err != nil {
		t.Fatal(err)
	}
	lease, err := authority.AcquireDistributionTransition(ctx, intent.Key)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := observer.AcquireDistributionTransition(ctx, intent.Key)
	if err != nil || retry != lease {
		t.Fatalf("owner retry: %+v %v", retry, err)
	}
	competing := intent.Key
	competing.OperationID[0]++
	if _, err := observer.AcquireDistributionTransition(ctx, competing); !errors.Is(err, ErrTransitionOwnerBusy) {
		t.Fatalf("competing owner=%v", err)
	}
	next, err := BuildGroupOwnedShardTransition(current, intent, TransitionPhasePreRemove, target, command)
	if err != nil {
		t.Fatal(err)
	}
	stale := lease
	stale.Revision++
	if _, err := authority.PublishGroupTransition(ctx, stale, intent, TransitionPhasePreRemove, next, [32]byte{}); !errors.Is(err, ErrTransitionOwnerStale) {
		t.Fatalf("stale owner=%v", err)
	}
	if receipt, found, err := observer.ReadGroupPublicationReceipt(ctx, intent.Key); err != nil || found {
		t.Fatalf("failed publication leaked receipt=%+v %t %v", receipt, found, err)
	}
	client.unknownNext = true
	if _, err = authority.PublishGroupTransition(ctx, lease, intent, TransitionPhasePreRemove, next, [32]byte{}); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("unknown publication=%v", err)
	}
	receipt, found, err := observer.ReadGroupPublicationReceipt(ctx, intent.Key)
	if err != nil || !found || receipt.CommittedHeadGeneration != next.Generation() {
		t.Fatalf("durable receipt=%+v %t %v", receipt, found, err)
	}
	refreshed, err := observer.Read(ctx)
	if err != nil || refreshed.Generation() != receipt.CommittedHeadGeneration {
		t.Fatalf("atomic head=%v %v", refreshed, err)
	}
	pending := bytes.Clone(authority.session.PendingCommand())
	client.holdUnknown = false
	if err := authority.RetryPending(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pending, client.unknownCommand) {
		t.Fatal("retry changed publication command")
	}
	again, err := authority.PublishGroupTransition(ctx, lease, intent, TransitionPhasePreRemove, next, [32]byte{})
	if err != nil || again != receipt {
		t.Fatalf("receipt retry=%+v %v", again, err)
	}
	if err := authority.ReleaseDistributionTransition(ctx, lease, receipt); err == nil {
		t.Fatal("released before post-remove")
	}
	predecessor, _ := receipt.ReceiptDigest()
	command.ReplicaSetVersion++
	post, err := BuildGroupOwnedShardTransition(next, intent, TransitionPhasePostRemove, target, command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.PublishGroupTransition(ctx, lease, intent, TransitionPhasePostRemove, post, [32]byte{}); err == nil {
		t.Fatal("missing predecessor accepted")
	}
	final, err := authority.PublishGroupTransition(ctx, lease, intent, TransitionPhasePostRemove, post, predecessor)
	if err != nil {
		t.Fatal(err)
	}
	if err := final.ValidateSuccessor(intent, &receipt); err != nil {
		t.Fatal(err)
	}
	if err := authority.ReleaseDistributionTransition(ctx, lease, final); err == nil {
		t.Fatal("released before membership grant retirement")
	}
	if err := authority.FinalizeReplicaReplacement(ctx, grant); err != nil {
		t.Fatal(err)
	}
	if err := authority.ReleaseDistributionTransition(ctx, lease, final); err != nil {
		t.Fatal(err)
	}
	if err := authority.ReleaseDistributionTransition(ctx, lease, final); err != nil {
		t.Fatal(err)
	}
	newer, err := authority.AcquireDistributionTransition(ctx, competing)
	if err != nil || newer.Revision <= lease.Revision {
		t.Fatalf("new owner=%+v %v", newer, err)
	}
	if err := authority.ReleaseDistributionTransition(ctx, lease, final); !errors.Is(err, ErrTransitionOwnerStale) {
		t.Fatalf("ABA release=%v", err)
	}
}
