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
	lagging := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x94)
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
	eventID := transitionDocumentID("move-head/", next.Generation())
	eventKey := string(fixedControlPlaneKey(eventID))
	savedEvent := bytes.Clone(client.rows[eventKey])
	for _, mutation := range []string{"missing", "grant", "command", "head", "owner-key"} {
		t.Run("reject-history-"+mutation, func(t *testing.T) {
			var event groupTransitionRecord
			if err := decodeTransitionDocument(savedEvent, eventID, &event, maxGroupTransitionRecordBytes); err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "missing":
				delete(client.rows, eventKey)
			case "grant":
				event.Grant.TargetMember++
			case "command":
				event.Command.ReplicaSetVersion++
			case "head":
				event.Receipt.CommittedHeadDigest[0]++
			case "owner-key":
				event.Intent.Key.OperationID[0]++
			}
			if mutation != "missing" {
				raw, err := encodeTransitionDocument(eventID, event, maxGroupTransitionRecordBytes)
				if err != nil {
					t.Fatal(err)
				}
				client.rows[eventKey] = raw
			}
			defer func() { client.rows[eventKey] = savedEvent }()
			if _, err := lagging.Read(ctx); err == nil {
				t.Fatal("unproven history accepted")
			}
			if lagging.holder.Current().Generation() != current.Generation() {
				t.Fatal("invalid history changed holder")
			}
		})
	}
	recovered, err := lagging.Read(ctx)
	if err != nil || recovered == nil || recovered.Generation() != post.Generation() {
		t.Fatalf("missed publication replay: snapshot=%v err=%v", recovered, err)
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
	if err := authority.ReleaseCompletedDistributionTransition(ctx, final); err != nil {
		t.Fatalf("lost release response retry: %v", err)
	}
	newer, err := authority.AcquireDistributionTransition(ctx, competing)
	if err != nil || newer.Revision <= lease.Revision {
		t.Fatalf("new owner=%+v %v", newer, err)
	}
	if err := authority.ReleaseCompletedDistributionTransition(ctx, final); !errors.Is(err, ErrTransitionOwnerStale) {
		t.Fatalf("completed release crossed owner revision: %v", err)
	}
	if err := authority.ReleaseDistributionTransition(ctx, lease, final); !errors.Is(err, ErrTransitionOwnerStale) {
		t.Fatalf("ABA release=%v", err)
	}
}

func advanceUnrelatedGroupTestHead(t *testing.T, authority *ReplicatedCatalogAuthority, current *Snapshot) *Snapshot {
	t.Helper()
	next, err := NewSnapshotWithReplicatedTableMetadata(cloneConfig(current.config), current.endpoints, current.Generation()+1,
		current.indexDescriptors(), current.statistics.Descriptors(), current.replicatedDescriptors(), current.replicatedTableProfiles(), current.ReplicatedTableDeclarations())
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.Publish(context.Background(), current.Generation(), next); err != nil {
		t.Fatal(err)
	}
	return authority.holder.Current()
}

func TestOwnedGroupPublicationSurvivesUnrelatedCatalogHead(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixtureWithDescriptor(t, func(source *ReplicatedShardDescriptor) { source.LogicalSchemaDigest = [32]byte{0x93} })
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
	current = advanceUnrelatedGroupTestHead(t, authority, current)
	next, err := BuildGroupOwnedShardTransition(current, intent, TransitionPhasePreRemove, target, command)
	if err != nil {
		t.Fatal(err)
	}
	// The legacy API must retain its original head fence.
	if err := authority.PublishReplicaReplacement(ctx, current.Generation(), next, grant); err == nil {
		t.Fatal("legacy publication accepted a different source head")
	}
	receipt, err := authority.PublishGroupTransition(ctx, lease, intent, TransitionPhasePreRemove, next, [32]byte{})
	if err != nil {
		t.Fatalf("owned publication after unrelated head: %v", err)
	}
	if receipt.PredecessorHeadGeneration != current.Generation() || receipt.PredecessorGroupGeneration != intent.SourceHeadGeneration {
		t.Fatal("lost independent head and group provenance")
	}
	current = advanceUnrelatedGroupTestHead(t, authority, next)
	predecessor, _ := receipt.ReceiptDigest()
	command.ReplicaSetVersion++
	post, err := BuildGroupOwnedShardTransition(current, intent, TransitionPhasePostRemove, target, command)
	if err != nil {
		t.Fatal(err)
	}
	final, err := authority.PublishGroupTransition(ctx, lease, intent, TransitionPhasePostRemove, post, predecessor)
	if err != nil {
		t.Fatalf("owned post-remove after unrelated head: %v", err)
	}
	if err = final.ValidateSuccessor(intent, &receipt); err != nil {
		t.Fatal(err)
	}
	current = advanceUnrelatedGroupTestHead(t, authority, post)
	if err = authority.FinalizeReplicaReplacement(ctx, grant); err != nil {
		t.Fatalf("owned finalization after unrelated head: %v", err)
	}
	if err = authority.ReleaseDistributionTransition(ctx, lease, final); err != nil {
		t.Fatal(err)
	}
}
