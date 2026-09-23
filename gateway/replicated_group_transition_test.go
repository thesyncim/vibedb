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

func TestBuildGroupOwnedShardTransitionReplacesNonFirstRouteLeader(t *testing.T) {
	_, _, current := newCatalogAuthorityFixtureWithDescriptor(t, func(source *ReplicatedShardDescriptor) {
		source.LogicalSchemaDigest = [32]byte{0x93}
		testReplicatedCatalogEnrollTarget(source)
	})
	source := current.ReplicatedShardDescriptors()[0]
	_, _, target, command := testCertifiedReplicaReplacement(t, current, source)
	retiring := source.Replicas[1].Member
	intent := testGroupTransitionIntent(t, current, source, target, retiring)

	next, err := BuildGroupOwnedShardTransition(current, intent, TransitionPhasePreRemove, target, command)
	if err != nil {
		t.Fatal(err)
	}
	manifest, found := next.Manifest(source.Distribution)
	if !found {
		t.Fatal("transition manifest missing")
	}
	ordinal, _ := manifestShardOrdinal(manifest, source.Shard)
	if leader, ok := manifest.ShardLeaderAt(ordinal, 0); !ok || leader != "ep-a" {
		t.Fatalf("leader 0 = %q, %v", leader, ok)
	}
	if leader, ok := manifest.ShardLeaderAt(ordinal, 1); !ok || leader != target.Endpoint {
		t.Fatalf("leader 1 = %q, %v", leader, ok)
	}
	if leader, ok := manifest.ShardLeaderAt(ordinal, 2); !ok || leader != "ep-d" {
		t.Fatalf("leader 2 = %q, %v", leader, ok)
	}
	descriptor := next.ReplicatedShardDescriptors()[0]
	if descriptor.Replicas[1] != target || descriptor.Replicas[0] != source.Replicas[0] || descriptor.Replicas[2] != source.Replicas[2] {
		t.Fatalf("transition roster = %+v", descriptor.Replicas)
	}
	if descriptor.RetiringSource == nil || *descriptor.RetiringSource != source.Replicas[1] {
		t.Fatalf("pre-remove retiring source=%+v, want exact displaced source %+v", descriptor.RetiringSource, source.Replicas[1])
	}

	postCommand := command
	postCommand.ReplicaSetVersion++
	post, err := BuildGroupOwnedShardTransition(next, intent, TransitionPhasePostRemove, target, postCommand)
	if err != nil {
		t.Fatalf("build post-remove transition: %v", err)
	}
	postDescriptor := post.ReplicatedShardDescriptors()[0]
	if postDescriptor.RetiringSource != nil {
		t.Fatalf("post-remove retained retiring source %+v", postDescriptor.RetiringSource)
	}

	for name, mutate := range map[string]func(*Snapshot){
		"missing source witness": func(candidate *Snapshot) {
			candidate.replicatedShards[0].hasRetiringSource = false
		},
		"forged source witness": func(candidate *Snapshot) {
			index := int(candidate.replicatedShards[0].replicaBase) + int(candidate.replicatedShards[0].replicaCount)
			candidate.replicatedReplicas[index].Node[0]++
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := snapshotWithCatalogLineage(next, next.indexIDHighWater, next.shardGenerationHighWaters)
			candidate.replicatedShards = append([]replicatedCatalogShard(nil), next.replicatedShards...)
			candidate.replicatedReplicas = append([]ReplicatedEndpoint(nil), next.replicatedReplicas...)
			mutate(candidate)
			if _, err := BuildGroupOwnedShardTransition(candidate, intent, TransitionPhasePostRemove, target, postCommand); err == nil {
				t.Fatal("post-remove accepted an absent or forged retiring-source witness")
			}
		})
	}
}

func TestGroupTransitionReceiptAtomicRecoveryAndOwnerFence(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixtureWithDescriptor(t, func(source *ReplicatedShardDescriptor) {
		source.LogicalSchemaDigest = [32]byte{0x93}
		testReplicatedCatalogEnrollTarget(source)
	})
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
	recoveredIntent, recoveredReceipt, operationReceiptFound, err := observer.ReadGroupPublicationReceiptForOperation(
		ctx, intent.Key.OperationID, intent.Key.Group,
	)
	if err != nil || !operationReceiptFound || recoveredIntent.Key != intent.Key || recoveredReceipt != receipt {
		t.Fatalf("operation-scoped receipt=%+v %+v %t %v", recoveredIntent.Key, recoveredReceipt, operationReceiptFound, err)
	}
	if _, _, operationReceiptFound, err := observer.ReadGroupPublicationReceiptForOperation(ctx, [32]byte{0x92}, intent.Key.Group); err != nil || operationReceiptFound {
		t.Fatalf("foreign operation receipt=%t err=%v", operationReceiptFound, err)
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

func TestMoveAdmittedAfterUnrelatedHeadUsesExactExistingGrant(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixtureWithDescriptor(t, func(source *ReplicatedShardDescriptor) {
		source.LogicalSchemaDigest = [32]byte{0x93}
		testReplicatedCatalogEnrollTarget(source)
	})
	source := current.ReplicatedShardDescriptors()[0]
	grant, _, target, command := testCertifiedReplicaReplacement(t, current, source)
	ctx := t.Context()
	if err := authority.PublishMembershipGrant(ctx, grant); err != nil {
		t.Fatal(err)
	}
	current = advanceUnrelatedGroupTestHead(t, authority, current)
	intent := testGroupTransitionIntent(t, current, source, target, grant.SourceMember)
	lease, err := authority.AcquireDistributionTransition(ctx, intent.Key)
	if err != nil {
		t.Fatal(err)
	}
	if got, found, err := authority.ReadMembershipGrant(ctx, grant.Group); err != nil || !found || got != grant {
		t.Fatalf("unchanged current group no longer authorizes grant: found=%t err=%v", found, err)
	}
	next, err := BuildGroupOwnedShardTransition(current, intent, TransitionPhasePreRemove, target, command)
	if err != nil {
		t.Fatal(err)
	}
	client.unknownNext = true
	if _, err = authority.PublishGroupTransition(ctx, lease, intent, TransitionPhasePreRemove, next, [32]byte{}); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("grant born at %d, move admitted at %d: %v", grant.CatalogGeneration, intent.SourceHeadGeneration, err)
	}
	client.holdUnknown = false
	if err = authority.RetryPending(ctx); err != nil {
		t.Fatal(err)
	}
	cold := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(nil), 0x97)
	if _, err = cold.Read(ctx); err != nil {
		t.Fatal(err)
	}
	receipt, err := cold.PublishGroupTransition(ctx, lease, intent, TransitionPhasePreRemove, next, [32]byte{})
	if err != nil || receipt.CommittedHeadGeneration != next.Generation() {
		t.Fatalf("cold lost-reply recovery: receipt=%+v err=%v", receipt, err)
	}
	if got, found, err := cold.ReadMembershipGrant(ctx, grant.Group); err != nil || !found || got != grant {
		t.Fatalf("recovered move lost its exact grant: found=%t err=%v", found, err)
	}
}

func TestMoveGrantRetainsGroupFencesAcrossUnrelatedHead(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*ReplicatedShardDescriptor)
	}{
		{"source-store", func(source *ReplicatedShardDescriptor) { source.Replicas[0].StoreID[0]++ }},
		{"source-incarnation", func(source *ReplicatedShardDescriptor) { source.Replicas[0].NodeIncarnation++ }},
		{"target-store", func(source *ReplicatedShardDescriptor) { source.EnrolledTarget.StoreID[0]++ }},
		{"target-endpoint", func(source *ReplicatedShardDescriptor) { source.EnrolledTarget.ControlEndpoint = "wrong-control" }},
		{"replica-set", func(source *ReplicatedShardDescriptor) { source.Command.ReplicaSetVersion++ }},
		{"schema", func(source *ReplicatedShardDescriptor) { source.Command.SchemaGeneration++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			authority, _, current := newCatalogAuthorityFixtureWithDescriptor(t, func(source *ReplicatedShardDescriptor) {
				source.LogicalSchemaDigest = [32]byte{0x93}
				testReplicatedCatalogEnrollTarget(source)
			})
			grant, _, _, _ := testCertifiedReplicaReplacement(t, current, current.ReplicatedShardDescriptors()[0])
			if err := authority.PublishMembershipGrant(t.Context(), grant); err != nil {
				t.Fatal(err)
			}
			current = advanceUnrelatedGroupTestHead(t, authority, current)
			source := current.ReplicatedShardDescriptors()[0]
			test.change(&source)
			intent := testGroupTransitionIntent(t, current, source, *source.EnrolledTarget, grant.SourceMember)
			if _, err := authority.AcquireDistributionTransition(t.Context(), intent.Key); err != nil {
				t.Fatal(err)
			}
			if _, found, err := authority.ReadMembershipGrant(t.Context(), grant.Group); err == nil || found {
				t.Fatalf("changed source identity authorized old grant: found=%t err=%v", found, err)
			}
		})
	}
}

func TestOwnedGroupPublicationSurvivesUnrelatedCatalogHead(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixtureWithDescriptor(t, func(source *ReplicatedShardDescriptor) {
		source.LogicalSchemaDigest = [32]byte{0x93}
		testReplicatedCatalogEnrollTarget(source)
	})
	lagging := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x95)
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
	refreshed, err := lagging.Read(ctx)
	if err != nil || refreshed == nil || refreshed.Generation() != current.Generation() {
		t.Fatalf("warm reader cannot recover interleaved committed heads: snapshot=%v err=%v", refreshed, err)
	}
	cold := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(nil), 0x96)
	loaded, err := cold.Read(ctx)
	equal, compareErr := equalCatalogSnapshots(refreshed, loaded)
	if err != nil || compareErr != nil || !equal {
		t.Fatalf("warm and cold committed readers disagree: read=%v compare=%v", err, compareErr)
	}
}
