package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type catalogAuthorityClient struct {
	mu                sync.Mutex
	state             shardservice.ReplicatedMemberState
	rows              map[string][]byte
	applied           uint64
	unknownNext       bool
	holdUnknown       bool
	unknownCommand    []byte
	unknownCompletion []byte
	unknownState      shardservice.ReplicatedMemberState
	wantAuthority     serviceauthz.Authority
	readMaximums      []uint32
	onRead            func([]byte)
}

func TestReplicatedMembershipGrantCatalogWitnessClosesStaleInterleaving(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	grant := testReplicatedMembershipGrant(authority.route.Group)
	grant.InitialRosterDigest = replicatedCatalogInitialRosterDigest(current, 0)
	grant.InitialDescriptorDigest = replicatedCatalogInitialDescriptorDigest(current, 0)
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	next, err := NewSnapshotWithReplicatedMetadata(config, endpoints, current.Generation()+1,
		nil, nil, []ReplicatedShardDescriptor{descriptor})
	if err != nil {
		t.Fatal(err)
	}
	secondSession, err := NewNativeSession(NativeSessionOptions{
		Executor: authority.executor, Route: authority.route,
		Distribution: string(ReplicatedCatalogDistribution), Shard: string(ReplicatedCatalogShard),
		Tenant: []byte("control-plane"), ClientID: replication.ID128{0x72},
		Resolver:           BaseRelationResolver{Relation: authority.relation},
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 4,
		InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	authorized, err := serviceauthz.WithAuthority(context.Background(), authority.authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = secondSession.Open(authorized, 1<<50); err != nil {
		t.Fatal(err)
	}
	second, err := NewReplicatedCatalogAuthority(ReplicatedCatalogAuthorityOptions{
		Executor: authority.executor, Route: authority.route, Relation: authority.relation,
		Holder: NewCatalogHolder(current), Session: secondSession, Authority: authority.authority,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, pageKey := replicatedMembershipGrantKeys(grant.Group)
	client.onRead = func(key []byte) {
		if !bytes.Equal(key, pageKey[:]) {
			return
		}
		client.onRead = nil
		if publishErr := second.Publish(context.Background(), current.Generation(), next); publishErr != nil {
			t.Fatalf("interleaved catalog publication=%v", publishErr)
		}
	}
	if err = authority.PublishMembershipGrant(context.Background(), grant); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale interleaved grant=%v", err)
	}
	recordKey, _ := replicatedMembershipGrantKeys(grant.Group)
	if _, found := client.rows[string(recordKey[:])]; found {
		t.Fatal("stale witness conflict partially installed a grant record")
	}
}

func TestPublishReplicaReplacementAtomicallySettlesCatalogGrantAndPage(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	observer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x80)
	_, _, descriptor := testReplicatedCatalogInput(t)
	grant, manifest, target, command := testCertifiedReplicaReplacement(t, current, descriptor)
	if err := authority.PublishMembershipGrant(context.Background(), grant); err != nil {
		t.Fatal(err)
	}
	next, err := BuildReplicaReplacementTransition(
		current, manifest, current.Generation()+1, grant, target, command,
	)
	if err != nil {
		t.Fatal(err)
	}
	recordKey, pageKey := replicatedMembershipGrantKeys(grant.Group)
	if _, found := client.rows[string(recordKey[:])]; !found {
		t.Fatal("grant record was not installed")
	}
	if _, found := client.rows[string(pageKey[:])]; !found {
		t.Fatal("grant occupancy page was not installed")
	}
	other := grant.Group
	foundCollision := false
	for candidate := uint64(1); candidate < 1<<20; candidate++ {
		other.GroupID[8] = byte(candidate)
		other.GroupID[9] = byte(candidate >> 8)
		other.GroupID[10] = byte(candidate >> 16)
		_, otherPage := replicatedMembershipGrantKeys(other)
		if other != grant.Group && otherPage == pageKey {
			foundCollision = true
			break
		}
	}
	if !foundCollision {
		t.Fatal("could not construct a deterministic occupancy-page collision")
	}
	groups := []raftmember.GroupKey{grant.Group, other}
	sort.Slice(groups, func(left, right int) bool {
		return compareMembershipGrantGroup(groups[left], groups[right]) < 0
	})
	pageBytes, err := appendReplicatedMembershipGrantPage(nil, pageKey[1], groups)
	if err != nil {
		t.Fatal(err)
	}
	client.rows[string(pageKey[:])] = pageBytes

	client.unknownNext = true
	err = authority.PublishReplicaReplacement(
		context.Background(), current.Generation(), next, grant,
	)
	if !errors.Is(err, ErrReplicatedCatalogPending) || !authority.session.Status().Pending {
		t.Fatalf("unknown final publication err=%v pending=%v", err, authority.session.Status().Pending)
	}
	if refreshed, refreshErr := observer.Read(context.Background()); refreshErr != nil ||
		refreshed.Generation() != next.Generation() {
		t.Fatalf("observer refresh across unknown response=%v err=%v", refreshed, refreshErr)
	}
	retained := authority.session.PendingCommand()
	client.holdUnknown = false
	if err = authority.RetryPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(retained, client.unknownCommand) {
		t.Fatal("final replacement retry changed its exact command bytes")
	}
	if authority.holder.Current().Generation() != next.Generation() {
		t.Fatalf("published generation=%d want=%d",
			authority.holder.Current().Generation(), next.Generation())
	}
	retainedGrant, found := client.rows[string(recordKey[:])]
	if !found {
		t.Fatal("catalog publication revoked source-removal authority")
	}
	openedGrant, grantErr := openReplicatedMembershipGrant(retainedGrant)
	if grantErr != nil || openedGrant != grant {
		t.Fatalf("retained grant=%+v err=%v", openedGrant, grantErr)
	}
	receiptKey, receiptPageKey := replicatedReplicaReplacementReceiptKeys(grant.Group)
	receiptRaw, found := client.rows[string(receiptKey[:])]
	if !found {
		t.Fatal("completed replacement did not retain its distinct receipt")
	}
	receipt, receiptErr := openReplicaReplacementReceipt(receiptRaw)
	if receiptErr != nil || receipt.Grant != grant ||
		receipt.OldGeneration != current.Generation() ||
		receipt.NewGeneration != next.Generation() {
		t.Fatalf("replacement receipt=%+v err=%v", receipt, receiptErr)
	}
	remainingPage, found := client.rows[string(pageKey[:])]
	if !found {
		t.Fatal("shared grant occupancy page was deleted")
	}
	remaining, err := openReplicatedMembershipGrantPage(pageKey[1], remainingPage)
	if err != nil || len(remaining) != 2 || remaining[0] != groups[0] ||
		remaining[1] != groups[1] {
		t.Fatalf("remaining occupancy groups=%+v err=%v", remaining, err)
	}
	receiptPageRaw, found := client.rows[string(receiptPageKey[:])]
	if !found {
		t.Fatal("replacement receipt occupancy page is missing")
	}
	receiptGroups, err := openReplicaReplacementReceiptPage(
		receiptPageKey[1], receiptPageRaw,
	)
	if err != nil || len(receiptGroups) != 1 || receiptGroups[0] != grant.Group {
		t.Fatalf("receipt occupancy=%+v err=%v", receiptGroups, err)
	}
	if grantResult, found, readErr := authority.ReadMembershipGrant(
		context.Background(), grant.Group,
	); readErr != nil || !found || grantResult != grant {
		t.Fatalf("grant before source removal=%+v found=%v err=%v", grantResult, found, readErr)
	}

	publishedVersion, ok := replicaSetVersionForGroup(next, grant.Group)
	if !ok {
		t.Fatal("published replica-set fence is missing")
	}
	postRemoveVersion := publishedVersion + 2
	postRemove, err := BuildReplicaReplacementPostRemoveTransition(
		next, next.Generation()+1, grant, postRemoveVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	client.unknownNext = true
	err = authority.PublishReplicaReplacementPostRemove(
		context.Background(), next.Generation(), postRemove, grant, postRemoveVersion,
	)
	if !errors.Is(err, ErrReplicatedCatalogPending) || !authority.session.Status().Pending {
		t.Fatalf("unknown post-remove publication err=%v pending=%v", err, authority.session.Status().Pending)
	}
	if refreshed, refreshErr := observer.Read(context.Background()); refreshErr != nil ||
		refreshed.Generation() != postRemove.Generation() {
		t.Fatalf("observer post-remove refresh=%v err=%v", refreshed, refreshErr)
	}
	postRemoveCommand := authority.session.PendingCommand()
	client.holdUnknown = false
	if err = authority.RetryPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(postRemoveCommand, client.unknownCommand) {
		t.Fatal("post-remove retry changed its exact command bytes")
	}
	if grantResult, found, readErr := authority.ReadMembershipGrant(
		context.Background(), grant.Group,
	); readErr != nil || !found || grantResult != grant {
		t.Fatalf("grant at post-remove fence=%+v found=%v err=%v", grantResult, found, readErr)
	}
	receipt, receiptErr = openReplicaReplacementReceipt(client.rows[string(receiptKey[:])])
	if receiptErr != nil || receipt.PostRemoveGeneration != postRemove.Generation() ||
		receipt.PostRemoveReplicaSetVersion != postRemoveVersion {
		t.Fatalf("post-remove receipt=%+v err=%v", receipt, receiptErr)
	}

	client.unknownNext = true
	err = authority.FinalizeReplicaReplacement(context.Background(), grant)
	if !errors.Is(err, ErrReplicatedCatalogPending) || !authority.session.Status().Pending {
		t.Fatalf("unknown finalization err=%v pending=%v", err, authority.session.Status().Pending)
	}
	finalizeCommand := authority.session.PendingCommand()
	client.holdUnknown = false
	if err = authority.RetryPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(finalizeCommand, client.unknownCommand) {
		t.Fatal("replacement finalization retry changed exact command bytes")
	}
	if _, found = client.rows[string(recordKey[:])]; found {
		t.Fatal("finalized replacement retained active grant")
	}
	remainingPage, found = client.rows[string(pageKey[:])]
	if !found {
		t.Fatal("finalization deleted shared active-grant page")
	}
	remaining, err = openReplicatedMembershipGrantPage(pageKey[1], remainingPage)
	if err != nil || len(remaining) != 1 || remaining[0] != other {
		t.Fatalf("final active occupancy=%+v err=%v", remaining, err)
	}
	if _, found = client.rows[string(receiptKey[:])]; !found {
		t.Fatal("finalization deleted replacement receipt")
	}
	if grantResult, found, readErr := authority.ReadMembershipGrant(
		context.Background(), grant.Group,
	); readErr != nil || found || grantResult != (membershipgrant.Grant{}) {
		t.Fatalf("grant after finalization=%+v found=%v err=%v", grantResult, found, readErr)
	}

	// A later move of the same group reuses its one receipt row and occupancy
	// slot; it neither grows the directory nor trusts the prior certificate for
	// the new transition.
	receiptPageBefore := append([]byte(nil), client.rows[string(receiptPageKey[:])]...)
	secondDescriptor := postRemove.replicatedDescriptors()[0]
	secondGrant := testReplicatedMembershipGrant(secondDescriptor.Group)
	secondGrant.TransitionID[0]++
	secondGrant.MetadataEpoch++
	secondGrant.CatalogGeneration = postRemove.Generation()
	secondGrant.InitialReplicaSetVersion = secondDescriptor.Command.ReplicaSetVersion
	secondGrant.InitialVoters = [3]uint64{2, 3, 4}
	secondGrant.InitialRosterDigest = replicatedCatalogInitialRosterDigest(postRemove, 0)
	secondGrant.InitialDescriptorDigest = replicatedCatalogInitialDescriptorDigest(postRemove, 0)
	secondGrant.SourceMember = 2
	secondGrant.TargetMember = 5
	secondGrant.TargetNode = [16]byte{5}
	currentManifest, ok := postRemove.Manifest(secondDescriptor.Distribution)
	if !ok {
		t.Fatal("second replacement manifest missing")
	}
	shardOrdinal, metadata := manifestShardOrdinal(currentManifest, secondDescriptor.Shard)
	secondManifest, err := currentManifest.ReplaceShardLeader(
		shardOrdinal, currentManifest.Version()+1, 1, "ep-a", metadata.Epoch+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondTarget := ReplicatedReplicaDescriptor{
		Member: 5, Node: [16]byte{5}, StoreID: [16]byte{15}, NodeIncarnation: 25,
		Endpoint: "ep-a", NativeEndpoint: "ep-a-native", ControlEndpoint: "ep-a-control",
	}
	secondCommand := secondDescriptor.Command
	secondCommand.ReplicaSetVersion += 3
	secondCommand.OwnershipEpoch++
	secondCommand.RoutingVersion++
	secondCommand.RouteGeneration++
	secondNext, err := BuildReplicaReplacementTransition(
		postRemove, secondManifest, postRemove.Generation()+1, secondGrant, secondTarget, secondCommand,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.PublishMembershipGrant(context.Background(), secondGrant); err != nil {
		t.Fatal(err)
	}
	if err = authority.PublishReplicaReplacement(
		context.Background(), postRemove.Generation(), secondNext, secondGrant,
	); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(receiptPageBefore, client.rows[string(receiptPageKey[:])]) {
		t.Fatal("repeated replacement rewrote stable receipt occupancy")
	}
	secondReceipt, err := openReplicaReplacementReceipt(client.rows[string(receiptKey[:])])
	if err != nil || secondReceipt.Grant != secondGrant ||
		secondReceipt.NewGeneration != secondNext.Generation() {
		t.Fatalf("second receipt=%+v err=%v", secondReceipt, err)
	}
}

func TestReplicaReplacementReceiptLetsConcurrentStaleGatewaysRefresh(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	first := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x81)
	second := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x82)
	skipper := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x84)
	_, _, descriptor := testReplicatedCatalogInput(t)
	grant, manifest, target, command := testCertifiedReplicaReplacement(t, current, descriptor)
	if err := authority.PublishMembershipGrant(context.Background(), grant); err != nil {
		t.Fatal(err)
	}
	next, err := BuildReplicaReplacementTransition(
		current, manifest, current.Generation()+1, grant, target, command,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.PublishReplicaReplacement(
		context.Background(), current.Generation(), next, grant,
	); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(2)
	errorsByGateway := make([]error, 2)
	for index, peer := range []*ReplicatedCatalogAuthority{first, second} {
		go func(index int, peer *ReplicatedCatalogAuthority) {
			defer wait.Done()
			<-start
			refreshed, refreshErr := peer.Read(context.Background())
			if refreshErr == nil && refreshed.Generation() != next.Generation() {
				refreshErr = ErrReplicatedCatalogConflict
			}
			errorsByGateway[index] = refreshErr
		}(index, peer)
	}
	close(start)
	wait.Wait()
	for index, refreshErr := range errorsByGateway {
		if refreshErr != nil {
			t.Fatalf("gateway %d refresh=%v", index, refreshErr)
		}
	}
	version, ok := replicaSetVersionForGroup(next, grant.Group)
	if !ok {
		t.Fatal("replacement fence is missing")
	}
	post, err := BuildReplicaReplacementPostRemoveTransition(
		next, next.Generation()+1, grant, version+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.PublishReplicaReplacementPostRemove(
		context.Background(), next.Generation(), post, grant, version+2,
	); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("mismatched observed replica-set fence=%v", err)
	}
	if err = authority.PublishReplicaReplacementPostRemove(
		context.Background(), next.Generation(), post, grant, version+1,
	); err != nil {
		t.Fatal(err)
	}
	start = make(chan struct{})
	wait.Add(2)
	for index, peer := range []*ReplicatedCatalogAuthority{first, second} {
		go func(index int, peer *ReplicatedCatalogAuthority) {
			defer wait.Done()
			<-start
			refreshed, refreshErr := peer.Read(context.Background())
			if refreshErr == nil && refreshed.Generation() != post.Generation() {
				refreshErr = ErrReplicatedCatalogConflict
			}
			errorsByGateway[index] = refreshErr
		}(index, peer)
	}
	close(start)
	wait.Wait()
	for index, refreshErr := range errorsByGateway {
		if refreshErr != nil {
			t.Fatalf("gateway %d post-remove refresh=%v", index, refreshErr)
		}
	}
	if _, err = skipper.Read(context.Background()); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("gateway skipped two certified cuts: %v", err)
	}
	if skipper.holder.Current().Generation() != current.Generation() {
		t.Fatal("skipped refresh advanced stale holder")
	}
}

func TestReplicaReplacementPostRemoveRejectsFenceDriftAndStaleObservation(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	_, _, descriptor := testReplicatedCatalogInput(t)
	grant, manifest, target, command := testCertifiedReplicaReplacement(t, current, descriptor)
	if err := authority.PublishMembershipGrant(context.Background(), grant); err != nil {
		t.Fatal(err)
	}
	next, err := BuildReplicaReplacementTransition(
		current, manifest, current.Generation()+1, grant, target, command,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.PublishReplicaReplacement(
		context.Background(), current.Generation(), next, grant,
	); err != nil {
		t.Fatal(err)
	}
	version, ok := replicaSetVersionForGroup(next, grant.Group)
	if !ok {
		t.Fatal("replacement fence is missing")
	}
	if _, err = BuildReplicaReplacementPostRemoveTransition(
		next, next.Generation()+1, grant, version,
	); err == nil {
		t.Fatal("stale replica-set observation was accepted")
	}
	post, err := BuildReplicaReplacementPostRemoveTransition(
		next, next.Generation()+1, grant, version+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	postRaw, err := appendReplicatedCatalogDocument(nil, post, maxReplicatedCatalogBytes)
	if err != nil {
		t.Fatal(err)
	}
	postPayload, err := openTypedControlPlaneDocument(
		postRaw, replicatedCatalogHeadDocumentID[:], maxReplicatedCatalogBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	drifted, err := OpenSnapshotDocument(postPayload)
	if err != nil {
		t.Fatal(err)
	}
	drifted.replicatedShards[0].command.OwnershipEpoch++
	if err = authority.PublishReplicaReplacementPostRemove(
		context.Background(), next.Generation(), drifted, grant, version+1,
	); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("post-remove command-fence drift=%v", err)
	}
	if authority.holder.Current().Generation() != next.Generation() {
		t.Fatal("rejected post-remove transition advanced holder")
	}
}

func TestReplicaReplacementRefreshFailsClosedWithoutCanonicalReceipt(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(map[string][]byte, [33]byte)
	}{
		{name: "missing", mutate: func(rows map[string][]byte, key [33]byte) {
			delete(rows, string(key[:]))
		}},
		{name: "corrupt", mutate: func(rows map[string][]byte, key [33]byte) {
			raw := append([]byte(nil), rows[string(key[:])]...)
			raw[len(raw)-2] ^= 1
			rows[string(key[:])] = raw
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			authority, client, current := newCatalogAuthorityFixture(t)
			peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x83)
			_, _, descriptor := testReplicatedCatalogInput(t)
			grant, manifest, target, command := testCertifiedReplicaReplacement(t, current, descriptor)
			if err := authority.PublishMembershipGrant(context.Background(), grant); err != nil {
				t.Fatal(err)
			}
			next, err := BuildReplicaReplacementTransition(
				current, manifest, current.Generation()+1, grant, target, command,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err = authority.PublishReplicaReplacement(
				context.Background(), current.Generation(), next, grant,
			); err != nil {
				t.Fatal(err)
			}
			recordKey, _ := replicatedReplicaReplacementReceiptKeys(grant.Group)
			testCase.mutate(client.rows, recordKey)
			if _, err = peer.Read(context.Background()); !errors.Is(err, ErrReplicatedCatalogConflict) {
				t.Fatalf("refresh without exact receipt=%v", err)
			}
			if peer.holder.Current().Generation() != current.Generation() {
				t.Fatal("failed receipt validation advanced stale holder")
			}
		})
	}
}

func TestReplicaReplacementPostRemoveRefreshRequiresCanonicalReceipt(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(map[string][]byte, [33]byte)
	}{
		{name: "missing", mutate: func(rows map[string][]byte, key [33]byte) {
			delete(rows, string(key[:]))
		}},
		{name: "corrupt", mutate: func(rows map[string][]byte, key [33]byte) {
			raw := append([]byte(nil), rows[string(key[:])]...)
			raw[len(raw)-2] ^= 1
			rows[string(key[:])] = raw
		}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			authority, client, current := newCatalogAuthorityFixture(t)
			peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0x85)
			_, _, descriptor := testReplicatedCatalogInput(t)
			grant, manifest, target, command := testCertifiedReplicaReplacement(t, current, descriptor)
			if err := authority.PublishMembershipGrant(context.Background(), grant); err != nil {
				t.Fatal(err)
			}
			next, err := BuildReplicaReplacementTransition(
				current, manifest, current.Generation()+1, grant, target, command,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err = authority.PublishReplicaReplacement(
				context.Background(), current.Generation(), next, grant,
			); err != nil {
				t.Fatal(err)
			}
			if _, err = peer.Read(context.Background()); err != nil {
				t.Fatal(err)
			}
			version, ok := replicaSetVersionForGroup(next, grant.Group)
			if !ok {
				t.Fatal("replacement fence is missing")
			}
			post, err := BuildReplicaReplacementPostRemoveTransition(
				next, next.Generation()+1, grant, version+1,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err = authority.PublishReplicaReplacementPostRemove(
				context.Background(), next.Generation(), post, grant, version+1,
			); err != nil {
				t.Fatal(err)
			}
			receiptKey, _ := replicatedReplicaReplacementReceiptKeys(grant.Group)
			testCase.mutate(client.rows, receiptKey)
			if _, err = peer.Read(context.Background()); !errors.Is(err, ErrReplicatedCatalogConflict) {
				t.Fatalf("post-remove refresh without exact receipt=%v", err)
			}
			if peer.holder.Current().Generation() != next.Generation() {
				t.Fatal("failed post-remove receipt validation advanced stale holder")
			}
		})
	}
}

func TestReplicaReplacementReceiptIsCanonicalAndBounded(t *testing.T) {
	if maxReplicatedMembershipLifecycleBytes != 28<<20 {
		t.Fatal("membership lifecycle retention bound drifted")
	}
	authority, client, current := newCatalogAuthorityFixture(t)
	_, _, descriptor := testReplicatedCatalogInput(t)
	grant, manifest, target, command := testCertifiedReplicaReplacement(t, current, descriptor)
	if err := authority.PublishMembershipGrant(context.Background(), grant); err != nil {
		t.Fatal(err)
	}
	next, err := BuildReplicaReplacementTransition(
		current, manifest, current.Generation()+1, grant, target, command,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.PublishReplicaReplacement(
		context.Background(), current.Generation(), next, grant,
	); err != nil {
		t.Fatal(err)
	}
	key, _ := replicatedReplicaReplacementReceiptKeys(grant.Group)
	raw := client.rows[string(key[:])]
	receipt, err := openReplicaReplacementReceipt(raw)
	if err != nil || len(raw) == 0 || len(raw) > maxReplicatedReplicaReplacementReceiptBytes {
		t.Fatalf("receipt bytes=%d value=%+v err=%v", len(raw), receipt, err)
	}
	canonical, err := appendReplicaReplacementReceiptRecord(nil, receipt)
	if err != nil || !bytes.Equal(raw, canonical) {
		t.Fatalf("receipt is not canonical: err=%v", err)
	}
	if _, err = openReplicaReplacementReceipt(append(append([]byte(nil), raw...), ' ')); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("trailing receipt=%v", err)
	}
	version, ok := replicaSetVersionForGroup(next, grant.Group)
	if !ok {
		t.Fatal("replacement fence is missing")
	}
	post, err := BuildReplicaReplacementPostRemoveTransition(
		next, next.Generation()+1, grant, version+1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.PublishReplicaReplacementPostRemove(
		context.Background(), next.Generation(), post, grant, version+1,
	); err != nil {
		t.Fatal(err)
	}
	raw = client.rows[string(key[:])]
	receipt, err = openReplicaReplacementReceipt(raw)
	if err != nil || len(raw) > maxReplicatedReplicaReplacementReceiptBytes ||
		receipt.PostRemoveGeneration != post.Generation() {
		t.Fatalf("extended receipt bytes=%d value=%+v err=%v", len(raw), receipt, err)
	}
	canonical, err = appendReplicaReplacementReceiptRecord(nil, receipt)
	if err != nil || !bytes.Equal(raw, canonical) {
		t.Fatalf("extended receipt is not canonical: err=%v", err)
	}
}

func TestReplicatedCatalogHeadWitnessCanonicalAndFailClosed(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	head := client.rows[string(replicatedCatalogHeadKey)]
	witness := client.rows[string(replicatedCatalogHeadWitnessKey)]
	canonical, err := appendReplicatedCatalogHeadWitness(nil, current.Generation(), head)
	if err != nil || !bytes.Equal(canonical, witness) {
		t.Fatalf("canonical witness=%x err=%v", canonical, err)
	}
	if err = validateReplicatedCatalogHeadWitness(append(append([]byte(nil), witness...), ' '),
		current.Generation(), head); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("trailing witness=%v", err)
	}
	client.rows[string(replicatedCatalogHeadWitnessKey)] = append([]byte(nil), witness...)
	client.rows[string(replicatedCatalogHeadWitnessKey)][len(witness)-2] ^= 1
	if _, err = authority.Read(context.Background()); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("corrupt witness read=%v", err)
	}
	delete(client.rows, string(replicatedCatalogHeadWitnessKey))
	if _, err = authority.Read(context.Background()); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("missing witness read=%v", err)
	}
}

func TestReplicatedMembershipGrantCanonicalCASUnknownRetryAndRevoke(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	grant := testReplicatedMembershipGrant(authority.route.Group)
	grant.InitialRosterDigest = replicatedCatalogInitialRosterDigest(current, 0)
	grant.InitialDescriptorDigest = replicatedCatalogInitialDescriptorDigest(current, 0)
	raw, err := appendReplicatedMembershipGrant(nil, grant)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openReplicatedMembershipGrant(raw)
	if err != nil || opened != grant {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	again, err := appendReplicatedMembershipGrant(nil, opened)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("membership grant encoding is not unique")
	}
	if _, err = openReplicatedMembershipGrant(append(append([]byte(nil), raw...), ' ')); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("trailing grant bytes=%v", err)
	}
	staleCatalog := grant
	staleCatalog.CatalogGeneration--
	if err = authority.PublishMembershipGrant(context.Background(), staleCatalog); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale catalog grant=%v", err)
	}
	foreignGroup := grant
	foreignGroup.Group.GroupID[0] ^= 0xff
	if err = authority.PublishMembershipGrant(context.Background(), foreignGroup); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("foreign group grant=%v", err)
	}

	client.unknownNext = true
	err = authority.PublishMembershipGrant(context.Background(), grant)
	if !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("unknown grant install=%v", err)
	}
	pending := authority.session.PendingCommand()
	client.holdUnknown = false
	if err = authority.RetryPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pending, client.unknownCommand) {
		t.Fatal("grant install retry changed command bytes")
	}
	loaded, found, err := authority.ReadMembershipGrant(context.Background(), grant.Group)
	if err != nil || !found || loaded != grant {
		t.Fatalf("loaded=%+v found=%t err=%v", loaded, found, err)
	}
	if err = authority.PublishMembershipGrant(context.Background(), grant); err != nil {
		t.Fatalf("same grant refresh=%v", err)
	}
	recordKey, _ := replicatedMembershipGrantKeys(grant.Group)
	staleRetained := grant
	staleRetained.CatalogGeneration--
	staleRaw, encodeErr := appendReplicatedMembershipGrant(nil, staleRetained)
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	client.rows[string(recordKey[:])] = staleRaw
	if _, _, err = authority.ReadMembershipGrant(context.Background(), grant.Group); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale retained grant read=%v", err)
	}
	client.rows[string(recordKey[:])] = raw
	foreign := grant
	foreign.MetadataEpoch++
	if err = authority.PublishMembershipGrant(context.Background(), foreign); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("foreign live grant=%v", err)
	}
	if err = authority.RevokeMembershipGrant(context.Background(), foreign); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("foreign revoke=%v", err)
	}
	client.unknownNext = true
	if err = authority.RevokeMembershipGrant(context.Background(), grant); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("unproved durable revoke=%v", err)
	}
	if authority.session.Status().Pending || client.unknownNext == false {
		t.Fatal("fail-closed revoke proposed a command")
	}
	if loaded, found, err = authority.ReadMembershipGrant(context.Background(), grant.Group); err != nil || !found || loaded != grant {
		t.Fatalf("retained after failed revoke=%+v found=%t err=%v", loaded, found, err)
	}
}

func TestReplicatedMembershipGrantPageBoundOrderAndCanonicality(t *testing.T) {
	group := raftmember.GroupKey{TopologyRecoveryEpoch: 3}
	group.ClusterID[0], group.ClusterIncarnation[0] = 1, 2
	group.ShardIncarnation[0], group.GroupID[0] = 4, 5
	base := testReplicatedMembershipGrant(group)
	const pageIndex = byte(0)
	groups := make([]raftmember.GroupKey, 0, maxReplicatedMembershipGrantsPerPage+1)
	for candidate := 1; len(groups) <= maxReplicatedMembershipGrantsPerPage; candidate++ {
		group := base.Group
		group.GroupID = [16]byte{byte(candidate >> 8), byte(candidate)}
		_, pageKey := replicatedMembershipGrantKeys(group)
		if pageKey[1] == pageIndex {
			groups = append(groups, group)
		}
	}
	sort.Slice(groups, func(left, right int) bool {
		return compareMembershipGrantGroup(groups[left], groups[right]) < 0
	})
	tooMany := append([]raftmember.GroupKey(nil), groups...)
	groups = groups[:maxReplicatedMembershipGrantsPerPage]
	raw, err := appendReplicatedMembershipGrantPage(nil, pageIndex, groups)
	if err != nil || len(raw) == 0 || len(raw) > maxReplicatedMembershipGrantPageBytes {
		t.Fatalf("bounded page bytes=%d err=%v", len(raw), err)
	}
	opened, err := openReplicatedMembershipGrantPage(pageIndex, raw)
	if err != nil || len(opened) != len(groups) {
		t.Fatalf("open count=%d err=%v", len(opened), err)
	}
	for index := range groups {
		if opened[index] != groups[index] {
			t.Fatalf("grant %d changed", index)
		}
		foundAt, found := findReplicatedMembershipGrantGroup(opened, groups[index])
		if !found || foundAt != index {
			t.Fatalf("lookup %d=(%d,%t)", index, foundAt, found)
		}
	}
	canonical, err := appendReplicatedMembershipGrantPage(nil, pageIndex, opened)
	if err != nil || !bytes.Equal(raw, canonical) {
		t.Fatal("directory encoding is not byte-unique")
	}
	if _, err = openReplicatedMembershipGrantPage(pageIndex,
		append(append([]byte(nil), raw...), ' ')); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("trailing directory bytes=%v", err)
	}
	if _, err = appendReplicatedMembershipGrantPage(nil, pageIndex, tooMany); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("65 grants=%v", err)
	}
	if _, err = appendReplicatedMembershipGrantPage(nil, pageIndex+1, groups); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("wrong hash page=%v", err)
	}
	duplicate := append([]raftmember.GroupKey(nil), groups...)
	duplicate[1] = duplicate[0]
	if _, err = appendReplicatedMembershipGrantPage(nil, pageIndex, duplicate); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("duplicate group=%v", err)
	}
	reordered := append([]raftmember.GroupKey(nil), groups...)
	reordered[0], reordered[1] = reordered[1], reordered[0]
	if _, err = appendReplicatedMembershipGrantPage(nil, pageIndex, reordered); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("reordered groups=%v", err)
	}
}

func testReplicatedMembershipGrant(group raftmember.GroupKey) membershipgrant.Grant {
	return membershipgrant.Grant{
		Group: group, TransitionID: [16]byte{0x91}, MetadataEpoch: 7,
		CatalogGeneration: 5, InitialReplicaSetVersion: 1,
		InitialVoters: [3]uint64{1, 2, 3}, InitialRosterDigest: [32]byte{1},
		InitialDescriptorDigest: [32]byte{2}, SourceMember: 1, TargetMember: 4,
		TargetNode: [16]byte{4},
	}
}

func (client *catalogAuthorityClient) DoReplicated(
	_ context.Context, _ ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Authority != client.wantAuthority ||
		request.Capability != serviceauthz.CapabilityTopology {
		return nil, errors.New("catalog request escaped exact topology authority")
	}
	if request.Operation == shardservice.ReplicatedProbe {
		client.mu.Lock()
		state := client.state
		client.mu.Unlock()
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	if request.Operation == shardservice.ReplicatedReadLeader ||
		request.Operation == shardservice.ReplicatedReadFollower {
		client.mu.Lock()
		client.readMaximums = append(client.readMaximums, request.MaxValueBytes)
		onRead := client.onRead
		client.mu.Unlock()
		if onRead != nil {
			onRead(request.Key)
		}
		client.mu.Lock()
		value, found := client.rows[string(request.Key)]
		value = append([]byte(nil), value...)
		state := client.state
		client.mu.Unlock()
		kind := shardservice.ReplicatedReadMissing
		if found {
			kind = shardservice.ReplicatedReadFound
		}
		return &shardservice.ReplicatedResponse{
			Kind: kind, HasState: true, State: state,
			ReadApplied: state.Applied, Value: value,
		}, nil
	}
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		return nil, err
	}
	if command.AuthorityClass != replication.CommandAuthorityTopology {
		return nil, errors.New("catalog command lost authenticated topology identity")
	}
	if bytes.Equal(request.Command, client.unknownCommand) && len(client.unknownCompletion) != 0 {
		if client.holdUnknown {
			return nil, errors.New("replicated outcome remains unknown")
		}
		return catalogCompletionResponse(
			client.unknownState, client.unknownCompletion, request.Command,
		), nil
	}
	client.applied++
	applied := uint64(100) + client.applied
	resultCode := uint32(replicatedstate.ResultApplied)
	clientEpoch := command.ClientEpoch
	if command.Kind() == replication.CommandSessionOpen {
		resultCode = replicatedstate.ResultSessionOpened
		clientEpoch = applied
	} else if command.Kind() == replication.CommandMutationBatch {
		resultCode = client.apply(command)
	}
	completion, err := appendNativeSessionCompletion(nil, command, clientEpoch, applied, resultCode)
	if err != nil {
		return nil, err
	}
	client.state.Commit, client.state.Applied = applied, applied
	if client.unknownNext && command.Kind() == replication.CommandMutationBatch {
		client.unknownNext = false
		client.holdUnknown = true
		client.unknownCommand = append([]byte(nil), request.Command...)
		client.unknownCompletion = append([]byte(nil), completion...)
		client.unknownState = client.state
		return nil, errors.New("connection lost after replicated apply")
	}
	return catalogCompletionResponse(client.state, completion, request.Command), nil
}

func TestReplicatedCatalogAuthorityUsesRelationReadBoundAndLogicalRowBound(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	ids, err := authority.ReadOperationIDs(context.Background())
	if err != nil || len(ids) != 0 || len(client.readMaximums) == 0 ||
		client.readMaximums[len(client.readMaximums)-1] != uint32(maxReplicatedCatalogBytes) {
		t.Fatalf("empty directory ids=%v err=%v readMaximums=%v", ids, err, client.readMaximums)
	}
	client.rows[string(replicatedOperationDirectoryKey[:])] =
		make([]byte, maxReplicatedOperationDirectoryBytes+1)
	if _, err = authority.ReadOperationIDs(context.Background()); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("oversized logical directory error=%v", err)
	}
	operationID := [32]byte{1}
	operationKey := replicatedOperationKey(operationID)
	client.rows[string(operationKey[:])] = make([]byte, MaxReplicatedOperationBytes+1)
	if _, err = authority.ReadOperation(context.Background(), operationID); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("oversized logical operation error=%v", err)
	}
	for index, maximum := range client.readMaximums {
		if maximum != uint32(maxReplicatedCatalogBytes) {
			t.Fatalf("read %d maximum=%d, want relation maximum %d",
				index, maximum, maxReplicatedCatalogBytes)
		}
	}
}

func catalogCompletionResponse(
	state shardservice.ReplicatedMemberState, completion, command []byte,
) *shardservice.ReplicatedResponse {
	view, _ := replication.OpenCompletion(completion)
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
		RequestDigest: replicatedRequestDigest(command),
		Outcome: raftserve.Outcome{
			Code: raftserve.OutcomeCompletion, AppliedIndex: view.AppliedSequence,
			CompletionAppliedSequence: view.AppliedSequence, CompletionBytes: len(completion),
		}, Completion: append([]byte(nil), completion...),
	}
}

func (client *catalogAuthorityClient) apply(command replication.CommandView) uint32 {
	relations := command.RelationBatches()
	for relations.Next() {
		mutations := relations.Batch().Mutations()
		for mutations.Next() {
			mutation := mutations.Mutation()
			key := string(mutation.Key)
			current, found := client.rows[key]
			switch mutation.Kind {
			case replication.MutationPutAbsentOrEqual:
				if found && !bytes.Equal(current, mutation.Value) {
					return replicatedstate.ResultIndexConflict
				}
				client.rows[key] = append([]byte(nil), mutation.Value...)
			case replication.MutationPutDigestEqual:
				digest := sha256.Sum256(current)
				if !found || uint64(len(current)) != mutation.ExpectedValueLength ||
					replication.Digest(digest) != mutation.ExpectedValueDigest {
					return replicatedstate.ResultIndexConflict
				}
				client.rows[key] = append([]byte(nil), mutation.Value...)
			case replication.MutationDeleteDigestEqual:
				if !found {
					continue
				}
				digest := sha256.Sum256(current)
				if uint64(len(current)) != mutation.ExpectedValueLength ||
					replication.Digest(digest) != mutation.ExpectedValueDigest {
					return replicatedstate.ResultIndexConflict
				}
				delete(client.rows, key)
			default:
				return replicatedstate.ResultInvalidDocument
			}
		}
	}
	return replicatedstate.ResultApplied
}

func newCatalogAuthorityFixture(t *testing.T) (
	*ReplicatedCatalogAuthority, *catalogAuthorityClient, *Snapshot,
) {
	t.Helper()
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	current, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 5, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	current, err = initialCatalogState(current)
	if err != nil {
		t.Fatal(err)
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	route, ok := current.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard, replicas[:0])
	if !ok {
		t.Fatal("missing control-plane route")
	}
	// This focused mock reuses the data catalog's compact RF3 identity while
	// exercising the reserved control-plane placement contract explicitly.
	route.Distribution = ReplicatedCatalogDistribution
	route.Shard = ReplicatedCatalogShard
	state := shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{
			Group: route.Group, AllocationGeneration: route.AllocationGeneration,
			MemberID: route.Replicas[0].Member, StoreID: route.Replicas[0].StoreID,
			NodeIncarnation: route.Replicas[0].NodeIncarnation, Term: 1, Command: route.Command,
		},
		LeaderID: route.Replicas[0].Member, Commit: 1, Applied: 1, CheckpointApplied: 1,
	}
	topologyAuthority := serviceauthz.Authority{Generation: 9}
	topologyAuthority.Node[0] = 0x71
	client := &catalogAuthorityClient{
		state: state, rows: make(map[string][]byte), wantAuthority: topologyAuthority,
	}
	raw, err := AppendSnapshotDocument(nil, current)
	if err != nil {
		t.Fatal(err)
	}
	raw, err = appendControlPlaneDocument(
		nil, replicatedCatalogHeadDocumentID[:], raw, maxReplicatedCatalogBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	client.rows[string(replicatedCatalogHeadKey[:])] = raw
	witness, err := appendReplicatedCatalogHeadWitness(nil, current.Generation(), raw)
	if err != nil {
		t.Fatal(err)
	}
	client.rows[string(replicatedCatalogHeadWitnessKey)] = witness
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: executor, Route: route, Distribution: string(ReplicatedCatalogDistribution),
		Shard: string(ReplicatedCatalogShard), Tenant: []byte("control-plane"),
		ClientID: replication.ID128{0x71}, Resolver: BaseRelationResolver{Relation: 1},
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 4,
		InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(context.Background(), topologyAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(ctx, 1<<50); err != nil {
		t.Fatal(err)
	}
	authority, err := NewReplicatedCatalogAuthority(ReplicatedCatalogAuthorityOptions{
		Executor: executor, Route: route, Relation: 1,
		Holder: NewCatalogHolder(current), Session: session,
		Authority: topologyAuthority,
	})
	if err != nil {
		t.Fatal(err)
	}
	return authority, client, current
}

func newCatalogAuthorityPeer(t *testing.T, source *ReplicatedCatalogAuthority,
	holder *CatalogHolder, clientByte byte) *ReplicatedCatalogAuthority {
	t.Helper()
	if source == nil || holder == nil {
		t.Fatal("invalid catalog authority peer input")
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: source.executor, Route: source.route,
		Distribution: string(ReplicatedCatalogDistribution), Shard: string(ReplicatedCatalogShard),
		Tenant: []byte("control-plane"), ClientID: replication.ID128{clientByte},
		Resolver:           BaseRelationResolver{Relation: source.relation},
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 4,
		InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(context.Background(), source.authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(ctx, 1<<50); err != nil {
		t.Fatal(err)
	}
	peer, err := NewReplicatedCatalogAuthority(ReplicatedCatalogAuthorityOptions{
		Executor: source.executor, Route: source.route, Relation: source.relation,
		Holder: holder, Session: session, Authority: source.authority,
	})
	if err != nil {
		t.Fatal(err)
	}
	return peer
}

func TestReplicatedCatalogAuthorityPublishUnknownRetryConflictAndRefresh(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	next, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	client.unknownNext = true
	err = authority.Publish(context.Background(), current.Generation(), next)
	if !errors.Is(err, ErrReplicatedCatalogPending) || !authority.session.Status().Pending {
		t.Fatalf("unknown publish err=%v pending=%v", err, authority.session.Status().Pending)
	}
	wantCommand := authority.session.PendingCommand()
	client.holdUnknown = false
	if err = authority.RetryPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantCommand, client.unknownCommand) {
		t.Fatal("outcome-unknown retry did not retain exact command bytes")
	}
	refreshed, err := authority.Refresh(context.Background(), 5)
	if err != nil || refreshed.Generation() != 6 {
		t.Fatalf("refresh=%v err=%v", refreshed, err)
	}
	if err = authority.Publish(context.Background(), 5, next); !errors.Is(err, ErrCatalogGenerationMismatch) {
		t.Fatalf("stale generation publish err=%v", err)
	}
}

func TestReplicatedCatalogAuthorityAuthorizesPruneOnlyAfterRF3PublishAndDrain(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	_, _, descriptor := testReplicatedCatalogInput(t)
	operation, certificate := [32]byte{0x51}, [32]byte{0x52}
	if grant, err := authority.AuthorizeRetainedPrune(
		context.Background(), descriptor.Distribution, operation, certificate,
	); err != nil || grant.Generation() != current.Generation() ||
		grant.Operation() != operation || grant.Certificate() != certificate {
		t.Fatalf("initial grant=%v err=%v", grant, err)
	}

	lease := authority.holder.pinCurrent()
	config, endpoints, replicated := testReplicatedCatalogInput(t)
	next, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, current.Generation()+1, nil, nil,
		[]ReplicatedShardDescriptor{replicated},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = authority.Publish(context.Background(), current.Generation(), next); err != nil {
		t.Fatal(err)
	}
	if _, err = authority.AuthorizeRetainedPrune(
		context.Background(), descriptor.Distribution, operation, certificate,
	); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("prune authorized before old-generation drain: %v", err)
	}
	lease.release()
	grant, err := authority.AuthorizeRetainedPrune(
		context.Background(), descriptor.Distribution, operation, certificate,
	)
	if err != nil || grant.Generation() != next.Generation() {
		t.Fatalf("post-drain grant=%v err=%v", grant, err)
	}
}

func TestReplicatedOperationCrashResumeCASAndTerminalGC(t *testing.T) {
	authority, _, _ := newCatalogAuthorityFixture(t)
	record := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{9}, Kind: ReplicatedOperationSplit,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: 5,
		Cursor: [8]uint64{1, 2, 3}, Proof: [32]byte{7},
	})
	if err := authority.SubmitOperation(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	// A fresh controller object reconstructs solely from the replicated record.
	loaded, err := authority.ReadOperation(context.Background(), record.ID)
	if err != nil || !loaded.Equal(record) {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	advanced := record
	advanced.State, advanced.Revision, advanced.Cursor[0] = ReplicatedOperationRunning, 2, 2
	if err = authority.PublishOperation(context.Background(), 1, advanced); err != nil {
		t.Fatal(err)
	}
	stale := advanced
	stale.Revision = 2
	if err = authority.PublishOperation(context.Background(), 1, stale); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale operation CAS err=%v", err)
	}
	complete := advanced
	complete.State, complete.Revision = ReplicatedOperationComplete, 3
	if err = authority.PublishOperation(context.Background(), 2, complete); err != nil {
		t.Fatal(err)
	}
	if err = authority.DeleteOperation(context.Background(), record.ID, 2); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale GC err=%v", err)
	}
	if err = authority.DeleteOperation(context.Background(), record.ID, 3); err != nil {
		t.Fatal(err)
	}
	if _, err = authority.ReadOperation(context.Background(), record.ID); !errors.Is(err, ErrReplicatedOperationMissing) {
		t.Fatalf("read after GC err=%v", err)
	}
}

func TestReplicatedMoveAbandonmentAtomicallyReplacesIntentOnlyAtCancellation(t *testing.T) {
	authority, _, _ := newCatalogAuthorityFixture(t)
	record := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{0x19}, Kind: ReplicatedOperationMove,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: 12,
		Cursor: [8]uint64{1}, Proof: [32]byte{7},
	})
	if err := authority.SubmitOperation(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	cancelled := record
	cancelled.State, cancelled.Revision = ReplicatedOperationCancelled, 2
	cancelled.Intent = []byte(`{"witness":"AQ=="}`)
	cancelled.IntentDigest = sha256.Sum256(cancelled.Intent)
	cancelled.Proof = cancelled.IntentDigest
	cancelled.Cursor = [8]uint64{1, 2}
	if err := authority.PublishOperation(context.Background(), 1, cancelled); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("ordinary publish replaced immutable intent: %v", err)
	}
	if err := authority.PublishReplicaMoveAbandonment(context.Background(), 1, cancelled); err != nil {
		t.Fatal(err)
	}
	loaded, err := authority.ReadOperation(context.Background(), record.ID)
	if err != nil || !loaded.Equal(cancelled) {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	if err = authority.PublishReplicaMoveAbandonment(context.Background(), 2, cancelled); !errors.Is(err, ErrReplicatedCatalog) && !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("terminal abandonment rewrite=%v", err)
	}
}

func TestReplicatedOperationSubmissionPublishesBoundedSortedDirectoryAtomically(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	second := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{9}, Kind: ReplicatedOperationSplit,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: 5,
		Cursor: [8]uint64{1}, Proof: [32]byte{7},
	})
	first := second
	first.ID = [32]byte{3}
	first.IntentDigest = sha256.Sum256(first.Intent)
	if err := authority.SubmitOperation(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	client.unknownNext = true
	if err := authority.SubmitOperation(context.Background(), first); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("unknown submit = %v", err)
	}
	pending := authority.session.PendingCommand()
	client.holdUnknown = false
	if err := authority.RetryPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(pending, client.unknownCommand) {
		t.Fatal("operation+directory retry changed command bytes")
	}
	ids, err := authority.ReadOperationIDs(context.Background())
	if err != nil || len(ids) != 2 || ids[0] != first.ID || ids[1] != second.ID {
		t.Fatalf("directory=%x err=%v", ids, err)
	}
	for _, record := range []ReplicatedOperationRecord{first, second} {
		loaded, readErr := authority.ReadOperation(context.Background(), record.ID)
		if readErr != nil || !loaded.Equal(record) {
			t.Fatalf("record %x = %+v err=%v", record.ID, loaded, readErr)
		}
	}
}

func TestReplicatedOperationUnknownPublishAndDeleteSettleExactCommand(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	record := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{0x31}, Kind: ReplicatedOperationSplit,
		State: ReplicatedOperationPlanned, Revision: 1, CatalogGeneration: 5,
		Cursor: [8]uint64{1}, Proof: [32]byte{0x41},
	})
	client.unknownNext = true
	err := authority.SubmitOperation(context.Background(), record)
	if !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("unknown operation publish = %v", err)
	}
	pending := authority.session.PendingCommand()
	client.holdUnknown = false
	if err = authority.RetryPending(context.Background()); err != nil {
		t.Fatalf("settle operation publish = %v", err)
	}
	if !bytes.Equal(pending, client.unknownCommand) {
		t.Fatal("operation publication retry changed command bytes")
	}
	complete := record
	complete.State, complete.Revision = ReplicatedOperationComplete, 2
	if err = authority.PublishOperation(context.Background(), 1, complete); err != nil {
		t.Fatal(err)
	}
	client.unknownNext = true
	err = authority.DeleteOperation(context.Background(), record.ID, complete.Revision)
	if !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("unknown operation delete = %v", err)
	}
	pending = authority.session.PendingCommand()
	client.holdUnknown = false
	if err = authority.RetryPending(context.Background()); err != nil {
		t.Fatalf("settle operation delete = %v", err)
	}
	if !bytes.Equal(pending, client.unknownCommand) {
		t.Fatal("operation delete retry changed command bytes")
	}
	if _, err = authority.ReadOperation(context.Background(), record.ID); !errors.Is(err, ErrReplicatedOperationMissing) {
		t.Fatalf("operation after settled GC = %v", err)
	}
}

func TestReplicatedOperationEncodingIsCanonicalAndBounded(t *testing.T) {
	record := testReplicatedOperation(ReplicatedOperationRecord{
		ID: [32]byte{1}, Kind: ReplicatedOperationMove,
		State: ReplicatedOperationRunning, Revision: 7, CatalogGeneration: 11,
		Cursor: [8]uint64{1, 2, 3, 4, 5, 6, 7, 8}, Proof: [32]byte{9},
	})
	raw, err := appendReplicatedOperation(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openReplicatedOperation(raw)
	if err != nil || !opened.Equal(record) {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	again, err := appendReplicatedOperation(nil, opened)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("operation encoding is not canonical")
	}
	for _, damaged := range [][]byte{
		append(append([]byte(nil), raw...), ' '),
		[]byte(`{"id":[1],"kind":1,"state":1,"revision":1,"catalog_generation":1,"cursor":[0,0,0,0,0,0,0,0],"proof":[1]}`),
		make([]byte, MaxReplicatedOperationBytes+1),
	} {
		if _, err = openReplicatedOperation(damaged); !errors.Is(err, ErrReplicatedCatalog) {
			t.Fatalf("damaged operation accepted: length=%d err=%v", len(damaged), err)
		}
	}
}

func testReplicatedOperation(record ReplicatedOperationRecord) ReplicatedOperationRecord {
	record.Intent = []byte(`{}`)
	record.IntentDigest = sha256.Sum256(record.Intent)
	return record
}

func TestReplicatedCatalogAuthorityRejectsMismatchedWriteSession(t *testing.T) {
	authority, _, _ := newCatalogAuthorityFixture(t)
	options := ReplicatedCatalogAuthorityOptions{
		Executor: authority.executor, Route: authority.route, Relation: authority.relation,
		Holder: NewCatalogHolder(authority.holder.Current()), Session: authority.session,
		Authority: authority.authority,
	}
	copySession := *authority.session
	options.Session = nil
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("nil session = %v", err)
	}
	options.Session = &copySession
	copySession.distribution = "tenant-data"
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("mismatched distribution = %v", err)
	}
	copySession = *authority.session
	options.Session = &copySession
	copySession.shard = "tenant-shard"
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("mismatched shard = %v", err)
	}
	copySession = *authority.session
	options.Session = &copySession
	copySession.resolver = BaseRelationResolver{Relation: authority.relation + 1}
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("mismatched relation = %v", err)
	}
	copySession = *authority.session
	options.Session = &copySession
	copySession.route.Replicas = append([]ReplicatedEndpoint(nil), copySession.route.Replicas...)
	copySession.route.Replicas[0].Address += "-other"
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("mismatched route = %v", err)
	}
	copySession = *authority.session
	options.Session = &copySession
	copySession.proposalCapability = serviceauthz.CapabilityDataWrite
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("mismatched capability = %v", err)
	}
	copySession = *authority.session
	options.Session = &copySession
	copySession.phase = nativeSessionNew
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("inactive session = %v", err)
	}
	copySession = *authority.session
	options.Session = &copySession
	copySession.pending = true
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("pending session = %v", err)
	}
	options.Session = authority.session
	options.Route.Distribution = "tenant-data"
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("ordinary data route = %v", err)
	}
}

func TestReplicatedCatalogDocumentRejectsNonCanonicalAndOversizedBytes(t *testing.T) {
	_, _, snapshot := newCatalogAuthorityFixture(t)
	raw, err := AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if opened, openErr := OpenSnapshotDocument(raw); openErr != nil ||
		opened.Generation() != snapshot.Generation() {
		t.Fatalf("canonical catalog open = %v, err=%v", opened, openErr)
	}
	nonCanonical := append(append([]byte(nil), raw...), ' ')
	if _, err = OpenSnapshotDocument(nonCanonical); !errors.Is(err, ErrInvalidCatalog) {
		t.Fatalf("noncanonical catalog = %v", err)
	}
	if _, err = OpenSnapshotDocument(make([]byte, maxCatalogBytes+1)); !errors.Is(err, ErrCatalogTooLarge) {
		t.Fatalf("oversized catalog = %v", err)
	}
}

func TestReplicatedCatalogReadRejectsEqualGenerationDivergence(t *testing.T) {
	authority, _, current := newCatalogAuthorityFixture(t)
	config, endpoints, descriptor := testReplicatedCatalogInput(t)
	endpoints["ep-a"] = "127.0.0.1:49999"
	divergent, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, current.Generation(), nil, nil,
		[]ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	authority.holder = NewCatalogHolder(divergent)
	if _, err = authority.Read(context.Background()); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("equal-generation divergent catalog = %v", err)
	}
}
