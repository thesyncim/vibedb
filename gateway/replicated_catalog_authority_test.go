package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sort"
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
		MaxRelationBatches: 1, MaxMutations: 3,
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
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.state,
		}, nil
	}
	if request.Operation == shardservice.ReplicatedReadLeader ||
		request.Operation == shardservice.ReplicatedReadFollower {
		client.readMaximums = append(client.readMaximums, request.MaxValueBytes)
		if client.onRead != nil {
			client.onRead(request.Key)
		}
		value, found := client.rows[string(request.Key)]
		kind := shardservice.ReplicatedReadMissing
		if found {
			kind = shardservice.ReplicatedReadFound
		}
		return &shardservice.ReplicatedResponse{
			Kind: kind, HasState: true, State: client.state,
			ReadApplied: client.state.Applied, Value: append([]byte(nil), value...),
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
		MaxRelationBatches: 1, MaxMutations: 3,
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
