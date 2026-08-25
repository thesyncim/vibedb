package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
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
}

func (client *catalogAuthorityClient) DoReplicated(
	_ context.Context, _ ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: client.state,
		}, nil
	}
	if request.Operation == shardservice.ReplicatedReadLeader ||
		request.Operation == shardservice.ReplicatedReadFollower {
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
	if bytes.Equal(request.Command, client.unknownCommand) && len(client.unknownCompletion) != 0 {
		if client.holdUnknown {
			return nil, errors.New("replicated outcome remains unknown")
		}
		return catalogCompletionResponse(client.unknownState, client.unknownCompletion), nil
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
	return catalogCompletionResponse(client.state, completion), nil
}

func catalogCompletionResponse(
	state shardservice.ReplicatedMemberState, completion []byte,
) *shardservice.ReplicatedResponse {
	view, _ := replication.OpenCompletion(completion)
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
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
	state := shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{
			Group: route.Group, AllocationGeneration: route.AllocationGeneration,
			MemberID: route.Replicas[0].Member, StoreID: route.Replicas[0].StoreID,
			NodeIncarnation: route.Replicas[0].NodeIncarnation, Term: 1, Command: route.Command,
		},
		LeaderID: route.Replicas[0].Member, Commit: 1, Applied: 1, CheckpointApplied: 1,
	}
	client := &catalogAuthorityClient{state: state, rows: make(map[string][]byte)}
	raw, err := AppendSnapshotDocument(nil, current)
	if err != nil {
		t.Fatal(err)
	}
	client.rows[string(replicatedCatalogHeadKey[:])] = raw
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: executor, Route: route, Distribution: string(descriptor.Distribution),
		Shard: string(descriptor.Shard), Tenant: []byte("control-plane"),
		ClientID: replication.ID128{0x71}, Resolver: BaseRelationResolver{Relation: 1},
		MaxRelationBatches: 1, MaxMutations: 2,
		InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Open(context.Background(), 1<<50); err != nil {
		t.Fatal(err)
	}
	authority, err := NewReplicatedCatalogAuthority(ReplicatedCatalogAuthorityOptions{
		Executor: executor, Route: route, Relation: 1,
		Holder: NewCatalogHolder(current), Session: session,
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
	}
	copySession := *authority.session
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
	copySession.phase = nativeSessionNew
	if _, err := NewReplicatedCatalogAuthority(options); !errors.Is(err, ErrReplicatedCatalog) {
		t.Fatalf("inactive session = %v", err)
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
