package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

type replicatedSQLIndexedReadClient struct {
	states map[string]shardservice.ReplicatedMemberState
	value  []byte
	reads  int
}

func (client *replicatedSQLIndexedReadClient) DoReplicated(
	_ context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	state, ok := client.states[endpoint.Address]
	if !ok {
		return nil, ErrReplicatedRoute
	}
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	if request.Operation != shardservice.ReplicatedReadLeader {
		return nil, ErrReplicatedRoute
	}
	client.reads++
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedReadFound, HasState: true, State: state,
		ReadApplied: state.Applied, Value: client.value,
	}, nil
}

func TestReplicatedSQLTransactionLowersExactMultiTableMutationsByGroupAndRelation(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	queries := []Query{
		{
			SQL:    `INSERT INTO messages VALUES (?)`,
			Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"message-1","n":1}`)},
		},
		{
			SQL:    `DELETE FROM accounts WHERE id = ?`,
			Params: []shardservice.Param{shardservice.StringParam("account-1")},
		},
		{
			SQL: `UPDATE logs SET "$doc" = ? WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"log-1","n":2}`),
				shardservice.StringParam("log-1"),
			},
		},
	}
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, queries, executor.profileFor(ClassInteractive),
	)
	if err != nil || !handled {
		t.Fatalf("plan = handled %v err %v", handled, err)
	}
	if len(participants) != 2 {
		t.Fatalf("participants = %d, want 2", len(participants))
	}
	var data, logs *ReplicatedTransactionParticipant
	for index := range participants {
		participant := &participants[index]
		switch participant.Route.Distribution {
		case "data":
			data = participant
		case "logs-data":
			logs = participant
		}
	}
	if data == nil || logs == nil {
		t.Fatalf("routes = %+v", participants)
	}
	if len(data.Batches) != 2 || data.Batches[0].Relation != 1 ||
		data.Batches[1].Relation != 2 {
		t.Fatalf("data batches = %+v", data.Batches)
	}
	if got := data.Batches[0].Mutations[0].Kind; got != replication.MutationDelete {
		t.Fatalf("account mutation = %d, want delete", got)
	}
	if got := data.Batches[1].Mutations[0].Kind; got != replication.MutationPutAbsent {
		t.Fatalf("message mutation = %d, want put-absent", got)
	}
	if len(logs.Batches) != 1 || logs.Batches[0].Relation != 1 ||
		logs.Batches[0].Mutations[0].Kind != replication.MutationPutPresent {
		t.Fatalf("logs batches = %+v", logs.Batches)
	}
	wantMessage, _ := orderedkey.AppendString(nil, []byte("message-1"), orderedkey.Ascending)
	if !bytes.Equal(data.Batches[1].Mutations[0].Key, wantMessage) ||
		!bytes.Equal(data.Batches[1].Mutations[0].Value, []byte(`{"id":"message-1","n":1}`)) {
		t.Fatalf("message mutation = %+v", data.Batches[1].Mutations[0])
	}
	for index := range participants {
		participant := &participants[index]
		if len(participant.Route.Replicas) != ServingReplicaCount ||
			participant.BucketBits != distribution.DefaultVirtualBucketBits ||
			len(participant.IntentScopes) == 0 {
			t.Fatalf("participant %d route/scope = %+v", index, participant)
		}
	}
}

func TestReplicatedSQLTransactionCallerDigestIsExactAndRouteIndependent(t *testing.T) {
	base := []Query{
		{SQL: "DELETE FROM messages WHERE id = ?", Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.StringBytesParam([]byte("message"))}},
		{SQL: "DELETE FROM logs WHERE id = ?", Class: ClassInteractive,
			Params: []shardservice.Param{shardservice.BoolParam(false)}},
	}
	want := replicatedSQLTransactionRequestDigest(base)
	if got := replicatedSQLTransactionRequestDigest(base); got != want {
		t.Fatal("identical caller request changed digest")
	}
	tests := []struct {
		name   string
		mutate func([]Query)
	}{
		{"sql", func(q []Query) { q[0].SQL += " " }},
		{"class", func(q []Query) { q[0].Class = ClassBatch }},
		{"kind", func(q []Query) { q[0].Params[0].Kind = shardservice.ParamNumber }},
		{"bool", func(q []Query) { q[1].Params[0].Bool = true }},
		{"bytes", func(q []Query) { q[0].Params[0].Bytes = []byte("other") }},
		{"order", func(q []Query) { q[0], q[1] = q[1], q[0] }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := append([]Query(nil), base...)
			changed[0].Params = append([]shardservice.Param(nil), base[0].Params...)
			changed[1].Params = append([]shardservice.Param(nil), base[1].Params...)
			test.mutate(changed)
			if got := replicatedSQLTransactionRequestDigest(changed); got == want {
				t.Fatalf("%s change did not change digest", test.name)
			}
		})
	}
}

func TestReplicatedSQLTransactionRejectsDuplicateAndResidualBeforeExecution(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	tests := []struct {
		name    string
		queries []Query
		want    error
	}{
		{
			name: "duplicate relation key",
			queries: []Query{
				{SQL: `INSERT INTO messages VALUES (?)`, Params: []shardservice.Param{
					shardservice.DocumentParam(`{"id":"same"}`),
				}},
				{SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("same"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionDuplicate,
		},
		{
			name: "residual predicate",
			queries: []Query{
				{SQL: `UPDATE messages SET "$doc" = ? WHERE id = ? AND n = 1`, Params: []shardservice.Param{
					shardservice.DocumentParam(`{"id":"message-1","n":2}`),
					shardservice.StringParam("message-1"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionUnsupported,
		},
		{
			name: "primary key move within same shard",
			queries: []Query{
				{SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`, Params: []shardservice.Param{
					shardservice.DocumentParam(`{"id":"message-2"}`),
					shardservice.StringParam("message-1"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrWriteShardKeyMove,
		},
		{
			name: "flat insert",
			queries: []Query{
				{SQL: `INSERT INTO messages (id) VALUES (?)`, Params: []shardservice.Param{
					shardservice.StringParam("message-1"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionUnsupported,
		},
		{
			name: "multi row insert",
			queries: []Query{
				{SQL: `INSERT INTO messages VALUES (?),(?)`, Params: []shardservice.Param{
					shardservice.DocumentParam(`{"id":"message-1"}`),
					shardservice.DocumentParam(`{"id":"message-2"}`),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionUnsupported,
		},
		{
			name: "returning",
			queries: []Query{
				{SQL: `DELETE FROM messages WHERE id = ? RETURNING id`, Params: []shardservice.Param{
					shardservice.StringParam("message-1"),
				}},
				{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
					shardservice.StringParam("log-1"),
				}},
			},
			want: ErrReplicatedSQLTransactionUnsupported,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			participants, handled, err := executor.planReplicatedSQLTransaction(
				t.Context(), snapshot, test.queries, executor.profileFor(ClassInteractive),
			)
			if !handled || !errors.Is(err, test.want) || len(participants) != 0 {
				t.Fatalf("plan = %d handled %v err %v, want %v", len(participants), handled, err, test.want)
			}
		})
	}
}

func TestReplicatedSQLTransactionRoutesReadyGlobalIndexAsIndependentRF3Participant(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true, true)
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, []Query{
			{SQL: `INSERT INTO messages VALUES (?)`, Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"message-1","email":"m@example.test"}`),
			}},
			{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
				shardservice.StringParam("log-1"),
			}},
		}, executor.profileFor(ClassInteractive),
	)
	if len(participants) != 3 || !handled || err != nil {
		t.Fatalf("plan = %d handled %v err %v", len(participants), handled, err)
	}
	var index *ReplicatedTransactionParticipant
	for ordinal := range participants {
		if participants[ordinal].Route.Distribution == "messages-email" {
			index = &participants[ordinal]
		}
	}
	if index == nil || len(index.Batches) != 1 || index.Batches[0].Relation != 1 ||
		len(index.Batches[0].Mutations) != 1 ||
		index.Batches[0].Mutations[0].Kind != replication.MutationPutAbsentOrEqual ||
		!bytes.Equal(index.Batches[0].Mutations[0].Value, []byte(`["message-1"]`)) {
		t.Fatalf("global index participant = %+v", index)
	}
}

func TestReplicatedSQLTransactionGlobalIndexMutationsConsumeAdmissionBudget(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true, true)
	query := []Query{{
		SQL: `INSERT INTO messages VALUES (?)`, Params: []shardservice.Param{
			shardservice.DocumentParam(`{"id":"message-1","email":"m@example.test"}`),
		},
	}}
	profile := executor.profileFor(ClassInteractive)
	profile.MaxTransactionMutations = 1
	if participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, query, profile,
	); len(participants) != 0 || !handled || !errors.Is(err, ErrTransactionMutationLimit) {
		t.Fatalf("mutation bound participants=%d handled=%v err=%v", len(participants), handled, err)
	}
	profile = executor.profileFor(ClassInteractive)
	profile.MaxTransactionBytes = uint64(len(`{"id":"message-1","email":"m@example.test"}`)) + 32
	if participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, query, profile,
	); len(participants) != 0 || !handled || !errors.Is(err, ErrTransactionByteLimit) {
		t.Fatalf("byte bound participants=%d handled=%v err=%v", len(participants), handled, err)
	}
}

func TestReplicatedSQLTransactionGlobalIndexRejectsStaleSplitFence(t *testing.T) {
	snapshot, _ := replicatedSQLTransactionFixture(t, true, true)
	query := Query{
		SQL: `INSERT INTO messages VALUES (?)`, Params: []shardservice.Param{
			shardservice.DocumentParam(`{"id":"message-1","email":"m@example.test"}`),
		},
	}
	prepared, err := snapshot.Prepare(t.Context(), query.SQL)
	if err != nil {
		t.Fatal(err)
	}
	args, err := queryRuntimeArgs(query.Params)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.BindWrite(args)
	if err != nil || len(bound.globalIndexes) != 1 {
		t.Fatalf("bound global indexes=%d err=%v", len(bound.globalIndexes), err)
	}
	stale := bound.globalIndexes[0]
	stale.target.OwnershipEpoch++
	if _, _, err := snapshot.resolveReplicatedSQLGlobalIndex(&stale); !errors.Is(
		err, ErrReplicatedSQLWriteUnavailable,
	) {
		t.Fatalf("stale split ownership fence error=%v", err)
	}
	stale = bound.globalIndexes[0]
	stale.routingVersion++
	if _, _, err := snapshot.resolveReplicatedSQLGlobalIndex(&stale); !errors.Is(
		err, ErrReplicatedSQLWriteUnavailable,
	) {
		t.Fatalf("stale split routing fence error=%v", err)
	}
}

func TestReplicatedSQLTransactionGlobalIndexUpdateAndDeleteBindExactOldValue(t *testing.T) {
	for _, test := range []struct {
		name       string
		query      Query
		baseKind   replication.MutationKind
		indexKinds []replication.MutationKind
	}{{
		name: "update", query: Query{
			SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"message-1","email":"new@example.test"}`),
				shardservice.StringParam("message-1"),
			},
		}, baseKind: replication.MutationPutDigestEqual,
		indexKinds: []replication.MutationKind{
			replication.MutationDeleteDigestEqual, replication.MutationPutAbsentOrEqual,
		},
	}, {
		name: "delete", query: Query{
			SQL:    `DELETE FROM messages WHERE id = ?`,
			Params: []shardservice.Param{shardservice.StringParam("message-1")},
		}, baseKind: replication.MutationDeleteDigestEqual,
		indexKinds: []replication.MutationKind{replication.MutationDeleteDigestEqual},
	}} {
		t.Run(test.name, func(t *testing.T) {
			snapshot, executor := replicatedSQLTransactionFixture(t, true, true)
			old := []byte(`{"id":"message-1","email":"old@example.test"}`)
			client, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
			participants, handled, err := executor.planReplicatedSQLTransactionWithData(
				t.Context(), snapshot, []Query{test.query}, executor.profileFor(ClassInteractive), data,
			)
			if err != nil || !handled || client.reads != 1 || len(participants) != 2 {
				t.Fatalf("plan=%d handled=%v reads=%d err=%v", len(participants), handled, client.reads, err)
			}
			var base, index *ReplicatedTransactionParticipant
			for ordinal := range participants {
				switch participants[ordinal].Route.Distribution {
				case "data":
					base = &participants[ordinal]
				case "messages-email":
					index = &participants[ordinal]
				}
			}
			if base == nil || len(base.Batches) != 1 || len(base.Batches[0].Mutations) != 1 {
				t.Fatalf("base participant=%+v", base)
			}
			baseMutation := base.Batches[0].Mutations[0]
			if baseMutation.Kind != test.baseKind ||
				baseMutation.ExpectedValueLength != uint64(len(old)) ||
				baseMutation.ExpectedValueDigest != replication.Digest(sha256.Sum256(old)) {
				t.Fatalf("base mutation=%+v", baseMutation)
			}
			if index == nil || len(index.Batches) != 1 ||
				len(index.Batches[0].Mutations) != len(test.indexKinds) {
				t.Fatalf("index participant=%+v", index)
			}
			for ordinal, want := range test.indexKinds {
				mutation := index.Batches[0].Mutations[ordinal]
				if mutation.Kind != want {
					t.Fatalf("index mutation %d kind=%d want=%d", ordinal, mutation.Kind, want)
				}
				if want == replication.MutationDeleteDigestEqual &&
					(mutation.ExpectedValueLength == 0 ||
						mutation.ExpectedValueDigest == (replication.Digest{})) {
					t.Fatalf("index delete lacks exact old-value fence: %+v", mutation)
				}
			}
		})
	}
}

func TestReplicatedSQLTransactionGlobalIndexSameKeyUsesExactReplacement(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true, true, true)
	old := []byte(`{"id":"message-1","email":"same@example.test","region":"old"}`)
	_, data := attachReplicatedSQLIndexedReadClient(t, snapshot, old)
	participants, handled, err := executor.planReplicatedSQLTransactionWithData(
		t.Context(), snapshot, []Query{{
			SQL: `UPDATE messages SET "$doc" = ? WHERE id = ?`,
			Params: []shardservice.Param{
				shardservice.DocumentParam(`{"id":"message-1","email":"same@example.test","region":"new"}`),
				shardservice.StringParam("message-1"),
			},
		}}, executor.profileFor(ClassInteractive), data,
	)
	if err != nil || !handled || len(participants) != 2 {
		t.Fatalf("plan=%d handled=%v err=%v", len(participants), handled, err)
	}
	for ordinal := range participants {
		participant := &participants[ordinal]
		if participant.Route.Distribution != "messages-email" {
			continue
		}
		if len(participant.Batches) != 1 || len(participant.Batches[0].Mutations) != 1 {
			t.Fatalf("same-key index participant=%+v", participant)
		}
		mutation := participant.Batches[0].Mutations[0]
		if mutation.Kind != replication.MutationPutDigestEqual ||
			mutation.ExpectedValueLength == 0 ||
			mutation.ExpectedValueDigest == (replication.Digest{}) || len(mutation.Value) == 0 {
			t.Fatalf("same-key exact replacement=%+v", mutation)
		}
		return
	}
	t.Fatal("same-key index participant missing")
}

func attachReplicatedSQLIndexedReadClient(
	t testing.TB,
	snapshot *Snapshot,
	value []byte,
) (*replicatedSQLIndexedReadClient, *ReplicatedExecutor) {
	t.Helper()
	key, ok := orderedkey.AppendString(nil, []byte("message-1"), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode indexed read key")
	}
	var replicas [ServingReplicaCount]ReplicatedEndpoint
	var scalar [replication.MaxMutationKeyBytes + 16]byte
	resolved, ok := snapshot.ResolveReplicatedTableKey(
		[]byte("messages"), key, scalar[:0], replicas[:0],
	)
	if !ok {
		t.Fatal("resolve indexed base route")
	}
	states := make(map[string]shardservice.ReplicatedMemberState, len(resolved.Route.Replicas))
	leader := resolved.Route.Replicas[0].Member
	for _, endpoint := range resolved.Route.Replicas {
		fence := shardservice.ReplicatedFence{
			Group: resolved.Route.Group, AllocationGeneration: resolved.Route.AllocationGeneration,
			Command: resolved.Route.Command, MemberID: endpoint.Member,
			StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation, Term: 1,
		}
		states[endpoint.Address] = shardservice.ReplicatedMemberState{
			Fence: fence, LeaderID: leader, Commit: 1, Applied: 1, CheckpointApplied: 1,
		}
	}
	client := &replicatedSQLIndexedReadClient{states: states, value: value}
	replicated, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client, replicated
}

func TestReplicatedSQLTransactionRejectsMixedAuthorityAndLeavesStaticBatchUnclaimed(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, false)
	mixed := []Query{
		{SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{shardservice.StringParam("m")}},
		{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{shardservice.StringParam("l")}},
	}
	participants, handled, err := executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, mixed, executor.profileFor(ClassInteractive),
	)
	if !handled || !errors.Is(err, ErrReplicatedSQLTransactionMixed) || len(participants) != 0 {
		t.Fatalf("mixed = %d handled %v err %v", len(participants), handled, err)
	}
	static := []Query{
		{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{shardservice.StringParam("l1")}},
		{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{shardservice.StringParam("l2")}},
	}
	participants, handled, err = executor.planReplicatedSQLTransaction(
		t.Context(), snapshot, static, executor.profileFor(ClassInteractive),
	)
	if handled || err != nil || len(participants) != 0 {
		t.Fatalf("static = %d handled %v err %v", len(participants), handled, err)
	}
}

func replicatedSQLTransactionFixture(
	t testing.TB,
	replicatedOnly bool,
	withReadyIndex ...bool,
) (*Snapshot, *Executor) {
	t.Helper()
	config, endpoints, descriptor, _ := testReplicatedTableInput(t)
	config.Placements = []distribution.TablePlacement{
		{Table: "accounts", Distribution: "data", Columns: []string{"/id"}},
		{Table: "messages", Distribution: "data", Columns: []string{"/id"}},
	}
	profiles := []ReplicatedTableProfile{
		{
			Table: "accounts", Relation: 1, PrimaryKey: "/id", SchemaGeneration: 8,
			RelationManifestDigest: replication.Digest{9}, MaxKeyBytes: 256,
			MaxDocumentBytes: 4 << 20,
		},
		{
			Table: "messages", Relation: 2, PrimaryKey: "/id", SchemaGeneration: 8,
			RelationManifestDigest: replication.Digest{9}, MaxKeyBytes: 256,
			MaxDocumentBytes: 4 << 20,
		},
	}

	logsManifest, err := distribution.NewManifest("logs-data", 3, []distribution.Shard{{
		ID: "logs-all", AllocationGeneration: 1,
		Range: distribution.KeyRange{
			Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
		},
		Leaders: []distribution.EndpointID{"logs-a", "logs-b", "logs-c"}, Epoch: 7,
	}})
	if err != nil {
		t.Fatal(err)
	}
	config.Distributions = append(config.Distributions, distribution.DistributionSpec{
		Name: "logs-data", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
	})
	config.Manifests = append(config.Manifests, logsManifest)
	config.Placements = append(config.Placements, distribution.TablePlacement{
		Table: "logs", Distribution: "logs-data", Columns: []string{"/id"},
	})
	profiles = append(profiles, ReplicatedTableProfile{
		Table: "logs", Relation: 1, PrimaryKey: "/id", SchemaGeneration: 8,
		RelationManifestDigest: replication.Digest{9}, MaxKeyBytes: 256,
		MaxDocumentBytes: 4 << 20,
	})
	endpoints["logs-a"], endpoints["logs-b"], endpoints["logs-c"] =
		"127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"
	endpoints["logs-native-a"], endpoints["logs-native-b"], endpoints["logs-native-c"] =
		"127.0.0.1:8101", "127.0.0.1:8102", "127.0.0.1:8103"
	endpoints["logs-control-a"], endpoints["logs-control-b"], endpoints["logs-control-c"] =
		"127.0.0.1:8201", "127.0.0.1:8202", "127.0.0.1:8203"
	var indexes []IndexDescriptor
	descriptors := []ReplicatedShardDescriptor{descriptor}
	if len(withReadyIndex) != 0 && withReadyIndex[0] {
		locatorPaths := []string{"/id"}
		if len(withReadyIndex) > 1 && withReadyIndex[1] {
			locatorPaths = append(locatorPaths, "/region")
		}
		indexManifest, indexErr := distribution.NewManifest(
			"messages-email", 1, []distribution.Shard{{
				ID: "messages-email-all", AllocationGeneration: 1,
				Range: distribution.KeyRange{
					Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
				},
				Leaders: []distribution.EndpointID{
					"messages-email-a", "messages-email-b", "messages-email-c",
				}, Epoch: 1,
			}},
		)
		if indexErr != nil {
			t.Fatal(indexErr)
		}
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name: "messages-email", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		})
		config.Manifests = append(config.Manifests, indexManifest)
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table: "messages_email", Distribution: "messages-email", Columns: []string{"/email"},
		})
		for ordinal, letter := range []string{"a", "b", "c"} {
			endpoints[distribution.EndpointID("messages-email-"+letter)] =
				"127.0.0.1:" + string(rune('1'+ordinal)) + "301"
			endpoints[distribution.EndpointID("messages-email-native-"+letter)] =
				"127.0.0.1:" + string(rune('1'+ordinal)) + "311"
			endpoints[distribution.EndpointID("messages-email-control-"+letter)] =
				"127.0.0.1:" + string(rune('1'+ordinal)) + "321"
		}
		indexes = []IndexDescriptor{{
			IndexID: 19, Incarnation: 1, Table: "messages", Name: "by_email",
			Relation: "messages_email", Paths: []string{"/email"},
			LocatorPaths: locatorPaths, PrimaryPath: "/id",
			Flags: IndexGlobal | IndexUnique | IndexOrdered, Lifecycle: IndexReady,
		}}
		indexDescriptor := descriptor
		indexDescriptor.Replicas = append(
			[]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...,
		)
		indexDescriptor.Distribution, indexDescriptor.Shard =
			"messages-email", "messages-email-all"
		indexDescriptor.Group.ShardIncarnation[0] += 2
		indexDescriptor.Group.GroupID[0] += 2
		indexDescriptor.Command.OwnershipEpoch = 1
		indexDescriptor.Command.RoutingVersion = 1
		for ordinal := range indexDescriptor.Replicas {
			letter := string(rune('a' + ordinal))
			indexDescriptor.Replicas[ordinal].Endpoint =
				distribution.EndpointID("messages-email-" + letter)
			indexDescriptor.Replicas[ordinal].NativeEndpoint =
				distribution.EndpointID("messages-email-native-" + letter)
			indexDescriptor.Replicas[ordinal].ControlEndpoint =
				distribution.EndpointID("messages-email-control-" + letter)
			indexDescriptor.Replicas[ordinal].StoreID[0] += 80
		}
		descriptors = append(descriptors, indexDescriptor)
		profiles = append(profiles, ReplicatedTableProfile{
			Table: "messages_email", Relation: 1, PrimaryKey: "/email",
			SchemaGeneration: 8, RelationManifestDigest: replication.Digest{9},
			MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20,
		})
	}
	logsDescriptor := descriptor
	logsDescriptor.Replicas = append(
		[]ReplicatedReplicaDescriptor(nil), descriptor.Replicas...,
	)
	logsDescriptor.Distribution, logsDescriptor.Shard = "logs-data", "logs-all"
	logsDescriptor.Group.ShardIncarnation[0]++
	logsDescriptor.Group.GroupID[0]++
	for index := range logsDescriptor.Replicas {
		letter := []byte{'a' + byte(index)}
		logsDescriptor.Replicas[index].Endpoint = distribution.EndpointID("logs-" + string(letter))
		logsDescriptor.Replicas[index].NativeEndpoint = distribution.EndpointID("logs-native-" + string(letter))
		logsDescriptor.Replicas[index].ControlEndpoint = distribution.EndpointID("logs-control-" + string(letter))
		logsDescriptor.Replicas[index].StoreID[0] += 40
	}

	if !replicatedOnly {
		legacyManifest, manifestErr := distribution.NewManifest("legacy-data", 1, []distribution.Shard{{
			ID: "legacy-all", AllocationGeneration: 1,
			Range: distribution.KeyRange{
				Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
			},
			Leaders: []distribution.EndpointID{"legacy"}, Epoch: 1,
		}})
		if manifestErr != nil {
			t.Fatal(manifestErr)
		}
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name: "legacy-data", Arity: 1, MapperVersion: distribution.NativeMapperVersion,
		})
		config.Manifests = append(config.Manifests, legacyManifest)
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table: "legacy", Distribution: "legacy-data", Columns: []string{"/id"},
		})
		endpoints["legacy"] = "127.0.0.1:9001"
	}

	snapshot, err := NewSnapshotWithReplicatedTableMetadata(
		config, endpoints, 7, indexes, nil,
		append(descriptors, logsDescriptor), profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(nil, NewCatalogHolder(snapshot), Options{})
	return snapshot, executor
}
