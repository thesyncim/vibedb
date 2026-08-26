package gateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/shardservice"
)

type replicatedSQLTransactionCapture struct {
	calls        int
	generation   uint64
	participants []ReplicatedTransactionParticipant
}

func (capture *replicatedSQLTransactionCapture) Execute(
	_ context.Context,
	generation uint64,
	participants []ReplicatedTransactionParticipant,
) (ReplicatedTransactionResult, error) {
	capture.calls++
	capture.generation = generation
	capture.participants = append(capture.participants[:0], participants...)
	return ReplicatedTransactionResult{
		ID: distributedtxn.ID{1}, Committed: true, AffectedRows: 2,
	}, nil
}

func (*replicatedSQLTransactionCapture) Recover(
	context.Context,
	*ReplicatedTransactionRecoveryHandle,
) (ReplicatedTransactionResult, error) {
	return ReplicatedTransactionResult{}, errors.New("unexpected recovery")
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

func TestReplicatedSQLTransactionExecBatchRequestUsesBoundedRequestRegistry(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	capture := new(replicatedSQLTransactionCapture)
	registry, err := NewReplicatedTransactionRequestRegistry(
		ReplicatedTransactionRequestRegistryOptions{
			Orchestrator: capture, MaxEntries: 4,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	executor.replicatedTransactionRequests = registry
	queries := []Query{
		{SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("message-1"),
		}},
		{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("log-1"),
		}},
	}
	requestID := replication.ID128{9}
	result, err := executor.ExecBatchRequest(t.Context(), requestID, queries)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected != 2 || result.TransactionID != (replication.ID128{1}) ||
		result.ShardsFanned != 2 || capture.calls != 1 ||
		capture.generation != snapshot.Generation() || len(capture.participants) != 2 {
		t.Fatalf("result=%+v capture=%+v", result, capture)
	}
	result, err = executor.ExecBatchRequest(t.Context(), requestID, queries)
	if err != nil || result.RowsAffected != 2 || capture.calls != 1 {
		t.Fatalf("cached result=%+v err=%v calls=%d", result, err, capture.calls)
	}
	conflict := append([]Query(nil), queries...)
	conflict[1].Params = []shardservice.Param{shardservice.StringParam("log-2")}
	if _, err = executor.ExecBatchRequest(t.Context(), requestID, conflict); !errors.Is(
		err, ErrReplicatedTransactionRequestConflict,
	) || capture.calls != 1 {
		t.Fatalf("conflict err=%v calls=%d", err, capture.calls)
	}
}

func TestReplicatedSQLTransactionExecBatchRequestServesSingletonRF3(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	capture := new(replicatedSQLTransactionCapture)
	registry, err := NewReplicatedTransactionRequestRegistry(
		ReplicatedTransactionRequestRegistryOptions{Orchestrator: capture, MaxEntries: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	executor.replicatedTransactionRequests = registry
	query := []Query{{
		SQL:    `DELETE FROM messages WHERE id = ?`,
		Params: []shardservice.Param{shardservice.StringParam("singleton")},
	}}
	if _, err := executor.ExecBatchRequest(t.Context(), replication.ID128{}, query); !errors.Is(err, ErrReplicatedTransactionRequestRegistry) || capture.calls != 0 {
		t.Fatalf("zero identity error=%v calls=%d", err, capture.calls)
	}
	result, err := executor.ExecBatchRequest(t.Context(), replication.ID128{27}, query)
	if err != nil || result == nil || result.RowsAffected != 2 ||
		result.Generation != snapshot.Generation() || result.ShardsFanned != 1 ||
		capture.calls != 1 || len(capture.participants) != 1 {
		t.Fatalf("singleton result=%+v err=%v capture=%+v", result, err, capture)
	}
	executor.catalog = NewCatalogHolder(nil)
	replayed, err := executor.ExecBatchRequest(t.Context(), replication.ID128{27}, query)
	if err != nil || replayed == nil || replayed.Generation != snapshot.Generation() ||
		replayed.ShardsFanned != 1 || capture.calls != 1 {
		t.Fatalf("singleton replay=%+v err=%v calls=%d", replayed, err, capture.calls)
	}
	conflict := []Query{{
		SQL:    `DELETE FROM messages WHERE id = ?`,
		Params: []shardservice.Param{shardservice.StringParam("different")},
	}}
	if _, err := executor.ExecBatchRequest(t.Context(), replication.ID128{27}, conflict); !errors.Is(err, ErrReplicatedTransactionRequestConflict) || capture.calls != 1 {
		t.Fatalf("singleton conflict error=%v calls=%d", err, capture.calls)
	}
}

func TestReplicatedSQLTransactionRequestIdentityFailsClosedForStaticBatch(t *testing.T) {
	_, executor := replicatedSQLTransactionFixture(t, false)
	capture := new(replicatedSQLTransactionCapture)
	registry, err := NewReplicatedTransactionRequestRegistry(
		ReplicatedTransactionRequestRegistryOptions{Orchestrator: capture, MaxEntries: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	executor.replicatedTransactionRequests = registry
	for _, queries := range [][]Query{
		{{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("one"),
		}}},
		{
			{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{
				shardservice.StringParam("one"),
			}},
			{SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{
				shardservice.StringParam("two"),
			}},
		},
	} {
		if _, err := executor.ExecBatchRequest(t.Context(), replication.ID128{28}, queries); !errors.Is(err, ErrBatchRequestIdentityUnsupported) {
			t.Fatalf("static request identity error=%v", err)
		}
	}
	if capture.calls != 0 {
		t.Fatalf("static request reached RF3 orchestrator %d times", capture.calls)
	}
}

func TestReplicatedSQLTransactionStaticSingletonZeroIdentityFallsBack(t *testing.T) {
	_, executor := replicatedSQLTransactionFixture(t, false)
	fallbackErr := errors.New("static singleton fallback dial")
	executor.client = NewClient(func(context.Context, string) (net.Conn, error) {
		return nil, fallbackErr
	})
	t.Cleanup(func() { _ = executor.client.Close() })

	_, err := executor.ExecBatchRequest(t.Context(), replication.ID128{}, []Query{{
		SQL: `DELETE FROM legacy WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("one"),
		},
	}})
	if !errors.Is(err, fallbackErr) {
		t.Fatalf("static singleton fallback error=%v", err)
	}
}

func TestReplicatedSQLTransactionIdentityRequiresRequestRegistry(t *testing.T) {
	_, executor := replicatedSQLTransactionFixture(t, true)
	_, err := executor.ExecBatchRequest(t.Context(), replication.ID128{29}, []Query{{
		SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("one"),
		},
	}})
	if !errors.Is(err, ErrBatchRequestIdentityUnsupported) {
		t.Fatalf("missing request registry error=%v", err)
	}
}

func TestReplicatedSQLTransactionReplayPrecedesCatalogPinAndPreservesMetadata(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	capture := new(replicatedSQLTransactionCapture)
	registry, err := NewReplicatedTransactionRequestRegistry(
		ReplicatedTransactionRequestRegistryOptions{Orchestrator: capture, MaxEntries: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	executor.replicatedTransactionRequests = registry
	queries := []Query{
		{SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("message-replay"),
		}},
		{SQL: `DELETE FROM logs WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("log-replay"),
		}},
	}
	id := replication.ID128{31}
	first, err := executor.ExecBatchRequest(t.Context(), id, queries)
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation != snapshot.Generation() || first.ShardsFanned != 2 {
		t.Fatalf("first metadata=%+v", first)
	}

	// Publish a later catalog with changed replica routing metadata. An exact
	// retry must retain the original response rather than replanning against it.
	endpoints := make(map[distribution.EndpointID]string, len(snapshot.endpoints))
	for endpoint, address := range snapshot.endpoints {
		endpoints[endpoint] = address
	}
	descriptors := snapshot.replicatedDescriptors()
	endpoints[descriptors[0].Replicas[0].NativeEndpoint] = "127.0.0.1:65534"
	profiles := make([]ReplicatedTableProfile, len(snapshot.replicatedTables))
	for index, entry := range snapshot.replicatedTables {
		profile, ok := snapshot.replicatedTableProfileAt(entry)
		if !ok {
			t.Fatalf("profile %d unavailable", index)
		}
		profiles[index] = profile
	}
	advanced, err := NewSnapshotWithReplicatedTableMetadata(
		snapshot.config, endpoints, snapshot.Generation()+1, nil, nil,
		descriptors, profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	executor.catalog = NewCatalogHolder(advanced)
	second, err := executor.ExecBatchRequest(t.Context(), id, queries)
	if err != nil || second.Generation != snapshot.Generation() || second.ShardsFanned != 2 {
		t.Fatalf("changed-catalog replay=%+v err=%v", second, err)
	}

	// Removing the catalog altogether makes any attempted pin or SQL planning
	// fail. The retained request must still replay without touching either.
	executor.catalog = NewCatalogHolder(nil)
	third, err := executor.ExecBatchRequest(t.Context(), id, queries)
	if err != nil || third.Generation != snapshot.Generation() || third.ShardsFanned != 2 ||
		capture.calls != 1 {
		t.Fatalf("catalog-free replay=%+v err=%v calls=%d", third, err, capture.calls)
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

func TestReplicatedSQLTransactionExecutesSameGroupMultiRelationAtomically(t *testing.T) {
	snapshot, executor := replicatedSQLTransactionFixture(t, true)
	capture := new(replicatedSQLTransactionCapture)
	registry, err := NewReplicatedTransactionRequestRegistry(
		ReplicatedTransactionRequestRegistryOptions{
			Orchestrator: capture, MaxEntries: 1,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	executor.replicatedTransactionRequests = registry
	queries := []Query{
		{SQL: `DELETE FROM accounts WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("account-1"),
		}},
		{SQL: `DELETE FROM messages WHERE id = ?`, Params: []shardservice.Param{
			shardservice.StringParam("message-1"),
		}},
	}
	result, handled, err := executor.executeReplicatedSQLTransaction(
		t.Context(), snapshot, replication.ID128{1},
		replicatedSQLTransactionRequestDigest(queries), queries,
		executor.profileFor(ClassInteractive),
	)
	if err != nil || !handled || result == nil || result.RowsAffected != 2 ||
		result.ShardsFanned != 1 || capture.calls != 1 ||
		capture.generation != snapshot.Generation() || len(capture.participants) != 1 {
		t.Fatalf("execute = result %+v handled %v err %v", result, handled, err)
	}
	participant := &capture.participants[0]
	if participant.Route.Distribution != "data" || len(participant.Batches) != 2 ||
		participant.Batches[0].Relation != 1 || participant.Batches[1].Relation != 2 ||
		len(participant.Batches[0].Mutations) != 1 ||
		len(participant.Batches[1].Mutations) != 1 {
		t.Fatalf("participant = %+v", participant)
	}
}

func TestReplicatedSQLTransactionRejectsReadyGlobalIndexBeforeExecution(t *testing.T) {
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
	if len(participants) != 0 || !handled ||
		!errors.Is(err, ErrReplicatedSQLTransactionUnsupported) {
		t.Fatalf("plan = %d handled %v err %v", len(participants), handled, err)
	}
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
	if len(withReadyIndex) != 0 && withReadyIndex[0] {
		indexManifest, indexErr := distribution.NewManifest(
			"messages-email", 1, []distribution.Shard{{
				ID: "messages-email-all", AllocationGeneration: 1,
				Range: distribution.KeyRange{
					Start: distribution.KeyspacePoint{}, End: distribution.KeyspaceEnd{Max: true},
				},
				Leaders: []distribution.EndpointID{"messages-email-a"}, Epoch: 1,
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
		endpoints["messages-email-a"] = "127.0.0.1:8301"
		indexes = []IndexDescriptor{{
			IndexID: 19, Incarnation: 1, Table: "messages", Name: "by_email",
			Relation: "messages_email", Paths: []string{"/email"},
			LocatorPaths: []string{"/id"}, PrimaryPath: "/id",
			Flags: IndexGlobal | IndexUnique | IndexOrdered, Lifecycle: IndexReady,
		}}
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
		[]ReplicatedShardDescriptor{descriptor, logsDescriptor}, profiles,
	)
	if err != nil {
		t.Fatal(err)
	}
	executor := NewExecutor(nil, NewCatalogHolder(snapshot), Options{
		ReplicatedTransactions: &ReplicatedTransactionOrchestrator{},
	})
	return snapshot, executor
}
