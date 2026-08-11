package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func testReplicatedApplyOptions() ReplicatedApplyOptions {
	return ReplicatedApplyOptions{
		MaxCompletions: 128,
		TxnLimits:      defaultDriverTxnLimits(),
		Placement: ReplicatedPlacementProfile{
			Format: ReplicatedPlacementProfileV1, ShardKey: "/id",
			TupleVersion: distribution.TupleVersion1, MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
	}
}

func testReplicatedApplyOptionsV1() ReplicatedApplyOptions {
	options := testReplicatedApplyOptions()
	options.Placement = ReplicatedPlacementProfile{}
	return options
}

func testReplicatedApplyBootstrap() *pb.Snapshot {
	index, term := uint64(1), uint64(1)
	return &pb.Snapshot{
		Data: []byte("sql-replicated-apply-bootstrap-v1"),
		Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term,
			ConfState: &pb.ConfState{Voters: []uint64{1}},
		},
	}
}

func bindReplicatedApplyTestRoot(
	t *testing.T,
	name string,
) (string, *Database, ReplicatedShardStoreIdentity) {
	t.Helper()
	path, database, binding, _ := prepareReplicatedTestRoot(t, name, false)
	identity, err := database.BindReplicatedShardStore(binding, "docs")
	if err != nil {
		t.Fatalf("BindReplicatedShardStore: %v", err)
	}
	return path, database, identity
}

func testReplicatedApplyCommand(
	identity ReplicatedShardStoreIdentity,
	sequence uint64,
	mutations ...replication.Mutation,
) []byte {
	fingerprint := sha256.Sum256([]byte{byte(sequence), 0x4a})
	binding := identity.Binding
	command, err := replication.AppendCommandV1(nil, replication.CommandV1{
		ClusterID:             replication.ID128(binding.ClusterID),
		ClusterIncarnation:    replication.ID128(binding.ClusterIncarnation),
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration:   binding.AllocationGeneration,
		ShardIncarnation:       replication.ID128(binding.ShardIncarnation),
		GroupID:                replication.ID128(binding.GroupID),
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         binding.Authority.OwnershipEpoch,
		SchemaGeneration:       binding.Authority.SchemaGeneration,
		RoutingVersion:         binding.Authority.RoutingVersion,
		RouteGeneration:        binding.Authority.RouteGeneration,
		Tenant:                 []byte("tenant"), ClientID: replication.ID128{9},
		ClientEpoch: 1, ClientSequence: sequence, Fingerprint: fingerprint,
		Collection: identity.UserTable, Mutations: mutations,
	})
	if err != nil {
		panic(err)
	}
	return command
}

func testReplicatedApplyMeta(index uint64) raftmodel.ApplyMeta {
	return raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal}
}

func testReplicatedApplyKey(t *testing.T, database *Database, document []byte) []byte {
	t.Helper()
	core := database.connector.db
	core.mu.RLock()
	table := core.tables["docs"]
	key, err := documentKey(document, table.meta.PrimaryKey, table.primary, table.collection.MaxKeyBytes())
	core.mu.RUnlock()
	if err != nil {
		t.Fatalf("documentKey(%s): %v", document, err)
	}
	return []byte(key)
}

type replicatedPlacementProbe struct {
	id       string
	document []byte
	key      []byte
	point    distribution.KeyspacePoint
}

func testReplicatedPlacementProbes(t *testing.T) []replicatedPlacementProbe {
	t.Helper()
	mapper := distribution.NewNativeMapper(1)
	probes := make([]replicatedPlacementProbe, 64)
	for i := range probes {
		id := fmt.Sprintf("placement-%02d", i)
		point, err := mapper.PointFor([]distribution.Scalar{distribution.NewString(id)})
		if err != nil {
			t.Fatal(err)
		}
		key, ok := orderedkey.AppendJSONString(nil, []byte(strconvQuote(id)), orderedkey.Ascending)
		if !ok {
			t.Fatal("test placement string did not encode")
		}
		probes[i] = replicatedPlacementProbe{
			id: id, document: []byte(fmt.Sprintf(`{"id":%s}`, strconvQuote(id))), key: key, point: point,
		}
	}
	sort.Slice(probes, func(i, j int) bool {
		return distribution.ComparePoints(probes[i].point, probes[j].point) < 0
	})
	return []replicatedPlacementProbe{probes[8], probes[24], probes[40]}
}

func strconvQuote(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func completionResultCode(t *testing.T, claim *ReplicatedApply, command []byte) uint32 {
	code, _ := completionResult(t, claim, command)
	return code
}

func completionResult(
	t *testing.T,
	claim *ReplicatedApply,
	command []byte,
) (uint32, uint16) {
	t.Helper()
	lookup, err := claim.LookupCompletion(command)
	if err != nil {
		t.Fatalf("LookupCompletion: %v", err)
	}
	completion, err := replication.OpenCompletionV1(lookup.Bytes)
	if err != nil {
		t.Fatalf("OpenCompletionV1: %v", err)
	}
	return completion.ResultCode, completion.ResultFormat
}

func TestReplicatedApplyCapacityQualificationProfile(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "capacity-profile")
	options := testReplicatedApplyOptions()
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}

	want := ReplicatedApplyCapacityProfile{
		Binding: base.Binding, ApplyFormat: ReplicatedApplyFormatV2,
		MaxCompletions: options.MaxCompletions,
	}
	if got, err := claim.CapacityQualificationProfile(); err != nil || got != want {
		t.Fatalf("uninitialized capacity profile = %+v, %v; want %+v", got, err, want)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	want.Initialized, want.Applied = true, 1
	if got, err := claim.CapacityQualificationProfile(); err != nil || got != want {
		t.Fatalf("bootstrap capacity profile = %+v, %v; want %+v", got, err, want)
	}

	document := []byte(`{"id":"capacity"}`)
	key := testReplicatedApplyKey(t, database, document)
	command := testReplicatedApplyCommand(base, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(2), command); err != nil {
		t.Fatal(err)
	}
	want.Applied, want.CompletionCount = 2, 1
	if got, err := claim.CapacityQualificationProfile(); err != nil || got != want {
		t.Fatalf("applied capacity profile = %+v, %v; want %+v", got, err, want)
	}

	conflict := testReplicatedApplyCommand(base, 2, replication.Mutation{
		Kind: replication.MutationDelete, Key: key,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(2), conflict); err == nil {
		t.Fatal("conflicting replay did not poison claim")
	}
	if got, err := claim.CapacityQualificationProfile(); got != (ReplicatedApplyCapacityProfile{}) ||
		!errors.Is(err, replicatedstate.ErrApplyPoisoned) {
		t.Fatalf("poisoned capacity profile = %+v, %v; want zero, ErrApplyPoisoned", got, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := claim.CapacityQualificationProfile(); got != (ReplicatedApplyCapacityProfile{}) ||
		!errors.Is(err, ErrReplicatedApplyClosed) {
		t.Fatalf("closed capacity profile = %+v, %v; want zero, ErrReplicatedApplyClosed", got, err)
	}
}

func TestReplicatedApplyActivateValidateAndExactReopen(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "apply")
	options := testReplicatedApplyOptions()
	bootstrap := testReplicatedApplyBootstrap()

	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatalf("OpenReplicatedApply: %v", err)
	}
	if identity.Format != ReplicatedApplyFormatV2 || identity.Storage == "" ||
		identity.ValidationDigest == ([32]byte{}) || identity.Placement != options.Placement ||
		identity.ValidationProfile != uint8(replicatedstate.ValidationDeterministicMutationV2) ||
		identity.TxnLimits != options.TxnLimits || identity.MaxCompletions != options.MaxCompletions {
		t.Fatalf("apply identity = %+v", identity)
	}
	if got, err := claim.Identity(); err != nil || got != identity {
		t.Fatalf("claim.Identity = %+v,%v; want %+v", got, err, identity)
	}
	core := database.connector.db
	core.mu.RLock()
	hiddenPath := core.replicatedApplyPath(core.catalog.ReplicatedApply)
	if core.catalog.ReplicatedApply == nil || core.replicatedApplyCollection == nil ||
		len(core.catalog.Tables) != 1 || core.tables["docs"] == nil {
		core.mu.RUnlock()
		t.Fatal("activation did not retain one visible table plus hidden participant")
	}
	core.mu.RUnlock()
	if _, err := os.Stat(hiddenPath); err != nil {
		t.Fatalf("hidden storage: %v", err)
	}
	if _, _, err := database.OpenReplicatedApply(base, bootstrap, options); !errors.Is(err, ErrReplicatedApplyBusy) {
		t.Fatalf("second claim = %v, want busy", err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}

	validDocument := []byte(`{"id":"a","n":1}`)
	validKey := testReplicatedApplyKey(t, database, validDocument)
	valid := testReplicatedApplyCommand(base, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: validKey, Value: validDocument,
	})
	beforeClock := core.tables["docs"].conflicts.observe()
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(2), valid); err != nil {
		t.Fatalf("apply valid PUT: %v", err)
	}
	if got := completionResultCode(t, claim, valid); got != replicatedstate.ResultApplied {
		t.Fatalf("valid result = %d, want applied", got)
	}
	if _, format := completionResult(t, claim, valid); format != replicatedstate.ResultFormatMutationV2 {
		t.Fatalf("valid result format = %d, want v2", format)
	}
	if after := core.tables["docs"].conflicts.observe(); after <= beforeClock {
		t.Fatalf("conflict clock did not advance: before=%d after=%d", beforeClock, after)
	}
	got, found, err := core.tables["docs"].collection.AppendRaw(nil, validKey)
	if err != nil || !found || !bytes.Equal(got, validDocument) {
		t.Fatalf("stored row = %q,%v,%v", got, found, err)
	}

	wrongKey, _ := orderedkey.AppendJSONString(nil, []byte(`"wrong"`), orderedkey.Ascending)
	invalid := testReplicatedApplyCommand(base, 2, replication.Mutation{
		Kind: replication.MutationPut, Key: wrongKey, Value: []byte(`{"id":"b"}`),
	})
	beforeRefusalClock := core.tables["docs"].conflicts.observe()
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(3), invalid); err != nil {
		t.Fatalf("apply invalid PUT refusal: %v", err)
	}
	if after := core.tables["docs"].conflicts.observe(); after != beforeRefusalClock {
		t.Fatalf("definite pre-user refusal advanced conflict clock: before=%d after=%d",
			beforeRefusalClock, after)
	}
	if got := completionResultCode(t, claim, invalid); got != replicatedstate.ResultInvalidDocument {
		t.Fatalf("invalid result = %d, want invalid-document", got)
	}
	if _, found, err := core.tables["docs"].collection.AppendRaw(nil, wrongKey); err != nil || found {
		t.Fatalf("invalid PUT durable row found=%v err=%v", found, err)
	}

	deleteAbsent := testReplicatedApplyCommand(base, 3, replication.Mutation{
		Kind: replication.MutationDelete, Key: wrongKey,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(4), deleteAbsent); err != nil {
		t.Fatalf("apply canonical absent DELETE: %v", err)
	}
	if got := completionResultCode(t, claim, deleteAbsent); got != replicatedstate.ResultApplied {
		t.Fatalf("absent DELETE result = %d, want applied", got)
	}
	deleteMalformed := testReplicatedApplyCommand(base, 4, replication.Mutation{
		Kind: replication.MutationDelete, Key: []byte("not-an-ordered-key"),
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(5), deleteMalformed); err != nil {
		t.Fatalf("apply malformed DELETE refusal: %v", err)
	}
	if got := completionResultCode(t, claim, deleteMalformed); got != replicatedstate.ResultInvalidDocument {
		t.Fatalf("malformed DELETE result = %d, want invalid-document", got)
	}

	semanticRefusals := []struct {
		name string
		key  []byte
		doc  []byte
		want uint32
	}{
		{"missing_primary", wrongKey, []byte(`{"other":1}`), replicatedstate.ResultInvalidDocument},
		{"null_primary", wrongKey, []byte(`{"id":null}`), replicatedstate.ResultInvalidDocument},
		{"object_primary", wrongKey, []byte(`{"id":{"x":1}}`), replicatedstate.ResultInvalidDocument},
		{"oversize_derived_key", wrongKey,
			[]byte(`{"id":"` + string(bytes.Repeat([]byte{'x'}, 300)) + `"}`),
			replicatedstate.ResultTargetBound},
	}
	nextIndex := uint64(6)
	nextSequence := uint64(5)
	for _, refusal := range semanticRefusals {
		t.Run(refusal.name, func(t *testing.T) {
			command := testReplicatedApplyCommand(base, nextSequence, replication.Mutation{
				Kind: replication.MutationPut, Key: refusal.key, Value: refusal.doc,
			})
			if _, err := claim.ApplyNormal(testReplicatedApplyMeta(nextIndex), command); err != nil {
				t.Fatalf("apply refusal: %v", err)
			}
			if got := completionResultCode(t, claim, command); got != refusal.want {
				t.Fatalf("result = %d, want %d", got, refusal.want)
			}
			nextIndex++
			nextSequence++
		})
	}

	// Validation observes only the final last-write-wins mutation, but still
	// runs before no-op elision. Invalid→valid is accepted; valid→invalid is a
	// deterministic refusal and cannot mutate the row.
	lwwDocument := []byte(`{"id":"lww","n":2}`)
	lwwKey := testReplicatedApplyKey(t, database, lwwDocument)
	lwwApplied := testReplicatedApplyCommand(base, nextSequence,
		replication.Mutation{Kind: replication.MutationPut, Key: lwwKey, Value: []byte(`{"id":"wrong"}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: lwwKey, Value: lwwDocument},
	)
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(nextIndex), lwwApplied); err != nil {
		t.Fatalf("apply final-valid LWW: %v", err)
	}
	if got := completionResultCode(t, claim, lwwApplied); got != replicatedstate.ResultApplied {
		t.Fatalf("final-valid LWW result = %d", got)
	}
	nextIndex++
	nextSequence++
	lwwRefused := testReplicatedApplyCommand(base, nextSequence,
		replication.Mutation{Kind: replication.MutationPut, Key: lwwKey, Value: lwwDocument},
		replication.Mutation{Kind: replication.MutationPut, Key: lwwKey, Value: []byte(`{"id":"wrong"}`)},
	)
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(nextIndex), lwwRefused); err != nil {
		t.Fatalf("apply final-invalid LWW refusal: %v", err)
	}
	if got := completionResultCode(t, claim, lwwRefused); got != replicatedstate.ResultInvalidDocument {
		t.Fatalf("final-invalid LWW result = %d", got)
	}
	nextIndex++

	deletePresent := testReplicatedApplyCommand(base, nextSequence+1, replication.Mutation{
		Kind: replication.MutationDelete, Key: validKey,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(nextIndex), deletePresent); err != nil {
		t.Fatalf("apply present DELETE: %v", err)
	}
	if got := completionResultCode(t, claim, deletePresent); got != replicatedstate.ResultApplied {
		t.Fatalf("present DELETE result = %d", got)
	}
	finalApplied := nextIndex

	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatalf("read session during apply claim: %v", err)
	}
	if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"direct"}`)}); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("direct INSERT = %v, want fenced", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("close apply claim: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close activated root: %v", err)
	}

	if db, err := OpenReplicatedShardStore(path, base); !errors.Is(err, ErrReplicatedApplyMismatch) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("base-only open = %v, want apply mismatch", err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatalf("exact activated open: %v", err)
	}
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || reopenedIdentity != identity {
		t.Fatalf("reopen claim = %+v,%v; want %+v", reopenedIdentity, err, identity)
	}
	if reopenedClaim.Applied() != finalApplied {
		t.Fatalf("reopened Applied = %d, want %d", reopenedClaim.Applied(), finalApplied)
	}
	if got := completionResultCode(t, reopenedClaim, valid); got != replicatedstate.ResultApplied {
		t.Fatalf("reopened completion result = %d", got)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyPlacementRangeAndAdmissionParity(t *testing.T) {
	below, inside, upper := func() (
		replicatedPlacementProbe, replicatedPlacementProbe, replicatedPlacementProbe,
	) {
		probes := testReplicatedPlacementProbes(t)
		return probes[0], probes[1], probes[2]
	}()
	_, database, base := bindReplicatedApplyTestRoot(t, "placement-range")
	options := testReplicatedApplyOptions()
	options.Placement.Range = distribution.KeyRange{
		Start: inside.point, End: distribution.KeyspaceEnd{Point: upper.point},
	}
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = claim.Close()
		_ = database.Close()
	}()
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}

	apply := func(index, sequence uint64, probe replicatedPlacementProbe, kind replication.MutationKind) []byte {
		t.Helper()
		mutation := replication.Mutation{Kind: kind, Key: probe.key}
		if kind == replication.MutationPut {
			mutation.Value = probe.document
		}
		command := testReplicatedApplyCommand(base, sequence, mutation)
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(index), command); err != nil {
			t.Fatalf("ApplyNormal(%s): %v", probe.id, err)
		}
		return command
	}

	insidePut := testReplicatedApplyCommand(base, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: inside.key, Value: inside.document,
	})
	if err := claim.AdmitCommand(insidePut); err != nil {
		t.Fatalf("admit range start: %v", err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(2), insidePut); err != nil {
		t.Fatal(err)
	}
	if code, format := completionResult(t, claim, insidePut); code != replicatedstate.ResultApplied || format != replicatedstate.ResultFormatMutationV2 {
		t.Fatalf("range start completion = code %d format %d", code, format)
	}

	for i, probe := range []replicatedPlacementProbe{below, upper} {
		sequence := uint64(i + 2)
		mutations := []replication.Mutation{{
			Kind: replication.MutationPut, Key: probe.key, Value: probe.document,
		}}
		if i == 1 {
			// Placement validation observes the final collapsed mutation. An
			// invalid earlier value cannot mask the final wrong-shard value.
			mutations = append([]replication.Mutation{{
				Kind: replication.MutationPut, Key: probe.key, Value: []byte(`{"id":"mismatch"}`),
			}}, mutations...)
		}
		command := testReplicatedApplyCommand(base, sequence, mutations...)
		if err := claim.AdmitCommand(command); !errors.Is(err, replicatedstate.ErrAdmissionBound) {
			t.Fatalf("admit wrong-shard %s = %v", probe.id, err)
		}
		if _, err := claim.ApplyNormal(testReplicatedApplyMeta(sequence+1), command); err != nil {
			t.Fatal(err)
		}
		if code, format := completionResult(t, claim, command); code != replicatedstate.ResultWrongShard || format != replicatedstate.ResultFormatMutationV2 {
			t.Fatalf("wrong-shard %s completion = code %d format %d", probe.id, code, format)
		}
	}

	absentUpper := testReplicatedApplyCommand(base, 4, replication.Mutation{
		Kind: replication.MutationDelete, Key: upper.key,
	})
	if err := claim.AdmitCommand(absentUpper); !errors.Is(err, replicatedstate.ErrAdmissionBound) {
		t.Fatalf("admit absent wrong-shard DELETE = %v", err)
	}
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(5), absentUpper); err != nil {
		t.Fatal(err)
	}
	if code, format := completionResult(t, claim, absentUpper); code != replicatedstate.ResultWrongShard || format != replicatedstate.ResultFormatMutationV2 {
		t.Fatalf("absent wrong-shard DELETE = code %d format %d", code, format)
	}

	presentInside := apply(6, 5, inside, replication.MutationDelete)
	if code := completionResultCode(t, claim, presentInside); code != replicatedstate.ResultApplied {
		t.Fatalf("present in-range DELETE = %d", code)
	}
	validator := newReplicatedSQLMutationValidator(
		base, database.connector.db.tables["docs"], options.Placement,
	)
	if got := validator.ValidateDelete(upper.key, upper.document, true); got != replicatedstate.MutationValidationWrongShard {
		t.Fatalf("pure present wrong-shard DELETE validation = %d", got)
	}

	// A forbidden out-of-band row is never followed by another apply. Snapshot
	// must independently re-route the complete logical image and poison closed.
	if _, err := database.connector.db.tables["docs"].collection.Put(upper.key, upper.document); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := claim.machine.Snapshot("docs"); snapshot != nil ||
		!errors.Is(err, replicatedstate.ErrSchemaProfile) {
		if snapshot != nil {
			_ = snapshot.Close()
		}
		t.Fatalf("snapshot wrong-shard scan = %p,%v", snapshot, err)
	}
}

func TestReplicatedSQLPlacementValidatorEscapedStringAndClosedScalarSet(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "placement-validator")
	defer database.Close()
	table := database.connector.db.tables["docs"]
	profile := testReplicatedApplyOptions().Placement
	validator := newReplicatedSQLMutationValidator(base, table, profile)

	escaped := []byte(`{"id":"quote\\\" slash\\\\ line\\n"}`)
	escapedKey, err := documentKey(escaped, "/id", table.primary, base.UserLimits.MaxKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got := validator.ValidatePut([]byte(escapedKey), escaped); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("escaped shard-key PUT = %d", got)
	}
	if got := validator.ValidateDelete([]byte(escapedKey), escaped, true); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("escaped present DELETE = %d", got)
	}

	boolean := []byte(`{"id":true}`)
	booleanKey, err := documentKey(boolean, "/id", table.primary, base.UserLimits.MaxKeyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if got := validator.ValidatePut([]byte(booleanKey), boolean); got != replicatedstate.MutationValidationInvalid {
		t.Fatalf("boolean placement scalar = %d, want invalid", got)
	}
}

func TestReplicatedSQLPlacementValidatorCanonicalNumberParity(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "placement-number")
	defer database.Close()
	table := database.connector.db.tables["docs"]
	number, err := distribution.NewNumber("1")
	if err != nil {
		t.Fatal(err)
	}
	point, err := distribution.NewNativeMapper(1).PointFor([]distribution.Scalar{number})
	if err != nil {
		t.Fatal(err)
	}
	profile := testReplicatedApplyOptions().Placement
	profile.Range = distribution.KeyRange{
		Start: point, End: distribution.KeyspaceEnd{Max: true},
	}
	validator := newReplicatedSQLMutationValidator(base, table, profile)

	documents := [][]byte{[]byte(`{"id":1}`), []byte(`{"id":1.0}`), []byte(`{"id":1e0}`)}
	var canonicalKey []byte
	for _, document := range documents {
		key, err := documentKey(document, "/id", table.primary, base.UserLimits.MaxKeyBytes)
		if err != nil {
			t.Fatalf("documentKey(%s): %v", document, err)
		}
		if canonicalKey == nil {
			canonicalKey = []byte(key)
		} else if !bytes.Equal(canonicalKey, []byte(key)) {
			t.Fatalf("number spelling %s encoded key %x, want %x", document, key, canonicalKey)
		}
		if got := validator.ValidatePut([]byte(key), document); got != replicatedstate.MutationValidationAccept {
			t.Fatalf("number spelling %s PUT = %d", document, got)
		}
		if got := validator.ValidateDelete([]byte(key), document, true); got != replicatedstate.MutationValidationAccept {
			t.Fatalf("number spelling %s present DELETE = %d", document, got)
		}
	}
	if got := validator.ValidateDelete(canonicalKey, nil, false); got != replicatedstate.MutationValidationAccept {
		t.Fatalf("canonical absent number DELETE = %d", got)
	}
}

func TestReplicatedApplySettlementAndStrictIdentity(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "settlement")
	options := testReplicatedApplyOptions()
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	settled, got, err := OpenReplicatedShardStoreWithApplyForSettlement(path, base, options)
	if err != nil || got != identity {
		t.Fatalf("settlement = %+v,%v; want %+v", got, err, identity)
	}
	if err := settled.Close(); err != nil {
		t.Fatal(err)
	}
	wrongOptions := options
	wrongOptions.MaxCompletions++
	if db, _, err := OpenReplicatedShardStoreWithApplyForSettlement(
		path, base, wrongOptions,
	); !errors.Is(err, ErrReplicatedApplyMismatch) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("wrong settlement options = %v, want mismatch", err)
	}
	wrongIdentity := identity
	wrongIdentity.Storage = "0" + wrongIdentity.Storage[1:]
	if wrongIdentity.Storage == identity.Storage {
		wrongIdentity.Storage = "1" + wrongIdentity.Storage[1:]
	}
	if db, err := OpenReplicatedShardStoreWithApply(path, base, wrongIdentity); !errors.Is(
		err, ErrReplicatedApplyMismatch,
	) {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("wrong exact identity = %v, want mismatch", err)
	}

	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip ReplicatedApplyIdentity
	if err := json.Unmarshal(raw, &roundTrip); err != nil || roundTrip != identity {
		t.Fatalf("identity JSON roundtrip = %+v,%v", roundTrip, err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatal(err)
	}
	object["unknown"] = json.RawMessage("1")
	corrupt, _ := json.Marshal(object)
	if err := json.Unmarshal(corrupt, &roundTrip); err == nil {
		t.Fatal("strict identity decoder accepted an unknown member")
	}

	missingPath := filepath.Join(path+".tables", identity.Storage+".vjc")
	if err := os.Remove(missingPath); err != nil {
		t.Fatalf("remove hidden store: %v", err)
	}
	if db, err := OpenReplicatedShardStoreWithApply(path, base, identity); err == nil {
		if db != nil {
			_ = db.Close()
		}
		t.Fatal("exact open accepted a missing hidden store")
	}
}

func TestReplicatedApplyActivationPublicationSettlement(t *testing.T) {
	t.Run("definite cleanup", func(t *testing.T) {
		_, db, base := bindReplicatedApplyTestRoot(t, "definite-apply")
		options := testReplicatedApplyOptions()
		claim, identity, err := db.openReplicatedApply(
			base, testReplicatedApplyBootstrap(), options,
			func(*database) (bool, error) {
				return false, errors.New("injected definite catalog failure")
			},
		)
		if claim != nil || identity != (ReplicatedApplyIdentity{}) || err == nil {
			t.Fatalf("definite activation = %p,%+v,%v", claim, identity, err)
		}
		core := db.connector.db
		core.mu.RLock()
		if core.catalog.ReplicatedApply != nil || core.replicatedApplyCollection != nil ||
			core.replicatedApplyFile != nil {
			core.mu.RUnlock()
			t.Fatal("definite failure retained a published apply descriptor or handle")
		}
		core.mu.RUnlock()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("unknown exact retry", func(t *testing.T) {
		_, db, base := bindReplicatedApplyTestRoot(t, "unknown-apply")
		options := testReplicatedApplyOptions()
		claim, identity, err := db.openReplicatedApply(
			base, testReplicatedApplyBootstrap(), options,
			func(*database) (bool, error) {
				return false, durable.ErrCommitOutcomeUnknown
			},
		)
		if claim != nil || identity == (ReplicatedApplyIdentity{}) ||
			!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			t.Fatalf("unknown activation = %p,%+v,%v", claim, identity, err)
		}
		claim, retried, err := db.OpenReplicatedApply(
			base, testReplicatedApplyBootstrap(), options,
		)
		if err != nil || claim == nil || retried != identity {
			t.Fatalf("exact retry = %p,%+v,%v; want %+v", claim, retried, err, identity)
		}
		if err := claim.Close(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestReplicatedApplyRetainsIncompleteReopenOwnership(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "retry-close")
	core := database.connector.db
	injected := errors.New("injected incomplete final-store close")
	closeCalls := 0
	core.closeCollection = func(collection *durable.Collection) error {
		closeCalls++
		if closeCalls == 2 {
			return injected
		}
		return collection.Close()
	}
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if claim != nil || identity != (ReplicatedApplyIdentity{}) || !errors.Is(err, injected) {
		t.Fatalf("activation with retryable close = %p,%+v,%v", claim, identity, err)
	}
	core.mu.RLock()
	if len(core.retired) != 1 || core.retired[0].collection == nil ||
		core.retired[0].file == nil || core.catalog.ReplicatedApply != nil {
		core.mu.RUnlock()
		t.Fatalf("incomplete reopen ownership = %+v", core.retired)
	}
	retainedPath := core.retired[0].path
	retainedFile := core.retired[0].file
	core.mu.RUnlock()
	if _, err := retainedFile.Stat(); err != nil {
		t.Fatalf("retained descriptor: %v", err)
	}
	if _, err := os.Stat(retainedPath); err != nil {
		t.Fatalf("retained final path: %v", err)
	}
	core.closeCollection = nil
	claim, identity, err = database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil || claim == nil || identity.Storage == "" {
		t.Fatalf("activation after close retry = %p,%+v,%v", claim, identity, err)
	}
	if _, err := os.Stat(retainedPath); !os.IsNotExist(err) {
		t.Fatalf("retired unpublished path remains: %v", err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyPreflightAndPreRecoveryFences(t *testing.T) {
	t.Run("reserved name before mutation", func(t *testing.T) {
		_, database, base := bindReplicatedApplyTestRoot(t, "reserved-apply")
		reserved := base
		reserved.UserTable = replicatedstate.SystemCollectionNameV1
		claim, identity, err := database.OpenReplicatedApply(
			reserved, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
		)
		if claim != nil || identity != (ReplicatedApplyIdentity{}) ||
			!errors.Is(err, ErrReplicatedApplyMismatch) {
			t.Fatalf("reserved activation = %p,%+v,%v", claim, identity, err)
		}
		core := database.connector.db
		core.mu.RLock()
		defer core.mu.RUnlock()
		if core.catalog.ReplicatedApply != nil || core.replicatedApplyCollection != nil ||
			core.replicatedApplyFile != nil {
			t.Fatal("reserved name mutated apply catalog or storage ownership")
		}
	})

	t.Run("mismatch before recovery", func(t *testing.T) {
		path, database, base := bindReplicatedApplyTestRoot(t, "pre-recovery")
		options := testReplicatedApplyOptions()
		claim, identity, err := database.OpenReplicatedApply(
			base, testReplicatedApplyBootstrap(), options,
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := claim.Close(); err != nil {
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}

		syncCalls := 0
		wrong := identity
		wrong.ValidationDigest[0] ^= 0x80
		opened, err := openDatabaseWithShardStorePolicy(path, func(string) error {
			syncCalls++
			return nil
		}, shardStoreOpenPolicy{
			mode:                    shardStoreOpenReplicatedApplyExisting,
			expectedReplicated:      base,
			expectedReplicatedApply: wrong,
		})
		if opened != nil {
			_ = opened.closeTerminal()
		}
		if !errors.Is(err, ErrReplicatedApplyMismatch) || syncCalls != 0 {
			t.Fatalf("exact mismatch = %v, sync calls=%d", err, syncCalls)
		}

		identityMismatches := []struct {
			name   string
			mutate func(*ReplicatedApplyIdentity, *ReplicatedShardStoreIdentity)
		}{
			{"placement format", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.Format++
			}},
			{"tuple version", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.TupleVersion++
			}},
			{"mapper version", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.MapperVersion++
			}},
			{"shard key", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.ShardKey += "/other"
			}},
			{"placement range", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.Range.Start[0] = 1
			}},
			{"placement range end", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				apply.Placement.Range.End.Point[7] = 1
			}},
			{"placement range end max", func(apply *ReplicatedApplyIdentity, _ *ReplicatedShardStoreIdentity) {
				for i := range apply.Placement.Range.End.Point {
					apply.Placement.Range.End.Point[i] = 0xff
				}
				apply.Placement.Range.End.Max = false
			}},
			{"allocation generation", func(_ *ReplicatedApplyIdentity, base *ReplicatedShardStoreIdentity) {
				base.Binding.AllocationGeneration++
			}},
			{"routing version", func(_ *ReplicatedApplyIdentity, base *ReplicatedShardStoreIdentity) {
				base.Binding.Authority.RoutingVersion++
			}},
			{"route generation", func(_ *ReplicatedApplyIdentity, base *ReplicatedShardStoreIdentity) {
				base.Binding.Authority.RouteGeneration++
			}},
		}
		for _, test := range identityMismatches {
			t.Run(test.name, func(t *testing.T) {
				wrongApply, wrongBase := identity, base
				test.mutate(&wrongApply, &wrongBase)
				syncCalls = 0
				opened, err := openDatabaseWithShardStorePolicy(path, func(string) error {
					syncCalls++
					return nil
				}, shardStoreOpenPolicy{
					mode:                    shardStoreOpenReplicatedApplyExisting,
					expectedReplicated:      wrongBase,
					expectedReplicatedApply: wrongApply,
				})
				if opened != nil {
					_ = opened.closeTerminal()
				}
				if err == nil || syncCalls != 0 {
					t.Fatalf("pre-recovery mismatch = %v, sync calls=%d", err, syncCalls)
				}
			})
		}

		syncCalls = 0
		wrongOptions := options
		wrongOptions.MaxCompletions++
		opened, err = openDatabaseWithShardStorePolicy(path, func(string) error {
			syncCalls++
			return nil
		}, shardStoreOpenPolicy{
			mode:                      shardStoreOpenReplicatedApplySettlement,
			expectedReplicated:        base,
			expectedReplicatedOptions: wrongOptions,
		})
		if opened != nil {
			_ = opened.closeTerminal()
		}
		if !errors.Is(err, ErrReplicatedApplyMismatch) || syncCalls != 0 {
			t.Fatalf("settlement mismatch = %v, sync calls=%d", err, syncCalls)
		}

		syncCalls = 0
		wrongOptions = options
		wrongOptions.Placement.Range.Start[0] = 1
		opened, err = openDatabaseWithShardStorePolicy(path, func(string) error {
			syncCalls++
			return nil
		}, shardStoreOpenPolicy{
			mode:                      shardStoreOpenReplicatedApplySettlement,
			expectedReplicated:        base,
			expectedReplicatedOptions: wrongOptions,
		})
		if opened != nil {
			_ = opened.closeTerminal()
		}
		if !errors.Is(err, ErrReplicatedApplyMismatch) || syncCalls != 0 {
			t.Fatalf("settlement placement mismatch = %v, sync calls=%d", err, syncCalls)
		}
	})
}

func TestReplicatedApplyOpenFullScanRejectsPrimaryMismatch(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "full-scan")
	options := testReplicatedApplyOptions()
	bootstrap := testReplicatedApplyBootstrap()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"id":"scan"}`)
	key := testReplicatedApplyKey(t, database, document)
	command := testReplicatedApplyCommand(base, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(2), command); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a forbidden out-of-band mutation to an individually valid JSON
	// row whose document primary no longer matches its physical key.
	if _, err := database.connector.db.tables["docs"].collection.Put(
		key, []byte(`{"id":"other"}`),
	); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatalf("exact storage open: %v", err)
	}
	badClaim, _, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if badClaim != nil {
		_ = badClaim.Close()
	}
	if err == nil {
		t.Fatal("Machine Open accepted a key/document primary mismatch")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyOpenFullScanRejectsWrongShard(t *testing.T) {
	probes := testReplicatedPlacementProbes(t)
	inside, upper := probes[1], probes[2]
	path, database, base := bindReplicatedApplyTestRoot(t, "full-scan-placement")
	options := testReplicatedApplyOptions()
	options.Placement.Range = distribution.KeyRange{
		Start: inside.point, End: distribution.KeyspaceEnd{Point: upper.point},
	}
	bootstrap := testReplicatedApplyBootstrap()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.connector.db.tables["docs"].collection.Put(upper.key, upper.document); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	badClaim, _, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if badClaim != nil {
		_ = badClaim.Close()
	}
	if !errors.Is(err, replicatedstate.ErrSchemaProfile) {
		t.Fatalf("wrong-shard full scan = %v, want schema-profile refusal", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyIdentityStrictGrammar(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "identity-grammar")
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	defer database.Close()
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		raw  []byte
	}{
		{"duplicate", bytes.Replace(raw, []byte(`{"format":`), []byte(`{"format":1,"format":`), 1)},
		{"null_digest", bytes.Replace(raw, fields["validation_digest"], []byte("null"), 1)},
		{"uppercase_digest", bytes.Replace(raw, fields["validation_digest"], bytes.ToUpper(fields["validation_digest"]), 1)},
		{"null_placement", bytes.Replace(raw, fields["placement"], []byte("null"), 1)},
		{"nested_duplicate", bytes.Replace(raw, []byte(`"placement":{"format":`), []byte(`"placement":{"format":1,"format":`), 1)},
		{"nested_unknown", bytes.Replace(raw, []byte(`"range_end_max":true`), []byte(`"unknown":0,"range_end_max":true`), 1)},
	}
	missing := make(map[string]json.RawMessage, len(fields)-1)
	for name, value := range fields {
		if name != "txn_max_bytes" {
			missing[name] = value
		}
	}
	missingRaw, _ := json.Marshal(missing)
	tests = append(tests, struct {
		name string
		raw  []byte
	}{"missing", missingRaw})
	missingPlacement := make(map[string]json.RawMessage, len(fields)-1)
	for name, value := range fields {
		if name != "placement" {
			missingPlacement[name] = value
		}
	}
	missingPlacementRaw, _ := json.Marshal(missingPlacement)
	tests = append(tests, struct {
		name string
		raw  []byte
	}{"v2_missing_placement", missingPlacementRaw})

	var placementFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["placement"], &placementFields); err != nil {
		t.Fatal(err)
	}
	placementCases := []struct {
		name   string
		mutate func(map[string]json.RawMessage)
	}{
		{"nested_missing", func(values map[string]json.RawMessage) { delete(values, "mapper_version") }},
		{"profile_format", func(values map[string]json.RawMessage) { values["format"] = []byte("2") }},
		{"profile_shard_key", func(values map[string]json.RawMessage) { values["shard_key"] = []byte(`"id"`) }},
		{"profile_tuple_version", func(values map[string]json.RawMessage) { values["tuple_version"] = []byte("2") }},
		{"profile_mapper_version", func(values map[string]json.RawMessage) { values["mapper_version"] = []byte("2") }},
		{"uppercase_start", func(values map[string]json.RawMessage) {
			values["range_start"] = []byte(`"ABCDEF0000000000"`)
		}},
		{"short_end", func(values map[string]json.RawMessage) { values["range_end"] = []byte(`"00"`) }},
		{"empty_range", func(values map[string]json.RawMessage) { values["range_end_max"] = []byte("false") }},
		{"noncanonical_max_end", func(values map[string]json.RawMessage) {
			values["range_end"] = []byte(`"0000000000000001"`)
		}},
	}
	for _, test := range placementCases {
		placementCopy := make(map[string]json.RawMessage, len(placementFields))
		for name, value := range placementFields {
			placementCopy[name] = bytes.Clone(value)
		}
		test.mutate(placementCopy)
		placementRaw, _ := json.Marshal(placementCopy)
		identityCopy := make(map[string]json.RawMessage, len(fields))
		for name, value := range fields {
			identityCopy[name] = bytes.Clone(value)
		}
		identityCopy["placement"] = placementRaw
		caseRaw, _ := json.Marshal(identityCopy)
		tests = append(tests, struct {
			name string
			raw  []byte
		}{test.name, caseRaw})
	}
	v1WithPlacement := make(map[string]json.RawMessage, len(fields))
	for name, value := range fields {
		v1WithPlacement[name] = bytes.Clone(value)
	}
	v1WithPlacement["format"] = []byte("1")
	v1WithPlacementRaw, _ := json.Marshal(v1WithPlacement)
	tests = append(tests, struct {
		name string
		raw  []byte
	}{"v1_forbids_placement", v1WithPlacementRaw})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var decoded ReplicatedApplyIdentity
			if err := json.Unmarshal(test.raw, &decoded); err == nil {
				t.Fatalf("accepted noncanonical identity: %s", test.raw)
			}
		})
	}
}

func TestReplicatedApplyClaimConnectorLifetime(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "claim-lifetime")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	); !errors.Is(err, ErrReplicatedApplyBusy) {
		t.Fatalf("second claim with live session = %v, want busy", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := claim.Identity(); err != nil {
		t.Fatalf("claim invalidated while connector refs remain: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatalf("idempotent claim Close: %v", err)
	}
}

func TestReplicatedApplyObserverConservativelyPublishesUnknownOutcome(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "observer-unknown")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = claim.Close()
		_ = database.Close()
	})
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"id":"unknown"}`)
	key := testReplicatedApplyKey(t, database, document)
	command := testReplicatedApplyCommand(base, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: key, Value: document,
	})
	clock := &database.connector.db.tables["docs"].conflicts
	before := clock.observe()
	restore := durable.InstallTxnMarkerSyncFaultForFacadeTest()
	defer restore()
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(2), command); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("decision-sync apply = %v, want unknown outcome", err)
	}
	if after := clock.observe(); after <= before {
		t.Fatalf("unknown publication did not advance conflict clock: before=%d after=%d",
			before, after)
	}
}

func TestReplicatedApplyRetainsHiddenIncompleteClose(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "hidden-close")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	core := database.connector.db
	hidden := core.replicatedApplyCollection
	hiddenFile := core.replicatedApplyFile
	injected := errors.New("injected retryable hidden close")
	failed := false
	core.closeCollection = func(collection *durable.Collection) error {
		if collection == hidden && !failed {
			failed = true
			return injected
		}
		return collection.Close()
	}
	if err := core.close(); !errors.Is(err, injected) {
		t.Fatalf("first close = %v, want injected retryable error", err)
	}
	if core.closeCompleted() || core.replicatedApplyCollection != hidden ||
		core.replicatedApplyFile != hiddenFile {
		t.Fatal("retryable hidden close dropped ownership")
	}
	if _, err := hiddenFile.Stat(); err != nil {
		t.Fatalf("retained hidden descriptor: %v", err)
	}
	core.closeCollection = nil
	if err := core.close(); err != nil {
		t.Fatalf("retry hidden close: %v", err)
	}
	if !core.closeCompleted() || core.replicatedApplyCollection != nil ||
		core.replicatedApplyFile != nil {
		t.Fatal("successful hidden close retained ownership")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyConcurrentClaimRetirement(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "claim-race")
	claim, _, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"id":"race"}`)
	command := testReplicatedApplyCommand(base, 1, replication.Mutation{
		Kind: replication.MutationPut,
		Key:  testReplicatedApplyKey(t, database, document), Value: document,
	})
	start := make(chan struct{})
	errs := make(chan error, 4)
	var workers sync.WaitGroup
	workers.Add(4)
	go func() {
		defer workers.Done()
		<-start
		_, applyErr := claim.ApplyNormal(testReplicatedApplyMeta(2), command)
		if applyErr != nil && !errors.Is(applyErr, ErrReplicatedApplyClosed) {
			errs <- applyErr
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		session, sessionErr := database.NewSession(context.Background())
		if sessionErr == nil {
			sessionErr = session.Close()
		}
		if sessionErr != nil && !errors.Is(sessionErr, ErrDatabaseClosed) {
			errs <- sessionErr
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		if closeErr := database.Close(); closeErr != nil {
			errs <- closeErr
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		if closeErr := claim.Close(); closeErr != nil {
			errs <- closeErr
		}
	}()
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent retirement: %v", err)
	}
	_ = claim.Close()
	_ = database.Close()
}

func TestReplicatedApplyProfileDigestV1Golden(t *testing.T) {
	identity := ReplicatedShardStoreIdentity{
		UserTable: "docs", UserPrimaryKey: "/id",
		UserLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: 123, MaxDocumentBytes: 456,
			MaxBatchDocuments: 7, MaxBatchBytes: 890,
		},
	}
	got := replicatedApplyProfileDigest(identity)
	if gotHex := hex.EncodeToString(got[:]); gotHex != "7ad36d42b2e030a1483d4e34b92a1a21e375eae6a28d68c02b745c3644def697" {
		t.Fatalf("profile digest = %s", gotHex)
	}
}

func TestReplicatedApplyProfileDigestV2GoldenAndBindings(t *testing.T) {
	identity := ReplicatedShardStoreIdentity{
		Binding: ReplicatedShardStoreBinding{
			Distribution: "dist", Shard: "shard", AllocationGeneration: 11,
			MemberID: 19, StoreID: [16]byte{20},
			Authority: ReplicatedAuthorityProfile{RoutingVersion: 12, RouteGeneration: 13},
		},
		LogID: [16]byte{21}, UserTable: "docs", UserStorage: "local-storage",
		UserPrimaryKey: "/id",
		UserLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: 123, MaxDocumentBytes: 456,
			MaxBatchDocuments: 7, MaxBatchBytes: 890,
		},
	}
	placement := ReplicatedPlacementProfile{
		Format: ReplicatedPlacementProfileV1, ShardKey: "/id",
		TupleVersion: distribution.TupleVersion1, MapperVersion: distribution.NativeMapperVersion,
		Range: distribution.KeyRange{
			Start: distribution.KeyspacePoint{0x10},
			End:   distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x90}},
		},
	}
	got := replicatedApplyProfileDigestV2(identity, placement)
	const wantDigest = "89d75e7034935dd12b6b1a25d35da064a8a8b81e901fa5a1d89be0916de09587"
	if gotHex := hex.EncodeToString(got[:]); gotHex != wantDigest {
		t.Fatalf("v2 profile digest = %s, want %s", gotHex, wantDigest)
	}

	boundMutations := []func(*ReplicatedShardStoreIdentity, *ReplicatedPlacementProfile){
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) { i.Binding.Distribution += "x" },
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) { i.Binding.Shard += "x" },
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) { i.Binding.AllocationGeneration++ },
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) {
			i.Binding.Authority.RoutingVersion++
		},
		func(i *ReplicatedShardStoreIdentity, _ *ReplicatedPlacementProfile) {
			i.Binding.Authority.RouteGeneration++
		},
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.Format++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.ShardKey += "x" },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.TupleVersion++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.MapperVersion++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.Range.Start[7]++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.Range.End.Point[7]++ },
		func(_ *ReplicatedShardStoreIdentity, p *ReplicatedPlacementProfile) { p.Range.End.Max = true },
	}
	for index, mutate := range boundMutations {
		changedIdentity, changedPlacement := identity, placement
		mutate(&changedIdentity, &changedPlacement)
		if digest := replicatedApplyProfileDigestV2(changedIdentity, changedPlacement); digest == got {
			t.Fatalf("bound mutation %d did not change digest", index)
		}
	}
	localMutations := []func(*ReplicatedShardStoreIdentity){
		func(i *ReplicatedShardStoreIdentity) { i.Binding.MemberID++ },
		func(i *ReplicatedShardStoreIdentity) { i.Binding.StoreID[0]++ },
		func(i *ReplicatedShardStoreIdentity) { i.LogID[0]++ },
		func(i *ReplicatedShardStoreIdentity) { i.UserStorage += "x" },
	}
	for index, mutate := range localMutations {
		changed := identity
		mutate(&changed)
		if digest := replicatedApplyProfileDigestV2(changed, placement); digest != got {
			t.Fatalf("member-local mutation %d changed digest: %x != %x", index, digest, got)
		}
	}
}

func TestReplicatedApplyIdentityV1JSONGolden(t *testing.T) {
	var validationDigest [32]byte
	for index := range validationDigest {
		validationDigest[index] = byte(index)
	}
	identity := ReplicatedApplyIdentity{
		Format: ReplicatedApplyFormatV1, Storage: "storage-v1",
		ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutationV1),
		ValidationDigest:  validationDigest,
		SystemLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: 1, MaxDocumentBytes: 2, MaxBatchDocuments: 3, MaxBatchBytes: 4,
		},
		MaxCompletions: 5,
		TxnLimits:      durable.TxnLimits{MaxCollections: 6, MaxDocuments: 7, MaxBytes: 8},
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"format":1,"storage":"storage-v1","validation_profile":2,"validation_digest":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f","system_limits":{"max_key_bytes":1,"max_document_bytes":2,"max_batch_documents":3,"max_batch_bytes":4},"max_completions":5,"txn_max_collections":6,"txn_max_documents":7,"txn_max_bytes":8}`
	if string(encoded) != want {
		t.Fatalf("v1 identity JSON = %s, want %s", encoded, want)
	}
	metaEncoded, err := json.Marshal(replicatedApplyMetaFromIdentity(identity))
	if err != nil || string(metaEncoded) != want {
		t.Fatalf("v1 catalog meta JSON = %s,%v, want %s", metaEncoded, err, want)
	}
	if bytes.Contains(encoded, []byte("placement")) {
		t.Fatalf("v1 identity JSON gained placement: %s", encoded)
	}
	var decoded ReplicatedApplyIdentity
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != identity {
		t.Fatalf("v1 identity round trip = %+v,%v", decoded, err)
	}
}

func TestReplicatedApplyIdentityV2JSONGolden(t *testing.T) {
	var validationDigest [32]byte
	for index := range validationDigest {
		validationDigest[index] = byte(index)
	}
	identity := ReplicatedApplyIdentity{
		Format: ReplicatedApplyFormatV2, Storage: "storage-v2",
		ValidationProfile: uint8(replicatedstate.ValidationDeterministicMutationV2),
		ValidationDigest:  validationDigest,
		SystemLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: 1, MaxDocumentBytes: 2, MaxBatchDocuments: 3, MaxBatchBytes: 4,
		},
		MaxCompletions: 5,
		TxnLimits:      durable.TxnLimits{MaxCollections: 6, MaxDocuments: 7, MaxBytes: 8},
		Placement: ReplicatedPlacementProfile{
			Format: ReplicatedPlacementProfileV1, ShardKey: "/id",
			TupleVersion:  distribution.TupleVersion1,
			MapperVersion: distribution.NativeMapperVersion,
			Range: distribution.KeyRange{
				Start: distribution.KeyspacePoint{0x10},
				End:   distribution.KeyspaceEnd{Point: distribution.KeyspacePoint{0x90}},
			},
		},
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"format":2,"storage":"storage-v2","validation_profile":3,"validation_digest":"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f","system_limits":{"max_key_bytes":1,"max_document_bytes":2,"max_batch_documents":3,"max_batch_bytes":4},"max_completions":5,"txn_max_collections":6,"txn_max_documents":7,"txn_max_bytes":8,"placement":{"format":1,"shard_key":"/id","tuple_version":1,"mapper_version":1,"range_start":"1000000000000000","range_end":"9000000000000000","range_end_max":false}}`
	if string(encoded) != want {
		t.Fatalf("v2 identity JSON = %s, want %s", encoded, want)
	}
	metaEncoded, err := json.Marshal(replicatedApplyMetaFromIdentity(identity))
	if err != nil || string(metaEncoded) != want {
		t.Fatalf("v2 catalog meta JSON = %s,%v, want %s", metaEncoded, err, want)
	}
	var decoded ReplicatedApplyIdentity
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != identity {
		t.Fatalf("v2 identity round trip = %+v,%v", decoded, err)
	}
}

func TestReplicatedApplyLegacyV1ActivationAndExactReopen(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "legacy-v1")
	options := testReplicatedApplyOptionsV1()
	bootstrap := testReplicatedApplyBootstrap()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Format != ReplicatedApplyFormatV1 || identity.Placement != (ReplicatedPlacementProfile{}) ||
		identity.ValidationProfile != uint8(replicatedstate.ValidationDeterministicMutationV1) {
		t.Fatalf("legacy identity = %+v", identity)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"id":"legacy"}`)
	command := testReplicatedApplyCommand(base, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: testReplicatedApplyKey(t, database, document), Value: document,
	})
	if _, err := claim.ApplyNormal(testReplicatedApplyMeta(2), command); err != nil {
		t.Fatal(err)
	}
	if code, format := completionResult(t, claim, command); code != replicatedstate.ResultApplied || format != replicatedstate.ResultFormatMutationV1 {
		t.Fatalf("legacy completion = code %d format %d", code, format)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	reopenedClaim, actual, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || actual != identity {
		t.Fatalf("legacy exact reopen = %p,%+v,%v", reopenedClaim, actual, err)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyPlacementOptionsFailClosed(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "placement-options")
	defer database.Close()
	valid := testReplicatedApplyOptions()
	if err := validateReplicatedApplyOptions(base, valid); err != nil {
		t.Fatalf("valid placement options: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*ReplicatedPlacementProfile)
	}{
		{"format", func(p *ReplicatedPlacementProfile) { p.Format++ }},
		{"shard key", func(p *ReplicatedPlacementProfile) { p.ShardKey += "x" }},
		{"tuple version", func(p *ReplicatedPlacementProfile) { p.TupleVersion++ }},
		{"mapper version", func(p *ReplicatedPlacementProfile) { p.MapperVersion++ }},
		{"empty range", func(p *ReplicatedPlacementProfile) {
			p.Range.End.Max = false
			p.Range.End.Point = p.Range.Start
		}},
		{"noncanonical max", func(p *ReplicatedPlacementProfile) { p.Range.End.Point[7] = 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.mutate(&options.Placement)
			if err := validateReplicatedApplyOptions(base, options); !errors.Is(err, ErrReplicatedApplyMismatch) {
				t.Fatalf("placement options error = %v", err)
			}
		})
	}
}
