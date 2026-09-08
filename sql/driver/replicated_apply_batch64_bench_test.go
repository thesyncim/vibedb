package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

const (
	batch64Rows               = 64
	batch64MeasurementBatches = 2048
	batch64PayloadAlphabet    = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-_"
	batch64TypedSchema        = `CREATE TABLE docs (id TEXT PRIMARY KEY, bucket INTEGER NOT NULL, score INTEGER NOT NULL, payload TEXT NOT NULL)`
	batch64Tenant             = "tenant"
	batch64ExecutionPinDomain = "vibedb/sql/replicated-apply-batch64/pin\x00"
	batch64FingerprintDomain  = "vibedb/sql/replicated-apply-batch64/fingerprint\x00"
	batch64LaneDomain         = "vibedb/sql/replicated-apply-batch64/lane\x00"
)

var batch64RetryHome = replication.RetryHome{'b', 'a', 't', 'c', 'h', '6', '4'}

// BenchmarkReplicatedApplyBatch64SequentialInsert measures the actual SQL
// ReplicatedApply path over its three durable members: hidden system state,
// the typed user relation, and transition capture. It is intentionally a
// bounded diagnostic rather than an RF3/network benchmark. The timed region
// excludes proposal admission, transport, and RF3; it measures bounded row /
// command preparation plus committed ApplyNormalBatchWithCompletions and its final
// full fold. One benchmark operation applies the same fixed 2048 x 64 rows in
// every run; use -benchtime=1x so Go's calibration cannot silently change the
// measured data set.
func BenchmarkReplicatedApplyBatch64SequentialInsert(b *testing.B) {
	if b.N != 1 {
		b.Fatalf("use -benchtime=1x for the bounded measurement, got %d operations", b.N)
	}
	database, claim, identity, group := newReplicatedApplyBatch64Fixture(b)
	beforeDurability, err := claim.DurabilityStats()
	if err != nil {
		b.Fatal(err)
	}
	beforeResources, err := claim.ResourceStats()
	if err != nil {
		b.Fatal(err)
	}
	beforeUser := replicatedApplyBatch64UserStats(b, beforeResources, identity)
	lanes := replicatedApplyBatch64Lanes()
	keys := make([][]byte, batch64Rows)
	values := make([][]byte, batch64Rows)
	mutations := make([]replication.Mutation, batch64Rows)
	command := make([]byte, 0, 128<<10)
	entries := make([]raftmodel.NormalApply, 1)
	witnesses := make([][32]byte, 1)
	var completions raftmodel.NormalApplyBatchCompletions
	latestCompletions := make([][]byte, 0, 16)

	b.ReportAllocs()
	b.ResetTimer()
	appendStarted := time.Now()
	for batch := 0; batch < batch64MeasurementBatches; batch++ {
		if err := fillReplicatedApplyBatch64Rows(database, batch*batch64Rows, keys, values); err != nil {
			b.Fatal(err)
		}
		for row := range mutations {
			mutations[row] = replication.Mutation{
				Kind: replication.MutationPutAbsent,
				Key:  keys[row], Value: values[row],
			}
		}
		lane := lanes[batch%len(lanes)]
		revision := uint64(batch/len(lanes)) + 1
		batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}}
		command, err = appendReplicatedApplyBatch64Command(command[:0], identity, lane, revision, batches)
		if err != nil {
			b.Fatal(err)
		}
		index := uint64(batch + 2)
		entries[0] = raftmodel.NormalApply{Meta: replicatedApplyBatch64Meta(index), Data: command}
		applied, publication, applyErr := claim.ApplyNormalBatchWithCompletions(entries, witnesses, &completions)
		if applyErr != nil || applied != 1 || publication.Applied != index ||
			witnesses[0] == ([32]byte{}) || publication.DataChainDigest != witnesses[0] {
			b.Fatalf("batch %d apply count=%d publication=%+v witness=%x err=%v", batch, applied, publication, witnesses[0], applyErr)
		}
		completion, present := completions.Completion(0)
		if !present || len(completion) == 0 {
			b.Fatalf("batch %d returned no original completion", batch)
		}
		if batch >= batch64MeasurementBatches-16 {
			latestCompletions = append(latestCompletions, append([]byte(nil), completion...))
		}
	}
	appendElapsed := time.Since(appendStarted)
	b.StopTimer()
	afterAppendDurability, err := claim.DurabilityStats()
	if err != nil {
		b.Fatal(err)
	}
	afterAppendResources, err := claim.ResourceStats()
	if err != nil {
		b.Fatal(err)
	}
	afterAppendUser := replicatedApplyBatch64UserStats(b, afterAppendResources, identity)

	for ordinal, completion := range latestCompletions {
		view, openErr := replication.OpenCompletion(completion)
		if openErr != nil {
			b.Fatalf("open completion %d: %v", ordinal, openErr)
		}
		result, resultErr := replicatedstate.OpenTransactionCompletionResult(view.ResultCode, view.InlineResult)
		expectedIndex := uint64(batch64MeasurementBatches - 16 + ordinal + 2)
		if resultErr != nil || view.ResultCode != replicatedstate.ResultApplied ||
			view.AppliedSequence != expectedIndex ||
			!result.AffectedRowsValid || result.AffectedRows != batch64Rows {
			b.Fatalf("completion %d sequence=%d want=%d result=%+v code=%d err=%v", ordinal, view.AppliedSequence, expectedIndex, result, view.ResultCode, resultErr)
		}
	}

	b.StartTimer()
	foldStarted := time.Now()
	if err := group.Checkpoint(); err != nil {
		b.Fatal(err)
	}
	foldElapsed := time.Since(foldStarted)
	b.StopTimer()
	afterFoldDurability, err := claim.DurabilityStats()
	if err != nil {
		b.Fatal(err)
	}
	afterFoldResources, err := claim.ResourceStats()
	if err != nil {
		b.Fatal(err)
	}
	afterFoldUser := replicatedApplyBatch64UserStats(b, afterFoldResources, identity)
	if err := verifyReplicatedApplyBatch64Rows(database, batch64MeasurementBatches*batch64Rows, identity); err != nil {
		b.Fatal(err)
	}

	rows := uint64(batch64MeasurementBatches * batch64Rows)
	wantApplied := uint64(batch64MeasurementBatches + 1)
	if afterFoldDurability.AppliedIndex != wantApplied ||
		afterFoldDurability.CheckpointAppliedIndex != wantApplied ||
		afterFoldDurability.Updates-beforeDurability.Updates != batch64MeasurementBatches {
		b.Fatalf("logical cut after fold = %+v, before=%+v, want applied/checkpoint=%d and %d updates",
			afterFoldDurability, beforeDurability, wantApplied, batch64MeasurementBatches)
	}
	totalElapsed := appendElapsed + foldElapsed
	b.ReportMetric(float64(rows), "rows")
	b.ReportMetric(float64(appendElapsed.Nanoseconds())/float64(rows), "prepare-apply-ns/row")
	b.ReportMetric(float64(foldElapsed.Nanoseconds())/float64(rows), "final-fold-ns/row")
	b.ReportMetric(float64(totalElapsed.Nanoseconds())/float64(rows), "total-ns/row")
	reportReplicatedApplyBatch64GroupMetrics(b, beforeDurability, afterAppendDurability, afterFoldDurability)
	reportReplicatedApplyBatch64UserMetrics(b, beforeUser, afterAppendUser, afterFoldUser)
	reportReplicatedApplyBatch64ResourceMetrics(b, beforeResources, afterAppendResources, afterFoldResources, identity)
	b.Logf("append_batches=%d rows=%d prepare_and_apply=%s final_full_fold=%s total=%s applied=%d checkpoint=%d pending_overlay_records=%d/%d->%d/%d reserved_fold_bytes=%d->%d",
		batch64MeasurementBatches, rows, appendElapsed, foldElapsed, totalElapsed,
		afterFoldDurability.AppliedIndex, afterFoldDurability.CheckpointAppliedIndex,
		afterAppendUser.PrimaryOverlayRetainedRecords, afterAppendUser.PrimaryOverlayDirtyBuckets,
		afterFoldUser.PrimaryOverlayRetainedRecords, afterFoldUser.PrimaryOverlayDirtyBuckets,
		afterAppendUser.PrimaryOverlayReservedFoldBytes, afterFoldUser.PrimaryOverlayReservedFoldBytes)
}

func TestReplicatedApplyBatch64Preflight(t *testing.T) {
	preflightReplicatedApplyBatch64(t)
}

// preflightReplicatedApplyBatch64 runs one real transaction on an independent
// fixture before the long fixed measurement. It keeps completion, full-fold,
// and exact-row oracle mistakes from consuming another full run.
func preflightReplicatedApplyBatch64(tb testing.TB) {
	tb.Helper()
	database, claim, identity, group := newReplicatedApplyBatch64Fixture(tb)
	keys := make([][]byte, batch64Rows)
	values := make([][]byte, batch64Rows)
	mutations := make([]replication.Mutation, batch64Rows)
	if err := fillReplicatedApplyBatch64Rows(database, 0, keys, values); err != nil {
		tb.Fatal(err)
	}
	for row := range mutations {
		mutations[row] = replication.Mutation{Kind: replication.MutationPutAbsent, Key: keys[row], Value: values[row]}
	}
	batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}}
	command, err := appendReplicatedApplyBatch64Command(nil, identity, replicatedApplyBatch64Lanes()[0], 1, batches)
	if err != nil {
		tb.Fatal(err)
	}
	entries := []raftmodel.NormalApply{{Meta: replicatedApplyBatch64Meta(2), Data: command}}
	witnesses := make([][32]byte, 1)
	var completions raftmodel.NormalApplyBatchCompletions
	applied, publication, err := claim.ApplyNormalBatchWithCompletions(entries, witnesses, &completions)
	if err != nil || applied != 1 || publication.Applied != 2 ||
		witnesses[0] == ([32]byte{}) || publication.DataChainDigest != witnesses[0] {
		tb.Fatalf("preflight apply count=%d publication=%+v witness=%x err=%v", applied, publication, witnesses[0], err)
	}
	completion, present := completions.Completion(0)
	if !present || len(completion) == 0 {
		tb.Fatal("preflight returned no original completion")
	}
	view, err := replication.OpenCompletion(completion)
	if err != nil {
		tb.Fatalf("preflight open completion: %v", err)
	}
	result, err := replicatedstate.OpenTransactionCompletionResult(view.ResultCode, view.InlineResult)
	if err != nil || view.ResultCode != replicatedstate.ResultApplied || view.AppliedSequence != 2 ||
		!result.AffectedRowsValid || result.AffectedRows != batch64Rows {
		tb.Fatalf("preflight completion result=%+v code=%d err=%v", result, view.ResultCode, err)
	}
	if err := group.Checkpoint(); err != nil {
		tb.Fatalf("preflight full fold: %v", err)
	}
	if err := verifyReplicatedApplyBatch64Rows(database, batch64Rows, identity); err != nil {
		tb.Fatalf("preflight row oracle: %v", err)
	}
}

func newReplicatedApplyBatch64Fixture(
	b testing.TB,
) (*Database, *ReplicatedApply, ReplicatedShardStoreIdentity, *durable.CheckpointGroup) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "replicated-apply-batch64.vdb")
	binding := testReplicatedBinding(31)
	database, err := InitializeShardStore(path, ShardStoreBinding{
		Distribution:         distribution.DistributionName(binding.Distribution),
		Shard:                distribution.ShardID(binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := database.Close(); err != nil {
			b.Errorf("close database: %v", err)
		}
	})
	session, err := database.NewSession(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	if err := testRuntimeExec(session, batch64TypedSchema, nil); err != nil {
		_ = session.Close()
		b.Fatal(err)
	}
	if err := session.Close(); err != nil {
		b.Fatal(err)
	}
	identity := requireReplicatedShardStoreBind(b, database, binding, "docs")
	claim, _, err := database.OpenReplicatedApply(
		identity, testReplicatedApplyBootstrap(), testReplicatedApplyOptions(),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := claim.Close(); err != nil {
			b.Errorf("close replicated apply: %v", err)
		}
	})
	if _, err := claim.InstallSnapshot(testReplicatedApplyBootstrap()); err != nil {
		b.Fatal(err)
	}
	core := database.connector.db
	core.mu.RLock()
	group := core.checkpointGroup
	system := core.replicatedApplyCollection
	capture := core.replicatedCaptureCollection
	userTable := core.tables[identity.UserTable]
	core.mu.RUnlock()
	if group == nil || system == nil || capture == nil || userTable == nil || userTable.collection == nil {
		b.Fatal("replicated apply did not create a checkpoint group")
	}
	if !group.Owns([]durable.NamedCollection{
		{Name: replicatedstate.SystemCollectionName, Collection: system},
		{Name: identity.UserTable, Collection: userTable.collection},
		{Name: replicatedstate.TransitionCaptureCollectionName, Collection: capture},
	}) {
		b.Fatal("replicated apply group does not own the system/user/capture members")
	}
	// Remove the bootstrap cut from the measured deltas. This is setup work;
	// every timed row starts after one clean certified group state.
	if err := group.Checkpoint(); err != nil {
		b.Fatalf("certify bootstrap: %v", err)
	}
	return database, claim, identity, group
}

func replicatedApplyBatch64Meta(index uint64) raftmodel.ApplyMeta {
	return raftmodel.ApplyMeta{Index: index, Term: 2, Type: pb.EntryNormal}
}

func replicatedApplyBatch64Lanes() []distributedtxn.ID {
	lanes := make([]distributedtxn.ID, 16)
	for ordinal := range lanes {
		var framed [len(batch64LaneDomain) + 1]byte
		copy(framed[:], batch64LaneDomain)
		framed[len(batch64LaneDomain)] = byte(ordinal)
		sum := sha256.Sum256(framed[:])
		copy(lanes[ordinal][:], sum[:])
	}
	return lanes
}

func appendReplicatedApplyBatch64Command(
	dst []byte,
	identity ReplicatedShardStoreIdentity,
	lane distributedtxn.ID,
	revision uint64,
	batches []replication.RelationMutationBatch,
) ([]byte, error) {
	digest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		return dst, err
	}
	control := distributedtxn.ReplicatedCommand{
		Role:             distributedtxn.ReplicatedRoleTarget,
		Operation:        distributedtxn.ReplicatedApplySingleTarget,
		ID:               lane,
		ExpectedRevision: revision,
		PayloadKind:      distributedtxn.ReplicatedPayloadTargetStage,
		ControllerEpoch:  1,
		ExecutionPinDigest: distributedtxn.Digest(sha256.Sum256(
			append([]byte(batch64ExecutionPinDomain), lane[:]...),
		)),
		Target: distributedtxn.TransactionTargetStage{
			CoordinatorGroup:            distributedtxn.ID(identity.Binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(identity.Binding.ShardIncarnation),
			CoordinatorAllocation:       identity.Binding.AllocationGeneration,
			BucketBits:                  8,
			IntentScopes:                []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest:              digest,
		},
	}
	transaction, err := distributedtxn.AppendReplicatedCommand(nil, control)
	if err != nil {
		return dst, err
	}
	fingerprintInput := make([]byte, 0, len(batch64FingerprintDomain)+len(transaction)+32)
	fingerprintInput = append(fingerprintInput, batch64FingerprintDomain...)
	fingerprintInput = append(fingerprintInput, transaction...)
	fingerprintInput = append(fingerprintInput, digest[:]...)
	command := replication.Command{
		Kind:                   replication.CommandTransaction,
		AuthorityClass:         replication.CommandAuthorityMembershipStableData,
		ClusterID:              replication.ID128(identity.Binding.ClusterID),
		ClusterIncarnation:     replication.ID128(identity.Binding.ClusterIncarnation),
		TopologyRecoveryEpoch:  identity.Binding.TopologyRecoveryEpoch,
		Distribution:           identity.Binding.Distribution,
		Shard:                  identity.Binding.Shard,
		AllocationGeneration:   identity.Binding.AllocationGeneration,
		ShardIncarnation:       replication.ID128(identity.Binding.ShardIncarnation),
		GroupID:                replication.ID128(identity.Binding.GroupID),
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: identity.Binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        identity.Binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         identity.Binding.Authority.OwnershipEpoch,
		SchemaGeneration:       identity.Binding.Authority.SchemaGeneration,
		RoutingVersion:         identity.Binding.Authority.RoutingVersion,
		RouteGeneration:        identity.Binding.Authority.RouteGeneration,
		Tenant:                 []byte(batch64Tenant),
		ClientID:               replication.ID128(lane),
		ClientEpoch:            uint64(control.Role),
		ClientSequence:         revision,
		RetryHome:              batch64RetryHome,
		Fingerprint:            sha256.Sum256(fingerprintInput),
		Transaction:            transaction,
		Batches:                batches,
	}
	return replication.AppendCommand(dst, command)
}

func fillReplicatedApplyBatch64Rows(
	database *Database,
	start int,
	keys [][]byte,
	values [][]byte,
) error {
	core := database.connector.db
	core.mu.RLock()
	table := core.tables["docs"]
	if table == nil || table.collection == nil {
		core.mu.RUnlock()
		return fmt.Errorf("docs table is unavailable")
	}
	for row := range values {
		ordinal := start + row
		value := []byte(fmt.Sprintf(
			`{"id":"key-%08d","bucket":%d,"score":%d,"payload":%q}`,
			ordinal, ordinal%16, ordinal%100, replicatedApplyBatch64Payload(ordinal),
		))
		key, err := documentKey(value, table.meta.PrimaryKey, table.primary, table.collection.MaxKeyBytes())
		if err != nil {
			core.mu.RUnlock()
			return err
		}
		values[row] = value
		keys[row] = append(keys[row][:0], key...)
	}
	core.mu.RUnlock()
	return nil
}

func replicatedApplyBatch64Payload(row int) string {
	var value [256]byte
	x := replicatedApplyBatch64Mix(uint64(row) + 0x1d2b79f5aa33cc77)
	for offset := range value {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 0x2545f4914f6cdd1d
		value[offset] = batch64PayloadAlphabet[(x>>58)&63]
	}
	return string(value[:])
}

func replicatedApplyBatch64Mix(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func verifyReplicatedApplyBatch64Rows(
	database *Database,
	rows int,
	identity ReplicatedShardStoreIdentity,
) error {
	core := database.connector.db
	core.mu.RLock()
	table := core.tables[identity.UserTable]
	if table == nil || table.collection == nil {
		core.mu.RUnlock()
		return fmt.Errorf("user table is unavailable")
	}
	collection := table.collection
	core.mu.RUnlock()
	snapshot, err := collection.Snapshot()
	if err != nil {
		return err
	}
	defer snapshot.Close()
	if snapshot.Len() != uint64(rows) {
		return fmt.Errorf("snapshot row count=%d want %d", snapshot.Len(), rows)
	}
	for ordinal := 0; ordinal < rows; ordinal++ {
		value := []byte(fmt.Sprintf(
			`{"id":"key-%08d","bucket":%d,"score":%d,"payload":%q}`,
			ordinal, ordinal%16, ordinal%100, replicatedApplyBatch64Payload(ordinal),
		))
		canonical, err := vibejson.AppendCanonicalize(nil, value)
		if err != nil {
			return err
		}
		value = canonical
		key, err := documentKey(value, table.meta.PrimaryKey, table.primary, collection.MaxKeyBytes())
		if err != nil {
			return err
		}
		got, found, err := snapshot.AppendRaw(nil, []byte(key))
		if err != nil {
			return err
		}
		if !found || !bytes.Equal(got, value) {
			return fmt.Errorf("row %d mismatch: found=%t got=%d want=%d", ordinal, found, len(got), len(value))
		}
	}
	return nil
}

func replicatedApplyBatch64UserStats(
	b testing.TB,
	resources ReplicatedApplyResourceStats,
	identity ReplicatedShardStoreIdentity,
) durable.Stats {
	b.Helper()
	for ordinal := uint16(0); ordinal < resources.RelationCount; ordinal++ {
		if identity.Relations[ordinal].Table == identity.UserTable {
			return resources.Relations[ordinal]
		}
	}
	b.Fatalf("user relation %q is absent from resource stats", identity.UserTable)
	return durable.Stats{}
}

func reportReplicatedApplyBatch64GroupMetrics(
	b *testing.B,
	before, afterAppend, afterFold durable.CheckpointGroupStats,
) {
	b.ReportMetric(float64(afterAppend.Checkpoints-before.Checkpoints), "append-certificates")
	b.ReportMetric(float64(afterFold.Checkpoints-afterAppend.Checkpoints), "fold-certificates")
	b.ReportMetric(float64(afterFold.Checkpoints-before.Checkpoints), "certificates")
	b.ReportMetric(float64(afterAppend.PhysicalCheckpoints-before.PhysicalCheckpoints), "append-physical-folds")
	b.ReportMetric(float64(afterFold.PhysicalCheckpoints-afterAppend.PhysicalCheckpoints), "final-physical-folds")
	b.ReportMetric(float64(afterFold.PhysicalCheckpoints-before.PhysicalCheckpoints), "physical-folds")
	b.ReportMetric(float64(afterFold.JournalSyncs-before.JournalSyncs), "journal-syncs")
	b.ReportMetric(float64(afterFold.CertificateSyncs-before.CertificateSyncs), "certificate-syncs")
	b.ReportMetric(float64(afterFold.BarrierSyncs-before.BarrierSyncs), "barrier-syncs")
	b.ReportMetric(float64(afterFold.PeriodicCheckpoints-before.PeriodicCheckpoints), "periodic-checkpoints")
	b.ReportMetric(float64(afterFold.PressureCheckpoints-before.PressureCheckpoints), "pressure-checkpoints")
}

func reportReplicatedApplyBatch64UserMetrics(
	b *testing.B,
	before, afterAppend, afterFold durable.Stats,
) {
	appendDelta := func(afterValue, beforeValue uint64) float64 {
		return float64(afterValue - beforeValue)
	}
	b.ReportMetric(float64(afterFold.PrimaryStructuralRoutingStagedBytes-before.PrimaryStructuralRoutingStagedBytes), "routing-staged-bytes")
	b.ReportMetric(float64(afterFold.PrimaryStructuralRoutingRetiredBytes-before.PrimaryStructuralRoutingRetiredBytes), "routing-retired-bytes")
	b.ReportMetric(float64(afterFold.PrimaryTabletRoutingRebuilds-before.PrimaryTabletRoutingRebuilds), "tablet-routing-rebuilds")
	b.ReportMetric(float64(afterFold.PrimaryLeafSplits-before.PrimaryLeafSplits), "leaf-splits")
	b.ReportMetric(float64(afterFold.DeviceBytes-before.DeviceBytes), "device-bytes")
	b.ReportMetric(float64(afterFold.DeviceCommits-before.DeviceCommits), "device-commits")
	b.ReportMetric(float64(afterFold.CommittedBatches-before.CommittedBatches), "committed-batches")
	b.ReportMetric(appendDelta(afterAppend.PrimaryStructuralRoutingStagedBytes, before.PrimaryStructuralRoutingStagedBytes), "append-routing-staged-bytes")
	b.ReportMetric(appendDelta(afterAppend.PrimaryStructuralRoutingRetiredBytes, before.PrimaryStructuralRoutingRetiredBytes), "append-routing-retired-bytes")
	b.ReportMetric(appendDelta(afterAppend.PrimaryTabletRoutingRebuilds, before.PrimaryTabletRoutingRebuilds), "append-tablet-routing-rebuilds")
	b.ReportMetric(appendDelta(afterAppend.PrimaryLeafSplits, before.PrimaryLeafSplits), "append-leaf-splits")
	b.ReportMetric(appendDelta(afterFold.DeviceBytes, afterAppend.DeviceBytes), "final-fold-device-bytes")
	b.ReportMetric(appendDelta(afterFold.DeviceCommits, afterAppend.DeviceCommits), "final-fold-device-commits")
	b.ReportMetric(appendDelta(afterFold.CommittedBatches, afterAppend.CommittedBatches), "final-fold-committed-batches")
	b.ReportMetric(float64(afterAppend.PrimaryOverlayRetainedRecords), "overlay-records-at-append-end")
	b.ReportMetric(float64(afterFold.PrimaryOverlayRetainedRecords), "overlay-records-after-fold")
}

func reportReplicatedApplyBatch64ResourceMetrics(
	b *testing.B,
	before, afterAppend, afterFold ReplicatedApplyResourceStats,
	identity ReplicatedShardStoreIdentity,
) {
	b.Helper()
	report := func(name string, before, afterAppend, afterFold durable.Stats) {
		b.ReportMetric(float64(afterFold.DeviceBytes-before.DeviceBytes), name+"-device-bytes")
		b.ReportMetric(float64(afterFold.DeviceCommits-before.DeviceCommits), name+"-device-commits")
		b.ReportMetric(float64(afterFold.CommittedBatches-before.CommittedBatches), name+"-committed-batches")
		b.ReportMetric(float64(afterAppend.DeviceBytes-before.DeviceBytes), name+"-append-device-bytes")
		b.ReportMetric(float64(afterFold.DeviceBytes-afterAppend.DeviceBytes), name+"-final-fold-device-bytes")
	}
	report("system", before.System, afterAppend.System, afterFold.System)
	report("capture", before.Capture, afterAppend.Capture, afterFold.Capture)
	for ordinal := uint16(0); ordinal < afterFold.RelationCount; ordinal++ {
		name := fmt.Sprintf("relation-%d", identity.Relations[ordinal].Relation)
		report(name, before.Relations[ordinal], afterAppend.Relations[ordinal], afterFold.Relations[ordinal])
	}
}
