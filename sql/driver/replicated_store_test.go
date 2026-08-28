package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestReplicatedBatchCeilingFitsConditionalJournal(t *testing.T) {
	if ReplicatedShardStoreFormat != 0 || ReplicatedApplyFormat != 0 ||
		ReplicatedPlacementProfileFormat != 0 {
		t.Fatalf(
			"current replicated formats = shard %d, apply %d, placement %d; want zero sentinels",
			ReplicatedShardStoreFormat, ReplicatedApplyFormat,
			ReplicatedPlacementProfileFormat,
		)
	}
	required := storeio.RecoveryBatchRecordPaddedSizeForPayload(
		storeio.RecoveryJournalMinSectorSize,
		replicatedMaxDistinctMutations,
		replicatedMaxBatchBytes+storeio.RecoveryConditionalHeaderSize,
	)
	if required <= 0 || uint64(required) != ReplicatedUserRecoveryJournalBytes ||
		uint64(required) > storeio.RecoveryJournalMaxCapacityBytes {
		t.Fatalf(
			"replicated batch ceiling record = %d, journal clamp = %d",
			required, storeio.RecoveryJournalMaxCapacityBytes,
		)
	}
	if got := canonicalReplicatedShardStoreSidecars(); got != (ReplicatedShardStoreSidecarProfile{
		UserRecoveryJournalBytes: 16794624,
		TransactionMarkerBytes:   1048576,
	}) {
		t.Fatalf("canonical base sidecars = %+v", got)
	}
	markerPadded, ok := storeio.TxnDecisionRecordPaddedSize(2)
	if !ok {
		t.Fatal("current marker grammar rejected two participants")
	}
	if markerPadded > int(ReplicatedTransactionMarkerBytes) ||
		int(ReplicatedTransactionMarkerBytes)/markerPadded != 2048 {
		t.Fatalf(
			"two-participant decision = %d padded bytes, marker profile = %d; want 2048-decision window",
			markerPadded, ReplicatedTransactionMarkerBytes,
		)
	}
	systemLimits := replicatedApplySystemLimits(replicatedstate.MaxSessionRetryWindow)
	systemRequired := storeio.RecoveryBatchRecordPaddedSizeForPayload(
		storeio.RecoveryJournalMinSectorSize,
		systemLimits.MaxBatchDocuments,
		systemLimits.MaxBatchBytes+storeio.RecoveryConditionalHeaderSize,
	)
	if systemRequired > int(ReplicatedSystemRecoveryJournalBytes) ||
		ReplicatedSystemRecoveryJournalBytes != 655872 {
		t.Fatalf(
			"system conditional record = %d, system profile = %d",
			systemRequired, ReplicatedSystemRecoveryJournalBytes,
		)
	}
	limits := ReplicatedShardStoreLimits{
		MaxKeyBytes:       replicatedMaxKeyBytes,
		MaxDocumentBytes:  replicatedMaxDocumentBytes,
		MaxBatchDocuments: replicatedMaxDistinctMutations,
		MaxBatchBytes:     replicatedMaxBatchBytes,
	}
	if err := validateReplicatedShardStoreLimits(limits); err != nil {
		t.Fatalf("replicated batch ceiling: %v", err)
	}
	limits.MaxBatchBytes++
	if err := validateReplicatedShardStoreLimits(limits); !errors.Is(err, ErrReplicatedShardStoreProfile) {
		t.Fatalf("replicated batch ceiling + 1 = %v, want profile error", err)
	}
}

func skipReplicatedStrictAllocationUnsupported(
	t testing.TB,
	database *Database,
	identity ReplicatedShardStoreIdentity,
	err error,
) {
	t.Helper()
	if !errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		return
	}
	if database == nil || database.connector == nil || database.connector.db == nil {
		t.Fatalf("strict-allocation refusal lost the database owner: %v", err)
	}
	core := database.connector.db
	core.mu.RLock()
	table := core.tables["docs"]
	invalid := !identity.IsZero() ||
		core.catalog.ReplicatedShardStore != nil || core.catalogWritePending ||
		table == nil || table.meta.Materialized ||
		table.meta.SealedRecoveryJournalBytes != 0 ||
		table.collection != nil || table.file != nil ||
		core.txnLog.Options() != (durable.TxnLogOptions{})
	dataDir := core.dataDir
	core.mu.RUnlock()
	entries, readErr := os.ReadDir(dataDir)
	_, markerErr := os.Stat(filepath.Join(dataDir, "txn.vtm"))
	if invalid || readErr != nil || len(entries) != 0 || !os.IsNotExist(markerErr) {
		t.Fatalf(
			"strict-allocation refusal retained publication state: identity=%+v invalid=%t entries=%v read=%v marker=%v",
			identity, invalid, entries, readErr, markerErr,
		)
	}
	t.Skipf("sealed replicated sidecars require strict allocation support: %v", err)
}

func requireReplicatedShardStoreBind(
	t testing.TB,
	database *Database,
	binding ReplicatedShardStoreBinding,
	userTable string,
) ReplicatedShardStoreIdentity {
	t.Helper()
	identity, err := database.BindReplicatedShardStore(binding, userTable)
	if userTable == "docs" {
		skipReplicatedStrictAllocationUnsupported(t, database, identity, err)
	} else if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		t.Fatalf("strict-allocation helper only supports the canonical docs fixture: %v", err)
	}
	if err != nil {
		t.Fatalf("BindReplicatedShardStore: %v", err)
	}
	return identity
}

func TestReplicatedShardStoreSealedBindPlatformGatePrecedesPublication(t *testing.T) {
	_, database, binding, _ := prepareReplicatedTestRoot(t, "sealed-platform", false)
	defer database.Close()
	identity, err := database.BindReplicatedShardStore(binding, "docs")
	if runtime.GOOS != "linux" {
		if !errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
			t.Fatalf("non-Linux sealed bind = %+v, %v; want strict-allocation unsupported", identity, err)
		}
		core := database.connector.db
		core.mu.RLock()
		defer core.mu.RUnlock()
		if !identity.IsZero() ||
			core.catalog.ReplicatedShardStore != nil ||
			core.catalog.Tables["docs"].Materialized ||
			core.tables["docs"].collection != nil || core.tables["docs"].file != nil {
			t.Fatalf("unsupported sealed bind published catalog or storage: %+v", identity)
		}
		return
	}
	if err != nil {
		t.Fatalf("Linux sealed bind: %v", err)
	}
	if identity.Sidecars != canonicalReplicatedShardStoreSidecars() {
		t.Fatalf("Linux sealed bind sidecars = %+v", identity.Sidecars)
	}
}

func TestReplicatedSidecarProfilesStrictCurrentGrammar(t *testing.T) {
	base := canonicalReplicatedShardStoreSidecars()
	baseRaw, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	const wantBase = `{"user_recovery_journal_bytes":16794624,"transaction_marker_bytes":1048576}`
	if string(baseRaw) != wantBase {
		t.Fatalf("base sidecar JSON = %s, want %s", baseRaw, wantBase)
	}
	var baseRoundTrip ReplicatedShardStoreSidecarProfile
	if err := json.Unmarshal(baseRaw, &baseRoundTrip); err != nil || baseRoundTrip != base {
		t.Fatalf("base sidecar round trip = %+v, %v", baseRoundTrip, err)
	}
	baseCases := map[string]string{
		"missing_user":   `{"transaction_marker_bytes":1048576}`,
		"missing_marker": `{"user_recovery_journal_bytes":16794624}`,
		"unknown":        `{"unknown":1,"user_recovery_journal_bytes":16794624,"transaction_marker_bytes":1048576}`,
		"duplicate":      `{"user_recovery_journal_bytes":16794624,"user_recovery_journal_bytes":16794624,"transaction_marker_bytes":1048576}`,
		"wrong_user":     `{"user_recovery_journal_bytes":16794112,"transaction_marker_bytes":1048576}`,
		"wrong_marker":   `{"user_recovery_journal_bytes":16794624,"transaction_marker_bytes":512}`,
		"null":           `null`,
		"non_object":     `[]`,
	}
	for name, raw := range baseCases {
		t.Run("base_"+name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(raw), new(ReplicatedShardStoreSidecarProfile)); err == nil {
				t.Fatalf("accepted noncanonical base sidecars: %s", raw)
			}
		})
	}
	if _, err := json.Marshal(ReplicatedShardStoreSidecarProfile{}); err == nil {
		t.Fatal("marshaled zero base sidecar profile")
	}

	apply := canonicalReplicatedApplySidecars()
	applyRaw, err := json.Marshal(apply)
	if err != nil {
		t.Fatal(err)
	}
	const wantApply = `{"system_recovery_journal_bytes":655872}`
	if string(applyRaw) != wantApply {
		t.Fatalf("apply sidecar JSON = %s, want %s", applyRaw, wantApply)
	}
	var applyRoundTrip ReplicatedApplySidecarProfile
	if err := json.Unmarshal(applyRaw, &applyRoundTrip); err != nil || applyRoundTrip != apply {
		t.Fatalf("apply sidecar round trip = %+v, %v", applyRoundTrip, err)
	}
	applyCases := map[string]string{
		"missing":    `{}`,
		"unknown":    `{"unknown":1,"system_recovery_journal_bytes":655872}`,
		"duplicate":  `{"system_recovery_journal_bytes":655872,"system_recovery_journal_bytes":655872}`,
		"wrong":      `{"system_recovery_journal_bytes":197120}`,
		"null":       `null`,
		"non_object": `[]`,
	}
	for name, raw := range applyCases {
		t.Run("apply_"+name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(raw), new(ReplicatedApplySidecarProfile)); err == nil {
				t.Fatalf("accepted noncanonical apply sidecars: %s", raw)
			}
		})
	}
	if _, err := json.Marshal(ReplicatedApplySidecarProfile{}); err == nil {
		t.Fatal("marshaled zero apply sidecar profile")
	}
}

func testReplicatedBinding(seed byte) ReplicatedShardStoreBinding {
	id := func(offset byte) [16]byte {
		var value [16]byte
		value[0] = seed + offset
		value[15] = seed ^ offset ^ 0xa5
		return value
	}
	return ReplicatedShardStoreBinding{
		ClusterID: id(1), ClusterIncarnation: id(2),
		TopologyRecoveryEpoch: 3,
		Distribution:          "accounts",
		Shard:                 "shard-7",
		AllocationGeneration:  5,
		ShardIncarnation:      id(3),
		GroupID:               id(4),
		MemberID:              9,
		StoreID:               id(5),
		Authority: ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 11,
			ProtectionEpoch:        13,
			OwnershipEpoch:         17,
			SchemaGeneration:       19,
			RoutingVersion:         23,
			RouteGeneration:        29,
		},
	}
}

func prepareReplicatedTestRoot(
	t testing.TB,
	name string,
	materialize bool,
) (string, *Database, ReplicatedShardStoreBinding, ShardStoreIdentity) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name+".vdb")
	binding := testReplicatedBinding(31)
	database, err := InitializeShardStore(path, ShardStoreBinding{
		Distribution:         distribution.DistributionName(binding.Distribution),
		Shard:                distribution.ShardID(binding.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration),
	})
	if err != nil {
		t.Fatalf("InitializeShardStore: %v", err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if err := testRuntimeExec(session, `CREATE TABLE docs (PRIMARY KEY (id))`, nil); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if materialize {
		if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"probe"}`)}); err != nil {
			t.Fatalf("materialize INSERT: %v", err)
		}
		if err := testRuntimeExec(session, `DELETE FROM docs WHERE id = ?`, []any{"probe"}); err != nil {
			t.Fatalf("materialize DELETE: %v", err)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatalf("close preparation session: %v", err)
	}
	local, err := database.ShardStoreIdentity()
	if err != nil {
		t.Fatalf("ShardStoreIdentity: %v", err)
	}
	return path, database, binding, local
}

func testRuntimeExec(session *Session, statement string, values []any) error {
	prepared, err := session.Prepare(context.Background(), statement)
	if err == nil {
		_, err = prepared.Exec(context.Background(), values)
	}
	if prepared != nil {
		err = errors.Join(err, prepared.Close())
	}
	return err
}

func TestReplicatedShardStoreBindOpenIdentityAndDirectFence(t *testing.T) {
	path, database, binding, local := prepareReplicatedTestRoot(t, "bound", false)
	identity := requireReplicatedShardStoreBind(t, database, binding, "docs")
	if identity.Format != ReplicatedShardStoreFormat || identity.LogID != local.LogID ||
		identity.Binding != binding || identity.UserTable != "docs" ||
		len(identity.UserStorage) != storageIdentityBytes*2 || identity.UserPrimaryKey != "/id" ||
		identity.Sidecars != canonicalReplicatedShardStoreSidecars() {
		t.Fatalf("bound identity = %+v, local = %+v", identity, local)
	}
	core := database.connector.db
	core.mu.RLock()
	boundTable := core.tables["docs"]
	if boundTable == nil || !boundTable.meta.Materialized ||
		boundTable.meta.Storage != identity.UserStorage ||
		boundTable.meta.SealedRecoveryJournalBytes != ReplicatedUserRecoveryJournalBytes ||
		boundTable.collection == nil ||
		boundTable.collection.SealedRecoveryJournalBytes() != ReplicatedUserRecoveryJournalBytes ||
		core.txnLog.Options() != (durable.TxnLogOptions{
			Capacity: ReplicatedTransactionMarkerBytes, SealedCapacity: true,
		}) {
		core.mu.RUnlock()
		t.Fatalf("bound storage did not retain exact sealed sidecar profile")
	}
	core.mu.RUnlock()
	if retried, err := database.BindReplicatedShardStore(binding, "docs"); err != nil || !retried.Equal(identity) {
		t.Fatalf("exact bind retry = %+v, %v; want %+v", retried, err, identity)
	}
	if got, err := database.RequireReplicatedShardStore(identity); err != nil || !got.Equal(identity) {
		t.Fatalf("RequireReplicatedShardStore = %+v, %v", got, err)
	}
	if _, err := database.ShardStoreIdentity(); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("legacy ShardStoreIdentity = %v, want direct fence", err)
	}
	if _, err := database.RequireShardStore(local.Binding()); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("legacy RequireShardStore = %v, want direct fence", err)
	}
	if _, err := database.ClaimShardStoreServing(local.Binding(), ShardStoreFence{
		OwnershipEpoch: 1, RoutingVersion: 1,
	}); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("legacy ClaimShardStoreServing = %v, want direct fence", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close bound root: %v", err)
	}

	if _, err := Open(path); !errors.Is(err, ErrShardStoreIdentityMismatch) {
		t.Fatalf("generic Open = %v, want shard identity mismatch", err)
	}
	if _, err := OpenShardStore(path, local.Binding()); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("OpenShardStore = %v, want direct fence", err)
	}
	if _, err := InitializeShardStore(path, local.Binding()); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("InitializeShardStore retry = %v, want direct fence", err)
	}

	mismatches := []struct {
		name   string
		mutate func(*ReplicatedShardStoreIdentity)
		want   error
	}{
		{"sql_log_id", func(i *ReplicatedShardStoreIdentity) { i.LogID[0] ^= 0x40 }, ErrReplicatedShardStoreIdentityMismatch},
		{"wal_store_id", func(i *ReplicatedShardStoreIdentity) { i.Binding.StoreID[0] ^= 0x40 }, ErrReplicatedShardStoreIdentityMismatch},
		{"authority", func(i *ReplicatedShardStoreIdentity) { i.Binding.Authority.RouteGeneration++ }, ErrReplicatedShardStoreIdentityMismatch},
		{"storage", func(i *ReplicatedShardStoreIdentity) {
			if i.UserStorage[0] == '0' {
				i.UserStorage = "1" + i.UserStorage[1:]
			} else {
				i.UserStorage = "0" + i.UserStorage[1:]
			}
			i.Relations[0].Storage = i.UserStorage
			i.RelationManifestDigest = replicatedRelationManifestDigest(*i)
		}, ErrReplicatedShardStoreIdentityMismatch},
		{"primary", func(i *ReplicatedShardStoreIdentity) { i.UserPrimaryKey = "/other" }, ErrReplicatedShardStoreIdentityMismatch},
		{"limits", func(i *ReplicatedShardStoreIdentity) {
			i.UserLimits.MaxBatchBytes--
			i.Relations[0].Limits = i.UserLimits
			i.RelationManifestDigest = replicatedRelationManifestDigest(*i)
		}, ErrReplicatedShardStoreIdentityMismatch},
		{"user_journal", func(i *ReplicatedShardStoreIdentity) { i.Sidecars.UserRecoveryJournalBytes-- }, ErrReplicatedShardStoreProfile},
		{"transaction_marker", func(i *ReplicatedShardStoreIdentity) { i.Sidecars.TransactionMarkerBytes++ }, ErrReplicatedShardStoreProfile},
	}
	for _, test := range mismatches {
		t.Run("mismatch_"+test.name, func(t *testing.T) {
			expected := identity.Clone()
			test.mutate(&expected)
			if _, err := OpenReplicatedShardStore(path, expected); !errors.Is(err, test.want) {
				t.Fatalf("OpenReplicatedShardStore = %v, want %v", err, test.want)
			}
		})
	}

	reopened, err := OpenReplicatedShardStore(path, identity)
	if err != nil {
		t.Fatalf("OpenReplicatedShardStore exact: %v", err)
	}
	// Model a trusted replicated apply below the SQL surface, then prove reopen
	// validates the frozen profile without incorrectly requiring the table to
	// remain empty forever.
	if _, err := reopened.connector.db.tables["docs"].collection.Put(
		[]byte("applied"), []byte(`{"id":"applied"}`),
	); err != nil {
		t.Fatalf("trusted test apply: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close after trusted test apply: %v", err)
	}
	reopened, err = OpenReplicatedShardStore(path, identity)
	if err != nil {
		t.Fatalf("reopen replicated root with rows: %v", err)
	}
	defer reopened.Close()
	session, err := reopened.NewSession(context.Background())
	if err != nil {
		t.Fatalf("NewSession replicated: %v", err)
	}
	defer session.Close()

	selectRows, err := session.Prepare(context.Background(), `SELECT COUNT(*) FROM docs`)
	if err != nil {
		t.Fatalf("prepare replicated read: %v", err)
	}
	cursor, err := selectRows.Query(context.Background(), nil)
	if err != nil {
		t.Fatalf("replicated read: %v", err)
	}
	if !cursor.Next() {
		t.Fatal("replicated read returned no row")
	}
	if err := cursor.Close(); err != nil {
		t.Fatal(err)
	}
	if err := selectRows.Close(); err != nil {
		t.Fatal(err)
	}

	writes := []struct {
		name string
		sql  string
		args []any
	}{
		{"insert", `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"a"}`)}},
		{"update", `UPDATE docs SET "$doc" = ? WHERE id = ?`, []any{[]byte(`{"id":"a"}`), "a"}},
		{"delete", `DELETE FROM docs WHERE id = ?`, []any{"a"}},
		{"create_table", `CREATE TABLE other (PRIMARY KEY (id))`, nil},
		{"drop_table", `DROP TABLE docs`, nil},
		{"truncate", `TRUNCATE docs`, nil},
		{"create_index", `CREATE INDEX by_id ON docs (id)`, nil},
		{"drop_index", `DROP INDEX IF EXISTS by_id ON docs`, nil},
		{"create_view", `CREATE VIEW selected AS SELECT id FROM docs`, nil},
		{"drop_view", `DROP VIEW IF EXISTS selected`, nil},
	}
	for _, write := range writes {
		t.Run("fence_"+write.name, func(t *testing.T) {
			if err := testRuntimeExec(session, write.sql, write.args); !errors.Is(err, ErrDirectWriteFenced) {
				t.Fatalf("%s = %v, want direct fence", write.sql, err)
			}
		})
	}
	returning, err := session.Prepare(context.Background(), `INSERT INTO docs VALUES (?) RETURNING id`)
	if err != nil {
		t.Fatalf("prepare RETURNING: %v", err)
	}
	if _, err := returning.Query(context.Background(), []any{[]byte(`{"id":"returning"}`)}); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("INSERT RETURNING = %v, want direct fence", err)
	}
	_ = returning.Close()
	// EXPLAIN accepts query expressions only; mutations cannot enter its
	// recursive ANALYZE path. The allowed control below proves that recursive
	// read execution remains available in replicated mode.
	analyze, err := session.Prepare(context.Background(),
		`EXPLAIN ANALYZE SELECT id FROM docs`)
	if err != nil {
		t.Fatalf("prepare EXPLAIN ANALYZE read: %v", err)
	}
	analyzeRows, err := analyze.Query(context.Background(), nil)
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE read: %v", err)
	}
	if !analyzeRows.Next() {
		t.Fatal("EXPLAIN ANALYZE read returned no plan row")
	}
	_ = analyzeRows.Close()
	_ = analyze.Close()
	explain, err := session.Prepare(context.Background(), `EXPLAIN SELECT id FROM docs`)
	if err != nil {
		t.Fatalf("prepare plain EXPLAIN read: %v", err)
	}
	explainRows, err := explain.Query(context.Background(), nil)
	if err != nil {
		t.Fatalf("plain EXPLAIN read: %v", err)
	}
	if !explainRows.Next() {
		t.Fatal("plain EXPLAIN read returned no plan row")
	}
	_ = explainRows.Close()
	_ = explain.Close()

	if err := session.Begin(context.Background(), TxOptions{}); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("read-write Begin = %v, want direct fence", err)
	}
	if err := session.Begin(context.Background(), TxOptions{ReadOnly: true}); err != nil {
		t.Fatalf("read-only Begin: %v", err)
	}
	if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"tx"}`)}); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("read-only transaction mutation = %v, want direct fence", err)
	}
	if err := session.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback read-only transaction: %v", err)
	}
	for _, isolation := range []struct {
		name  string
		level IsolationLevel
	}{
		{"read_committed", IsolationReadCommitted},
		{"repeatable_read", IsolationRepeatableRead},
		{"snapshot", IsolationSnapshot},
		{"serializable", IsolationSerializable},
	} {
		t.Run("typed_read_only_"+isolation.name, func(t *testing.T) {
			if err := session.Begin(context.Background(), TxOptions{
				ReadOnly: true, Isolation: isolation.level,
			}); err != nil {
				t.Fatalf("Begin: %v", err)
			}
			read, err := session.Prepare(context.Background(), `SELECT COUNT(*) FROM docs`)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := read.Query(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				t.Fatal("read-only transaction returned no row")
			}
			_ = rows.Close()
			_ = read.Close()
			if err := session.Commit(context.Background()); err != nil {
				t.Fatalf("clean Commit: %v", err)
			}
		})
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}

	// database/sql and its prepared-statement/transaction adapters retain the
	// same typed fence; the runtime checks above are not a privileged path.
	sqlDB := stdsql.OpenDB(reopened.connector)
	if _, err := sqlDB.ExecContext(context.Background(),
		`INSERT INTO docs VALUES (?)`, []byte(`{"id":"database-sql"}`),
	); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("database/sql Exec = %v, want direct fence", err)
	}
	preparedSQL, err := sqlDB.PrepareContext(context.Background(),
		`INSERT INTO docs VALUES (?)`)
	if err != nil {
		t.Fatalf("database/sql Prepare: %v", err)
	}
	if _, err := preparedSQL.ExecContext(context.Background(), []byte(`{"id":"prepared"}`)); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("database/sql prepared Exec = %v, want direct fence", err)
	}
	_ = preparedSQL.Close()
	if _, err := sqlDB.BeginTx(context.Background(), nil); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("database/sql read-write Begin = %v, want direct fence", err)
	}
	readOnly, err := sqlDB.BeginTx(context.Background(), &stdsql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("database/sql read-only Begin: %v", err)
	}
	var count int64
	if err := readOnly.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM docs`).Scan(&count); err != nil {
		t.Fatalf("database/sql read-only SELECT: %v", err)
	}
	if count != 1 {
		t.Fatalf("database/sql read-only count = %d, want 1", count)
	}
	if err := readOnly.Commit(); err != nil {
		t.Fatalf("database/sql read-only Commit: %v", err)
	}
	for _, isolation := range []struct {
		name  string
		level stdsql.IsolationLevel
	}{
		{"read_committed", stdsql.LevelReadCommitted},
		{"repeatable_read", stdsql.LevelRepeatableRead},
		{"serializable", stdsql.LevelSerializable},
	} {
		t.Run("database_sql_read_only_"+isolation.name, func(t *testing.T) {
			tx, err := sqlDB.BeginTx(context.Background(), &stdsql.TxOptions{
				ReadOnly: true, Isolation: isolation.level,
			})
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			var got int64
			if err := tx.QueryRowContext(context.Background(),
				`SELECT COUNT(*) FROM docs`).Scan(&got); err != nil {
				t.Fatalf("SELECT: %v", err)
			}
			if got != 1 {
				t.Fatalf("count = %d, want 1", got)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("clean Commit: %v", err)
			}
		})
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBindReplicatedShardStorePublicationOutcomes(t *testing.T) {
	t.Run("definite", func(t *testing.T) {
		_, db, binding, _ := prepareReplicatedTestRoot(t, "definite", false)
		defer db.Close()
		core := db.connector.db
		core.mu.RLock()
		priorTable := core.tables["docs"]
		priorStorage := priorTable.meta.Storage
		priorMarkerOptions := core.txnLog.Options()
		priorEntries, readErr := os.ReadDir(core.dataDir)
		core.mu.RUnlock()
		if readErr != nil {
			t.Fatal(readErr)
		}
		injected := errors.New("definite bind publication failure")
		identity, err := db.bindReplicatedShardStore(binding, "docs", func(*database) (bool, error) {
			return false, injected
		})
		skipReplicatedStrictAllocationUnsupported(t, db, identity, err)
		if !errors.Is(err, injected) || !identity.IsZero() {
			t.Fatalf("definite bind = %+v, %v", identity, err)
		}
		if _, err := db.ReplicatedShardStoreIdentity(); !errors.Is(err, ErrReplicatedShardStoreUnbound) {
			t.Fatalf("identity after definite failure = %v", err)
		}
		core.mu.RLock()
		afterTable := core.tables["docs"]
		afterMarkerOptions := core.txnLog.Options()
		published := core.catalog.ReplicatedShardStore
		core.mu.RUnlock()
		afterEntries, readErr := os.ReadDir(core.dataDir)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if afterMarkerOptions != priorMarkerOptions || published != nil ||
			afterTable != priorTable || afterTable.meta.Storage != priorStorage ||
			afterTable.meta.Materialized || afterTable.collection != nil || afterTable.file != nil ||
			len(afterEntries) != len(priorEntries) {
			t.Fatalf(
				"definite rollback retained state: options=%+v want=%+v published=%+v table=%+v entries=%d want=%d",
				afterMarkerOptions, priorMarkerOptions, published, afterTable,
				len(afterEntries), len(priorEntries),
			)
		}
		session, err := db.NewSession(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close()
		if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"still-local"}`)}); err != nil {
			t.Fatalf("local write after definite failure: %v", err)
		}
	})

	t.Run("unknown_hook_same_process_exact_retry", func(t *testing.T) {
		_, db, binding, _ := prepareReplicatedTestRoot(t, "unknown-hook", false)
		defer db.Close()
		identity, err := db.bindReplicatedShardStore(binding, "docs", func(*database) (bool, error) {
			return false, durable.ErrCommitOutcomeUnknown
		})
		skipReplicatedStrictAllocationUnsupported(t, db, identity, err)
		if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || identity.IsZero() {
			t.Fatalf("unknown hook bind = %+v, %v", identity, err)
		}
		markerPath := filepath.Join(db.connector.db.dataDir, "txn.vtm")
		if _, statErr := os.Stat(markerPath); !os.IsNotExist(statErr) {
			t.Fatalf("unknown catalog outcome minted transaction marker: %v", statErr)
		}
		retried, err := db.BindReplicatedShardStore(binding, "docs")
		if err != nil || !retried.Equal(identity) {
			t.Fatalf("unknown hook exact retry = %+v, %v; want %+v", retried, err, identity)
		}
		markerInfo, err := os.Stat(markerPath)
		if err != nil {
			t.Fatalf("exact retry did not mint sealed transaction marker: %v", err)
		}
		wantMarkerSize := int64(2*storeio.TxnMarkerHeaderSize) +
			int64(ReplicatedTransactionMarkerBytes)
		if markerInfo.Size() != wantMarkerSize ||
			db.connector.db.txnLog.Options() != (durable.TxnLogOptions{
				Capacity: ReplicatedTransactionMarkerBytes, SealedCapacity: true,
			}) {
			t.Fatalf(
				"exact retry marker = size %d options %+v; want size %d sealed profile",
				markerInfo.Size(), db.connector.db.txnLog.Options(), wantMarkerSize,
			)
		}
	})

	t.Run("rename_then_directory_fence_unknown", func(t *testing.T) {
		path, database, binding, local := prepareReplicatedTestRoot(t, "unknown", false)
		injected := errors.New("directory fence failure")
		core := database.connector.db
		markerPath := filepath.Join(core.dataDir, "txn.vtm")
		requireMarkerAbsent := func(label string) {
			t.Helper()
			if _, statErr := os.Lstat(markerPath); !os.IsNotExist(statErr) {
				t.Fatalf("%s changed absent transaction marker: %v", label, statErr)
			}
		}
		catalogParent := filepath.Clean(filepath.Dir(path))
		core.syncDir = func(candidate string) error {
			if filepath.Clean(candidate) == catalogParent {
				return injected
			}
			return syncDirectory(candidate)
		}
		identity, err := database.BindReplicatedShardStore(binding, "docs")
		skipReplicatedStrictAllocationUnsupported(t, database, identity, err)
		if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, injected) ||
			identity.IsZero() {
			t.Fatalf("unknown bind = %+v, %v", identity, err)
		}
		core.syncDir = nil
		if err := database.Close(); err != nil {
			t.Fatalf("close unknown-outcome root: %v", err)
		}
		requireMarkerAbsent("interrupted bind")
		if ordinary, err := OpenReplicatedShardStore(path, identity); ordinary != nil || err == nil {
			if ordinary != nil {
				_ = ordinary.Close()
			}
			t.Fatalf("ordinary exact open with absent marker = %v, %v; want fail closed", ordinary, err)
		}
		requireMarkerAbsent("ordinary exact open")
		wrongLogID := local.LogID
		wrongLogID[0] ^= 0x80
		if wrong, _, err := OpenReplicatedShardStoreForSettlement(
			path, binding, wrongLogID, "docs",
		); wrong != nil || !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
			if wrong != nil {
				_ = wrong.Close()
			}
			t.Fatalf("settlement with wrong retained LogID = %v", err)
		}
		requireMarkerAbsent("wrong-LogID settlement")
		if wrong, _, err := OpenReplicatedShardStoreForSettlement(
			path, binding, local.LogID, "other",
		); wrong != nil || !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
			if wrong != nil {
				_ = wrong.Close()
			}
			t.Fatalf("settlement with wrong intended table = %v", err)
		}
		requireMarkerAbsent("wrong-table settlement")
		wrongBinding := binding
		wrongBinding.Authority.SchemaGeneration++
		if wrong, _, err := OpenReplicatedShardStoreForSettlement(
			path, wrongBinding, local.LogID, "docs",
		); wrong != nil || !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
			if wrong != nil {
				_ = wrong.Close()
			}
			t.Fatalf("settlement with wrong WAL binding = %v", err)
		}
		requireMarkerAbsent("wrong-binding settlement")
		recoveryCalls := 0
		if wrong, err := openDatabaseWithShardStorePolicy(path, func(string) error {
			recoveryCalls++
			return errors.New("recovery must not run")
		}, shardStoreOpenPolicy{
			mode:                        shardStoreOpenReplicatedSettlement,
			expectedReplicated:          ReplicatedShardStoreIdentity{Binding: wrongBinding},
			expectedReplicatedLogID:     local.LogID,
			expectedReplicatedUserTable: "docs",
		}); wrong != nil || !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
			if wrong != nil {
				_ = wrong.closeTerminal()
			}
			t.Fatalf("private wrong-binding settlement = %v", err)
		}
		if recoveryCalls != 0 {
			t.Fatalf("wrong settlement identity reached %d recovery fence(s)", recoveryCalls)
		}
		requireMarkerAbsent("private wrong-binding settlement")
		reopened, settled, err := OpenReplicatedShardStoreForSettlement(
			path, binding, local.LogID, "docs",
		)
		if err != nil {
			t.Fatalf("settlement reopen without full return identity: %v", err)
		}
		if !settled.Equal(identity) {
			t.Fatalf("settled identity = %+v, want proposed %+v", settled, identity)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBindReplicatedShardStoreRejectsLivePreexistingMarker(t *testing.T) {
	_, db, binding, _ := prepareReplicatedTestRoot(t, "preexisting-marker", false)
	defer db.Close()
	core := db.connector.db
	priorTable := core.tables["docs"]
	priorStorage := priorTable.meta.Storage
	priorOptions := core.txnLog.Options()
	if err := core.txnLog.EnsureMinted(); err != nil {
		t.Fatalf("mint ordinary marker fixture: %v", err)
	}
	persistCalls := 0
	identity, err := db.bindReplicatedShardStore(
		binding, "docs", func(*database) (bool, error) {
			persistCalls++
			return true, nil
		},
	)
	if !identity.IsZero() ||
		!errors.Is(err, ErrReplicatedShardStoreProfile) ||
		!errors.Is(err, durable.ErrTransactionLogRecoveryRequired) {
		t.Fatalf("bind with live ordinary marker = %+v, %v", identity, err)
	}
	if persistCalls != 0 || core.catalog.ReplicatedShardStore != nil ||
		core.tables["docs"] != priorTable || priorTable.meta.Storage != priorStorage ||
		priorTable.meta.Materialized || priorTable.collection != nil || priorTable.file != nil ||
		core.txnLog.Options() != priorOptions {
		t.Fatalf(
			"live-marker refusal mutated state: persist=%d catalog=%+v table=%+v options=%+v",
			persistCalls, core.catalog.ReplicatedShardStore, priorTable, core.txnLog.Options(),
		)
	}
	if _, statErr := os.Stat(filepath.Join(core.dataDir, "txn.vtm")); statErr != nil {
		t.Fatalf("live-marker refusal removed marker: %v", statErr)
	}
}

func TestBindReplicatedShardStoreMarkerMintResidueSettlesExactly(t *testing.T) {
	path, database, binding, local := prepareReplicatedTestRoot(t, "marker-mint-residue", false)
	core := database.connector.db
	storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{
		Phase: storeio.TxnMarkerFaultCreateParentDirSync,
	})
	t.Cleanup(func() {
		storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
	})

	identity, err := database.BindReplicatedShardStore(binding, "docs")
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		if storeio.TxnMarkerCreateFaulted() {
			t.Fatal("marker-create fault fired before unsupported sealed user storage rolled back")
		}
		skipReplicatedStrictAllocationUnsupported(t, database, identity, err)
	}
	if identity.IsZero() || !errors.Is(err, syscall.EIO) ||
		!storeio.TxnMarkerCreateFaulted() {
		t.Fatalf("post-catalog marker mint fault = %+v, %v; fired=%t",
			identity, err, storeio.TxnMarkerCreateFaulted())
	}
	markerPath := filepath.Join(core.dataDir, "txn.vtm")
	markerInfo, statErr := os.Stat(markerPath)
	if statErr != nil {
		t.Fatalf("valid marker mint residue is absent: %v", statErr)
	}
	wantMarkerSize := int64(2*storeio.TxnMarkerHeaderSize) +
		int64(ReplicatedTransactionMarkerBytes)
	core.mu.RLock()
	published := core.catalog.ReplicatedShardStore
	table := core.tables["docs"]
	publishedExactly := published != nil && published.Equal(identity) &&
		table != nil && table.meta.Materialized &&
		table.meta.Storage == identity.UserStorage &&
		table.meta.SealedRecoveryJournalBytes == ReplicatedUserRecoveryJournalBytes &&
		table.collection != nil &&
		table.collection.SealedRecoveryJournalBytes() == ReplicatedUserRecoveryJournalBytes &&
		core.txnLog.Options() == (durable.TxnLogOptions{
			Capacity: ReplicatedTransactionMarkerBytes, SealedCapacity: true,
		})
	core.mu.RUnlock()
	if !publishedExactly || markerInfo.Size() != wantMarkerSize {
		t.Fatalf(
			"marker mint failure did not retain the exact published recovery identity: published=%+v size=%d want=%d",
			published, markerInfo.Size(), wantMarkerSize,
		)
	}

	storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
	if closeErr := database.Close(); closeErr != nil {
		t.Fatalf("close after marker mint residue: %v", closeErr)
	}
	settled, settledIdentity, err := OpenReplicatedShardStoreForSettlement(
		path, binding, local.LogID, "docs",
	)
	if err != nil || !settledIdentity.Equal(identity) {
		if settled != nil {
			_ = settled.Close()
		}
		t.Fatalf("settle valid marker mint residue = %+v, %v; want %+v",
			settledIdentity, err, identity)
	}
	settledCore := settled.connector.db
	if settledCore.txnLog.Options() != (durable.TxnLogOptions{
		Capacity: ReplicatedTransactionMarkerBytes, SealedCapacity: true,
	}) {
		t.Fatalf("settled marker options = %+v", settledCore.txnLog.Options())
	}
	settledInfo, err := os.Stat(markerPath)
	if err != nil || settledInfo.Size() != wantMarkerSize {
		t.Fatalf("settled marker = %v, size %d; want %d",
			err, func() int64 {
				if settledInfo == nil {
					return -1
				}
				return settledInfo.Size()
			}(), wantMarkerSize)
	}
	if err := settled.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenReplicatedShardStore(path, identity)
	if err != nil {
		t.Fatalf("exact open after marker residue settlement: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedShardStoreSettlementMarkerMintFaultRetry(t *testing.T) {
	tests := []struct {
		name         string
		phase        storeio.TxnMarkerFaultPhase
		wantErr      error
		validResidue bool
	}{
		{
			name: "header_write_unusable_residue", phase: storeio.TxnMarkerFaultCreateHeaderWrite,
			wantErr: storeio.ErrFaultInjected,
		},
		{
			name: "parent_directory_sync_valid_residue", phase: storeio.TxnMarkerFaultCreateParentDirSync,
			wantErr: syscall.EIO, validResidue: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, db, binding, local := prepareReplicatedTestRoot(t, "settlement-mint-"+test.name, false)
			identity, err := db.bindReplicatedShardStore(
				binding, "docs", func(core *database) (bool, error) {
					published, persistErr := core.persistCatalogLocked()
					if persistErr != nil {
						return published, persistErr
					}
					return published, durable.ErrCommitOutcomeUnknown
				},
			)
			skipReplicatedStrictAllocationUnsupported(t, db, identity, err)
			if identity.IsZero() ||
				!errors.Is(err, durable.ErrCommitOutcomeUnknown) {
				t.Fatalf("published unminted bind = %+v, %v", identity, err)
			}
			markerPath := filepath.Join(db.connector.db.dataDir, "txn.vtm")
			if _, statErr := os.Lstat(markerPath); !os.IsNotExist(statErr) {
				t.Fatalf("published unminted bind marker = %v, want absent", statErr)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close published unminted bind: %v", err)
			}

			storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{Phase: test.phase})
			t.Cleanup(func() {
				storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
			})
			failed, failedIdentity, err := OpenReplicatedShardStoreForSettlement(
				path, binding, local.LogID, "docs",
			)
			if failed != nil {
				_ = failed.Close()
			}
			if errors.Is(err, storeio.ErrStrictAllocationUnsupported) &&
				!storeio.TxnMarkerCreateFaulted() {
				t.Skipf("sealed transaction marker requires strict allocation support: %v", err)
			}
			if failed != nil || !failedIdentity.IsZero() ||
				!errors.Is(err, test.wantErr) ||
				errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
				!storeio.TxnMarkerCreateFaulted() {
				t.Fatalf(
					"faulted exact settlement = %v, %+v, %v; faulted=%t",
					failed, failedIdentity, err, storeio.TxnMarkerCreateFaulted(),
				)
			}

			inspection, decisions, inspectErr := storeio.InspectTxnMarker(markerPath)
			if test.validResidue {
				if inspectErr != nil || inspection == nil || decisions == nil ||
					inspection.Header().Capacity != ReplicatedTransactionMarkerBytes ||
					!inspection.Header().SealedCapacity || decisions.MaxTxnID() != 0 {
					t.Fatalf(
						"valid settlement mint residue = %v, %+v, %v",
						inspection, decisions, inspectErr,
					)
				}
			} else if !errors.Is(inspectErr, storeio.ErrTxnMarkerNoValidHeader) {
				t.Fatalf("early settlement mint residue = %v, want no valid header", inspectErr)
			}
			if inspection != nil {
				if err := inspection.Close(); err != nil {
					t.Fatalf("close settlement mint residue inspection: %v", err)
				}
			}

			storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
			settled, settledIdentity, err := OpenReplicatedShardStoreForSettlement(
				path, binding, local.LogID, "docs",
			)
			if err != nil || !settledIdentity.Equal(identity) {
				if settled != nil {
					_ = settled.Close()
				}
				t.Fatalf("exact settlement retry = %+v, %v; want %+v", settledIdentity, err, identity)
			}
			markerInfo, err := os.Stat(markerPath)
			wantMarkerSize := int64(2*storeio.TxnMarkerHeaderSize) +
				int64(ReplicatedTransactionMarkerBytes)
			markerOptions := settled.connector.db.txnLog.Options()
			if err != nil || markerInfo.Size() != wantMarkerSize ||
				markerOptions != (durable.TxnLogOptions{
					Capacity: ReplicatedTransactionMarkerBytes, SealedCapacity: true,
				}) {
				_ = settled.Close()
				t.Fatalf(
					"retried settlement marker = %v, size %d, options %+v; want size %d sealed profile",
					err, func() int64 {
						if markerInfo == nil {
							return -1
						}
						return markerInfo.Size()
					}(), markerOptions, wantMarkerSize,
				)
			}
			if err := settled.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenReplicatedShardStore(path, identity)
			if err != nil {
				t.Fatalf("ordinary exact open after settlement retry: %v", err)
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReplicatedShardStoreProfileAndBindConnectExclusion(t *testing.T) {
	t.Run("schemaful_preflight_has_no_materialization_side_effect", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "schema.vdb")
		binding := testReplicatedBinding(41)
		database, err := InitializeShardStore(path, ShardStoreBinding{
			Distribution:         distribution.DistributionName(binding.Distribution),
			Shard:                distribution.ShardID(binding.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		session, _ := database.NewSession(context.Background())
		if err := testRuntimeExec(session, `CREATE TABLE docs (id STRING PRIMARY KEY)`, nil); err != nil {
			t.Fatal(err)
		}
		_ = session.Close()
		if database.connector.db.tables["docs"].collection != nil {
			t.Fatal("test table unexpectedly materialized")
		}
		if _, err := database.BindReplicatedShardStore(binding, "docs"); !errors.Is(err, ErrReplicatedShardStoreProfile) {
			t.Fatalf("schemaful bind = %v", err)
		}
		if database.connector.db.tables["docs"].collection != nil {
			t.Fatal("rejected bind materialized schemaful table")
		}
	})

	t.Run("nonempty", func(t *testing.T) {
		_, database, binding, _ := prepareReplicatedTestRoot(t, "nonempty", false)
		defer database.Close()
		session, _ := database.NewSession(context.Background())
		if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"occupied"}`)}); err != nil {
			t.Fatal(err)
		}
		_ = session.Close()
		if _, err := database.BindReplicatedShardStore(binding, "docs"); !errors.Is(err, ErrReplicatedShardStoreProfile) {
			t.Fatalf("nonempty bind = %v", err)
		}
	})

	t.Run("materialized_empty", func(t *testing.T) {
		_, database, binding, _ := prepareReplicatedTestRoot(t, "materialized-empty", true)
		defer database.Close()
		table := database.connector.db.tables["docs"]
		if table == nil || table.collection == nil || table.collection.Len() != 0 {
			t.Fatal("fixture is not an empty materialized table")
		}
		if _, err := database.BindReplicatedShardStore(binding, "docs"); !errors.Is(err, ErrReplicatedShardStoreProfile) {
			t.Fatalf("empty materialized bind = %v, want profile error", err)
		}
	})

	for _, test := range []struct {
		name  string
		setup func(*testing.T, *Session)
	}{
		{"multiple_tables", func(t *testing.T, session *Session) {
			if err := testRuntimeExec(session, `CREATE TABLE other (PRIMARY KEY (id))`, nil); err != nil {
				t.Fatal(err)
			}
		}},
		{"view", func(t *testing.T, session *Session) {
			if err := testRuntimeExec(session, `CREATE VIEW selected AS SELECT id FROM docs`, nil); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, database, binding, _ := prepareReplicatedTestRoot(t, test.name, false)
			defer database.Close()
			session, err := database.NewSession(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			test.setup(t, session)
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := database.BindReplicatedShardStore(binding, "docs"); !errors.Is(err, ErrReplicatedShardStoreProfile) {
				t.Fatalf("bind with %s = %v, want profile error", test.name, err)
			}
		})
	}

	t.Run("prior_local_serving_fence", func(t *testing.T) {
		_, database, binding, local := prepareReplicatedTestRoot(t, "served", false)
		defer database.Close()
		claim, err := database.ClaimShardStoreServing(local.Binding(), ShardStoreFence{
			OwnershipEpoch: 1, RoutingVersion: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := claim.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := database.BindReplicatedShardStore(binding, "docs"); !errors.Is(err, ErrReplicatedShardStoreBusy) {
			t.Fatalf("bind after local serving = %v, want Busy", err)
		}
	})

	t.Run("connect_race", func(t *testing.T) {
		_, database, binding, _ := prepareReplicatedTestRoot(t, "race", false)
		defer database.Close()
		type bindResult struct {
			identity ReplicatedShardStoreIdentity
			err      error
		}
		connectResult := make(chan struct {
			session *Session
			err     error
		}, 1)
		bindResults := make(chan bindResult, 1)
		database.connector.mu.Lock()
		go func() {
			session, err := database.NewSession(context.Background())
			connectResult <- struct {
				session *Session
				err     error
			}{session, err}
		}()
		go func() {
			identity, err := database.BindReplicatedShardStore(binding, "docs")
			bindResults <- bindResult{identity, err}
		}()
		database.connector.mu.Unlock()
		connected := <-connectResult
		bound := <-bindResults
		if connected.err != nil {
			t.Fatalf("racing Connect: %v", connected.err)
		}
		unsupported := errors.Is(bound.err, storeio.ErrStrictAllocationUnsupported)
		if bound.err == nil {
			if !connected.session.conn.directWritesFenced {
				t.Fatal("bind-first connection did not inherit direct-write fence")
			}
			if err := testRuntimeExec(connected.session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"race"}`)}); !errors.Is(err, ErrDirectWriteFenced) {
				t.Fatalf("bind-first racing session write = %v", err)
			}
		} else if !unsupported {
			if !errors.Is(bound.err, ErrReplicatedShardStoreBusy) {
				t.Fatalf("session-first bind = %v, want Busy", bound.err)
			}
			if connected.session.conn.directWritesFenced {
				t.Fatal("session-first connection unexpectedly fenced before bind")
			}
		}
		if err := connected.session.Close(); err != nil {
			t.Fatal(err)
		}
		if unsupported {
			t.Skipf("sealed replicated sidecars require strict allocation support: %v", bound.err)
		}
		if bound.err != nil {
			_ = requireReplicatedShardStoreBind(t, database, binding, "docs")
		}
	})
}

func TestReplicatedShardStoreStrictIdentityDecode(t *testing.T) {
	identity := ReplicatedShardStoreIdentity{
		Format:    ReplicatedShardStoreFormat,
		Binding:   testReplicatedBinding(77),
		LogID:     [16]byte{0xab, 1},
		UserTable: "docs", UserStorage: strings.Repeat("a", storageIdentityBytes*2),
		UserPrimaryKey: "/id",
		UserLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: replicatedMaxKeyBytes, MaxDocumentBytes: replicatedMaxDocumentBytes,
			MaxBatchDocuments: replicatedMaxDistinctMutations, MaxBatchBytes: replicatedMaxBatchBytes,
		},
		Sidecars: canonicalReplicatedShardStoreSidecars(), RelationCount: 1,
		Relations: make([]ReplicatedShardRelationIdentity, 1),
	}
	identity.RelationSchemaGeneration = identity.Binding.Authority.SchemaGeneration
	identity.Relations[0] = ReplicatedShardRelationIdentity{
		Relation: 1, Kind: ReplicatedShardRelationJSON,
		Table: identity.UserTable, Storage: identity.UserStorage, Limits: identity.UserLimits,
	}
	identity.RelationManifestDigest = replicatedRelationManifestDigest(identity)
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, extension := range [][]byte{
		[]byte(`"relation_schema_generation"`),
		[]byte(`"relation_manifest_digest"`),
		[]byte(`"relations"`),
	} {
		if !bytes.Contains(raw, extension) {
			t.Fatalf("singleton identity omitted relation field %s: %s", extension, raw)
		}
	}
	var decoded ReplicatedShardStoreIdentity
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("valid decode: %v", err)
	}
	if !decoded.Equal(identity) {
		t.Fatalf("singleton relation model changed on decode: %+v", decoded)
	}
	if cap(decoded.Relations) != len(decoded.Relations) {
		t.Fatalf("decoded relation capacity = %d, want exact %d", cap(decoded.Relations), len(decoded.Relations))
	}
	cloned := decoded.Clone()
	if cap(cloned.Relations) != len(cloned.Relations) {
		t.Fatalf("cloned relation capacity = %d, want exact %d", cap(cloned.Relations), len(cloned.Relations))
	}
	cloned.Relations[0].Table = "mutated"
	if decoded.Relations[0].Table != identity.Relations[0].Table {
		t.Fatal("identity clone retained caller-owned relation storage")
	}
	if reencoded, err := json.Marshal(decoded); err != nil || !bytes.Equal(reencoded, raw) {
		t.Fatalf("singleton canonical re-encode = %s, %v; want %s", reencoded, err, raw)
	}
	direct, err := identity.MarshalJSON()
	if err != nil || !bytes.Equal(direct, raw) {
		t.Fatalf("direct canonical image = %s, %v; want %s", direct, err, raw)
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(direct)); digest !=
		"ca0a84d864e17c24c172dbd9eb3dee21e5d3bf5b0d4faa518a1709a5217fc6a5" {
		t.Fatalf("singleton identity golden digest = %s", digest)
	}
	ownedInput := bytes.Clone(direct)
	var owned ReplicatedShardStoreIdentity
	if err := owned.UnmarshalJSON(ownedInput); err != nil {
		t.Fatalf("direct owned decode: %v", err)
	}
	for index := range ownedInput {
		ownedInput[index] = 'x'
	}
	if !owned.Equal(identity) {
		t.Fatal("decoded identity retained caller-owned catalog bytes")
	}

	var encoded []byte
	var codecErr error
	encodeAllocs := testing.AllocsPerRun(100, func() {
		encoded, codecErr = identity.MarshalJSON()
	})
	if codecErr != nil {
		t.Fatalf("allocation encode: %v", codecErr)
	}
	runtime.KeepAlive(encoded)
	var allocationDecoded ReplicatedShardStoreIdentity
	decodeAllocs := testing.AllocsPerRun(100, func() {
		allocationDecoded = ReplicatedShardStoreIdentity{}
		codecErr = allocationDecoded.UnmarshalJSON(direct)
	})
	if codecErr != nil || !allocationDecoded.Equal(identity) {
		t.Fatalf("allocation decode = %+v, %v", allocationDecoded, codecErr)
	}
	if encodeAllocs > 12 || decodeAllocs > 24 {
		t.Fatalf(
			"singleton identity codec allocations = encode %.1f, decode %.1f",
			encodeAllocs, decodeAllocs,
		)
	}
	var identityFields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &identityFields); err != nil {
		t.Fatal(err)
	}
	delete(identityFields, "sidecars")
	missingSidecars, err := json.Marshal(identityFields)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{
		"unknown":         append([]byte(`{"unknown":1,`), raw[1:]...),
		"duplicate":       []byte(strings.Replace(string(raw), `"format":0`, `"format":0,"format":0`, 1)),
		"missing":         []byte(strings.Replace(string(raw), `"format":0,`, ``, 1)),
		"null_format":     []byte(strings.Replace(string(raw), `"format":0`, `"format":null`, 1)),
		"null":            []byte(`null`),
		"uppercase_hex":   []byte(strings.Replace(string(raw), `"log_id":"ab`, `"log_id":"AB`, 1)),
		"binding_unknown": []byte(strings.Replace(string(raw), `"binding":{`, `"binding":{"unknown":1,`, 1)),
		"authority_duplicate": []byte(strings.Replace(string(raw),
			`"authority":{"active_policy_generation":11`,
			`"authority":{"active_policy_generation":11,"active_policy_generation":11`, 1)),
		"limits_unknown":   []byte(strings.Replace(string(raw), `"user_limits":{`, `"user_limits":{"unknown":1,`, 1)),
		"sidecars_unknown": []byte(strings.Replace(string(raw), `"sidecars":{`, `"sidecars":{"unknown":1,`, 1)),
		"sidecars_missing_member": []byte(strings.Replace(
			string(raw), `"transaction_marker_bytes":1048576`, `"unused":1048576`, 1,
		)),
		"sidecars_wrong_user_journal": []byte(strings.Replace(
			string(raw), `"user_recovery_journal_bytes":16794624`,
			`"user_recovery_journal_bytes":16794112`, 1,
		)),
		"sidecars_missing": missingSidecars,
		"trailing":         append(bytes.Clone(raw), []byte(` {}`)...),
		"truncated":        bytes.Clone(raw[:len(raw)-1]),
	}
	oversizedBindingText := `"` + strings.Repeat(`\u0061`, maxReplicatedBindingJSONBytes) + `"`
	cases["oversized_binding"] = []byte(strings.Replace(
		string(raw), `"distribution":"accounts"`,
		`"distribution":`+oversizedBindingText, 1,
	))
	relationRaw, err := identity.Relations[0].MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	relationStart := bytes.Index(raw, relationRaw)
	if relationStart < 0 {
		t.Fatalf("canonical relation missing from identity: %s", raw)
	}
	relationEnd := relationStart + len(relationRaw)
	// The relations member is last in the canonical image, so removing its sole
	// element produces an exact, syntactically valid empty-array fixture.
	cases["empty_relations"] = append(
		bytes.Clone(raw[:relationStart]), raw[relationEnd:]...,
	)
	oversizedTable := `"` + strings.Repeat(`\u0061`, maxReplicatedRelationJSONBytes) + `"`
	oversizedRelation := bytes.Replace(
		relationRaw, []byte(`"table":"docs"`), []byte(`"table":`+oversizedTable), 1,
	)
	if len(oversizedRelation) <= maxReplicatedRelationJSONBytes {
		t.Fatal("oversized relation fixture stayed within its byte bound")
	}
	cases["oversized_relation"] = append(
		append(bytes.Clone(raw[:relationStart]), oversizedRelation...), raw[relationEnd:]...,
	)
	repeatedRelations := make([][]byte, replication.MaxRelationsPerBundle+1)
	for index := range repeatedRelations {
		repeatedRelations[index] = relationRaw
	}
	cases["too_many_relations"] = append(
		append(bytes.Clone(raw[:relationStart]), bytes.Join(repeatedRelations, []byte{','})...),
		raw[relationEnd:]...,
	)
	for name, image := range cases {
		t.Run(name, func(t *testing.T) {
			var decoded ReplicatedShardStoreIdentity
			err := decoded.UnmarshalJSON(image)
			if err == nil {
				t.Fatalf("accepted noncanonical image: %s", image)
			}
			if name == "oversized_relation" &&
				!strings.Contains(err.Error(), "relation exceeds its byte bound") {
				t.Fatalf("oversized relation error = %v", err)
			}
			if name == "oversized_binding" &&
				!strings.Contains(err.Error(), "binding exceeds its byte bound") {
				t.Fatalf("oversized binding error = %v", err)
			}
		})
	}
	if err := decoded.UnmarshalJSON(bytes.Repeat(
		[]byte{' '}, maxReplicatedStoreJSONBytes+1,
	)); err == nil || !strings.Contains(err.Error(), "identity exceeds its byte bound") {
		t.Fatalf("oversized identity error = %v", err)
	}
	reserved := identity.Binding
	reserved.MemberID = ^uint64(0)
	if err := validateReplicatedShardStoreBinding(reserved); err == nil {
		t.Fatal("accepted reserved local-message member id")
	}
}

func TestReplicatedShardRelationManifestCanonicalRoundTripAndRejection(t *testing.T) {
	identity := ReplicatedShardStoreIdentity{
		Format: ReplicatedShardStoreFormat, Binding: testReplicatedBinding(78),
		LogID: [16]byte{0xac, 1}, UserTable: "docs",
		UserStorage: strings.Repeat("a", storageIdentityBytes*2), UserPrimaryKey: "/id",
		UserLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: replicatedMaxKeyBytes, MaxDocumentBytes: replicatedMaxDocumentBytes,
			MaxBatchDocuments: replicatedMaxDistinctMutations, MaxBatchBytes: replicatedMaxBatchBytes,
		},
		Sidecars: canonicalReplicatedShardStoreSidecars(), RelationCount: 2,
		Relations: make([]ReplicatedShardRelationIdentity, 2),
	}
	identity.RelationSchemaGeneration = identity.Binding.Authority.SchemaGeneration
	identity.Relations[0] = ReplicatedShardRelationIdentity{
		Relation: 1, Kind: ReplicatedShardRelationJSON,
		Table: identity.UserTable, Storage: identity.UserStorage, Limits: identity.UserLimits,
		LocalIndexDigest: sha256.Sum256([]byte("by_email:/email")),
	}
	identity.Relations[1] = ReplicatedShardRelationIdentity{
		Relation: 2, Kind: ReplicatedShardRelationGlobalIndex,
		Table: "email_claims", Storage: strings.Repeat("b", storageIdentityBytes*2),
		Limits: identity.UserLimits, IndexID: 41, Incarnation: 7,
		LocatorCount: 1, Unique: true,
		KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
		TupleVersion:  distribution.CurrentTupleVersion,
		MapperVersion: distribution.NativeMapperVersion,
		BucketBits:    distribution.DefaultVirtualBucketBits,
	}
	identity.RelationManifestDigest = replicatedRelationManifestDigest(identity)
	if err := validateReplicatedShardStoreIdentity(identity); err != nil {
		t.Fatal(err)
	}
	logicalManifest := replicatedRelationApplyManifestDigest(identity)
	otherReplica := identity.Clone()
	otherReplica.UserStorage = strings.Repeat("c", storageIdentityBytes*2)
	otherReplica.Relations[0].Storage = otherReplica.UserStorage
	otherReplica.Relations[1].Storage = strings.Repeat("d", storageIdentityBytes*2)
	otherReplica.RelationManifestDigest = replicatedRelationManifestDigest(otherReplica)
	if otherReplica.RelationManifestDigest == identity.RelationManifestDigest ||
		replicatedRelationApplyManifestDigest(otherReplica) != logicalManifest {
		t.Fatal("replica-local storage identity leaked into the portable apply manifest")
	}
	placement := testReplicatedApplyOptions().Placement
	if replicatedApplyProfileDigest(identity, placement) !=
		replicatedApplyProfileDigest(otherReplica, placement) ||
		replicatedGlobalIndexValidationDigest(
			identity, identity.Relations[1], logicalManifest,
		) != replicatedGlobalIndexValidationDigest(
			otherReplica, otherReplica.Relations[1], logicalManifest,
		) {
		t.Fatal("replica-local storage identity changed a replicated validation contract")
	}
	otherSchema := identity.Clone()
	otherSchema.Relations[1].IndexID++
	otherSchema.RelationManifestDigest = replicatedRelationManifestDigest(otherSchema)
	if replicatedRelationApplyManifestDigest(otherSchema) == logicalManifest {
		t.Fatal("logical global-index identity was omitted from the portable apply manifest")
	}
	raw, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ReplicatedShardStoreIdentity
	if err := json.Unmarshal(raw, &decoded); err != nil || !decoded.Equal(identity) {
		t.Fatalf("round trip = %+v, %v", decoded, err)
	}
	if reencoded, err := json.Marshal(decoded); err != nil || !bytes.Equal(reencoded, raw) {
		t.Fatalf("canonical re-encode differs: %s, %v", reencoded, err)
	}

	for _, test := range []struct {
		name   string
		mutate func(*ReplicatedShardStoreIdentity)
	}{
		{"reordered", func(i *ReplicatedShardStoreIdentity) {
			i.Relations[0], i.Relations[1] = i.Relations[1], i.Relations[0]
		}},
		{"duplicate", func(i *ReplicatedShardStoreIdentity) {
			i.Relations[1].Table = i.Relations[0].Table
		}},
		{"sparse", func(i *ReplicatedShardStoreIdentity) { i.Relations[1].Relation = 3 }},
		{"schema_generation", func(i *ReplicatedShardStoreIdentity) {
			i.RelationSchemaGeneration++
		}},
		{"digest", func(i *ReplicatedShardStoreIdentity) { i.RelationManifestDigest[0] ^= 1 }},
		{"count_mismatch", func(i *ReplicatedShardStoreIdentity) {
			i.Relations = append(i.Relations, ReplicatedShardRelationIdentity{Relation: 3})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := identity.Clone()
			test.mutate(&candidate)
			if err := validateReplicatedShardStoreIdentity(candidate); err == nil {
				t.Fatal("accepted malformed relation manifest")
			}
		})
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	delete(fields, "relation_manifest_digest")
	partial, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(partial, new(ReplicatedShardStoreIdentity)); err == nil {
		t.Fatal("accepted partial relation-manifest extension")
	}
}

func TestReplicatedShardStoreIdentityMarshalRejectsOversizedRelationCountWithoutPanic(t *testing.T) {
	identity := ReplicatedShardStoreIdentity{
		Format:    ReplicatedShardStoreFormat,
		Binding:   testReplicatedBinding(79),
		LogID:     [16]byte{0xad, 1},
		UserTable: "docs", UserStorage: strings.Repeat("a", storageIdentityBytes*2),
		UserPrimaryKey: "/id",
		UserLimits: ReplicatedShardStoreLimits{
			MaxKeyBytes: replicatedMaxKeyBytes, MaxDocumentBytes: replicatedMaxDocumentBytes,
			MaxBatchDocuments: replicatedMaxDistinctMutations, MaxBatchBytes: replicatedMaxBatchBytes,
		},
		Sidecars:      canonicalReplicatedShardStoreSidecars(),
		RelationCount: replication.MaxRelationsPerBundle + 1,
	}
	deferred := func() (raw []byte, err error, recovered any) {
		defer func() { recovered = recover() }()
		raw, err = json.Marshal(identity)
		return raw, err, nil
	}
	raw, err, recovered := deferred()
	if recovered != nil || err == nil || raw != nil {
		t.Fatalf("oversized relation marshal = %q, %v, panic=%v", raw, err, recovered)
	}
}

func TestBindReplicatedShardStoreBundleRejectsOverlongRelationBeforeMutation(t *testing.T) {
	_, database, binding, _ := prepareReplicatedTestRoot(t, "bundle-long-name", false)
	defer database.Close()
	name := strings.Repeat("g", replication.MaxIdentityBytes+1)
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(
		session, fmt.Sprintf("CREATE TABLE %s (PRIMARY KEY (key))", name), nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	core := database.connector.db
	core.mu.RLock()
	beforeOptions := core.txnLog.Options()
	baseBefore := core.tables["docs"].collection
	globalBefore := core.tables[name].collection
	core.mu.RUnlock()
	if baseBefore != nil || globalBefore != nil {
		t.Fatal("fixture relations were materialized before bundle bind")
	}
	_, err = database.BindReplicatedShardStoreBundle(
		binding, "docs", []ReplicatedGlobalIndexRelation{{
			Relation: 2, Table: name, IndexID: 41, Incarnation: 7,
			LocatorCount: 1, Unique: true,
			KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
			TupleVersion:  distribution.CurrentTupleVersion,
			MapperVersion: distribution.NativeMapperVersion,
			BucketBits:    distribution.DefaultVirtualBucketBits,
		}},
	)
	if !errors.Is(err, ErrReplicatedShardStoreProfile) {
		t.Fatalf("overlong bundle relation bind = %v", err)
	}
	core.mu.RLock()
	defer core.mu.RUnlock()
	if core.catalog.ReplicatedShardStore != nil ||
		core.tables["docs"].collection != baseBefore ||
		core.tables[name].collection != globalBefore ||
		core.txnLog.Options() != beforeOptions {
		t.Fatal("rejected overlong relation changed catalog, storage, or transaction-log profile")
	}
}

func TestReplicatedCatalogStrictRootAndProfileDecode(t *testing.T) {
	mutations := []struct {
		name string
		edit func(string) string
		want error
	}{
		{"unknown_root", func(raw string) string { return `{"unknown":1,` + raw[1:] }, nil},
		{"duplicate_root", func(raw string) string {
			return strings.Replace(raw, `"version":0`, `"version":0,
  "version": 0`, 1)
		}, nil},
		{"null_version", func(raw string) string {
			return strings.Replace(raw, `"version":0`, `"version":null`, 1)
		}, nil},
		{"profile_primary", func(raw string) string {
			return strings.Replace(raw, `"user_primary_key":"/id"`, `"user_primary_key":"/other"`, 1)
		}, ErrReplicatedShardStoreProfile},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			path, database, binding, _ := prepareReplicatedTestRoot(t, mutation.name, false)
			identity := requireReplicatedShardStoreBind(t, database, binding, "docs")
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			changed := mutation.edit(string(raw))
			if changed == string(raw) {
				t.Fatal("catalog mutation pattern did not match")
			}
			if err := os.WriteFile(path, []byte(changed), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err = OpenReplicatedShardStore(path, identity)
			if err == nil {
				t.Fatal("corrupt catalog opened")
			}
			if mutation.want != nil && !errors.Is(err, mutation.want) {
				t.Fatalf("corrupt catalog = %v, want %v", err, mutation.want)
			}
		})
	}
}

func TestPrimaryOnlySQLTableHasSchemaFreeDurableProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema-free.vdb")
	database, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(session, `CREATE TABLE docs (PRIMARY KEY (id))`, nil); err != nil {
		t.Fatal(err)
	}
	if table := database.connector.db.tables["docs"]; table == nil || table.schema != nil || table.meta.Schema != nil {
		t.Fatalf("primary-only table durable schema = %+v", table)
	}
	for _, document := range []string{`{"value":1}`, `{"id":null}`, `{"id":{}}`} {
		if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(document)}); err == nil {
			t.Fatalf("primary-only table accepted invalid key document %s", document)
		}
	}
	if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"valid","extra":true}`)}); err != nil {
		t.Fatalf("schema-free primary table rejected flexible document: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if table := reopened.connector.db.tables["docs"]; table.schema != nil || table.collection.HasSchema() {
		t.Fatal("schema-free primary table reopened with a durable schema")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedDirtyCommitDefense(t *testing.T) {
	_, database, _, _ := prepareReplicatedTestRoot(t, "dirty-commit", true)
	defer database.Close()
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Begin(context.Background(), TxOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := testRuntimeExec(session, `INSERT INTO docs VALUES (?)`, []any{[]byte(`{"id":"staged"}`)}); err != nil {
		t.Fatal(err)
	}
	session.conn.directWritesFenced = true
	if err := session.Commit(context.Background()); !errors.Is(err, ErrDirectWriteFenced) {
		t.Fatalf("dirty Commit = %v, want direct fence", err)
	}
	session.conn.directWritesFenced = false
	if got := database.connector.db.tables["docs"].collection.Len(); got != 0 {
		t.Fatalf("dirty fenced Commit published %d rows", got)
	}
}

func TestReplicatedSeparatelyPreparedRootRequiresRetainedSQLIdentity(t *testing.T) {
	binding := testReplicatedBinding(91)
	bindRoot := func(name string) (string, ReplicatedShardStoreIdentity) {
		path := filepath.Join(t.TempDir(), name+".vdb")
		database, err := InitializeShardStore(path, ShardStoreBinding{
			Distribution:         distribution.DistributionName(binding.Distribution),
			Shard:                distribution.ShardID(binding.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(binding.AllocationGeneration),
		})
		if err != nil {
			t.Fatal(err)
		}
		session, sessionErr := database.NewSession(context.Background())
		if sessionErr != nil {
			t.Fatal(sessionErr)
		}
		if err := testRuntimeExec(session, `CREATE TABLE docs (PRIMARY KEY (id))`, nil); err != nil {
			t.Fatal(err)
		}
		_ = session.Close()
		identity := requireReplicatedShardStoreBind(t, database, binding, "docs")
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
		return path, identity
	}
	firstPath, first := bindRoot("first")
	secondPath, second := bindRoot("second")
	if first.LogID == second.LogID && first.UserStorage == second.UserStorage {
		t.Fatal("separately prepared roots unexpectedly share both SQL identities")
	}
	if _, err := OpenReplicatedShardStore(secondPath, first); !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
		t.Fatalf("second root under first identity = %v", err)
	}
	opened, err := OpenReplicatedShardStore(firstPath, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
	opened, err = OpenReplicatedShardStore(secondPath, second)
	if err != nil {
		t.Fatal(err)
	}
	if err := opened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedBindingAcceptsByteIdenticalSQLRootCopy(t *testing.T) {
	path, database, binding, _ := prepareReplicatedTestRoot(t, "original", false)
	identity := requireReplicatedShardStoreBind(t, database, binding, "docs")
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	clonePath := filepath.Join(t.TempDir(), "clone.vdb")
	copyReplicatedTestPath(t, path, clonePath)
	copyReplicatedTestPath(t, path+".tables", clonePath+".tables")

	clone, err := OpenReplicatedShardStore(clonePath, identity)
	if err != nil {
		t.Fatalf("exact byte-copied SQL root was distinguishable: %v", err)
	}
	if err := clone.Close(); err != nil {
		t.Fatal(err)
	}
}

func copyReplicatedTestPath(t testing.TB, source, destination string) {
	t.Helper()
	info, err := os.Lstat(source)
	if err != nil {
		t.Fatal(err)
	}
	if info.IsDir() {
		if err := os.Mkdir(destination, info.Mode().Perm()); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(source)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			copyReplicatedTestPath(t,
				filepath.Join(source, entry.Name()),
				filepath.Join(destination, entry.Name()),
			)
		}
		return
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("copy fixture %s is not a regular file", source)
	}
	input, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		_ = input.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	copyErr = errors.Join(copyErr, output.Sync(), output.Close(), input.Close())
	if copyErr != nil {
		t.Fatal(copyErr)
	}
}
