package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store"
)

// These tests are a green state-machine scaffold for the RF3 transaction
// protocol. Transaction commands are not part of replication.Command yet, so
// the scaffold represents coordinator and participant records as ordinary
// relation rows and exercises the exact primitives the eventual hidden system
// records must use: digest-guarded monotone transitions, atomic relation-bundle
// publication, exact retry, snapshot certification, and reopen.
//
// Once first-class transaction commands exist, keep the assertions and replace
// only transactionStateScaffold's command construction and record reads. A
// production transaction test must additionally prove that the replicated
// active-barrier index blocks intersecting reads and writes; no such hook
// exists in Machine today, so this file deliberately does not pretend to test
// that contract.

type transactionStateScaffold struct {
	fixture  relationBundleFixture
	sequence uint64
	index    uint64
}

func newTransactionStateScaffold(t testing.TB) *transactionStateScaffold {
	t.Helper()
	return &transactionStateScaffold{
		fixture: newRelationBundleFixture(t, true),
		// newRelationBundleFixture installs index one and opens its session at
		// index two. User commands therefore begin at sequence one/index three.
		sequence: 1,
		index:    3,
	}
}

func (scaffold *transactionStateScaffold) command(
	t testing.TB,
	batches ...replication.RelationMutationBatch,
) []byte {
	t.Helper()
	command := scaffold.fixture.command(t, scaffold.sequence, batches...)
	scaffold.sequence++
	return command
}

func (scaffold *transactionStateScaffold) apply(
	t testing.TB,
	command []byte,
) uint32 {
	t.Helper()
	if _, err := scaffold.fixture.machine.ApplyNormal(normalMeta(scaffold.index), command); err != nil {
		t.Fatalf("apply transaction scaffold command at %d: %v", scaffold.index, err)
	}
	scaffold.index++
	return bundleCompletionResult(t, scaffold.fixture.machine, command)
}

func (scaffold *transactionStateScaffold) read(
	t testing.TB,
	relation replication.RelationID,
	key []byte,
) ([]byte, bool) {
	t.Helper()
	var collection = scaffold.fixture.base.Collection
	if relation == 2 {
		collection = scaffold.fixture.global.Collection
	}
	value, found, err := collection.AppendRaw(nil, key)
	if err != nil {
		t.Fatalf("read transaction scaffold relation %d key %q: %v", relation, key, err)
	}
	return value, found
}

func transactionStatePut(
	relation replication.RelationID,
	key, value []byte,
) replication.RelationMutationBatch {
	return replication.RelationMutationBatch{
		Relation: relation,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual,
			Key:  key, Value: value,
		}},
	}
}

func transactionStateCAS(
	relation replication.RelationID,
	key, before, after []byte,
) replication.RelationMutationBatch {
	return replication.RelationMutationBatch{
		Relation: relation,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPutDigestEqual,
			Key:  key, Value: after,
			ExpectedValueLength: uint64(len(before)),
			ExpectedValueDigest: sha256.Sum256(before),
		}},
	}
}

func TestTransactionStateScaffoldCoordinatorDecisionIsMonotoneCAS(t *testing.T) {
	key := []byte("txn/coordinator/0000000000000001")
	staging := []byte(`{"revision":1,"state":"staging"}`)
	committed := []byte(`{"revision":2,"state":"committed"}`)
	aborted := []byte(`{"revision":2,"state":"aborted"}`)

	for _, test := range []struct {
		name       string
		winner     []byte
		loser      []byte
		wantWinner string
	}{
		{name: "commit_wins", winner: committed, loser: aborted, wantWinner: "committed"},
		{name: "abort_wins", winner: aborted, loser: committed, wantWinner: "aborted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			scaffold := newTransactionStateScaffold(t)
			if result := scaffold.apply(t, scaffold.command(
				t, transactionStatePut(1, key, staging),
			)); result != ResultApplied {
				t.Fatalf("stage result=%d, want %d", result, ResultApplied)
			}

			winner := scaffold.command(t, transactionStateCAS(1, key, staging, test.winner))
			loser := scaffold.command(t, transactionStateCAS(1, key, staging, test.loser))
			if result := scaffold.apply(t, winner); result != ResultApplied {
				t.Fatalf("%s decision result=%d, want %d", test.wantWinner, result, ResultApplied)
			}
			firstCompletion, err := scaffold.fixture.machine.LookupCompletion(winner)
			if err != nil {
				t.Fatal(err)
			}
			if result := scaffold.apply(t, loser); result != ResultIndexConflict {
				t.Fatalf("losing decision result=%d, want %d", result, ResultIndexConflict)
			}
			if got, found := scaffold.read(t, 1, key); !found || !bytes.Equal(got, test.winner) {
				t.Fatalf("decision row=%s found=%t, want %s", got, found, test.winner)
			}

			// A response-lost retry of the winning decision retains its original
			// deterministic completion even after the competing decision ran.
			if result := scaffold.apply(t, winner); result != ResultApplied {
				t.Fatalf("exact winner retry result=%d, want %d", result, ResultApplied)
			}
			retried, err := scaffold.fixture.machine.LookupCompletion(winner)
			if err != nil || retried.AppliedSequence != firstCompletion.AppliedSequence ||
				!bytes.Equal(retried.Bytes, firstCompletion.Bytes) {
				t.Fatalf("winner retry completion=%+v err=%v, first=%+v",
					retried, err, firstCompletion)
			}
		})
	}
}

func TestTransactionStateScaffoldParticipantApplyIsAtomicAndIdempotent(t *testing.T) {
	scaffold := newTransactionStateScaffold(t)
	documentKey := []byte("doc-1")
	document := []byte(`{"email":"a","n":1}`)
	markerKey := []byte("txn/participant/0000000000000001")
	marker := []byte(`{"revision":3,"rows":1,"state":"applied"}`)
	conflictIndexKey := []byte{0x91, 0x01, 'x'}
	conflictLocator := []byte(`["someone-else"]`)

	if result := scaffold.apply(t, scaffold.command(
		t, transactionStatePut(2, conflictIndexKey, conflictLocator),
	)); result != ResultApplied {
		t.Fatalf("seed global claim result=%d, want %d", result, ResultApplied)
	}
	failed := scaffold.command(t,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{
			{Kind: replication.MutationPut, Key: documentKey, Value: document},
			{Kind: replication.MutationPutAbsentOrEqual, Key: markerKey, Value: marker},
		}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual,
			Key:  conflictIndexKey, Value: []byte(`["doc-1"]`),
		}}},
	)
	if result := scaffold.apply(t, failed); result != ResultIndexConflict {
		t.Fatalf("conflicting participant apply result=%d, want %d", result, ResultIndexConflict)
	}
	for _, key := range [][]byte{documentKey, markerKey} {
		if value, found := scaffold.read(t, 1, key); found {
			t.Fatalf("failed participant apply published key %q value %s", key, value)
		}
	}

	globalKey := []byte{0x91, 0x01, 'a'}
	locator := []byte(`["doc-1"]`)
	applied := scaffold.command(t,
		replication.RelationMutationBatch{Relation: 1, Mutations: []replication.Mutation{
			{Kind: replication.MutationPut, Key: documentKey, Value: document},
			{Kind: replication.MutationPutAbsentOrEqual, Key: markerKey, Value: marker},
		}},
		replication.RelationMutationBatch{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: locator,
		}}},
	)
	if result := scaffold.apply(t, applied); result != ResultApplied {
		t.Fatalf("participant apply result=%d, want %d", result, ResultApplied)
	}
	firstCompletion, err := scaffold.fixture.machine.LookupCompletion(applied)
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		relation replication.RelationID
		key      []byte
		want     []byte
	}{
		{relation: 1, key: documentKey, want: document},
		{relation: 1, key: markerKey, want: marker},
		{relation: 2, key: globalKey, want: locator},
	} {
		if value, found := scaffold.read(t, check.relation, check.key); !found ||
			!bytes.Equal(value, check.want) {
			t.Fatalf("relation %d key %q=%s found=%t, want %s",
				check.relation, check.key, value, found, check.want)
		}
	}

	if result := scaffold.apply(t, applied); result != ResultApplied {
		t.Fatalf("exact participant retry result=%d, want %d", result, ResultApplied)
	}
	retried, err := scaffold.fixture.machine.LookupCompletion(applied)
	if err != nil || retried.AppliedSequence != firstCompletion.AppliedSequence ||
		!bytes.Equal(retried.Bytes, firstCompletion.Bytes) {
		t.Fatalf("participant retry completion=%+v err=%v, first=%+v",
			retried, err, firstCompletion)
	}
}

func TestTransactionStateScaffoldSnapshotAndReopenRetainRecords(t *testing.T) {
	scaffold := newTransactionStateScaffold(t)
	coordinatorKey := []byte("txn/coordinator/0000000000000002")
	coordinator := []byte(`{"revision":2,"state":"committed"}`)
	participantKey := []byte("txn/participant/0000000000000002")
	participant := []byte(`{"revision":2,"state":"prepared"}`)

	command := scaffold.command(t, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{
			{Kind: replication.MutationPutAbsentOrEqual, Key: coordinatorKey, Value: coordinator},
			{Kind: replication.MutationPutAbsentOrEqual, Key: participantKey, Value: participant},
		},
	})
	if result := scaffold.apply(t, command); result != ResultApplied {
		t.Fatalf("transaction state publish result=%d, want %d", result, ResultApplied)
	}

	base, manifest, err := scaffold.fixture.machine.BuildBundleSnapshotBase()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Relations) != 2 ||
		manifest.RelationManifestDigest != scaffold.fixture.machine.manifestDigest {
		t.Fatalf("transaction snapshot manifest=%+v", manifest)
	}
	publication, err := scaffold.fixture.machine.InstallSnapshot(base)
	if err != nil || publication.Applied != scaffold.index-1 {
		t.Fatalf("install transaction snapshot publication=%+v err=%v", publication, err)
	}
	if err := scaffold.fixture.group.Checkpoint(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenBundle(
		scaffold.fixture.binding,
		testBootstrap(),
		scaffold.fixture.system,
		[]RelationCollection{
			{
				Relation: 1, Kind: RelationJSON, Name: "base", Target: scaffold.fixture.base,
				LocalIndexes: []store.IndexDefinition{scaffold.fixture.index},
			},
			{
				Relation: 2, Kind: RelationGlobalIndex, Name: "global", Target: scaffold.fixture.global,
				GlobalIndex: GlobalIndexProfile{
					IndexID: 91, Incarnation: 7, LocatorCount: 1, Unique: true,
				},
			},
		},
		scaffold.fixture.log,
		scaffold.fixture.machine.options,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Applied() != publication.Applied ||
		reopened.Published().DataChainDigest != publication.DataChainDigest {
		t.Fatalf("reopened transaction state publication=%+v snapshot=%+v",
			reopened.Published(), publication)
	}
	for _, check := range []struct {
		key  []byte
		want []byte
	}{
		{key: coordinatorKey, want: coordinator},
		{key: participantKey, want: participant},
	} {
		result, readErr := reopened.PointReadInto(
			1, check.key, publication.Applied,
			scaffold.fixture.base.Limits.MaxDocumentBytes, nil,
		)
		if readErr != nil || !result.Found || !bytes.Equal(result.Value, check.want) {
			t.Fatalf("reopened transaction key %q=%s found=%t err=%v, want %s",
				check.key, result.Value, result.Found, readErr, check.want)
		}
	}
}
