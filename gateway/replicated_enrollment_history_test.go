package gateway

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

func TestEnrollmentHistoryKeepsServingAndEnrolledBootstrapEvidence(t *testing.T) {
	_, _, snapshot := newCatalogAuthorityFixtureWithDescriptor(t, testReplicatedCatalogEnrollTarget)
	descriptor := snapshot.ReplicatedShardDescriptors()[0]
	serving := enrollmentHistoryTestIntent(t, 1, descriptor, descriptor.Replicas[0], EnrollmentComplete)
	enrolled := enrollmentHistoryTestIntent(t, 2, descriptor, *descriptor.EnrolledTarget, EnrollmentComplete)
	history := []enrollmentHistoryRecord{
		enrollmentHistoryTestRecord(t, serving), enrollmentHistoryTestRecord(t, enrolled),
	}
	for index := range maxEnrollmentHistory {
		history = append(history, enrollmentHistoryTestRecord(t,
			enrollmentHistoryTestIntent(t, uint64(index+3), descriptor, *descriptor.EnrolledTarget, EnrollmentCancelled)))
	}
	terminal := enrollmentHistoryTestIntent(t, 0, descriptor, *descriptor.EnrolledTarget, EnrollmentCancelled)
	terminalRecord := enrollmentHistoryTestRecord(t, terminal)
	plan, err := planEnrollmentHistory(snapshot, history, terminal, scalingDigest(terminalRecord.raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.entries) != maxEnrollmentHistory+2 || len(plan.evicted) != 1 {
		t.Fatalf("retained=%d evicted=%d; want %d retained and one inactive eviction", len(plan.entries), len(plan.evicted), maxEnrollmentHistory+2)
	}
	for _, retained := range []GroupEnrollmentIntent{serving, enrolled, terminal} {
		if _, found := findScalingDirectoryEntry(plan.entries, retained.IntentID); !found {
			t.Fatalf("lost retained enrollment %x in state %v", retained.IntentID, retained.State)
		}
	}
	if plan.evicted[0].intent.State != EnrollmentCancelled {
		t.Fatalf("evicted live bootstrap evidence: %+v", plan.evicted[0].intent)
	}
	for index := 1; index < len(plan.entries); index++ {
		if bytes.Compare(plan.entries[index-1].ID, plan.entries[index].ID) >= 0 {
			t.Fatalf("history directory is not strictly ordered at %d", index)
		}
	}
}

func TestEnrollmentHistoryPinsOnlyTheExactCatalogTarget(t *testing.T) {
	_, _, snapshot := newCatalogAuthorityFixtureWithDescriptor(t, testReplicatedCatalogEnrollTarget)
	descriptor := snapshot.ReplicatedShardDescriptors()[0]
	for _, placement := range []struct {
		name   string
		target ReplicatedReplicaDescriptor
	}{
		{"serving", descriptor.Replicas[0]},
		{"enrolled", *descriptor.EnrolledTarget},
	} {
		for _, change := range []struct {
			name   string
			mutate func(*GroupEnrollmentIntent)
		}{
			{"member", func(intent *GroupEnrollmentIntent) { intent.Target.Member += 100 }},
			{"node", func(intent *GroupEnrollmentIntent) { intent.Target.Node[1]++ }},
			{"incarnation", func(intent *GroupEnrollmentIntent) { intent.Target.NodeIncarnation++ }},
			{"store", func(intent *GroupEnrollmentIntent) { intent.Target.StoreID[1]++ }},
			{"data-endpoint", func(intent *GroupEnrollmentIntent) { intent.Target.Endpoint += "-different" }},
			{"native-endpoint", func(intent *GroupEnrollmentIntent) { intent.Target.NativeEndpoint += "-different" }},
			{"control-endpoint", func(intent *GroupEnrollmentIntent) { intent.Target.ControlEndpoint += "-different" }},
			{"group", func(intent *GroupEnrollmentIntent) { intent.Group.GroupID[0]++ }},
			{"distribution", func(intent *GroupEnrollmentIntent) { intent.Distribution += "-different" }},
			{"shard", func(intent *GroupEnrollmentIntent) { intent.Shard += "-different" }},
			{"allocation", func(intent *GroupEnrollmentIntent) { intent.AllocationGeneration++ }},
		} {
			t.Run(placement.name+"/"+change.name, func(t *testing.T) {
				candidate := enrollmentHistoryTestIntent(t, 1, descriptor, placement.target, EnrollmentComplete)
				change.mutate(&candidate)
				enrollmentHistoryTestProofs(&candidate)
				history := []enrollmentHistoryRecord{enrollmentHistoryTestRecord(t, candidate)}
				for index := range maxEnrollmentHistory - 1 {
					history = append(history, enrollmentHistoryTestRecord(t,
						enrollmentHistoryTestIntent(t, uint64(index+2), descriptor, placement.target, EnrollmentCancelled)))
				}
				terminal := enrollmentHistoryTestIntent(t, uint64(maxEnrollmentHistory+1), descriptor, placement.target, EnrollmentCancelled)
				terminalRecord := enrollmentHistoryTestRecord(t, terminal)
				plan, err := planEnrollmentHistory(snapshot, history, terminal, scalingDigest(terminalRecord.raw))
				if err != nil {
					t.Fatal(err)
				}
				if len(plan.entries) != maxEnrollmentHistory || len(plan.evicted) != 1 {
					t.Fatalf("nonmatching identity consumed a pinned slot: retained=%d evicted=%d", len(plan.entries), len(plan.evicted))
				}
				if plan.evicted[0].intent.IntentID != candidate.IntentID {
					t.Fatalf("deterministic oldest ordered inactive row was not evicted: got %x want %x", plan.evicted[0].intent.IntentID, candidate.IntentID)
				}
			})
		}
	}
}

func TestEnrollmentHistoryRejectsDuplicateLiveBootstrapIdentity(t *testing.T) {
	_, _, snapshot := newCatalogAuthorityFixture(t)
	descriptor := snapshot.ReplicatedShardDescriptors()[0]
	first := enrollmentHistoryTestIntent(t, 1, descriptor, descriptor.Replicas[0], EnrollmentComplete)
	duplicate := enrollmentHistoryTestIntent(t, 2, descriptor, descriptor.Replicas[0], EnrollmentComplete)
	duplicateRecord := enrollmentHistoryTestRecord(t, duplicate)
	if _, err := planEnrollmentHistory(snapshot, []enrollmentHistoryRecord{enrollmentHistoryTestRecord(t, first)}, duplicate, scalingDigest(duplicateRecord.raw)); !errors.Is(err, ErrScalingIdentity) {
		t.Fatalf("duplicate current bootstrap identity accepted: %v", err)
	}
}

func TestEnrollmentHistoryCollectionComparesCertifiedCatalogCut(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	ctx, err := authority.authorizedContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	descriptor := current.ReplicatedShardDescriptors()[0]
	history, originalDirectory := enrollmentHistoryTestSeedInactive(t, client, descriptor, maxEnrollmentHistory)
	terminal := enrollmentHistoryTestIntent(t, maxEnrollmentHistory+1, descriptor, descriptor.Replicas[0], EnrollmentCancelled)
	terminalRecord := enrollmentHistoryTestRecord(t, terminal)
	authority.mu.Lock()
	mutations, err := authority.enrollmentHistoryMutations(ctx, terminal, scalingDigest(terminalRecord.raw), MaxReplicatedCatalogBatchMutations)
	authority.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(mutations) != 4 {
		t.Fatalf("history collection mutations=%d, want head, witness, directory and one delete", len(mutations))
	}
	for _, key := range [][]byte{replicatedCatalogHeadKey, replicatedCatalogHeadWitnessKey} {
		found := false
		for _, mutation := range mutations {
			if bytes.Equal(mutation.Key, key) && mutation.Kind == replication.MutationPutDigestEqual && mutation.ExpectedValueDigest != (replication.Digest{}) {
				found = true
			}
		}
		if !found {
			t.Fatalf("collection omitted exact catalog fence for %x", key)
		}
	}
	peer := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(current), 0xa8)
	persisted := toPersisted(current)
	persisted.Generation++
	if persisted.RequestLedger != nil {
		persisted.RequestLedger.Generation = persisted.Generation
	}
	raw, err := vibejson.Marshal(&persisted)
	if err != nil {
		t.Fatal(err)
	}
	next, err := decodeSnapshotBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Publish(ctx, current.Generation(), next); err != nil {
		t.Fatal(err)
	}
	mutations = append(mutations, scalingRecordMutation(ReplicatedPointResult{}, enrollmentIntentKey(terminal.IntentID), terminalRecord.raw))
	result, mutationErr := authority.session.MutateBatch(ctx, mutations)
	if err := scalingMutationError(result, mutationErr, authority.session); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale catalog collection was not rejected: %v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if !bytes.Equal(client.rows[string(enrollmentHistoryKey)], originalDirectory) {
		t.Fatal("stale collection changed the history directory")
	}
	for _, record := range history {
		if !bytes.Equal(client.rows[string(enrollmentIntentKey(record.intent.IntentID))], record.raw) {
			t.Fatalf("stale collection deleted or changed enrollment %x", record.intent.IntentID)
		}
	}
	if _, found := client.rows[string(enrollmentIntentKey(terminal.IntentID))]; found {
		t.Fatal("stale collection partially committed the new terminal record")
	}
}

func TestEnrollmentHistoryCollectionChunksBeforeTerminalCommit(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	ctx, err := authority.authorizedContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	descriptor := current.ReplicatedShardDescriptors()[0]
	history, _ := enrollmentHistoryTestSeedInactive(t, client, descriptor, maxEnrollmentHistory+MaxReplicatedCatalogBatchMutations)
	terminal := enrollmentHistoryTestIntent(t, maxEnrollmentHistory+MaxReplicatedCatalogBatchMutations+1, descriptor, descriptor.Replicas[0], EnrollmentCancelled)
	terminalRecord := enrollmentHistoryTestRecord(t, terminal)
	client.mu.Lock()
	beforeApplied := client.applied
	client.mu.Unlock()
	authority.mu.Lock()
	mutations, err := authority.enrollmentHistoryMutations(ctx, terminal, scalingDigest(terminalRecord.raw), 3)
	authority.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if len(mutations) != 3 {
		t.Fatalf("final batch consumed %d slots with only three available", len(mutations))
	}
	client.mu.Lock()
	applied := client.applied - beforeApplied
	_, prematurelyCommitted := client.rows[string(enrollmentIntentKey(terminal.IntentID))]
	directoryBytes := bytes.Clone(client.rows[string(enrollmentHistoryKey)])
	_, firstRetained := client.rows[string(enrollmentIntentKey(history[0].intent.IntentID))]
	client.mu.Unlock()
	if applied != 2 {
		t.Fatalf("collection used %d batches, want two bounded batches", applied)
	}
	if prematurelyCommitted || firstRetained {
		t.Fatalf("intermediate collection committed terminal=%t or retained first evicted row=%t", prematurelyCommitted, firstRetained)
	}
	entries, err := openScalingIDDirectory(directoryBytes, enrollmentHistoryDocumentID[:], maxEnrollmentHistoryBytes, maxEnrollmentRecoveryHistoryEntries)
	if err != nil || len(entries) != maxEnrollmentHistory-1 {
		t.Fatalf("intermediate history retained=%d err=%v", len(entries), err)
	}
	if _, found := findScalingDirectoryEntry(entries, terminal.IntentID); found {
		t.Fatal("intermediate directory names the still-uncommitted terminal")
	}
	mutations = append(mutations, scalingRecordMutation(ReplicatedPointResult{}, enrollmentIntentKey(terminal.IntentID), terminalRecord.raw))
	result, mutationErr := authority.session.MutateBatch(ctx, mutations)
	if err := scalingMutationError(result, mutationErr, authority.session); err != nil {
		t.Fatal(err)
	}
	stored, err := authority.ReadEnrollmentIntent(ctx, terminal.IntentID)
	if err != nil || stored.State != EnrollmentCancelled {
		t.Fatalf("final terminal commit=%+v err=%v", stored, err)
	}
	entries, _, err = authority.readTerminalHistory(ctx, enrollmentHistoryKey, enrollmentHistoryDocumentID[:], maxEnrollmentHistoryBytes, maxEnrollmentRecoveryHistoryEntries)
	if err != nil || len(entries) != maxEnrollmentHistory {
		t.Fatalf("final history retained=%d err=%v", len(entries), err)
	}
}

func enrollmentHistoryTestSeedInactive(t testing.TB, client *catalogAuthorityClient, descriptor ReplicatedShardDescriptor, count int) ([]enrollmentHistoryRecord, []byte) {
	t.Helper()
	history := make([]enrollmentHistoryRecord, 0, count)
	entries := make([]scalingIDDirectoryEntry, 0, count)
	for index := range count {
		record := enrollmentHistoryTestRecord(t, enrollmentHistoryTestIntent(t, uint64(index+1), descriptor, descriptor.Replicas[0], EnrollmentCancelled))
		history = append(history, record)
		entries = append(entries, record.entry)
	}
	directory, err := appendScalingIDDirectory(nil, enrollmentHistoryDocumentID[:], entries, maxEnrollmentHistoryBytes)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.rows[string(enrollmentHistoryKey)] = bytes.Clone(directory)
	for _, record := range history {
		client.rows[string(enrollmentIntentKey(record.intent.IntentID))] = bytes.Clone(record.raw)
	}
	return history, directory
}

func enrollmentHistoryTestIntent(t testing.TB, sequence uint64, descriptor ReplicatedShardDescriptor, target ReplicatedReplicaDescriptor, state EnrollmentState) GroupEnrollmentIntent {
	t.Helper()
	intent := scalingTestEnrollmentIntent(0x9a, 4, 2)
	intent.IntentID = [32]byte{}
	binary.BigEndian.PutUint64(intent.IntentID[:8], sequence)
	if sequence == 0 {
		intent.IntentID[31] = 1
	}
	intent.Group = descriptor.Group
	intent.Distribution = descriptor.Distribution
	intent.Shard = descriptor.Shard
	intent.AllocationGeneration = descriptor.AllocationGeneration
	intent.ExpectedCommand = descriptor.Command
	intent.ExpectedManifestDigest = replication.Digest(descriptor.Command.RelationManifestDigest)
	intent.ExpectedCatalogHeadDigest = replication.Digest{0x51}
	intent.Source = enrollmentHistoryTestIdentity(descriptor.Replicas[1])
	intent.SnapshotSourceMember = intent.Source.Member
	intent.Target = enrollmentHistoryTestIdentity(target)
	intent.State = state
	intent.Revision = 6
	enrollmentHistoryTestProofs(&intent)
	if !intent.Valid() {
		t.Fatalf("invalid enrollment history fixture: %+v", intent)
	}
	return intent
}

func enrollmentHistoryTestProofs(intent *GroupEnrollmentIntent) {
	if intent.State == EnrollmentCancelled {
		intent.Proof, intent.Receipt = nil, nil
		intent.MoveOperationID = [32]byte{}
		return
	}
	proof := scalingTestPreparedProof(*intent, intent.TargetNodeRevision)
	intent.Proof = &proof
	receipt := scalingTestEnrolledReceipt(*intent)
	receipt.BaseCatalogHeadDigest = intent.ExpectedCatalogHeadDigest
	receipt.PublicationPredecessorGeneration = intent.CatalogGeneration
	receipt.PublicationPredecessorHeadDigest = intent.ExpectedCatalogHeadDigest
	receipt.TransitionID = EnrollmentTransitionDigest(*intent)
	intent.Receipt = &receipt
	intent.MoveOperationID = intent.IntentID
}

func enrollmentHistoryTestIdentity(replica ReplicatedReplicaDescriptor) ReplicaIdentity {
	return ReplicaIdentity{
		Member: replica.Member, Node: replica.Node, StoreID: replica.StoreID, NodeIncarnation: replica.NodeIncarnation,
		Endpoint: replica.Endpoint, NativeEndpoint: replica.NativeEndpoint, ControlEndpoint: replica.ControlEndpoint,
	}
}

func enrollmentHistoryTestRecord(t testing.TB, intent GroupEnrollmentIntent) enrollmentHistoryRecord {
	t.Helper()
	raw, err := appendEnrollmentIntentRecord(nil, intent)
	if err != nil {
		t.Fatal(err)
	}
	digest := scalingDigest(raw)
	return enrollmentHistoryRecord{
		entry:  scalingIDDirectoryEntry{ID: bytes.Clone(intent.IntentID[:]), Revision: intent.Revision, Digest: bytes.Clone(digest[:])},
		intent: intent,
		raw:    raw,
	}
}
