package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
)

func transactionCommandWithFingerprint(
	t testing.TB,
	fixture relationBundleFixture,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
	fingerprint replication.Digest,
) []byte {
	t.Helper()
	transaction, err := distributedtxn.AppendReplicatedCommand(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := replication.TransactionClientSequence(transaction)
	if err != nil {
		t.Fatal(err)
	}
	command := replication.Command{
		Kind:      replication.CommandTransaction,
		ClusterID: fixture.binding.ClusterID, ClusterIncarnation: fixture.binding.ClusterIncarnation,
		TopologyRecoveryEpoch: fixture.binding.TopologyRecoveryEpoch,
		Distribution:          fixture.binding.Distribution, Shard: fixture.binding.Shard,
		AllocationGeneration: fixture.binding.AllocationGeneration,
		ShardIncarnation:     fixture.binding.ShardIncarnation, GroupID: fixture.binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: fixture.binding.ActivePolicyGeneration,
		ProtectionEpoch: fixture.binding.ProtectionEpoch, OwnershipEpoch: fixture.binding.OwnershipEpoch,
		SchemaGeneration: fixture.binding.SchemaGeneration, RoutingVersion: fixture.binding.RoutingVersion,
		RouteGeneration: fixture.binding.RouteGeneration,
		Tenant:          []byte("tenant"), ClientID: replication.ID128(control.ID),
		ClientEpoch: uint64(control.Role), ClientSequence: sequence,
		Fingerprint: fingerprint, Transaction: transaction, Batches: batches,
	}
	return encodeCommand(t, command)
}

func transactionParticipantStageCommand(
	t testing.TB,
	fixture relationBundleFixture,
	id distributedtxn.ID,
	batches []replication.RelationMutationBatch,
) []byte {
	t.Helper()
	digest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	return transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:        distributedtxn.ReplicatedRoleParticipant,
		Operation:   distributedtxn.ReplicatedStageParticipant,
		ID:          id,
		PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID(fixture.binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(fixture.binding.ShardIncarnation),
			CoordinatorAllocation:       fixture.binding.AllocationGeneration,
			BucketBits:                  8,
			IntentScopes:                []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest:              digest,
		},
	}, batches)
}

func transactionParticipantTransitionCommand(
	t testing.TB,
	fixture relationBundleFixture,
	id distributedtxn.ID,
	operation distributedtxn.ReplicatedOperation,
	expected uint64,
) []byte {
	t.Helper()
	return transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:             distributedtxn.ReplicatedRoleParticipant,
		Operation:        operation,
		ID:               id,
		ExpectedRevision: expected,
		PayloadKind:      distributedtxn.ReplicatedPayloadNone,
	}, nil)
}

func applyTransactionCommand(
	t testing.TB,
	machine *Machine,
	index uint64,
	command []byte,
) TransactionCompletionResult {
	t.Helper()
	if err := machine.AdmitCommand(command); err != nil {
		t.Fatalf("admit transaction at %d: %v", index, err)
	}
	if _, err := machine.ApplyNormal(normalMeta(index), command); err != nil {
		t.Fatalf("apply transaction at %d: %v", index, err)
	}
	completion, result := openTransactionCompletion(t, machine, command)
	if completion.ResultCode != ResultApplied {
		t.Fatalf("transaction result at %d = %d, want %d", index, completion.ResultCode, ResultApplied)
	}
	return result
}

func TestTransactionParticipantIntentBarrierPrepareApplyAndRelease(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(231)
	documentKey := []byte("txn-document")
	document := []byte(`{"email":"txn@example.com","n":1}`)
	globalKey := []byte{0x91, 0x01, 't'}
	globalValue := []byte(`["txn-document"]`)
	batches := []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: documentKey, Value: document,
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: globalValue,
		}}},
	}
	stage := transactionParticipantStageCommand(t, fixture, id, batches)
	applyTransactionCommand(t, fixture.machine, 3, stage)
	if fixture.machine.state.TransactionControlCount != 1 ||
		fixture.machine.state.ActiveTransactionCount != 1 ||
		fixture.machine.state.TransactionPayloadRows != 2 ||
		fixture.machine.state.TransactionIntentRows != 2 ||
		fixture.machine.state.TransactionResidentBytes == 0 {
		t.Fatalf("packed stage accounting = %+v", fixture.machine.state)
	}

	if _, err := fixture.machine.PointReadInto(
		1, documentKey, 3, fixture.base.Limits.MaxDocumentBytes, nil,
	); !errors.Is(err, ErrTransactionIntentActive) {
		t.Fatalf("intersecting point read error = %v, want active-intent fence", err)
	}

	conflicting := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: documentKey,
			Value: []byte(`{"email":"other@example.com"}`),
		}},
	})
	if err := fixture.machine.AdmitCommand(conflicting); err != nil {
		t.Fatalf("deterministic intent conflict was not admissible: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), conflicting); err != nil {
		t.Fatal(err)
	}
	if got := bundleCompletionResult(t, fixture.machine, conflicting); got != ResultIntentBusy {
		t.Fatalf("intersecting ordinary write result = %d, want %d", got, ResultIntentBusy)
	}

	disjointKey := []byte("disjoint-document")
	disjointValue := []byte(`{"email":"disjoint@example.com"}`)
	disjoint := fixture.command(t, 2, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: disjointKey, Value: disjointValue,
		}},
	})
	if err := fixture.machine.AdmitCommand(disjoint); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), disjoint); err != nil {
		t.Fatal(err)
	}
	if got := bundleCompletionResult(t, fixture.machine, disjoint); got != ResultApplied {
		t.Fatalf("disjoint ordinary write result = %d, want %d", got, ResultApplied)
	}

	prepare := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedPrepareParticipant, 1,
	)
	applyTransactionCommand(t, fixture.machine, 6, prepare)
	apply := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedApplyParticipant, 2,
	)
	result := applyTransactionCommand(t, fixture.machine, 7, apply)
	if !result.AffectedRowsValid || result.AffectedRows != 1 {
		t.Fatalf("participant apply result = %+v, want one base logical row", result)
	}
	for _, check := range []struct {
		collection interface {
			AppendRaw([]byte, []byte) ([]byte, bool, error)
		}
		key, value []byte
	}{
		{fixture.base.Collection, documentKey, document},
		{fixture.global.Collection, globalKey, globalValue},
	} {
		got, found, err := check.collection.AppendRaw(nil, check.key)
		if err != nil || !found || !bytes.Equal(got, check.value) {
			t.Fatalf("committed value %q = %q found=%v err=%v", check.key, got, found, err)
		}
	}

	release := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedReleaseParticipant, 3,
	)
	applyTransactionCommand(t, fixture.machine, 8, release)
	read, err := fixture.machine.PointReadInto(
		1, documentKey, 8, fixture.base.Limits.MaxDocumentBytes, nil,
	)
	if err != nil || !read.Found || !bytes.Equal(read.Value, document) {
		t.Fatalf("released point read = %+v err=%v", read, err)
	}
	state := fixture.machine.Published()
	if state.Applied != 8 || fixture.machine.state.ActiveTransactionCount != 0 ||
		fixture.machine.state.TransactionPayloadRows != 0 ||
		fixture.machine.state.TransactionIntentRows != 0 {
		t.Fatalf("released transaction state = %+v", fixture.machine.state)
	}
}

func fusedParticipantControl(
	t testing.TB,
	fixture relationBundleFixture,
	id distributedtxn.ID,
	operation distributedtxn.ReplicatedOperation,
	expected uint64,
	batches []replication.RelationMutationBatch,
) distributedtxn.ReplicatedCommand {
	t.Helper()
	control := distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleParticipant, Operation: operation,
		ID: id, ExpectedRevision: expected, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}
	if operation == distributedtxn.ReplicatedStagePrepareParticipant {
		digest, err := replication.TransactionMutationDigest(batches)
		if err != nil {
			t.Fatal(err)
		}
		control.PayloadKind = distributedtxn.ReplicatedPayloadParticipantStage
		control.Participant = distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID(fixture.binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(fixture.binding.ShardIncarnation),
			CoordinatorAllocation:       fixture.binding.AllocationGeneration,
			BucketBits:                  8,
			IntentScopes:                []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest:              digest,
		}
	}
	return control
}

func TestTransactionFusedPrepareApplyReleaseIsOneAtomicFinish(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(241)
	documentKey := []byte("fused-document")
	document := []byte(`{"email":"fused@example.com","n":1}`)
	globalKey := []byte{0x91, 0x02, 'f'}
	globalValue := []byte(`["fused-document"]`)
	batches := []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: documentKey, Value: document,
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: globalValue,
		}}},
	}
	prepareControl := fusedParticipantControl(
		t, fixture, id, distributedtxn.ReplicatedStagePrepareParticipant, 0, batches,
	)
	prepare := transactionCompletionCommand(t, fixture.binding, prepareControl, batches)
	prepareResult := applyTransactionCommand(t, fixture.machine, 3, prepare)
	if prepareResult.Revision != 2 || prepareResult.AffectedRowsValid {
		t.Fatalf("fused prepare result = %+v", prepareResult)
	}
	controlKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleParticipant, id,
	)
	raw, found, err := fixture.system.Collection.AppendRaw(nil, controlKey[:])
	if err != nil || !found {
		t.Fatalf("read fused prepare control: found=%v err=%v", found, err)
	}
	prepared, err := OpenTransactionControl(raw)
	if err != nil || prepared.State != uint8(distributedtxn.ParticipantPrepared) ||
		prepared.Revision != 2 || prepared.PrepareResultCode != ResultApplied ||
		prepared.PrepareCommandDigest == (replication.Digest{}) || !prepared.FusedPath {
		t.Fatalf("fused prepared control = %+v err=%v", prepared.TransactionControl, err)
	}
	if _, err := fixture.machine.PointReadInto(
		1, documentKey, 3, fixture.base.Limits.MaxDocumentBytes, nil,
	); !errors.Is(err, ErrTransactionIntentActive) {
		t.Fatalf("prepared point read error = %v", err)
	}

	finishControl := fusedParticipantControl(
		t, fixture, id, distributedtxn.ReplicatedApplyReleaseParticipant, 2, nil,
	)
	finish := transactionCompletionCommand(t, fixture.binding, finishControl, nil)
	finishResult := applyTransactionCommand(t, fixture.machine, 4, finish)
	if !finishResult.AffectedRowsValid || finishResult.AffectedRows != 1 ||
		finishResult.Revision != 4 {
		t.Fatalf("fused finish result = %+v", finishResult)
	}
	raw, found, err = fixture.system.Collection.AppendRaw(nil, controlKey[:])
	if err != nil || !found {
		t.Fatalf("read fused released control: found=%v err=%v", found, err)
	}
	released, err := OpenTransactionControl(raw)
	if err != nil || released.State != uint8(distributedtxn.ParticipantReleased) ||
		released.Revision != 4 || released.ResidentMutationBytes != 0 ||
		released.ResidentIntentBytes != 0 || released.PrepareResultCode != ResultApplied ||
		!released.FusedPath {
		t.Fatalf("fused released control = %+v err=%v", released.TransactionControl, err)
	}
	if fixture.machine.state.ActiveTransactionCount != 0 ||
		fixture.machine.state.TransactionPayloadRows != 0 ||
		fixture.machine.state.TransactionIntentRows != 0 {
		t.Fatalf("fused release accounting = %+v", fixture.machine.state)
	}
	for _, check := range []struct {
		collection interface {
			AppendRaw([]byte, []byte) ([]byte, bool, error)
		}
		key, value []byte
	}{
		{fixture.base.Collection, documentKey, document},
		{fixture.global.Collection, globalKey, globalValue},
	} {
		got, ok, readErr := check.collection.AppendRaw(nil, check.key)
		if readErr != nil || !ok || !bytes.Equal(got, check.value) {
			t.Fatalf("fused committed %q = %q found=%v err=%v", check.key, got, ok, readErr)
		}
	}
	completion, _ := openTransactionCompletion(t, fixture.machine, prepare)
	if completion.ResultCode != ResultApplied {
		t.Fatalf("historical fused prepare result = %d", completion.ResultCode)
	}
}

func TestTransactionFusedPrepareAbortReleaseReclaimsWithoutApply(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(242)
	key := []byte("fused-abort")
	batches := []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: key,
			Value: []byte(`{"email":"abort@example.com"}`),
		}},
	}}
	prepare := transactionCompletionCommand(t, fixture.binding, fusedParticipantControl(
		t, fixture, id, distributedtxn.ReplicatedStagePrepareParticipant, 0, batches,
	), batches)
	applyTransactionCommand(t, fixture.machine, 3, prepare)
	abort := transactionCompletionCommand(t, fixture.binding, fusedParticipantControl(
		t, fixture, id, distributedtxn.ReplicatedAbortReleaseParticipant, 2, nil,
	), nil)
	result := applyTransactionCommand(t, fixture.machine, 4, abort)
	if result.AffectedRowsValid || result.Revision != 4 {
		t.Fatalf("fused abort result = %+v", result)
	}
	if _, found, err := fixture.base.Collection.AppendRaw(nil, key); err != nil || found {
		t.Fatalf("aborted value found=%v err=%v", found, err)
	}
	if fixture.machine.state.ActiveTransactionCount != 0 ||
		fixture.machine.state.TransactionPayloadRows != 0 ||
		fixture.machine.state.TransactionIntentRows != 0 {
		t.Fatalf("fused abort accounting = %+v", fixture.machine.state)
	}
}

func TestTransactionFusedBeginPreparesLocalParticipantAndSurvivesRetire(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(243)
	key := []byte("fused-local")
	document := []byte(`{"email":"local@example.com"}`)
	batches := []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: key, Value: document,
		}},
	}}
	digest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: fixture.binding.SchemaGeneration, RecoveryDeadline: 1,
		Participants: []distributedtxn.ParticipantRef{{
			Distribution:         []byte(fixture.binding.Distribution),
			Shard:                []byte(fixture.binding.Shard),
			RoutingVersion:       fixture.binding.RoutingVersion,
			AllocationGeneration: fixture.binding.AllocationGeneration,
			OwnershipEpoch:       fixture.binding.OwnershipEpoch,
			MutationDigest:       digest, State: distributedtxn.ParticipantStaged,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	beginControl := distributedtxn.ReplicatedCommand{
		Role:        distributedtxn.ReplicatedRoleCoordinator,
		Operation:   distributedtxn.ReplicatedBeginPrepareCoordinator,
		ID:          id,
		PayloadKind: distributedtxn.ReplicatedPayloadCoordinator,
		Payload:     payload,
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID(fixture.binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(fixture.binding.ShardIncarnation),
			CoordinatorAllocation:       fixture.binding.AllocationGeneration,
			BucketBits:                  8,
			IntentScopes:                []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest:              digest,
			ParticipantOrdinal:          0,
		},
	}
	for name, mutate := range map[string]func(*distributedtxn.ReplicatedCommand){
		"coordinator group": func(candidate *distributedtxn.ReplicatedCommand) {
			candidate.Participant.CoordinatorGroup[0] ^= 0xff
		},
		"coordinator shard incarnation": func(candidate *distributedtxn.ReplicatedCommand) {
			candidate.Participant.CoordinatorShardIncarnation[0] ^= 0xff
		},
		"coordinator allocation": func(candidate *distributedtxn.ReplicatedCommand) {
			candidate.Participant.CoordinatorAllocation++
		},
	} {
		t.Run(name+" binding", func(t *testing.T) {
			candidate := beginControl
			mutate(&candidate)
			command := transactionCompletionCommand(t, fixture.binding, candidate, batches)
			if err := fixture.machine.AdmitCommand(command); !errors.Is(err, ErrStaleCommand) {
				t.Fatalf("binding mismatch admission error=%v", err)
			}
		})
	}
	begin := transactionCompletionCommand(t, fixture.binding, beginControl, batches)
	result := applyTransactionCommand(t, fixture.machine, 3, begin)
	if result.Role != distributedtxn.ReplicatedRoleCoordinator || result.Revision != 1 {
		t.Fatalf("fused begin result = %+v", result)
	}
	coordinatorKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleCoordinator, id,
	)
	participantKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleParticipant, id,
	)
	coordinatorRaw, found, err := fixture.system.Collection.AppendRaw(nil, coordinatorKey[:])
	if err != nil || !found {
		t.Fatalf("read fused coordinator: found=%v err=%v", found, err)
	}
	coordinator, err := OpenTransactionControl(coordinatorRaw)
	if err != nil || coordinator.State != uint8(distributedtxn.CoordinatorStaging) ||
		coordinator.PrepareResultCode != ResultApplied ||
		coordinator.CoordinatorParticipantOrdinal != 0 || !coordinator.FusedPath {
		t.Fatalf("fused coordinator = %+v err=%v", coordinator.TransactionControl, err)
	}
	participantRaw, found, err := fixture.system.Collection.AppendRaw(nil, participantKey[:])
	if err != nil || !found {
		t.Fatalf("read local participant: found=%v err=%v", found, err)
	}
	participant, err := OpenTransactionControl(participantRaw)
	if err != nil || participant.State != uint8(distributedtxn.ParticipantPrepared) ||
		participant.Revision != 2 || participant.PrepareResultCode != ResultApplied ||
		!participant.FusedPath {
		t.Fatalf("local participant = %+v err=%v", participant.TransactionControl, err)
	}
	if fixture.machine.state.TransactionControlCount != 2 ||
		fixture.machine.state.ActiveTransactionCount != 2 ||
		fixture.machine.state.TransactionPayloadRows != 2 ||
		fixture.machine.state.TransactionIntentRows != 1 {
		t.Fatalf("fused begin accounting = %+v", fixture.machine.state)
	}

	commit := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator, ID: id,
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	applyTransactionCommand(t, fixture.machine, 4, commit)
	finish := transactionCompletionCommand(t, fixture.binding, fusedParticipantControl(
		t, fixture, id, distributedtxn.ReplicatedApplyReleaseParticipant, 2, nil,
	), nil)
	applyTransactionCommand(t, fixture.machine, 5, finish)
	retire := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedRetireCoordinator, ID: id,
		ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	applyTransactionCommand(t, fixture.machine, 6, retire)
	completion, _ := openTransactionCompletion(t, fixture.machine, begin)
	if completion.ResultCode != ResultApplied {
		t.Fatalf("retired fused begin result = %d", completion.ResultCode)
	}
	competing := transactionCommandWithFingerprint(
		t, fixture, beginControl, batches, sha256.Sum256([]byte("other-fused-begin")),
	)
	competingCompletion, _ := openTransactionCompletion(t, fixture.machine, competing)
	if competingCompletion.ResultCode != ResultTransactionConflict {
		t.Fatalf("competing fused begin result = %d", competingCompletion.ResultCode)
	}
}

func TestTransactionFusedManifestDeferredParticipantBinding(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(245)
	key := []byte("deferred-fused-local")
	batches := []replication.RelationMutationBatch{{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: key,
			Value: []byte(`{"email":"deferred@example.com"}`),
		}},
	}}
	digest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare([]byte("a"), []byte(fixture.binding.Distribution)) >= 0 {
		t.Fatalf("fixture distribution %q does not follow synthetic manifest prefix", fixture.binding.Distribution)
	}
	refs := make([]distributedtxn.ParticipantRef, 0, 17_001)
	for index := 0; index < 17_000; index++ {
		refs = append(refs, distributedtxn.ParticipantRef{
			Distribution: []byte("a"), Shard: []byte(fmt.Sprintf("shard-%08d", index)),
			RoutingVersion: 1, AllocationGeneration: 1, OwnershipEpoch: 1,
			MutationDigest: sha256.Sum256([]byte(fmt.Sprintf("mutation-%08d", index))),
			State:          distributedtxn.ParticipantStaged,
		})
	}
	selectedOrdinal := uint32(len(refs))
	refs = append(refs, distributedtxn.ParticipantRef{
		Distribution:         []byte(fixture.binding.Distribution),
		Shard:                []byte(fixture.binding.Shard),
		RoutingVersion:       fixture.binding.RoutingVersion,
		AllocationGeneration: fixture.binding.AllocationGeneration,
		OwnershipEpoch:       fixture.binding.OwnershipEpoch,
		MutationDigest:       digest,
		State:                distributedtxn.ParticipantStaged,
	})
	build := func(participants []distributedtxn.ParticipantRef) (
		distributedtxn.ManifestDescriptor, [][]byte,
	) {
		pageScratch := make([]byte, distributedtxn.ManifestSegmentBytes)
		pages := make([][]byte, 0, 24)
		builder, buildErr := distributedtxn.NewManifestBuilder(
			pageScratch, func(segment distributedtxn.ManifestSegment) error {
				pages = append(pages, bytes.Clone(segment.Raw))
				return nil
			},
		)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		for index := range participants {
			if buildErr = builder.Append(participants[index]); buildErr != nil {
				t.Fatalf("append participant %d: %v", index, buildErr)
			}
		}
		descriptor, buildErr := builder.Seal()
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		return descriptor, pages
	}
	descriptor, pages := build(refs)
	initialCount := distributedtxn.MaxManifestSegmentsPerCommand
	if len(pages) <= initialCount || len(pages) > 2*initialCount {
		t.Fatalf("manifest pages=%d, want one deferred greedy pack", len(pages))
	}
	selectedPage := -1
	for index := range pages {
		sequence, openErr := distributedtxn.OpenManifestSegmentSequence(pages[index])
		if openErr != nil {
			t.Fatal(openErr)
		}
		if uint64(selectedOrdinal) >= sequence.FirstParticipant() &&
			uint64(selectedOrdinal) < sequence.FirstParticipant()+sequence.ParticipantCount() {
			selectedPage = index
			break
		}
	}
	if selectedPage < initialCount {
		t.Fatalf("selected ordinal %d landed in initial page %d", selectedOrdinal, selectedPage)
	}
	manifest, err := distributedtxn.AppendManifestCoordinator(nil, distributedtxn.ManifestCoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: fixture.binding.SchemaGeneration, RecoveryDeadline: 1,
		Manifest: descriptor,
	})
	if err != nil {
		t.Fatal(err)
	}
	initial := bytes.Clone(manifest)
	for _, page := range pages[:initialCount] {
		initial = append(initial, page...)
	}
	participant := distributedtxn.ParticipantStage{
		CoordinatorGroup:            distributedtxn.ID(fixture.binding.GroupID),
		CoordinatorShardIncarnation: distributedtxn.ID(fixture.binding.ShardIncarnation),
		CoordinatorAllocation:       fixture.binding.AllocationGeneration,
		BucketBits:                  8,
		IntentScopes:                []distributedtxn.IntentScope{{Start: 0, End: 256}},
		MutationDigest:              digest,
		ParticipantOrdinal:          selectedOrdinal,
	}
	beginControl := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadManifestCoordinator,
		Payload: initial, Participant: participant,
	}
	begin := transactionCompletionCommand(t, fixture.binding, beginControl, batches)
	applyTransactionCommand(t, fixture.machine, 3, begin)
	if _, err := fixture.machine.PointReadInto(
		1, key, 3, fixture.base.Limits.MaxDocumentBytes, nil,
	); !errors.Is(err, ErrTransactionIntentActive) {
		t.Fatalf("deferred participant intent error = %v", err)
	}

	earlyCommit := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator, ID: id,
		ExpectedRevision: uint64(initialCount), PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	if err := fixture.machine.AdmitCommand(earlyCommit); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), earlyCommit); err != nil {
		t.Fatal(err)
	}
	coordinatorKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleCoordinator, id,
	)
	coordinatorRaw, found, err := fixture.system.Collection.AppendRaw(nil, coordinatorKey[:])
	if err != nil || !found {
		t.Fatalf("read unsealed coordinator: found=%v err=%v", found, err)
	}
	unsealed, err := OpenTransactionControl(coordinatorRaw)
	if err != nil || unsealed.State != uint8(distributedtxn.CoordinatorStaging) ||
		unsealed.Revision != uint64(initialCount) {
		t.Fatalf("unsealed coordinator=%+v err=%v", unsealed.TransactionControl, err)
	}
	assertUnchangedRevision := func(want uint64) {
		t.Helper()
		raw, found, readErr := fixture.system.Collection.AppendRaw(nil, coordinatorKey[:])
		if readErr != nil || !found {
			t.Fatalf("read coordinator after refused append: found=%v err=%v", found, readErr)
		}
		view, openErr := OpenTransactionControl(raw)
		if openErr != nil || view.Revision != want || !view.FusedPath {
			t.Fatalf("coordinator after refused append=%+v err=%v", view.TransactionControl, openErr)
		}
	}
	singleton := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedStageManifestSegment, ID: id,
		ExpectedRevision: uint64(initialCount),
		PayloadKind:      distributedtxn.ReplicatedPayloadManifestSegment, Payload: pages[initialCount],
	}, nil)
	if err := fixture.machine.AdmitCommand(singleton); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), singleton); err != nil {
		t.Fatal(err)
	}
	assertUnchangedRevision(uint64(initialCount))

	shortPack := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedAppendManifestSegments, ID: id,
		ExpectedRevision: uint64(initialCount),
		PayloadKind:      distributedtxn.ReplicatedPayloadManifestSegments, Payload: pages[initialCount],
	}, nil)
	if err := fixture.machine.AdmitCommand(shortPack); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(6), shortPack); err != nil {
		t.Fatal(err)
	}
	assertUnchangedRevision(uint64(initialCount))

	wrongRefs := append([]distributedtxn.ParticipantRef(nil), refs...)
	wrongRefs[selectedOrdinal].MutationDigest[0] ^= 0xff
	_, wrongPages := build(wrongRefs)
	wrongPack := appendManifestPageBytes(nil, wrongPages[initialCount:])
	wrongAppend := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedAppendManifestSegments, ID: id,
		ExpectedRevision: uint64(initialCount),
		PayloadKind:      distributedtxn.ReplicatedPayloadManifestSegments, Payload: wrongPack,
	}, nil)
	if err := fixture.machine.AdmitCommand(wrongAppend); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("mismatched selected page admission error=%v", err)
	}

	matchingPack := appendManifestPageBytes(nil, pages[initialCount:])
	matchingAppend := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedAppendManifestSegments, ID: id,
		ExpectedRevision: uint64(initialCount),
		PayloadKind:      distributedtxn.ReplicatedPayloadManifestSegments, Payload: matchingPack,
	}, nil)
	applyTransactionCommand(t, fixture.machine, 7, matchingAppend)
	commit := transactionCompletionCommand(t, fixture.binding, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator, ID: id,
		ExpectedRevision: uint64(len(pages)), PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	applyTransactionCommand(t, fixture.machine, 8, commit)
}

func appendManifestPageBytes(dst []byte, pages [][]byte) []byte {
	for _, page := range pages {
		dst = append(dst, page...)
	}
	return dst
}

func TestTransactionFusedPrepareConflictLeavesOnlyExactVote(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte("fused-conflict")
	seed := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: key,
			Value: []byte(`{"email":"existing@example.com"}`),
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(244)
	batches := []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: key,
			Value: []byte(`{"email":"other@example.com"}`),
		}},
	}}
	control := fusedParticipantControl(
		t, fixture, id, distributedtxn.ReplicatedStagePrepareParticipant, 0, batches,
	)
	command := transactionCompletionCommand(t, fixture.binding, control, batches)
	if err := fixture.machine.AdmitCommand(command); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), command); err != nil {
		t.Fatal(err)
	}
	completion, result := openTransactionCompletion(t, fixture.machine, command)
	if completion.ResultCode != ResultIndexConflict || result.Revision != 3 ||
		result.AffectedRowsValid {
		t.Fatalf("fused conflict completion=%+v result=%+v", completion, result)
	}
	keyControl, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleParticipant, id,
	)
	raw, found, err := fixture.system.Collection.AppendRaw(nil, keyControl[:])
	if err != nil || !found {
		t.Fatalf("read rejected vote: found=%v err=%v", found, err)
	}
	vote, err := OpenTransactionControl(raw)
	if err != nil || vote.State != uint8(distributedtxn.ParticipantReleased) ||
		vote.PrepareResultCode != ResultIndexConflict || vote.Revision != 3 ||
		vote.ResidentMutationBytes != 0 || vote.ResidentIntentBytes != 0 || !vote.FusedPath {
		t.Fatalf("rejected vote = %+v err=%v", vote.TransactionControl, err)
	}
	if fixture.machine.state.ActiveTransactionCount != 0 ||
		fixture.machine.state.TransactionPayloadRows != 0 ||
		fixture.machine.state.TransactionIntentRows != 0 {
		t.Fatalf("rejected vote accounting = %+v", fixture.machine.state)
	}
	competing := transactionCommandWithFingerprint(
		t, fixture, control, batches, sha256.Sum256([]byte("other-fused-prepare")),
	)
	competingCompletion, _ := openTransactionCompletion(t, fixture.machine, competing)
	if competingCompletion.ResultCode != ResultTransactionConflict {
		t.Fatalf("competing fused prepare result = %d", competingCompletion.ResultCode)
	}
}

func TestTransactionPrepareConflictStaysStagedAndCanAbort(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	key := []byte("prepare-conflict")
	seed := fixture.command(t, 1, replication.RelationMutationBatch{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: key,
			Value: []byte(`{"email":"existing@example.com"}`),
		}},
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	id := transactionCodecID(232)
	stage := transactionParticipantStageCommand(t, fixture, id, []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: key,
			Value: []byte(`{"email":"competing@example.com"}`),
		}},
	}})
	applyTransactionCommand(t, fixture.machine, 4, stage)
	prepare := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedPrepareParticipant, 1,
	)
	if err := fixture.machine.AdmitCommand(prepare); err != nil {
		t.Fatalf("admit deterministic prepare conflict: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(5), prepare); err != nil {
		t.Fatal(err)
	}
	completion, result := openTransactionCompletion(t, fixture.machine, prepare)
	if completion.ResultCode != ResultIndexConflict || result.AffectedRowsValid {
		t.Fatalf("prepare conflict completion=%+v result=%+v", completion, result)
	}
	controlKey, _ := TransactionControlStorageKey(
		distributedtxn.ReplicatedRoleParticipant, id,
	)
	raw, found, err := fixture.system.Collection.AppendRaw(nil, controlKey[:])
	if err != nil || !found {
		t.Fatalf("read participant control: found=%v err=%v", found, err)
	}
	control, err := OpenTransactionControl(raw)
	if err != nil || control.State != uint8(distributedtxn.ParticipantStaged) || control.Revision != 1 ||
		control.LastResultCode != ResultIndexConflict {
		t.Fatalf("failed prepare control=%+v err=%v", control.TransactionControl, err)
	}
	competingControl := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleParticipant,
		Operation: distributedtxn.ReplicatedPrepareParticipant, ID: id,
		ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}
	competing := transactionCommandWithFingerprint(
		t, fixture, competingControl, nil, sha256.Sum256([]byte("competing-prepare")),
	)
	if err := fixture.machine.AdmitCommand(competing); err != nil {
		t.Fatalf("admit deterministic competing prepare: %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(6), competing); err != nil {
		t.Fatal(err)
	}
	competingCompletion, _ := openTransactionCompletion(t, fixture.machine, competing)
	if competingCompletion.ResultCode != ResultTransactionConflict {
		t.Fatalf("competing prepare result=%d, want %d",
			competingCompletion.ResultCode, ResultTransactionConflict)
	}
	abort := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedAbortParticipant, 1,
	)
	applyTransactionCommand(t, fixture.machine, 7, abort)
	release := transactionParticipantTransitionCommand(
		t, fixture, id, distributedtxn.ReplicatedReleaseParticipant, 2,
	)
	applyTransactionCommand(t, fixture.machine, 8, release)
}

func TestTransactionCommandIsAnExplicitNormalBatchBoundary(t *testing.T) {
	fixture := newRelationBundleFixture(t, true)
	id := transactionCodecID(233)
	stage := transactionParticipantStageCommand(t, fixture, id, []replication.RelationMutationBatch{{
		Relation: 1,
		Mutations: []replication.Mutation{{
			Kind: replication.MutationPut, Key: []byte("batch-boundary"),
			Value: []byte(`{"email":"boundary@example.com"}`),
		}},
	}})
	entries := []raftmodel.NormalApply{{Meta: normalMeta(3), Data: stage}}
	applied, publication, err := fixture.machine.ApplyNormalBatch(
		entries, normalBatchWitnesses(entries),
	)
	if err != nil || applied != 0 || publication.Applied != 0 {
		t.Fatalf("transaction batch boundary = applied %d publication %+v err=%v", applied, publication, err)
	}
	applyTransactionCommand(t, fixture.machine, 3, stage)
}
