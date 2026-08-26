package replication

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"testing"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

var (
	transactionPerfBytesSink []byte
	transactionPerfIntSink   int
)

// TestTransactionBodyOrdinaryCommandCopyBudget fixes the properties a future
// replicated transaction body inherits from the ordinary command envelope.
// Construction retains one output buffer, pre-sized reconstruction does not
// allocate, and decode borrows the complete input rather than copying a
// command-sized transaction body. The ordinary build uses one owned-append
// allocation; race instrumentation may expose one additional bookkeeping
// allocation, so the hard invariant here is no more than two and never zero.
func TestTransactionBodyOrdinaryCommandCopyBudget(t *testing.T) {
	tests := []struct {
		name         string
		payloadBytes int
		runs         int
	}{
		{name: "1KiB", payloadBytes: 1 << 10, runs: 100},
		{name: "64KiB", payloadBytes: 64 << 10, runs: 50},
		{name: "1MiB", payloadBytes: 1 << 20, runs: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, payload := transactionPerfCommand(test.payloadBytes)
			encoded := encodeCommand(t, command)
			wantBytes := transactionPerfCommandBytes(command)
			if len(encoded) != wantBytes {
				t.Fatalf("encoded bytes = %d, want exact %d", len(encoded), wantBytes)
			}
			framingBytes := len(encoded) - len(payload)
			if framingBytes <= 0 || framingBytes > 512 {
				t.Fatalf("ordinary command framing = %dB, want (0,512]", framingBytes)
			}

			// Warm the checksum table and both append growth paths before the
			// allocation measurements.
			scratch := make([]byte, 0, len(encoded))
			var err error
			transactionPerfBytesSink, err = AppendCommand(scratch[:0], command)
			if err != nil {
				t.Fatal(err)
			}
			transactionPerfBytesSink, err = AppendCommand(nil, command)
			if err != nil {
				t.Fatal(err)
			}

			if allocations := testing.AllocsPerRun(test.runs, func() {
				transactionPerfBytesSink, err = AppendCommand(scratch[:0], command)
				if err != nil {
					panic(err)
				}
			}); allocations != 0 {
				t.Fatalf("pre-sized append allocations = %v, want 0", allocations)
			}
			if allocations := testing.AllocsPerRun(test.runs, func() {
				transactionPerfBytesSink, err = AppendCommand(nil, command)
				if err != nil {
					panic(err)
				}
			}); allocations < 1 || allocations > 2 {
				t.Fatalf("owned append allocations = %v, want in [1,2]", allocations)
			}

			if allocations := testing.AllocsPerRun(test.runs, func() {
				view, openErr := OpenCommand(encoded)
				if openErr != nil {
					panic(openErr)
				}
				relations := view.RelationBatches()
				for relations.Next() {
					mutations := relations.Batch().Mutations()
					for mutations.Next() {
						mutation := mutations.Mutation()
						transactionPerfIntSink += len(mutation.Key) + len(mutation.Value)
					}
				}
			}); allocations != 0 {
				t.Fatalf("borrowed open and iteration allocations = %v, want 0", allocations)
			}

			view, err := OpenCommand(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if len(view.Bytes()) != len(encoded) || cap(view.Bytes()) != len(encoded) ||
				unsafe.SliceData(view.Bytes()) != unsafe.SliceData(encoded) {
				t.Fatal("command view does not capacity-clamp and borrow the encoded input")
			}
			relations := view.RelationBatches()
			if !relations.Next() {
				t.Fatal("borrowed command has no relation batch")
			}
			mutations := relations.Batch().Mutations()
			if !mutations.Next() {
				t.Fatal("borrowed command has no mutation")
			}
			mutation := mutations.Mutation()
			if cap(mutation.Key) != len(mutation.Key) || cap(mutation.Value) != len(mutation.Value) ||
				!sliceInside(mutation.Key, encoded) || !sliceInside(mutation.Value, encoded) {
				t.Fatal("mutation key or value was copied or retained excess capacity")
			}
			if mutations.Next() || relations.Next() {
				t.Fatal("performance fixture unexpectedly contains extra work")
			}
		})
	}
}

// TestFusedTransactionBodyCopyBudget exercises the real CommandTransaction
// envelope used by the response-critical participant prepare wave. Native
// relation bytes are owned once by the output buffer and borrowed through
// both nested decoders; payload size does not introduce another full copy.
func TestFusedTransactionBodyCopyBudget(t *testing.T) {
	tests := []struct {
		name         string
		payloadBytes int
		runs         int
	}{
		{name: "1KiB", payloadBytes: 1 << 10, runs: 100},
		{name: "64KiB", payloadBytes: 64 << 10, runs: 50},
		{name: "1MiB", payloadBytes: 1 << 20, runs: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command, payload := transactionPerfFusedCommand(t, test.payloadBytes)
			encoded := encodeCommand(t, command)
			if len(encoded) <= len(payload) || len(encoded)-len(payload) > 1024 {
				t.Fatalf("fused command bytes=%d payload=%d", len(encoded), len(payload))
			}
			scratch := make([]byte, 0, len(encoded))
			var err error
			transactionPerfBytesSink, err = AppendCommand(scratch[:0], command)
			if err != nil {
				t.Fatal(err)
			}
			transactionPerfBytesSink, err = AppendCommand(nil, command)
			if err != nil {
				t.Fatal(err)
			}
			if allocations := testing.AllocsPerRun(test.runs, func() {
				transactionPerfBytesSink, err = AppendCommand(scratch[:0], command)
				if err != nil {
					panic(err)
				}
			}); allocations != 0 {
				t.Fatalf("pre-sized fused append allocations=%v, want 0", allocations)
			}
			if allocations := testing.AllocsPerRun(test.runs, func() {
				transactionPerfBytesSink, err = AppendCommand(nil, command)
				if err != nil {
					panic(err)
				}
			}); allocations < 1 || allocations > 2 {
				t.Fatalf("owned fused append allocations=%v, want in [1,2]", allocations)
			}
			if allocations := testing.AllocsPerRun(test.runs, func() {
				view, openErr := OpenCommand(encoded)
				if openErr != nil {
					panic(openErr)
				}
				transactionPerfIntSink += len(view.TransactionBytes())
				relations := view.RelationBatches()
				for relations.Next() {
					mutations := relations.Batch().Mutations()
					for mutations.Next() {
						mutation := mutations.Mutation()
						transactionPerfIntSink += len(mutation.Key) + len(mutation.Value)
					}
				}
			}); allocations != 0 {
				t.Fatalf("borrowed fused open allocations=%v, want 0", allocations)
			}

			view, err := OpenCommand(encoded)
			if err != nil || !sliceInside(view.TransactionBytes(), encoded) ||
				cap(view.TransactionBytes()) != len(view.TransactionBytes()) {
				t.Fatalf("fused transaction body was copied or retained capacity: %v", err)
			}
			relations := view.RelationBatches()
			if !relations.Next() {
				t.Fatal("fused transaction has no relation batch")
			}
			mutations := relations.Batch().Mutations()
			if !mutations.Next() {
				t.Fatal("fused transaction has no mutation")
			}
			mutation := mutations.Mutation()
			if len(mutation.Value) != len(payload) || !sliceInside(mutation.Value, encoded) ||
				cap(mutation.Value) != len(mutation.Value) {
				t.Fatal("fused mutation payload was copied or retained capacity")
			}
		})
	}
}

// TestReplicatedTransactionEncodedSchedulePerformanceTargets builds and opens
// every route-bound fused CommandTransaction in the success schedule. It proves
// the canonical envelope count, identities, route diversity, manifest packing,
// and revision CAS values. State-machine execution and two-real-RF3-group fault
// coverage belong in the cross-layer replicatedstate/shard-service suites; this
// package cannot import those layers without an import cycle.
func TestReplicatedTransactionEncodedSchedulePerformanceTargets(t *testing.T) {
	tests := []struct {
		participants  int
		critical      int
		total         int
		barriers      int
		manifestPages int
	}{
		{participants: 1, critical: 1, total: 1, barriers: 1},
		{participants: 2, critical: 5, total: 6, barriers: 4},
		{participants: 65, critical: 131, total: 132, barriers: 4, manifestPages: 1},
		// 4097 deliberately crosses both the inline threshold and one page.
		{participants: 4097, critical: 8195, total: 8196, barriers: 4, manifestPages: 5},
	}
	for _, test := range tests {
		got := buildReplicatedTransactionPerformanceSchedule(t, test.participants)
		if got.critical != test.critical || got.total != test.total ||
			got.barriers != test.barriers || got.manifestPages != test.manifestPages {
			t.Fatalf(
				"participants=%d schedule=critical:%d total:%d barriers:%d pages:%d, want %d/%d/%d/%d",
				test.participants, got.critical, got.total, got.barriers, got.manifestPages,
				test.critical, test.total, test.barriers, test.manifestPages,
			)
		}
		if got.maxCommandBytes > 1<<20 {
			t.Fatalf("participants=%d max command=%dB, want <=1MiB",
				test.participants, got.maxCommandBytes)
		}
		if test.participants > 1 {
			if got.uniquePrepares != test.participants || got.uniqueFinishes != test.participants {
				t.Fatalf("participants=%d unique prepares/finishes=%d/%d, want %d/%d",
					test.participants, got.uniquePrepares, got.uniqueFinishes,
					test.participants, test.participants)
			}
			wantDecisionRevision := uint64(1)
			if test.manifestPages != 0 {
				wantDecisionRevision = uint64(test.manifestPages)
			}
			if got.decisionRevision != wantDecisionRevision ||
				got.retireRevision != wantDecisionRevision+1 {
				t.Fatalf("participants=%d coordinator revisions=%d/%d, want %d/%d",
					test.participants, got.decisionRevision, got.retireRevision,
					wantDecisionRevision, wantDecisionRevision+1)
			}
			legacyProposals := 4*test.participants + 3
			if got.total >= legacyProposals {
				t.Fatalf("participants=%d target proposals=%d do not improve legacy=%d",
					test.participants, got.total, legacyProposals)
			}
		}
	}
}

// TestReplicatedTransactionManifestPackingTarget prevents a future RF3 path
// from turning every canonical 64 KiB logical page into one synchronous Raft
// round trip. Pages remain independently verifiable, but proposal payloads are
// packed to the existing 1 MiB Raft batching target.
func TestReplicatedTransactionManifestPackingTarget(t *testing.T) {
	const (
		logicalPageBytes       = 64 << 10
		proposalPackBytes      = 1 << 20
		maxCommandFramingBytes = 512
	)
	pagesPerProposal := (proposalPackBytes - maxCommandFramingBytes) / logicalPageBytes
	if pagesPerProposal != 15 {
		t.Fatalf("pages per sub-1MiB proposal = %d, want 15", pagesPerProposal)
	}
	tests := []struct {
		manifestBytes int
		pages         int
		proposals     int
	}{
		{manifestBytes: logicalPageBytes, pages: 1, proposals: 1},
		{manifestBytes: proposalPackBytes, pages: 16, proposals: 2},
		{manifestBytes: 16 << 20, pages: 256, proposals: 18},
		{manifestBytes: 64 << 20, pages: 1024, proposals: 69},
	}
	for _, test := range tests {
		pages := ceilPositive(test.manifestBytes, logicalPageBytes)
		proposals := ceilPositive(pages, pagesPerProposal)
		if pages != test.pages || proposals != test.proposals {
			t.Fatalf("manifest=%dB packing = %d pages/%d proposals, want %d/%d",
				test.manifestBytes, pages, proposals, test.pages, test.proposals)
		}
		if proposals > pages || pages > 1 && proposals >= pages {
			t.Fatalf("manifest=%dB did not amortize logical pages: %d/%d",
				test.manifestBytes, proposals, pages)
		}
	}
}

func BenchmarkTransactionBodyOrdinaryCommandEnvelope(b *testing.B) {
	tests := []struct {
		name         string
		payloadBytes int
	}{
		{name: "1KiB", payloadBytes: 1 << 10},
		{name: "64KiB", payloadBytes: 64 << 10},
		{name: "1MiB", payloadBytes: 1 << 20},
		{name: "4MiB", payloadBytes: MaxMutationValueBytes},
	}
	for _, test := range tests {
		command, payload := transactionPerfCommand(test.payloadBytes)
		encoded := encodeCommand(b, command)
		b.Run(test.name+"/append-presized", func(b *testing.B) {
			scratch := make([]byte, 0, len(encoded))
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				var err error
				transactionPerfBytesSink, err = AppendCommand(scratch[:0], command)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(encoded)-len(payload)), "framing-B/op")
		})
		b.Run(test.name+"/open-borrowed", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				view, err := OpenCommand(encoded)
				if err != nil {
					b.Fatal(err)
				}
				relations := view.RelationBatches()
				for relations.Next() {
					mutations := relations.Batch().Mutations()
					for mutations.Next() {
						mutation := mutations.Mutation()
						transactionPerfIntSink += len(mutation.Key) + len(mutation.Value)
					}
				}
			}
			b.ReportMetric(float64(len(encoded)-len(payload)), "framing-B/op")
		})
	}
}

func BenchmarkFusedTransactionEnvelope(b *testing.B) {
	tests := []struct {
		name         string
		payloadBytes int
	}{
		{name: "1KiB", payloadBytes: 1 << 10},
		{name: "64KiB", payloadBytes: 64 << 10},
		{name: "1MiB", payloadBytes: 1 << 20},
		{name: "4MiB", payloadBytes: MaxMutationValueBytes},
	}
	for _, test := range tests {
		command, payload := transactionPerfFusedCommand(b, test.payloadBytes)
		encoded := encodeCommand(b, command)
		b.Run(test.name+"/append-presized", func(b *testing.B) {
			scratch := make([]byte, 0, len(encoded))
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				var err error
				transactionPerfBytesSink, err = AppendCommand(scratch[:0], command)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(encoded)-len(payload)), "framing-B/op")
		})
		b.Run(test.name+"/open-borrowed", func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			for b.Loop() {
				view, err := OpenCommand(encoded)
				if err != nil {
					b.Fatal(err)
				}
				transactionPerfIntSink += len(view.TransactionBytes())
				relations := view.RelationBatches()
				for relations.Next() {
					mutations := relations.Batch().Mutations()
					for mutations.Next() {
						mutation := mutations.Mutation()
						transactionPerfIntSink += len(mutation.Key) + len(mutation.Value)
					}
				}
			}
			b.ReportMetric(float64(len(encoded)-len(payload)), "framing-B/op")
		})
	}
}

func transactionPerfCommand(payloadBytes int) (Command, []byte) {
	command := testCommand()
	payload := make([]byte, payloadBytes)
	for index := range payload {
		payload[index] = byte(index*131 + 17)
	}
	command.Batches[0].Mutations = []Mutation{{
		Kind: MutationPut, Key: []byte{'k'}, Value: payload,
	}}
	return command, payload
}

func transactionPerfFusedCommand(t testing.TB, payloadBytes int) (Command, []byte) {
	t.Helper()
	command := testFusedParticipantStageCommand(t)
	payload := make([]byte, payloadBytes)
	for index := range payload {
		payload[index] = byte(index*131 + 17)
	}
	command.Batches = []RelationMutationBatch{{
		Relation:  1,
		Mutations: []Mutation{{Kind: MutationPut, Key: []byte{'k'}, Value: payload}},
	}}
	digest, err := TransactionMutationDigest(command.Batches)
	if err != nil {
		t.Fatal(err)
	}
	control, err := distributedtxn.OpenReplicatedCommand(command.Transaction)
	if err != nil {
		t.Fatal(err)
	}
	construction := control.Command()
	construction.Participant.MutationDigest = digest
	command.Transaction, err = distributedtxn.AppendReplicatedCommand(nil, construction)
	if err != nil {
		t.Fatal(err)
	}
	return command, payload
}

type transactionPerformanceSchedule struct {
	critical         int
	total            int
	barriers         int
	manifestPages    int
	maxCommandBytes  int
	uniquePrepares   int
	uniqueFinishes   int
	decisionRevision uint64
	retireRevision   uint64
}

func buildReplicatedTransactionPerformanceSchedule(
	t testing.TB,
	participants int,
) transactionPerformanceSchedule {
	t.Helper()
	if participants <= 0 {
		return transactionPerformanceSchedule{}
	}
	if participants == 1 {
		command := testMultiRelationCommand()
		encoded := encodeCommand(t, command)
		return transactionPerformanceSchedule{
			critical: 1, total: 1, barriers: 1, maxCommandBytes: len(encoded),
		}
	}

	base := testMultiRelationCommand()
	digest, err := TransactionMutationDigest(base.Batches)
	if err != nil {
		t.Fatal(err)
	}
	id := distributedtxn.ID(transactionControlID(0xb7))
	refs := transactionPerformanceParticipants(participants, digest)
	participant := distributedtxn.ParticipantStage{
		CoordinatorGroup:            distributedtxn.ID(base.GroupID),
		CoordinatorShardIncarnation: distributedtxn.ID(base.ShardIncarnation),
		CoordinatorAllocation:       refs[0].AllocationGeneration,
		BucketBits:                  8,
		IntentScopes:                []distributedtxn.IntentScope{{Start: 0, End: 256}},
		MutationDigest:              digest,
	}
	metrics := transactionPerformanceSchedule{}
	prepareEnvelopes := make(map[[32]byte]struct{}, participants)
	finishEnvelopes := make(map[[32]byte]struct{}, participants)
	appendCommand := func(
		control distributedtxn.ReplicatedCommand,
		batches []RelationMutationBatch,
		route distributedtxn.ParticipantRef,
		critical bool,
		identitySet map[[32]byte]struct{},
	) {
		controlBytes, appendErr := distributedtxn.AppendReplicatedCommand(nil, control)
		if appendErr != nil {
			t.Fatalf("append operation %d: %v", control.Operation, appendErr)
		}
		command := base
		command.Kind = CommandTransaction
		command.Transaction = controlBytes
		command.ClientID = ID128(id)
		if control.Role == distributedtxn.ReplicatedRoleParticipant {
			command.ClientEpoch = transactionParticipantEpoch
		} else {
			command.ClientEpoch = transactionCoordinatorEpoch
		}
		command.ClientSequence, appendErr = TransactionClientSequence(controlBytes)
		if appendErr != nil {
			t.Fatalf("operation %d sequence: %v", control.Operation, appendErr)
		}
		command.AckThrough = 0
		command.Batches = batches
		command.Distribution = string(route.Distribution)
		command.Shard = string(route.Shard)
		command.RoutingVersion = route.RoutingVersion
		command.AllocationGeneration = route.AllocationGeneration
		command.OwnershipEpoch = route.OwnershipEpoch
		encoded := encodeCommand(t, command)
		view, openErr := OpenCommand(encoded)
		if openErr != nil || !bytes.Equal(view.TransactionBytes(), controlBytes) {
			t.Fatalf("open operation %d: %v", control.Operation, openErr)
		}
		if !bytes.Equal(view.Distribution, route.Distribution) ||
			!bytes.Equal(view.Shard, route.Shard) ||
			view.RoutingVersion != route.RoutingVersion ||
			view.AllocationGeneration != route.AllocationGeneration ||
			view.OwnershipEpoch != route.OwnershipEpoch {
			t.Fatalf("operation %d route does not match participant", control.Operation)
		}
		metrics.maxCommandBytes = max(metrics.maxCommandBytes, len(encoded))
		if identitySet != nil {
			identity := sha256.Sum256(encoded)
			if _, exists := identitySet[identity]; exists {
				t.Fatalf("operation %d emitted a duplicate route-bound envelope", control.Operation)
			}
			identitySet[identity] = struct{}{}
		}
		metrics.total++
		if critical {
			metrics.critical++
		}
	}

	coordinator := distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, RecoveryDeadline: 1, Participants: refs,
	}
	participant.ParticipantOrdinal = 0
	if participants <= distributedtxn.MaxInlineParticipants {
		record, appendErr := distributedtxn.AppendCoordinator(nil, coordinator)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		appendCommand(distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleCoordinator,
			Operation: distributedtxn.ReplicatedBeginPrepareCoordinator,
			ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator,
			Payload: record, Participant: participant,
		}, base.Batches, refs[0], true, prepareEnvelopes)
	} else {
		pageScratch := make([]byte, distributedtxn.ManifestSegmentBytes)
		packed := make([]byte, 0, distributedtxn.MaxManifestSegmentSequenceBytes)
		builder, buildErr := distributedtxn.NewManifestBuilder(
			pageScratch,
			func(segment distributedtxn.ManifestSegment) error {
				metrics.manifestPages++
				packed = append(packed, segment.Raw...)
				return nil
			},
		)
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		for index := range refs {
			if buildErr = builder.Append(refs[index]); buildErr != nil {
				t.Fatalf("manifest participant %d: %v", index, buildErr)
			}
		}
		descriptor, buildErr := builder.Seal()
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		if metrics.manifestPages > distributedtxn.MaxManifestSegmentsPerCommand {
			t.Fatalf("participants=%d require %d initial pages, packing gate supports %d",
				participants, metrics.manifestPages, distributedtxn.MaxManifestSegmentsPerCommand)
		}
		manifest, appendErr := distributedtxn.AppendManifestCoordinator(
			nil, distributedtxn.ManifestCoordinatorRecord{
				ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
				CatalogGeneration: 1, RecoveryDeadline: 1, Manifest: descriptor,
			},
		)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		payload := append(manifest, packed...)
		appendCommand(distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleCoordinator,
			Operation: distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
			ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadManifestCoordinator,
			Payload: payload, Participant: participant,
		}, base.Batches, refs[0], true, prepareEnvelopes)
	}
	metrics.barriers++ // coordinator begin plus its local prepare
	metrics.decisionRevision = uint64(max(1, metrics.manifestPages))

	for ordinal := 1; ordinal < participants; ordinal++ {
		participant.ParticipantOrdinal = uint32(ordinal)
		appendCommand(distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedStagePrepareParticipant,
			ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
			Participant: participant,
		}, base.Batches, refs[ordinal], true, prepareEnvelopes)
	}
	metrics.uniquePrepares = len(prepareEnvelopes)
	metrics.barriers++ // parallel remote prepare wave

	appendCommand(distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator,
		ID:        id, ExpectedRevision: metrics.decisionRevision,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil, refs[0], true, nil)
	metrics.barriers++

	for ordinal := range participants {
		appendCommand(distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedApplyReleaseParticipant,
			ID:        id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
		}, nil, refs[ordinal], true, finishEnvelopes)
	}
	metrics.uniqueFinishes = len(finishEnvelopes)
	metrics.barriers++ // parallel apply plus release wave

	metrics.retireRevision = metrics.decisionRevision + 1
	appendCommand(distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedRetireCoordinator,
		ID:        id, ExpectedRevision: metrics.retireRevision,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil, refs[0], false, nil)
	return metrics
}

func transactionPerformanceParticipants(
	count int,
	digest distributedtxn.Digest,
) []distributedtxn.ParticipantRef {
	participants := make([]distributedtxn.ParticipantRef, count)
	for index := range participants {
		participants[index] = distributedtxn.ParticipantRef{
			Distribution:   []byte("data"),
			Shard:          []byte(fmt.Sprintf("s%08d", index)),
			RoutingVersion: 1, AllocationGeneration: uint64(index + 1),
			OwnershipEpoch: uint64(index + 1), MutationDigest: digest,
			State: distributedtxn.ParticipantStaged,
		}
	}
	return participants
}

func transactionPerfCommandBytes(command Command) int {
	total := commandHeaderBytes + envelopeChecksumBytes + len(command.Tenant) +
		len(command.Distribution) + len(command.Shard)
	if len(command.Batches) > 1 {
		total += len(command.Batches) * relationBatchHeaderBytes
	}
	for batch := range command.Batches {
		for mutation := range command.Batches[batch].Mutations {
			item := &command.Batches[batch].Mutations[mutation]
			total += mutationHeaderBytes + len(item.Key) + mutationWireValueBytes(*item)
		}
	}
	return total
}

func sliceInside(inner, outer []byte) bool {
	if len(inner) == 0 || len(outer) == 0 {
		return false
	}
	innerStart := uintptr(unsafe.Pointer(unsafe.SliceData(inner)))
	innerEnd := innerStart + uintptr(len(inner))
	outerStart := uintptr(unsafe.Pointer(unsafe.SliceData(outer)))
	outerEnd := outerStart + uintptr(len(outer))
	return innerStart >= outerStart && innerEnd <= outerEnd
}

func ceilPositive(value, unit int) int {
	if value <= 0 || unit <= 0 {
		return 0
	}
	return 1 + (value-1)/unit
}
