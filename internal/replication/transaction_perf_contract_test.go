package replication

import (
	"testing"
	"unsafe"
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

// TestReplicatedTransactionProtocolPerformanceTargets is intentionally a
// target contract, not a claim about an integrated transaction runner. It
// keeps the desired proposal and ordered-barrier geometry executable while the
// transaction command grammar is developed independently.
func TestReplicatedTransactionProtocolPerformanceTargets(t *testing.T) {
	tests := []struct {
		participants int
		critical     int
		total        int
		barriers     int
	}{
		{participants: 1, critical: 1, total: 1, barriers: 1},
		{participants: 2, critical: 5, total: 6, barriers: 4},
		{participants: 8, critical: 17, total: 18, barriers: 4},
		{participants: 64, critical: 129, total: 130, barriers: 4},
		// The arithmetic is byte-bounded, not restricted to 64 participants.
		{participants: 4096, critical: 8193, total: 8194, barriers: 4},
	}
	for _, test := range tests {
		critical, total, barriers := replicatedTransactionPerformanceTarget(test.participants)
		if critical != test.critical || total != test.total || barriers != test.barriers {
			t.Fatalf(
				"participants=%d target = critical:%d total:%d barriers:%d, want %d/%d/%d",
				test.participants, critical, total, barriers,
				test.critical, test.total, test.barriers,
			)
		}
		if test.participants > 1 {
			legacyProposals := 4*test.participants + 3
			if total >= legacyProposals {
				t.Fatalf("participants=%d target proposals=%d do not improve legacy=%d",
					test.participants, total, legacyProposals)
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

func replicatedTransactionPerformanceTarget(participants int) (
	criticalProposals int,
	totalProposals int,
	orderedBarriers int,
) {
	if participants <= 0 {
		return 0, 0, 0
	}
	if participants == 1 {
		// Same-shard, multi-relation mutations are one atomic command.
		return 1, 1, 1
	}
	// Begin+prepare the coordinator, prepare every remote participant, commit,
	// and apply+release every participant are response-critical. Coordinator
	// retirement is one non-critical cleanup proposal after all apply acks.
	criticalProposals = 2*participants + 1
	return criticalProposals, criticalProposals + 1, 4
}

func ceilPositive(value, unit int) int {
	if value <= 0 || unit <= 0 {
		return 0
	}
	return 1 + (value-1)/unit
}
