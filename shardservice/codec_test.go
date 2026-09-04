package shardservice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedagg"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
)

func testTransactionID(seed byte) distributedtxn.ID {
	var id distributedtxn.ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return id
}

func testTransactionDigest(seed byte) distributedtxn.Digest {
	var digest distributedtxn.Digest
	for i := range digest {
		digest[i] = seed + byte(i)
	}
	return digest
}

func testExchangeKey(seed byte) exchange.Key {
	var id exchange.ID
	for i := range id {
		id[i] = seed + byte(i)
	}
	return exchange.Key{Operation: id, Stage: 2, Partition: 3, Attempt: 4}
}

func testParticipantRecord(t *testing.T) []byte {
	t.Helper()
	record, err := distributedtxn.AppendParticipant(nil, distributedtxn.ParticipantRecord{
		ID: testTransactionID(1), State: distributedtxn.ParticipantStaged,
		Revision: 1, RoutingVersion: 7, AllocationGeneration: 5,
		OwnershipEpoch: 3, CoordinatorDistribution: []byte("docs"), CoordinatorShard: []byte("-40"),
		CoordinatorAllocation: 5, CoordinatorRoutingVersion: 7, CoordinatorOwnershipEpoch: 3,
		MutationDigest: testTransactionDigest(33),
		Mutation:       []byte{0x56, 0x4d, 0x31, 0, 1, 2, 3},
	})
	if err != nil {
		t.Fatalf("AppendParticipant: %v", err)
	}
	return record
}

// encodeRequest / encodeResponse are one-shot helpers that fail the test rather
// than thread an error through every case.
func encodeRequest(t *testing.T, req *ShardRequest) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := EncodeRequest(&buf, req); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	return buf.Bytes()
}

func encodeResponse(t *testing.T, resp *ShardResponse) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := EncodeResponse(&buf, resp); err != nil {
		t.Fatalf("EncodeResponse: %v", err)
	}
	return buf.Bytes()
}

// TestRequestRoundTrip encodes a request, decodes it, and re-encodes the decoded
// value: equal bytes both times prove the codec is deterministic and lossless.
func TestRequestRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		req  *ShardRequest
	}{
		{
			name: "empty",
			req:  &ShardRequest{},
		},
		{
			name: "select_no_params",
			req: &ShardRequest{
				SQL:                  "SELECT 1",
				Distribution:         "tenant_data",
				Shard:                "-80",
				AllocationGeneration: 5,
				RoutingVersion:       7,
				OwnershipEpoch:       3,
				ReadPolicy:           ReadStrong,
				Deadline:             5 * time.Second,
				MaxResultBytes:       1 << 20,
				MaxRows:              1000,
			},
		},
		{
			name: "partial_aggregate",
			req: &ShardRequest{
				SQL:    `SELECT n, COUNT(*) FROM messages GROUP BY n ORDER BY n LIMIT ?`,
				Params: []Param{NumberParam("2")}, PartialAggregate: true,
			},
		},
		{
			name: "row_batch",
			req: &ShardRequest{
				SQL: "SELECT id FROM messages", MaxRows: 1024, MaxResultBytes: 1 << 20,
				RowBatch: RowBatchRequest{
					BatchRows: 128, BatchBytes: 64 << 10,
				},
			},
		},
		{
			name: "all_param_kinds",
			req: &ShardRequest{
				SQL:           "INSERT INTO messages VALUES ($1,$2,$3,$4,$5)",
				ExecutionMode: ExecutionReadWrite,
				Params: []Param{
					NullParam(),
					BoolParam(true),
					NumberParam("50e-1"),
					StringParam("hello\x00world"),
					DocumentParam(`{"a":[1,2,3]}`),
				},
				Distribution:         "tenant_data",
				Shard:                "80-",
				AllocationGeneration: 6,
				RoutingVersion:       42,
				OwnershipEpoch:       9,
			},
		},
		{
			name: "analysis_parameter_types",
			req: &ShardRequest{
				SQL: "SELECT $1,$2,$3,$4,$5,$6",
				Params: []Param{
					BoolParam(true), StringParam("text"), StringParam("varchar"),
					StringParam("name"), StringParam("bpchar"), NullParam(),
				},
				ParamTypes: []sqldriver.ParamType{
					sqldriver.ParamTypeBool, sqldriver.ParamTypeText,
					sqldriver.ParamTypeVarchar, sqldriver.ParamTypeName,
					sqldriver.ParamTypeBPChar, sqldriver.ParamTypeOther,
				},
			},
		},
		{
			name: "empty_strings",
			req: &ShardRequest{
				SQL:    "",
				Params: []Param{StringParam(""), NumberParam("0"), BoolParam(false)},
			},
		},
		{
			name: "scoped_access",
			req: &ShardRequest{
				SQL:          "SELECT n FROM messages WHERE tenant_id = ?",
				Params:       []Param{StringParam("acme")},
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				BucketBits:   20,
				AccessScopes: []distributedtxn.IntentScope{{Start: 17, End: 18}, {Start: 99, End: 101}},
				ReadFenceID:  testTransactionID(41),
			},
		},
		{
			name: "global_index_lookup",
			req: &ShardRequest{
				Distribution: "messages_by_email", Shard: "40-80",
				AllocationGeneration: 6, RoutingVersion: 42, OwnershipEpoch: 9,
				BucketBits: 20, AccessScopes: []distributedtxn.IntentScope{{Start: 31, End: 32}},
				ReadFenceID: testTransactionID(51),
				GlobalIndexLookup: GlobalIndexLookupRequest{
					Relation: []byte("messages_by_email_17"), IndexID: 17,
					Incarnation: 3, KeyTuples: [][]byte{
						{1, 5, 'a', '@', 'b'}, {1, 5, 'c', '@', 'd'},
					},
					LocatorCount: 2, Unique: true,
				},
			},
		},
		{
			name: "primary_candidates",
			req: &ShardRequest{
				SQL:          `SELECT id FROM messages WHERE email = ?`,
				Params:       []Param{StringParam("a@example.com")},
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				BucketBits: 20, AccessScopes: []distributedtxn.IntentScope{{Start: 31, End: 32}},
				ReadFenceID: testTransactionID(52),
				PrimaryKeyRead: PrimaryKeyReadRequest{
					Relation: 1, MaxDocumentBytes: 4 << 20,
					PrimaryPath: []byte("/id"), Keys: [][]byte{{1, 'a'}, {1, 'b'}},
				},
			},
		},
		{
			name: "mutation_capture",
			req: &ShardRequest{
				SQL:          `DELETE FROM messages WHERE tenant_id = ?`,
				Params:       []Param{StringParam("acme")},
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				BucketBits: 20, AccessScopes: []distributedtxn.IntentScope{{Start: 31, End: 32}},
				MutationCapture: true,
			},
		},
		{
			name: "document_scan",
			req: &ShardRequest{
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				MaxRows: 128, MaxResultBytes: 1 << 20,
				DocumentScan: DocumentScanRequest{
					Relation: []byte("messages"), After: []byte{1, 'a'},
				},
			},
		},
		{
			name: "stage_participant",
			req: &ShardRequest{
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				ExecutionMode: ExecutionReadWrite,
				Transaction: TransactionRequest{
					Operation: TransactionStageParticipant,
					Record:    testParticipantRecord(t),
				},
			},
		},
		{
			name: "apply_participant",
			req: &ShardRequest{
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				ExecutionMode: ExecutionReadWrite,
				Transaction: TransactionRequest{
					Operation: TransactionApplyParticipant,
					ID:        testTransactionID(1), Revision: 1,
				},
			},
		},
		{
			name: "exchange_open",
			req: &ShardRequest{
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				ExecutionMode: ExecutionReadWrite, Deadline: 5 * time.Second,
				Exchange: ExchangeRequest{
					Operation: ExchangeOpen, Key: testExchangeKey(61),
					Producers: 3, QueuedBatches: 8, ProducerBatches: 2,
					BufferedRows: 1024, BufferedBytes: 1 << 20,
					TotalRows: 4096, TotalBytes: 4 << 20,
				},
			},
		},
		{
			name: "exchange_push",
			req: &ShardRequest{
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				ExecutionMode: ExecutionReadWrite,
				Exchange: ExchangeRequest{
					Operation: ExchangePush, Key: testExchangeKey(61),
					Batch: exchange.Batch{Producer: 2, Sequence: 7, Rows: 2, Data: []byte{1, 2, 3}, Final: true},
				},
			},
		},
		{
			name: "exchange_pull_ack",
			req: &ShardRequest{
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				ExecutionMode: ExecutionReadOnly,
				Exchange: ExchangeRequest{
					Operation: ExchangePull, Key: testExchangeKey(61),
					HasAck: true, AckProducer: 2, AckSequence: 7,
				},
			},
		},
		{
			name: "exchange_reduce",
			req: &ShardRequest{
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				ExecutionMode: ExecutionReadWrite,
				Exchange: ExchangeRequest{
					Operation: ExchangeReduce, Key: testExchangeKey(62),
					Output: func() exchange.Key {
						key := testExchangeKey(62)
						key.Stage++
						return key
					}(),
					Kinds: []distributedagg.Kind{
						distributedagg.None, distributedagg.Count, distributedagg.Sum,
					},
					GroupKeys: []uint16{0}, MaxStateBytes: 1 << 20,
					BlockRows: 128, BlockBytes: 64 << 10,
				},
			},
		},
		{
			name: "direct_repartition",
			req: &ShardRequest{
				SQL:          "SELECT tenant_id, COUNT(*) FROM messages GROUP BY tenant_id",
				Distribution: "tenant_data", Shard: "40-80",
				AllocationGeneration: 5, RoutingVersion: 7, OwnershipEpoch: 3,
				ExecutionMode: ExecutionReadOnly, MaxRows: 1024, MaxResultBytes: 1 << 20,
				Repartition: RepartitionRequest{
					Operation: testExchangeKey(72).Operation, Stage: 3, Attempt: 2, Producer: 4,
					KeyColumns: []uint16{0}, BlockRows: 128, BlockBytes: 64 << 10, MaxMemory: 128 << 10,
					Targets: []RepartitionTarget{
						{Address: []byte("127.0.0.1:9001"), Distribution: "workers", Shard: "-80", AllocationGeneration: 11, RoutingVersion: 8, OwnershipEpoch: 5},
						{Address: []byte("127.0.0.1:9002"), Distribution: "workers", Shard: "80-", AllocationGeneration: 12, RoutingVersion: 8, OwnershipEpoch: 6},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b1 := encodeRequest(t, tc.req)
			got, err := DecodeRequest(bytes.NewReader(b1))
			if err != nil {
				t.Fatalf("DecodeRequest: %v", err)
			}
			b2 := encodeRequest(t, got)
			if !bytes.Equal(b1, b2) {
				t.Fatalf("re-encode not stable:\n first %x\nsecond %x", b1, b2)
			}
			if got.SQL != tc.req.SQL {
				t.Errorf("SQL = %q, want %q", got.SQL, tc.req.SQL)
			}
			if len(got.Params) != len(tc.req.Params) {
				t.Fatalf("params = %d, want %d", len(got.Params), len(tc.req.Params))
			}
			if !slices.Equal(got.ParamTypes, tc.req.ParamTypes) {
				t.Errorf("ParamTypes = %v, want %v", got.ParamTypes, tc.req.ParamTypes)
			}
			for i := range got.Params {
				if got.Params[i].Kind != tc.req.Params[i].Kind ||
					got.Params[i].Bool != tc.req.Params[i].Bool ||
					!bytes.Equal(got.Params[i].Bytes, tc.req.Params[i].Bytes) {
					t.Errorf("param %d = %+v, want %+v", i, got.Params[i], tc.req.Params[i])
				}
			}
			if got.Deadline != tc.req.Deadline {
				t.Errorf("Deadline = %v, want %v", got.Deadline, tc.req.Deadline)
			}
			if got.ExecutionMode != tc.req.ExecutionMode {
				t.Errorf("ExecutionMode = %v, want %v", got.ExecutionMode, tc.req.ExecutionMode)
			}
			if got.PartialAggregate != tc.req.PartialAggregate {
				t.Errorf("PartialAggregate = %v, want %v", got.PartialAggregate, tc.req.PartialAggregate)
			}
			if got.RowBatch != tc.req.RowBatch {
				t.Errorf("RowBatch = %+v, want %+v", got.RowBatch, tc.req.RowBatch)
			}
			if got.Exchange.Operation != tc.req.Exchange.Operation || got.Exchange.Key != tc.req.Exchange.Key ||
				got.Exchange.Output != tc.req.Exchange.Output ||
				!slices.Equal(got.Exchange.Kinds, tc.req.Exchange.Kinds) ||
				!slices.Equal(got.Exchange.GroupKeys, tc.req.Exchange.GroupKeys) ||
				got.Exchange.MaxStateBytes != tc.req.Exchange.MaxStateBytes ||
				got.Exchange.BlockRows != tc.req.Exchange.BlockRows ||
				got.Exchange.BlockBytes != tc.req.Exchange.BlockBytes ||
				got.Exchange.Producers != tc.req.Exchange.Producers ||
				got.Exchange.QueuedBatches != tc.req.Exchange.QueuedBatches ||
				got.Exchange.ProducerBatches != tc.req.Exchange.ProducerBatches ||
				got.Exchange.BufferedRows != tc.req.Exchange.BufferedRows ||
				got.Exchange.BufferedBytes != tc.req.Exchange.BufferedBytes ||
				got.Exchange.TotalRows != tc.req.Exchange.TotalRows ||
				got.Exchange.TotalBytes != tc.req.Exchange.TotalBytes ||
				got.Exchange.HasAck != tc.req.Exchange.HasAck ||
				got.Exchange.AckProducer != tc.req.Exchange.AckProducer ||
				got.Exchange.AckSequence != tc.req.Exchange.AckSequence ||
				got.Exchange.Batch.Producer != tc.req.Exchange.Batch.Producer ||
				got.Exchange.Batch.Sequence != tc.req.Exchange.Batch.Sequence ||
				got.Exchange.Batch.Rows != tc.req.Exchange.Batch.Rows ||
				got.Exchange.Batch.Final != tc.req.Exchange.Batch.Final ||
				!bytes.Equal(got.Exchange.Batch.Data, tc.req.Exchange.Batch.Data) {
				t.Errorf("Exchange = %+v, want %+v", got.Exchange, tc.req.Exchange)
			}
			if got.Repartition.Operation != tc.req.Repartition.Operation ||
				got.Repartition.Stage != tc.req.Repartition.Stage ||
				got.Repartition.Attempt != tc.req.Repartition.Attempt ||
				got.Repartition.Producer != tc.req.Repartition.Producer ||
				got.Repartition.BlockRows != tc.req.Repartition.BlockRows ||
				got.Repartition.BlockBytes != tc.req.Repartition.BlockBytes ||
				got.Repartition.MaxMemory != tc.req.Repartition.MaxMemory ||
				len(got.Repartition.KeyColumns) != len(tc.req.Repartition.KeyColumns) ||
				len(got.Repartition.Targets) != len(tc.req.Repartition.Targets) {
				t.Fatalf("Repartition = %+v, want %+v", got.Repartition, tc.req.Repartition)
			}
			for i := range got.Repartition.KeyColumns {
				if got.Repartition.KeyColumns[i] != tc.req.Repartition.KeyColumns[i] {
					t.Errorf("Repartition.KeyColumns[%d] = %d, want %d", i, got.Repartition.KeyColumns[i], tc.req.Repartition.KeyColumns[i])
				}
			}
			for i := range got.Repartition.Targets {
				if !bytes.Equal(got.Repartition.Targets[i].Address, tc.req.Repartition.Targets[i].Address) ||
					got.Repartition.Targets[i].Distribution != tc.req.Repartition.Targets[i].Distribution ||
					got.Repartition.Targets[i].Shard != tc.req.Repartition.Targets[i].Shard ||
					got.Repartition.Targets[i].AllocationGeneration != tc.req.Repartition.Targets[i].AllocationGeneration ||
					got.Repartition.Targets[i].RoutingVersion != tc.req.Repartition.Targets[i].RoutingVersion ||
					got.Repartition.Targets[i].OwnershipEpoch != tc.req.Repartition.Targets[i].OwnershipEpoch {
					t.Errorf("Repartition.Targets[%d] = %+v, want %+v", i, got.Repartition.Targets[i], tc.req.Repartition.Targets[i])
				}
			}
			if got.Transaction.Operation != tc.req.Transaction.Operation ||
				got.Transaction.ID != tc.req.Transaction.ID ||
				got.Transaction.Revision != tc.req.Transaction.Revision ||
				got.Transaction.SegmentIndex != tc.req.Transaction.SegmentIndex ||
				!bytes.Equal(got.Transaction.Record, tc.req.Transaction.Record) ||
				!bytes.Equal(got.Transaction.ManifestSegment, tc.req.Transaction.ManifestSegment) {
				t.Errorf("Transaction = %+v, want %+v", got.Transaction, tc.req.Transaction)
			}
			if got.ReadFenceID != tc.req.ReadFenceID {
				t.Errorf("ReadFenceID = %x, want %x", got.ReadFenceID, tc.req.ReadFenceID)
			}
			if got.GlobalIndexLookup.IndexID != tc.req.GlobalIndexLookup.IndexID ||
				got.GlobalIndexLookup.Incarnation != tc.req.GlobalIndexLookup.Incarnation ||
				got.GlobalIndexLookup.LocatorCount != tc.req.GlobalIndexLookup.LocatorCount ||
				got.GlobalIndexLookup.Unique != tc.req.GlobalIndexLookup.Unique ||
				!bytes.Equal(got.GlobalIndexLookup.Relation, tc.req.GlobalIndexLookup.Relation) ||
				len(got.GlobalIndexLookup.KeyTuples) != len(tc.req.GlobalIndexLookup.KeyTuples) {
				t.Errorf("GlobalIndexLookup = %+v, want %+v", got.GlobalIndexLookup, tc.req.GlobalIndexLookup)
			}
			if len(got.GlobalIndexLookup.KeyTuples) == len(tc.req.GlobalIndexLookup.KeyTuples) {
				for i := range got.GlobalIndexLookup.KeyTuples {
					if !bytes.Equal(got.GlobalIndexLookup.KeyTuples[i], tc.req.GlobalIndexLookup.KeyTuples[i]) {
						t.Errorf("GlobalIndexLookup.KeyTuples[%d] = %x, want %x",
							i, got.GlobalIndexLookup.KeyTuples[i], tc.req.GlobalIndexLookup.KeyTuples[i])
					}
				}
			}
			if got.PrimaryKeyRead.Relation != tc.req.PrimaryKeyRead.Relation ||
				got.PrimaryKeyRead.MaxDocumentBytes != tc.req.PrimaryKeyRead.MaxDocumentBytes ||
				!bytes.Equal(got.PrimaryKeyRead.PrimaryPath, tc.req.PrimaryKeyRead.PrimaryPath) ||
				len(got.PrimaryKeyRead.Keys) != len(tc.req.PrimaryKeyRead.Keys) {
				t.Fatalf("PrimaryKeyRead = %+v, want %+v", got.PrimaryKeyRead, tc.req.PrimaryKeyRead)
			}
			for i := range got.PrimaryKeyRead.Keys {
				if !bytes.Equal(got.PrimaryKeyRead.Keys[i], tc.req.PrimaryKeyRead.Keys[i]) {
					t.Errorf("PrimaryKeyRead.Keys[%d] = %x, want %x", i, got.PrimaryKeyRead.Keys[i], tc.req.PrimaryKeyRead.Keys[i])
				}
			}
			if got.MutationCapture != tc.req.MutationCapture {
				t.Errorf("MutationCapture = %v, want %v", got.MutationCapture, tc.req.MutationCapture)
			}
			if !bytes.Equal(got.DocumentScan.Relation, tc.req.DocumentScan.Relation) ||
				!bytes.Equal(got.DocumentScan.After, tc.req.DocumentScan.After) {
				t.Errorf("DocumentScan = %+v, want %+v", got.DocumentScan, tc.req.DocumentScan)
			}
		})
	}
}

func TestPrimaryKeyReadWireCompatibilityAndExtendedBounds(t *testing.T) {
	legacy := &ShardRequest{
		SQL: `SELECT id FROM messages WHERE id = ?`,
		PrimaryKeyRead: PrimaryKeyReadRequest{
			PrimaryPath: []byte("/id"), Keys: [][]byte{{1, 'a'}, {1, 'b'}},
		},
	}
	legacyWire := encodeRequest(t, legacy)
	legacySuffix := []byte{primaryKeyReadMarker}
	legacySuffix = binary.BigEndian.AppendUint32(legacySuffix, uint32(len(legacy.PrimaryKeyRead.PrimaryPath)))
	legacySuffix = append(legacySuffix, legacy.PrimaryKeyRead.PrimaryPath...)
	legacySuffix = binary.BigEndian.AppendUint32(legacySuffix, uint32(len(legacy.PrimaryKeyRead.Keys)))
	for _, key := range legacy.PrimaryKeyRead.Keys {
		legacySuffix = binary.BigEndian.AppendUint32(legacySuffix, uint32(len(key)))
		legacySuffix = append(legacySuffix, key...)
	}
	if !bytes.HasSuffix(legacyWire[5:], legacySuffix) {
		t.Fatalf("legacy primary-key grammar changed: suffix=%x want=%x", legacyWire[5:], legacySuffix)
	}
	if got, err := RequestFrameBytes(legacy); err != nil || got != len(legacyWire) {
		t.Fatalf("legacy RequestFrameBytes = %d, %v; encoded length = %d", got, err, len(legacyWire))
	}
	decoded, err := DecodeRequest(bytes.NewReader(legacyWire))
	if err != nil {
		t.Fatalf("DecodeRequest legacy: %v", err)
	}
	if decoded.PrimaryKeyRead.Relation != 0 || decoded.PrimaryKeyRead.MaxDocumentBytes != 0 {
		t.Fatalf("legacy bounds = relation %d max %d, want zero", decoded.PrimaryKeyRead.Relation, decoded.PrimaryKeyRead.MaxDocumentBytes)
	}
	if reencoded := encodeRequest(t, decoded); !bytes.Equal(reencoded, legacyWire) {
		t.Fatalf("legacy request was not byte-for-byte stable:\n got %x\nwant %x", reencoded, legacyWire)
	}

	extended := *legacy
	extended.PrimaryKeyRead.Relation = 1
	extended.PrimaryKeyRead.MaxDocumentBytes = 4 << 20
	extendedWire := encodeRequest(t, &extended)
	// Build the fixed-width bound explicitly so this check remains independent
	// of EncodeRequest's optional-field implementation.
	extendedSuffix := []byte{primaryKeyReadExtendedMarker, 1}
	extendedSuffix = binary.BigEndian.AppendUint32(extendedSuffix, 4<<20)
	extendedSuffix = append(extendedSuffix, legacySuffix[1:]...)
	if !bytes.HasSuffix(extendedWire[5:], extendedSuffix) {
		t.Fatalf("extended primary-key grammar = %x, want suffix %x", extendedWire[5:], extendedSuffix)
	}
	if got, want := len(extendedWire), len(legacyWire)+5; got != want {
		t.Fatalf("extended frame length = %d, want legacy length + 5 = %d", got, want)
	}
	if got, err := RequestFrameBytes(&extended); err != nil || got != len(extendedWire) {
		t.Fatalf("extended RequestFrameBytes = %d, %v; encoded length = %d", got, err, len(extendedWire))
	}
	decoded, err = DecodeRequest(bytes.NewReader(extendedWire))
	if err != nil {
		t.Fatalf("DecodeRequest extended: %v", err)
	}
	if decoded.PrimaryKeyRead.Relation != extended.PrimaryKeyRead.Relation ||
		decoded.PrimaryKeyRead.MaxDocumentBytes != extended.PrimaryKeyRead.MaxDocumentBytes {
		t.Fatalf("extended bounds = relation %d max %d, want relation %d max %d",
			decoded.PrimaryKeyRead.Relation, decoded.PrimaryKeyRead.MaxDocumentBytes,
			extended.PrimaryKeyRead.Relation, extended.PrimaryKeyRead.MaxDocumentBytes)
	}
	if reencoded := encodeRequest(t, decoded); !bytes.Equal(reencoded, extendedWire) {
		t.Fatalf("extended request was not byte-for-byte stable")
	}
}

// TestResponseRoundTrip does the same for every response shape.
func TestResponseRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		resp *ShardResponse
	}{
		{
			name: "rows",
			resp: RowsResponse(
				[]Column{{Name: "id", TypeOID: 20}, {Name: "doc", TypeOID: 114}},
				[][]Cell{
					{{Bytes: []byte("1")}, {Bytes: []byte(`{"x":1}`)}},
					{{Bytes: []byte("2")}, {Null: true}},
				},
			),
		},
		{
			name: "rows_empty",
			resp: RowsResponse([]Column{{Name: "n", TypeOID: 20}}, nil),
		},
		{
			name: "row_batch_first",
			resp: &ShardResponse{
				Kind: ResponseRowBatch, Columns: []Column{{Name: "id", TypeOID: 114}},
				Rows:     [][]Cell{{{Bytes: []byte(`"a"`)}}},
				RowBatch: RowBatchReply{ColumnCount: 1},
			},
		},
		{
			name: "row_batch_continuation_final",
			resp: &ShardResponse{
				Kind: ResponseRowBatch, Rows: [][]Cell{{{Null: true}}},
				RowBatch: RowBatchReply{Sequence: 3, ColumnCount: 1, Final: true},
			},
		},
		{
			name: "document_scan_page",
			resp: func() *ShardResponse {
				resp := RowsResponse(
					[]Column{{Name: "primary_key"}, {Name: "document"}},
					[][]Cell{{{Bytes: []byte{1, 'a'}}, {Bytes: []byte(`{"id":"a"}`)}}},
				)
				resp.DocumentScan = DocumentScanReply{
					Present: true, Next: []byte{1, 'a'},
				}
				return resp
			}(),
		},
		{
			name: "completion",
			resp: CompletionResponse(42),
		},
		{
			name: "completion_zero",
			resp: CompletionResponse(0),
		},
		{
			name: "exchange_push_ack",
			resp: &ShardResponse{Kind: ResponseCompletion, Exchange: ExchangeReply{Operation: ExchangePush}},
		},
		{
			name: "exchange_pull_batch",
			resp: &ShardResponse{Kind: ResponseCompletion, Exchange: ExchangeReply{
				Operation: ExchangePull,
				Batch:     exchange.Batch{Producer: 1, Sequence: 8, Rows: 2, Data: []byte{4, 5}, Final: true},
			}},
		},
		{
			name: "exchange_pull_eof",
			resp: &ShardResponse{Kind: ResponseCompletion, Exchange: ExchangeReply{Operation: ExchangePull, EOF: true}},
		},
		{
			name: "error",
			resp: NewErrorResponse(ErrorNotOwner, "not owner: shard -80"),
		},
		{
			name: "read_only",
			resp: NewErrorResponse(ErrorReadOnly, "mutation refused"),
		},
		{
			name: "commit_outcome_unknown",
			resp: NewErrorResponse(ErrorCommitOutcomeUnknown, "completion unknown"),
		},
		{
			name: "participant_state",
			resp: &ShardResponse{
				Kind: ResponseCompletion, RowsAffected: 3,
				Transaction: TransactionReply{
					Role: TransactionRoleParticipant, ID: testTransactionID(1), Revision: 2,
					ParticipantState: distributedtxn.ParticipantApplied,
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b1 := encodeResponse(t, tc.resp)
			got, err := DecodeResponse(bytes.NewReader(b1))
			if err != nil {
				t.Fatalf("DecodeResponse: %v", err)
			}
			b2 := encodeResponse(t, got)
			if !bytes.Equal(b1, b2) {
				t.Fatalf("re-encode not stable:\n first %x\nsecond %x", b1, b2)
			}
			if got.Kind != tc.resp.Kind {
				t.Errorf("Kind = %v, want %v", got.Kind, tc.resp.Kind)
			}
			if !got.Transaction.Equal(tc.resp.Transaction) {
				t.Errorf("Transaction = %+v, want %+v", got.Transaction, tc.resp.Transaction)
			}
			if got.DocumentScan.Present != tc.resp.DocumentScan.Present ||
				got.DocumentScan.Complete != tc.resp.DocumentScan.Complete ||
				!bytes.Equal(got.DocumentScan.Next, tc.resp.DocumentScan.Next) {
				t.Errorf("DocumentScan = %+v, want %+v", got.DocumentScan, tc.resp.DocumentScan)
			}
			if got.Exchange.Operation != tc.resp.Exchange.Operation ||
				got.Exchange.EOF != tc.resp.Exchange.EOF ||
				got.Exchange.Batch.Producer != tc.resp.Exchange.Batch.Producer ||
				got.Exchange.Batch.Sequence != tc.resp.Exchange.Batch.Sequence ||
				got.Exchange.Batch.Rows != tc.resp.Exchange.Batch.Rows ||
				got.Exchange.Batch.Final != tc.resp.Exchange.Batch.Final ||
				!bytes.Equal(got.Exchange.Batch.Data, tc.resp.Exchange.Batch.Data) {
				t.Errorf("Exchange = %+v, want %+v", got.Exchange, tc.resp.Exchange)
			}
		})
	}
}

func TestTransactionStageRecordRemainsBorrowed(t *testing.T) {
	req := &ShardRequest{
		Transaction: TransactionRequest{
			Operation: TransactionStageParticipant,
			Record:    testParticipantRecord(t),
		},
	}
	decoded, err := DecodeRequest(bytes.NewReader(encodeRequest(t, req)))
	if err != nil {
		t.Fatalf("DecodeRequest: %v", err)
	}
	record, err := distributedtxn.OpenParticipant(decoded.Transaction.Record)
	if err != nil {
		t.Fatalf("OpenParticipant: %v", err)
	}
	record.Mutation[0] ^= 0xff
	openedAgain := decoded.Transaction.Record[len(decoded.Transaction.Record)-len(record.Mutation)-4]
	if openedAgain != record.Mutation[0] {
		t.Fatal("participant mutation was copied instead of aliasing the request frame")
	}
}

// TestEncodeDeterministic proves the same value encodes to identical bytes
// across repeated calls, which the golden vectors depend on.
func TestEncodeDeterministic(t *testing.T) {
	req := &ShardRequest{
		SQL:                  "SELECT $1",
		Params:               []Param{NumberParam("5"), StringParam("k")},
		Distribution:         "d",
		Shard:                "s",
		AllocationGeneration: 3,
		RoutingVersion:       1,
		OwnershipEpoch:       2,
		MaxRows:              10,
	}
	first := encodeRequest(t, req)
	for i := 0; i < 8; i++ {
		if got := encodeRequest(t, req); !bytes.Equal(got, first) {
			t.Fatalf("encode %d differs: %x vs %x", i, got, first)
		}
	}
}

func TestExchangeEnvelopeRejectsMixedLanes(t *testing.T) {
	base := ShardRequest{
		Distribution: "d", Shard: "s", AllocationGeneration: 1,
		RoutingVersion: 1, OwnershipEpoch: 1, ExecutionMode: ExecutionReadWrite,
		Exchange: ExchangeRequest{
			Operation: ExchangeCancel, Key: testExchangeKey(71),
		},
	}
	for _, mutate := range []func(*ShardRequest){
		func(r *ShardRequest) { r.SQL = "SELECT 1" },
		func(r *ShardRequest) { r.MaxRows = 1 },
		func(r *ShardRequest) { r.BucketBits = 8 },
		func(r *ShardRequest) { r.Transaction.Operation = TransactionScanCoordinator },
		func(r *ShardRequest) { r.ExecutionMode = ExecutionReadOnly },
	} {
		req := base
		mutate(&req)
		if err := EncodeRequest(io.Discard, &req); !errors.Is(err, errBadExchange) {
			t.Fatalf("mixed exchange request error = %v, want errBadExchange", err)
		}
	}

	pull := base
	pull.Exchange.Operation = ExchangePull
	pull.ExecutionMode = ExecutionReadOnly
	if err := EncodeRequest(io.Discard, &pull); err != nil {
		t.Fatalf("canonical pull: %v", err)
	}
	reduce := base
	reduce.Exchange.Operation = ExchangeReduce
	reduce.Exchange.Output = reduce.Exchange.Key
	reduce.Exchange.Output.Stage++
	reduce.Exchange.Kinds = []distributedagg.Kind{distributedagg.None, distributedagg.Count}
	reduce.Exchange.GroupKeys = []uint16{0}
	reduce.Exchange.MaxStateBytes = 1 << 20
	reduce.Exchange.BlockRows = 16
	reduce.Exchange.BlockBytes = 4096
	if err := EncodeRequest(io.Discard, &reduce); err != nil {
		t.Fatalf("canonical reduce: %v", err)
	}
	for _, mutate := range []func(*ExchangeRequest){
		func(r *ExchangeRequest) { r.Output.Operation[0]++ },
		func(r *ExchangeRequest) { r.Output.Stage = r.Key.Stage },
		func(r *ExchangeRequest) { r.Kinds[1] = distributedagg.Kind(99) },
		func(r *ExchangeRequest) { r.Kinds[1] = distributedagg.None },
		func(r *ExchangeRequest) { r.GroupKeys = []uint16{0, 0} },
		func(r *ExchangeRequest) { r.MaxStateBytes = 0 },
		func(r *ExchangeRequest) { r.BlockBytes = 8 },
	} {
		req := reduce
		req.Exchange.Kinds = append([]distributedagg.Kind(nil), reduce.Exchange.Kinds...)
		req.Exchange.GroupKeys = append([]uint16(nil), reduce.Exchange.GroupKeys...)
		mutate(&req.Exchange)
		if err := EncodeRequest(io.Discard, &req); !errors.Is(err, errBadExchange) {
			t.Fatalf("invalid reduce request error = %v, want errBadExchange", err)
		}
	}
	badReply := &ShardResponse{
		Kind: ResponseCompletion, RowsAffected: 1,
		Exchange: ExchangeReply{Operation: ExchangeOpen},
	}
	if err := EncodeResponse(io.Discard, badReply); !errors.Is(err, errBadExchange) {
		t.Fatalf("mixed exchange response error = %v, want errBadExchange", err)
	}
	var body encbuf
	body.u8(wireVersion)
	body.u8(uint8(ResponseCompletion))
	body.u64(0)
	body.u8(0) // absent read position
	body.u8(exchangeMarker)
	body.u8(uint8(ExchangeNone))
	if _, err := DecodeResponse(bytes.NewReader(rawFrame(tagResponse, body.b))); !errors.Is(err, errBadEnum) {
		t.Fatalf("present exchange marker with absent operation = %v, want errBadEnum", err)
	}
}

func TestRepartitionEnvelopeRejectsMixedOrUnboundedLanes(t *testing.T) {
	base := ShardRequest{
		SQL: "SELECT tenant_id FROM docs", Distribution: "d", Shard: "s",
		AllocationGeneration: 1, RoutingVersion: 1, OwnershipEpoch: 1,
		ExecutionMode: ExecutionReadOnly, MaxRows: 100, MaxResultBytes: 1 << 20,
		Repartition: RepartitionRequest{
			Operation: testExchangeKey(73).Operation, Stage: 1, Attempt: 1, KeyColumns: []uint16{0},
			Targets: []RepartitionTarget{{
				Address: []byte("127.0.0.1:9000"), Distribution: "d", Shard: "s",
				AllocationGeneration: 1, RoutingVersion: 1, OwnershipEpoch: 1,
			}},
			BlockRows: 16, BlockBytes: 4096, MaxMemory: 4096,
		},
	}
	if err := EncodeRequest(io.Discard, &base); err != nil {
		t.Fatalf("canonical repartition: %v", err)
	}
	for _, mutate := range []func(*ShardRequest){
		func(r *ShardRequest) { r.ExecutionMode = ExecutionReadWrite },
		func(r *ShardRequest) { r.MaxRows = 0 },
		func(r *ShardRequest) { r.RowBatch = RowBatchRequest{BatchRows: 1, BatchBytes: 64} },
		func(r *ShardRequest) { r.Repartition.KeyColumns = []uint16{0, 0} },
		func(r *ShardRequest) { r.Repartition.MaxMemory-- },
		func(r *ShardRequest) { r.Repartition.Targets[0].Address = []byte("bad\x00address") },
	} {
		req := base
		req.Repartition.KeyColumns = append([]uint16(nil), base.Repartition.KeyColumns...)
		req.Repartition.Targets = append([]RepartitionTarget(nil), base.Repartition.Targets...)
		req.Repartition.Targets[0].Address = bytes.Clone(base.Repartition.Targets[0].Address)
		mutate(&req)
		if err := EncodeRequest(io.Discard, &req); !errors.Is(err, errBadRepartition) {
			t.Fatalf("invalid repartition error = %v, want errBadRepartition", err)
		}
	}
}

// rawFrame assembles a tag + self-covering length + body frame, the shape both
// decoders read, so a test can hand a decoder a body it built byte by byte.
func rawFrame(tag byte, body []byte) []byte {
	f := make([]byte, 5+len(body))
	f[0] = tag
	binary.BigEndian.PutUint32(f[1:5], uint32(len(body)+4))
	copy(f[5:], body)
	return f
}

// TestDecodeRequestMalformed proves every malformed or oversized request frame
// is rejected with a typed error rather than a panic or a large allocation.
func TestDecodeRequestMalformed(t *testing.T) {
	// A minimal valid request body for mutation: version, three empty strings,
	// three u64 (allocation, routing, epoch), policy and execution-mode bytes, three u64
	// (deadline, maxbytes, maxrows), a zero param count, and an absent minimum
	// position marker.
	valid := func() []byte {
		var e encbuf
		e.u8(wireVersion)
		e.str("SELECT 1")
		e.str("d")
		e.str("s")
		e.u64(0)
		e.u64(0)
		e.u64(0)
		e.u8(uint8(ReadStrong))
		e.u8(uint8(ExecutionReadOnly))
		e.u64(0)
		e.u64(0)
		e.u64(0)
		e.u32(0)
		e.u8(0)
		return e.b
	}

	tests := []struct {
		name  string
		frame []byte
		want  error
	}{
		{
			name:  "short_header",
			frame: []byte{tagRequest, 0, 0},
			want:  io.ErrUnexpectedEOF,
		},
		{
			name:  "wrong_tag",
			frame: rawFrame(tagResponse, valid()),
			want:  errBadTag,
		},
		{
			name:  "bad_length_too_small",
			frame: []byte{tagRequest, 0, 0, 0, 3},
			want:  errBadLength,
		},
		{
			name:  "oversized_length",
			frame: []byte{tagRequest, 0x7f, 0xff, 0xff, 0xff},
			want:  errFrameTooLarge,
		},
		{
			name:  "bad_version",
			frame: rawFrame(tagRequest, []byte{0x02}),
			want:  errBadVersion,
		},
		{
			name: "bad_read_policy",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u8(0x7f) // policy byte out of range
				e.u8(uint8(ExecutionReadOnly))
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u32(0)
				e.u8(0)
				return rawFrame(tagRequest, e.b)
			}(),
			want: errBadEnum,
		},
		{
			name: "bad_execution_mode",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u8(uint8(ReadStrong))
				e.u8(0x7f) // execution mode out of range
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u32(0)
				e.u8(0)
				return rawFrame(tagRequest, e.b)
			}(),
			want: errBadEnum,
		},
		{
			name: "impossible_param_count",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u8(uint8(ReadStrong))
				e.u8(uint8(ExecutionReadOnly))
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u32(1000000) // claims a million params in a tiny body
				return rawFrame(tagRequest, e.b)
			}(),
			want: errImpossibleCount,
		},
		{
			name: "bad_param_kind",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u8(uint8(ReadStrong))
				e.u8(uint8(ExecutionReadOnly))
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u32(1)
				e.u8(0xff) // invalid ParamKind
				return rawFrame(tagRequest, e.b)
			}(),
			want: errBadEnum,
		},
		{
			name: "trailing_bytes",
			frame: func() []byte {
				b := append(valid(), 0xaa)
				return rawFrame(tagRequest, b)
			}(),
			want: errTrailing,
		},
		{
			name: "negative_deadline",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion)
				e.str("")
				e.str("")
				e.str("")
				e.u64(0)
				e.u64(0)
				e.u64(0)
				e.u8(uint8(ReadStrong))
				e.u8(uint8(ExecutionReadOnly))
				e.u64(1 << 63) // high bit set: not a valid non-negative duration
				e.u64(0)
				e.u64(0)
				e.u32(0)
				e.u8(0)
				return rawFrame(tagRequest, e.b)
			}(),
			want: errNegativeDuration,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeRequest(bytes.NewReader(tc.frame))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestDecodeResponseMalformed proves the response decoder is bounded the same
// way, including the zero-column/nonzero-row amplification guard.
func TestDecodeResponseMalformed(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
		want  error
	}{
		{
			name:  "wrong_tag",
			frame: rawFrame(tagRequest, []byte{wireVersion, uint8(ResponseCompletion), 0, 0, 0, 0, 0, 0, 0, 0}),
			want:  errBadTag,
		},
		{
			name:  "bad_version",
			frame: rawFrame(tagResponse, []byte{0x09}),
			want:  errBadVersion,
		},
		{
			name:  "bad_response_kind",
			frame: rawFrame(tagResponse, []byte{wireVersion, 0xff}),
			want:  errBadEnum,
		},
		{
			name: "rows_without_columns",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion)
				e.u8(uint8(ResponseRows))
				e.u32(0)       // zero columns
				e.u32(1000000) // but a million rows
				return rawFrame(tagResponse, e.b)
			}(),
			want: errRowArity,
		},
		{
			name: "impossible_row_count",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion)
				e.u8(uint8(ResponseRows))
				e.u32(1) // one column
				e.str("c")
				e.u32(0)       // its OID
				e.u32(1000000) // a million rows in a tiny body
				return rawFrame(tagResponse, e.b)
			}(),
			want: errImpossibleCount,
		},
		{
			name: "bad_error_kind",
			frame: func() []byte {
				var e encbuf
				e.u8(wireVersion)
				e.u8(uint8(ResponseError))
				e.u8(0xff) // invalid ErrorKind
				e.str("boom")
				return rawFrame(tagResponse, e.b)
			}(),
			want: errBadEnum,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeResponse(bytes.NewReader(tc.frame))
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestEncodeRejectsInvalid proves the encoders fail closed on values the wire
// cannot represent, rather than emitting a frame no decoder would accept.
func TestEncodeRejectsInvalid(t *testing.T) {
	tests := []struct {
		name string
		enc  func() error
		want error
	}{
		{
			name: "negative_deadline",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{Deadline: -1})
			},
			want: errNegativeDuration,
		},
		{
			name: "bad_read_policy",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{ReadPolicy: ReadPolicy(9)})
			},
			want: errBadEnum,
		},
		{
			name: "bad_execution_mode",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{ExecutionMode: ExecutionMode(9)})
			},
			want: errBadEnum,
		},
		{
			name: "invalid_param_kind",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{Params: []Param{{Kind: ParamInvalid}}})
			},
			want: errBadEnum,
		},
		{
			name: "invalid_number_param",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{Params: []Param{NumberParam("01")}})
			},
			want: errBadParam,
		},
		{
			name: "invalid_document_param",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{Params: []Param{DocumentParam("{")}})
			},
			want: errBadParam,
		},
		{
			name: "invalid_string_param",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{Params: []Param{StringBytesParam([]byte{0xff})}})
			},
			want: errBadParam,
		},
		{
			name: "parameter_type_count_mismatch",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: "SELECT $1", Params: []Param{NullParam()},
					ParamTypes: []sqldriver.ParamType{
						sqldriver.ParamTypeBool, sqldriver.ParamTypeText,
					},
				})
			},
			want: errBadParameterTypes,
		},
		{
			name: "all_unspecified_parameter_types",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: "SELECT $1", Params: []Param{NullParam()},
					ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeUnspecified},
				})
			},
			want: errBadParameterTypes,
		},
		{
			name: "invalid_parameter_type",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: "SELECT $1", Params: []Param{NullParam()},
					ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeInvalid},
				})
			},
			want: errBadParameterTypes,
		},
		{
			name: "document_parameter_has_scalar_type",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL:        "INSERT INTO docs VALUES ($1)",
					Params:     []Param{DocumentParam(`{"id":"a"}`)},
					ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeOther},
				})
			},
			want: errBadParameterTypes,
		},
		{
			name: "read_fence_on_transaction_command",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					ReadFenceID: testTransactionID(70),
					Transaction: TransactionRequest{
						Operation: TransactionReleaseReadFence,
						ID:        testTransactionID(71), Revision: 1,
					},
				})
			},
			want: errBadTransaction,
		},
		{
			name: "parameter_types_on_transaction_command",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL:        "SELECT $1",
					Params:     []Param{NullParam()},
					ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeBool},
					Transaction: TransactionRequest{
						Operation: TransactionReleaseReadFence,
						ID:        testTransactionID(72), Revision: 1,
					},
				})
			},
			want: errBadTransaction,
		},
		{
			name: "global_index_lookup_with_sql",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: "SELECT 1",
					GlobalIndexLookup: GlobalIndexLookupRequest{
						Relation: []byte("idx"), IndexID: 1, Incarnation: 1,
						KeyTuples: [][]byte{{1}}, LocatorCount: 1,
					},
				})
			},
			want: errBadGlobalIndexLookup,
		},
		{
			name: "malformed_global_index_lookup",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					GlobalIndexLookup: GlobalIndexLookupRequest{
						Relation: []byte("idx"), IndexID: 1, Incarnation: 1,
						KeyTuples: [][]byte{{0}}, LocatorCount: 1,
					},
				})
			},
			want: errBadGlobalIndexLookup,
		},
		{
			name: "candidate_keys_without_sql",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					PrimaryKeyRead: PrimaryKeyReadRequest{
						PrimaryPath: []byte("/id"), Keys: [][]byte{{1, 'a'}},
					},
				})
			},
			want: errBadPrimaryKeyRead,
		},
		{
			name: "candidate_keys_not_strictly_sorted",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: `SELECT id FROM messages`,
					PrimaryKeyRead: PrimaryKeyReadRequest{
						PrimaryPath: []byte("/id"), Keys: [][]byte{{1, 'b'}, {1, 'a'}},
					},
				})
			},
			want: errBadPrimaryKeyRead,
		},
		{
			name: "candidate_keys_relation_without_document_bound",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: `SELECT id FROM messages`,
					PrimaryKeyRead: PrimaryKeyReadRequest{
						Relation: 1, PrimaryPath: []byte("/id"), Keys: [][]byte{{1, 'a'}},
					},
				})
			},
			want: errBadPrimaryKeyRead,
		},
		{
			name: "candidate_keys_document_bound_without_relation",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: `SELECT id FROM messages`,
					PrimaryKeyRead: PrimaryKeyReadRequest{
						MaxDocumentBytes: 1, PrimaryPath: []byte("/id"), Keys: [][]byte{{1, 'a'}},
					},
				})
			},
			want: errBadPrimaryKeyRead,
		},
		{
			name: "candidate_keys_relation_out_of_bounds",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: `SELECT id FROM messages`,
					PrimaryKeyRead: PrimaryKeyReadRequest{
						Relation: replication.MaxRelationID + 1, MaxDocumentBytes: 1,
						PrimaryPath: []byte("/id"), Keys: [][]byte{{1, 'a'}},
					},
				})
			},
			want: errBadPrimaryKeyRead,
		},
		{
			name: "candidate_keys_document_bound_out_of_bounds",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: `SELECT id FROM messages`,
					PrimaryKeyRead: PrimaryKeyReadRequest{
						Relation: 1, MaxDocumentBytes: replication.MaxMutationValueBytes + 1,
						PrimaryPath: []byte("/id"), Keys: [][]byte{{1, 'a'}},
					},
				})
			},
			want: errBadPrimaryKeyRead,
		},
		{
			name: "mutation_capture_requires_read_only_sql",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL:           `DELETE FROM messages WHERE tenant_id = ?`,
					Params:        []Param{StringParam("acme")},
					ExecutionMode: ExecutionReadWrite, MutationCapture: true,
				})
			},
			want: errBadMutationCapture,
		},
		{
			name: "document_scan_requires_explicit_bounds",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					DocumentScan: DocumentScanRequest{Relation: []byte("messages")},
				})
			},
			want: errBadDocumentScan,
		},
		{
			name: "partial_aggregate_requires_read_only_sql",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL:           "SELECT n, COUNT(*) FROM messages GROUP BY n",
					ExecutionMode: ExecutionReadWrite, PartialAggregate: true,
				})
			},
			want: errBadPartialAggregate,
		},
		{
			name: "row_batch_requires_both_limits",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL:      "SELECT id FROM messages",
					RowBatch: RowBatchRequest{BatchRows: 1},
				})
			},
			want: errBadRowBatch,
		},
		{
			name: "row_batch_requires_total_limits",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL:      "SELECT id FROM messages",
					RowBatch: RowBatchRequest{BatchRows: 1, BatchBytes: 64},
				})
			},
			want: errBadRowBatch,
		},
		{
			name: "row_batch_rejects_writes",
			enc: func() error {
				return EncodeRequest(io.Discard, &ShardRequest{
					SQL: "DELETE FROM messages", ExecutionMode: ExecutionReadWrite,
					RowBatch: RowBatchRequest{BatchRows: 1, BatchBytes: 64},
				})
			},
			want: errBadRowBatch,
		},
		{
			name: "row_batch_continuation_rejects_schema",
			enc: func() error {
				return EncodeResponse(io.Discard, &ShardResponse{
					Kind: ResponseRowBatch, Columns: []Column{{Name: "id"}},
					Rows:     [][]Cell{{{Bytes: []byte("1")}}},
					RowBatch: RowBatchReply{Sequence: 1, ColumnCount: 1, Final: true},
				})
			},
			want: errBadRowBatch,
		},
		{
			name: "row_arity_mismatch",
			enc: func() error {
				return EncodeResponse(io.Discard, &ShardResponse{
					Kind:    ResponseRows,
					Columns: []Column{{Name: "a"}, {Name: "b"}},
					Rows:    [][]Cell{{{Bytes: []byte("x")}}}, // one cell, two columns
				})
			},
			want: errRowArity,
		},
		{
			name: "invalid_response_kind",
			enc: func() error {
				return EncodeResponse(io.Discard, &ShardResponse{Kind: ResponseInvalid})
			},
			want: errBadEnum,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.enc(); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRequestParameterTypeDecodeRejectsNonCanonicalMetadata(t *testing.T) {
	request := &ShardRequest{
		SQL: "SELECT $1", Params: []Param{NullParam()},
		ParamTypes: []sqldriver.ParamType{sqldriver.ParamTypeBool},
	}
	for _, test := range []struct {
		name     string
		typeByte byte
	}{
		{name: "invalid", typeByte: byte(sqldriver.ParamTypeInvalid)},
		{name: "all_unspecified", typeByte: byte(sqldriver.ParamTypeUnspecified)},
	} {
		t.Run(test.name, func(t *testing.T) {
			frame := append([]byte(nil), encodeRequest(t, request)...)
			frame[len(frame)-1] = test.typeByte
			if _, err := DecodeRequest(bytes.NewReader(frame)); !errors.Is(err, errBadParameterTypes) {
				t.Fatalf("DecodeRequest = %v, want %v", err, errBadParameterTypes)
			}
		})
	}
}

// TestParamRuntimeValue proves each wire parameter stays byte-native when it
// crosses into sql/driver.
func TestParamRuntimeValue(t *testing.T) {
	if (Param{Kind: ParamInvalid}).Valid() {
		t.Error("invalid parameter kind reported valid")
	}
	if !StringParam("k").Valid() {
		t.Error("string parameter reported invalid")
	}
	if got := NullParam().RuntimeValue(); got != nil {
		t.Errorf("null = %v, want nil", got)
	}
	if got := BoolParam(true).RuntimeValue(); got != true {
		t.Errorf("bool = %v, want true", got)
	}
	if got, ok := NumberParam("5e0").RuntimeValue().(vibejson.RawValue); !ok || !bytes.Equal(got.Bytes(), []byte("5e0")) {
		t.Errorf("number = %v, want vibejson raw number 5e0", NumberParam("5e0").RuntimeValue())
	}
	if got, ok := StringParam("k").RuntimeValue().([]byte); !ok || !bytes.Equal(got, []byte("k")) {
		t.Errorf("string = %v, want byte-native k", got)
	}
	if got, ok := DocumentParam(`{"a":1}`).RuntimeValue().([]byte); !ok || !bytes.Equal(got, []byte(`{"a":1}`)) {
		t.Errorf("document = %v, want JSON bytes", got)
	}

	// Byte constructors and RuntimeValue borrow the exact caller storage; the
	// distributed bridge does not materialize an intermediate string or copy.
	numberBytes := []byte("123.5")
	number := NumberBytesParam(numberBytes).RuntimeValue().(vibejson.RawValue)
	if &number.Bytes()[0] != &numberBytes[0] {
		t.Fatal("number runtime value did not borrow input bytes")
	}
	stringBytes := []byte("tenant")
	stringValue := StringBytesParam(stringBytes).RuntimeValue().([]byte)
	if &stringValue[0] != &stringBytes[0] {
		t.Fatal("string runtime value did not borrow input bytes")
	}
	documentBytes := []byte(`{"tenant":"a"}`)
	document := DocumentBytesParam(documentBytes).RuntimeValue().([]byte)
	if &document[0] != &documentBytes[0] {
		t.Fatal("document runtime value did not borrow input bytes")
	}
}

// TestReuseDistributionTypes is a compile-time-style check that the request
// carries the distribution package's own typed IDs, not private copies.
func TestReuseDistributionTypes(t *testing.T) {
	req := &ShardRequest{
		Distribution:         distribution.DistributionName("d"),
		Shard:                distribution.ShardID("s"),
		AllocationGeneration: distribution.ShardAllocationGeneration(1),
		RoutingVersion:       distribution.RoutingVersion(1),
		OwnershipEpoch:       distribution.OwnershipEpoch(1),
	}
	if req.Distribution != "d" {
		t.Fatal("distribution type mismatch")
	}
}
