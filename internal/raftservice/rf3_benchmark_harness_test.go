package raftservice_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/rf3bench"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const (
	rf3EvidenceSeed           = uint64(0x5649424544425246)
	rf3EvidenceMaxValueBytes  = 128
	rf3EvidenceRouteAttempts  = 8
	rf3EvidenceReadAttempts   = 64
	rf3EvidenceReadBackoffCap = 32 * time.Millisecond
)

type rf3EvidenceClient struct {
	id       replication.ID128
	epoch    uint64
	sequence uint64
	key      []byte
}

type rf3EvidenceCut struct {
	transportSentBytes   uint64
	transportSentFrames  uint64
	transportDials       uint64
	transportFailures    uint64
	inboundAccepted      uint64
	inboundRejected      uint64
	inboundFailed        uint64
	storageDeviceBytes   uint64
	storageFileEnd       uint64
	storageReadBytes     uint64
	storagePageReads     uint64
	storageBatches       uint64
	storageAllocated     uint64
	storageApparent      uint64
	storageFiles         uint64
	checkpointUpdates    uint64
	checkpointSyncs      uint64
	checkpointCount      uint64
	checkpointExplicit   uint64
	checkpointPeriodic   uint64
	checkpointPressure   uint64
	checkpointMarker     uint64
	checkpointAppliedSum uint64
	proposalCommands     uint64
	proposalBatches      uint64
	readyPersisted       uint64
	walEntries           uint64
	walLiveBytes         uint64
	walSyncs             uint64
	appliedEntries       uint64
	applyBatches         uint64
	completionBatches    uint64
	completionEntries    uint64
	completionComplete   uint64
}

type rf3EvidenceRun struct {
	report     rf3bench.Report
	encoded    []byte
	readCount  uint64
	writeCount uint64
}

func TestRF3EvidenceHarnessSmoke(t *testing.T) {
	run := runRF3Evidence(t, rf3bench.Config{
		Clients: 2, Operations: 8, Warmup: 2, Seed: rf3EvidenceSeed,
		Workload: rf3bench.WorkloadMixed,
	})
	if run.readCount == 0 || run.writeCount == 0 {
		t.Fatalf("mixed run reads=%d writes=%d", run.readCount, run.writeCount)
	}
	for _, required := range [][]byte{
		[]byte("meta\tdurability\tpower-safe\n"),
		[]byte("meta\treplicas\t3\n"),
		[]byte("summary_header\toperation\tsamples\tp50_ns\tp95_ns\tp99_ns\tp99.9_ns\tmax_ns\n"),
		[]byte("summary\tread\t"), []byte("summary\twrite\t"),
		[]byte("counter\tnetwork\tsent_bytes\t"),
		[]byte("counter\tstorage\tdevice_bytes\t"),
	} {
		if !bytes.Contains(run.encoded, required) {
			t.Fatalf("canonical evidence omits %q:\n%s", required, run.encoded)
		}
	}
	var second bytes.Buffer
	if err := rf3bench.WriteTSV(&second, run.report); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(run.encoded, second.Bytes()) {
		t.Fatal("same detached run emitted different evidence bytes")
	}
}

// TestRF3EvidenceMatrix is deliberately opt-in. It emits raw evidence rather
// than claims. The standard client matrix is 1,8,32; callers may select one
// member with VIBEDB_RF3_CLIENTS to shard qualification across machines.
func TestRF3EvidenceMatrix(t *testing.T) {
	if os.Getenv("VIBEDB_RF3_BENCH") != "1" {
		t.Skip("set VIBEDB_RF3_BENCH=1 to run the RF3 evidence matrix")
	}
	output := os.Getenv("VIBEDB_RF3_OUTPUT")
	if output == "" {
		t.Fatal("VIBEDB_RF3_OUTPUT must name an existing output directory")
	}
	clients := []uint32{1, 8, 32}
	if raw := os.Getenv("VIBEDB_RF3_CLIENTS"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 32)
		if err != nil || (parsed != 1 && parsed != 8 && parsed != 32) {
			t.Fatalf("VIBEDB_RF3_CLIENTS=%q, want 1, 8, or 32", raw)
		}
		clients = []uint32{uint32(parsed)}
	}
	operations := evidenceEnvU64(t, "VIBEDB_RF3_OPERATIONS", 1000, rf3bench.MaximumOperations)
	warmup := evidenceEnvU64(t, "VIBEDB_RF3_WARMUP", 64, rf3bench.MaximumOperations)
	workload := evidenceWorkload(t, os.Getenv("VIBEDB_RF3_WORKLOAD"))
	for _, clientCount := range clients {
		t.Run(strconv.FormatUint(uint64(clientCount), 10)+"-clients", func(t *testing.T) {
			run := runRF3Evidence(t, rf3bench.Config{Clients: clientCount,
				Operations: operations, Warmup: warmup, Seed: rf3EvidenceSeed, Workload: workload})
			name := "rf3-" + string(workloadBytes(workload)) + "-clients-" +
				strconv.FormatUint(uint64(clientCount), 10) + ".tsv"
			file, err := os.OpenFile(filepath.Join(output, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = file.Write(run.encoded); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err = file.Sync(); err != nil {
				_ = file.Close()
				t.Fatal(err)
			}
			if err = file.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func runRF3Evidence(t *testing.T, config rf3bench.Config) rf3EvidenceRun {
	t.Helper()
	// Evidence uses the production WAL geometry. The small shared RF3 fixture
	// intentionally reaches generation pressure after 12k entries and is useful
	// for rollover tests, but it turns a sustained throughput run into a
	// compaction benchmark at an artificial boundary.
	cluster := newMultiGroupRF3ClusterWithWALOptions(t, multiGroupRF3Groups, raftstore.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ctx, err := serviceauthz.WithAuthority(ctx, serviceauthz.Authority{
		Node: rafttransport.NodeID{0xe1}, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.owners[0].Campaign(ctx, cluster.groups[0].key); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.groups[0].key)
	roundTripper := newMultiGroupRF3RoundTripper(t, cluster)
	executor, err := gateway.NewReplicatedExecutorWithOptions(roundTripper, gateway.ReplicatedExecutorOptions{
		MaxAttempts: rf3EvidenceRouteAttempts, AttemptTimeout: 10 * time.Second, LeaderHintCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	route := cluster.route(0)
	readLimit := uint32(cluster.groups[0].bases[leader].UserLimits.MaxDocumentBytes)
	clients := make([]rf3EvidenceClient, config.Clients)
	var minimumApplied uint64
	for index := range clients {
		clients[index] = openRF3EvidenceClient(t, uint32(index))
		seed := evidenceValue(uint32(index), clients[index].sequence)
		result := proposeRF3Evidence(t, ctx, executor, route, &clients[index], seed)
		minimumApplied = max(minimumApplied, result.Applied)
	}
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.groups[0].key, minimumApplied)

	for ordinal := uint64(0); ordinal < config.Warmup; ordinal++ {
		client := &clients[ordinal%uint64(len(clients))]
		if evidenceOperation(config.Workload, config.Seed, ordinal) == rf3bench.OperationRead {
			readRF3Evidence(t, ctx, executor, route, client.key, minimumApplied, readLimit)
		} else {
			result := proposeRF3Evidence(t, ctx, executor, route, client,
				evidenceValue(uint32(ordinal%uint64(len(clients))), client.sequence))
			minimumApplied = max(minimumApplied, result.Applied)
		}
	}
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.groups[0].key, minimumApplied)
	before := captureRF3EvidenceCut(t, cluster)

	samples := make([]rf3bench.Sample, config.Operations)
	start := make(chan struct{})
	var workers sync.WaitGroup
	var maximumApplied atomic.Uint64
	maximumApplied.Store(minimumApplied)
	for clientIndex := uint32(0); clientIndex < config.Clients; clientIndex++ {
		workers.Add(1)
		go func(clientIndex uint32) {
			defer workers.Done()
			client := &clients[clientIndex]
			<-start
			var clientOperation uint64
			for ordinal := uint64(clientIndex); ordinal < config.Operations; ordinal += uint64(config.Clients) {
				clientOperation++
				operation := evidenceOperation(config.Workload, config.Seed, ordinal+config.Warmup)
				started := time.Now()
				sample := rf3bench.Sample{Ordinal: ordinal + 1, Client: clientIndex,
					ClientSequence: clientOperation, Operation: operation,
					PayloadBytes: uint32(len(client.key))}
				if operation == rf3bench.OperationRead {
					result, readErr := readRF3EvidencePoint(ctx, executor, route,
						evidencePointRead(client.key, minimumApplied, readLimit))
					if readErr != nil {
						t.Errorf("read client=%d ordinal=%d: %v", clientIndex, ordinal+1, readErr)
						return
					}
					sample.Applied, sample.Found = result.Applied, result.Found
					sample.Commit, sample.Checkpoint = result.State.Commit, result.State.CheckpointApplied
					sample.Retries = uint32(result.Retries)
				} else {
					value := evidenceValue(clientIndex, client.sequence)
					result, proposeErr := directRF3Evidence(ctx, executor, route, client, value)
					if proposeErr != nil {
						t.Errorf("write client=%d ordinal=%d: %v", clientIndex, ordinal+1, proposeErr)
						return
					}
					sample.Applied = result.Applied
					sample.Commit, sample.Checkpoint = result.Commit, result.Checkpoint
					sample.Retries = uint32(result.Retries)
					sample.PayloadBytes = uint32(len(client.key) + len(value))
					atomicMaximum(&maximumApplied, sample.Applied)
				}
				sample.LatencyNS = uint64(max(time.Since(started), time.Nanosecond))
				samples[ordinal] = sample
			}
		}(clientIndex)
	}
	windowStart := time.Now()
	close(start)
	workers.Wait()
	elapsed := time.Since(windowStart)
	if t.Failed() {
		t.FailNow()
	}
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.groups[0].key, maximumApplied.Load())
	after := captureRF3EvidenceCut(t, cluster)
	var reads, writes, retries, logicalWriteBytes uint64
	for _, sample := range samples {
		retries += uint64(sample.Retries)
		if sample.Operation == rf3bench.OperationRead {
			reads++
		} else {
			writes++
			logicalWriteBytes += uint64(sample.PayloadBytes)
		}
	}
	config.ElapsedNS = uint64(max(elapsed, time.Nanosecond))
	report := rf3bench.Report{Config: config, Metadata: evidenceMetadata(readLimit), Samples: samples,
		Counters: evidenceCounters(before, after, reads, writes, retries, logicalWriteBytes)}
	var encoded bytes.Buffer
	if err := rf3bench.WriteTSV(&encoded, report); err != nil {
		t.Fatal(err)
	}
	return rf3EvidenceRun{report: report, encoded: encoded.Bytes(), readCount: reads, writeCount: writes}
}

func openRF3EvidenceClient(t *testing.T, clientIndex uint32) rf3EvidenceClient {
	t.Helper()
	client := rf3EvidenceClient{epoch: 1, sequence: 1}
	client.id[0], client.id[1] = 0xe1, byte(clientIndex+1)
	keyJSON := strconv.AppendQuote(nil, "rf3-evidence-"+strconv.FormatUint(uint64(clientIndex), 10))
	var ok bool
	client.key, ok = orderedkey.AppendJSONString(nil, keyJSON, orderedkey.Ascending)
	if !ok {
		t.Fatal("encode evidence key")
	}
	return client
}

func proposeRF3Evidence(t *testing.T, ctx context.Context, executor *gateway.ReplicatedExecutor,
	route gateway.ReplicatedRoute, client *rf3EvidenceClient, value []byte,
) gateway.ReplicatedDirectMutationResult {
	t.Helper()
	result, err := directRF3Evidence(ctx, executor, route, client, value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func directRF3Evidence(ctx context.Context, executor *gateway.ReplicatedExecutor,
	route gateway.ReplicatedRoute, client *rf3EvidenceClient, value []byte,
) (gateway.ReplicatedDirectMutationResult, error) {
	tenant := []byte("rf3-evidence")
	var request requestledger.RequestID
	request[0] = client.id[1]
	for offset := 0; offset < 8; offset++ {
		request[8+offset] = byte(client.sequence >> (8 * offset))
	}
	var lane requestledger.IssuerLane
	lane[0] = client.id[1]
	direct := gateway.ReplicatedDirectMutation{
		Key: requestledger.RequestKey{
			Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID(client.id),
			Request: request, TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
			IssuerEpoch: client.epoch, IssuerSequence: client.sequence, IssuerLane: lane,
		},
		RequestDigest: replication.Digest(sha256.Sum256(value)), Tenant: tenant,
		Target: gateway.ReplicatedTransactionTarget{
			Route: route, BucketBits: 8,
			IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			Batches: []replication.RelationMutationBatch{{
				Relation: 1, Mutations: []replication.Mutation{{
					Kind: replication.MutationPut, Key: client.key, Value: value,
				}},
			}},
		},
	}
	result, err := executor.DirectMutate(ctx, direct)
	if err == nil {
		client.sequence++
	}
	return result, err
}

func readRF3Evidence(t *testing.T, ctx context.Context, executor *gateway.ReplicatedExecutor,
	route gateway.ReplicatedRoute, key []byte, minimumApplied uint64, readLimit uint32,
) {
	t.Helper()
	result, err := readRF3EvidencePoint(ctx, executor, route, evidencePointRead(key, minimumApplied, readLimit))
	if err != nil || !result.Found {
		t.Fatalf("warmup read found=%t err=%v", result.Found, err)
	}
}

func evidencePointRead(key []byte, minimumApplied uint64, readLimit uint32) gateway.ReplicatedPointRead {
	// Point-read admission uses the relation's frozen maximum document size,
	// not the smaller current evidence value. Keep production admission intact.
	return gateway.ReplicatedPointRead{Relation: 1, Key: key, MinimumApplied: minimumApplied,
		MaxValueBytes: readLimit, Linearizable: true}
}

type rf3EvidencePointReader interface {
	ReadPoint(context.Context, gateway.ReplicatedRoute, gateway.ReplicatedPointRead) (gateway.ReplicatedPointResult, error)
}

func readRF3EvidencePoint(ctx context.Context, reader rf3EvidencePointReader,
	route gateway.ReplicatedRoute, read gateway.ReplicatedPointRead,
) (gateway.ReplicatedPointResult, error) {
	// The fixture retains its fixed owner memory budget. At higher client
	// counts a full-size response reservation can meet backpressure even for a
	// small value. Bounded client retries include their waiting time in the
	// caller's measured latency; no owner queue or resource limit is enlarged.
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return gateway.ReplicatedPointResult{}, err
		}
		result, err := reader.ReadPoint(ctx, route, read)
		if err == nil {
			result.Retries += attempt * rf3EvidenceRouteAttempts
			return result, nil
		}
		// Only an exhausted routing attempt carrying an admission refusal is
		// retryable here. Fence, schema, response-bound and intent failures stay
		// visible instead of being turned into a successful benchmark sample.
		if attempt+1 == rf3EvidenceReadAttempts || !errors.Is(err, gateway.ErrReplicatedLeader) ||
			!errors.Is(err, raftmodel.ErrAdmissionBound) {
			return gateway.ReplicatedPointResult{}, err
		}
		// A bounded 1.9s pressure window covers the fixed owner budget at 32
		// clients without exponentially growing waits or enlarging that budget.
		// Every round and its wait remain part of the sample's measured latency.
		timer := time.NewTimer(rf3EvidenceReadBackoff(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return gateway.ReplicatedPointResult{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func rf3EvidenceReadBackoff(attempt int) time.Duration {
	return min(time.Millisecond<<min(attempt, 5), rf3EvidenceReadBackoffCap)
}

type rf3EvidencePointReadFunc func(context.Context, gateway.ReplicatedRoute, gateway.ReplicatedPointRead) (gateway.ReplicatedPointResult, error)

func (f rf3EvidencePointReadFunc) ReadPoint(ctx context.Context, route gateway.ReplicatedRoute,
	read gateway.ReplicatedPointRead,
) (gateway.ReplicatedPointResult, error) {
	return f(ctx, route, read)
}

func TestRF3EvidenceReadUsesFrozenRelationBound(t *testing.T) {
	const frozenLimit = 4 << 20
	key := []byte("evidence-key")
	read := evidencePointRead(key, 37, frozenLimit)
	if read.MaxValueBytes != frozenLimit || read.MaxValueBytes <= rf3EvidenceMaxValueBytes ||
		read.Relation != 1 || read.MinimumApplied != 37 || !read.Linearizable || !bytes.Equal(read.Key, key) {
		t.Fatalf("read did not retain frozen response and consistency contract: %+v", read)
	}
	metadata := evidenceMetadata(frozenLimit)
	for i := 1; i < len(metadata); i++ {
		if bytes.Compare(metadata[i-1].Key, metadata[i].Key) >= 0 {
			t.Fatal("evidence metadata lost canonical key order")
		}
	}
}

func TestRF3EvidenceAdmissionBackoffIsBounded(t *testing.T) {
	var total time.Duration
	for attempt := 0; attempt < rf3EvidenceReadAttempts-1; attempt++ {
		delay := rf3EvidenceReadBackoff(attempt)
		if delay <= 0 || delay > rf3EvidenceReadBackoffCap {
			t.Fatalf("attempt %d delay=%s", attempt, delay)
		}
		total += delay
	}
	if total != 1887*time.Millisecond || rf3EvidenceReadBackoff(1000) != rf3EvidenceReadBackoffCap {
		t.Fatalf("unexpected pressure retry window: %s", total)
	}
}

func TestRF3EvidenceReadAdmissionRetryBound(t *testing.T) {
	pressure := errors.Join(gateway.ErrReplicatedLeader, raftmodel.ErrAdmissionBound)
	for _, test := range []struct {
		name     string
		failures int
		err      error
		want     int
	}{
		{"pressure-then-success", 2, pressure, 3},
		{"pressure-exhausted", rf3EvidenceReadAttempts, pressure, rf3EvidenceReadAttempts},
		{"response-bound", 1, gateway.ErrReplicatedReadBufferBound, 1},
		{"active-intent", 1, gateway.ErrReplicatedReadIntentActive, 1},
		{"other-route-failure", 1, gateway.ErrReplicatedLeader, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			read := evidencePointRead([]byte("key"), 7, 4<<20)
			calls := 0
			reader := rf3EvidencePointReadFunc(func(_ context.Context, _ gateway.ReplicatedRoute,
				got gateway.ReplicatedPointRead,
			) (gateway.ReplicatedPointResult, error) {
				calls++
				if got.MinimumApplied != read.MinimumApplied || got.MaxValueBytes != read.MaxValueBytes ||
					got.Relation != read.Relation || got.Linearizable != read.Linearizable || !bytes.Equal(got.Key, read.Key) {
					t.Fatal("retry changed read contract")
				}
				if calls <= test.failures {
					return gateway.ReplicatedPointResult{}, test.err
				}
				return gateway.ReplicatedPointResult{Found: true, Applied: 9, Retries: 1}, nil
			})
			result, err := readRF3EvidencePoint(context.Background(), reader, gateway.ReplicatedRoute{}, read)
			if calls != test.want {
				t.Fatalf("attempts=%d want=%d", calls, test.want)
			}
			if test.failures < test.want {
				if err != nil || !result.Found || result.Applied != 9 || result.Retries != 1+test.failures*rf3EvidenceRouteAttempts {
					t.Fatalf("success result=%+v err=%v", result, err)
				}
			} else if err != test.err {
				t.Fatalf("terminal error=%v want=%v", err, test.err)
			}
		})
	}
}

func TestRF3EvidenceReadAdmissionRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	reader := rf3EvidencePointReadFunc(func(context.Context, gateway.ReplicatedRoute,
		gateway.ReplicatedPointRead,
	) (gateway.ReplicatedPointResult, error) {
		calls++
		cancel()
		return gateway.ReplicatedPointResult{}, errors.Join(gateway.ErrReplicatedLeader, raftmodel.ErrAdmissionBound)
	})
	if _, err := readRF3EvidencePoint(ctx, reader, gateway.ReplicatedRoute{}, evidencePointRead([]byte("key"), 7, 4<<20)); !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancelled read calls=%d err=%v", calls, err)
	}
}

func evidenceValue(client uint32, sequence uint64) []byte {
	raw := strconv.AppendUint([]byte(`{"id":"rf3-evidence-`), uint64(client), 10)
	raw = append(raw, `","client":`...)
	raw = strconv.AppendUint(raw, uint64(client), 10)
	raw = append(raw, []byte(`,"sequence":`)...)
	raw = strconv.AppendUint(raw, sequence, 10)
	raw = append(raw, '}')
	canonical, err := vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		panic(err)
	}
	return canonical
}

func TestRF3EvidenceValueIncludesMatchingPrimaryKey(t *testing.T) {
	want := []byte(`{"client":7,"id":"rf3-evidence-7","sequence":11}`)
	if got := evidenceValue(7, 11); !bytes.Equal(got, want) {
		t.Fatalf("evidence document = %s, want %s", got, want)
	}
	if got := evidenceValue(^uint32(0), ^uint64(0)); len(got) > rf3EvidenceMaxValueBytes {
		t.Fatalf("maximum evidence document needs %d bytes, bound is %d", len(got), rf3EvidenceMaxValueBytes)
	}
}

func evidenceOperation(workload rf3bench.Workload, seed, ordinal uint64) rf3bench.Operation {
	switch workload {
	case rf3bench.WorkloadRead:
		return rf3bench.OperationRead
	case rf3bench.WorkloadWrite:
		return rf3bench.OperationWrite
	default:
		mixed := seed + ordinal*0x9e3779b97f4a7c15
		mixed ^= mixed >> 30
		mixed *= 0xbf58476d1ce4e5b9
		mixed ^= mixed >> 27
		if mixed&1 == 0 {
			return rf3bench.OperationRead
		}
		return rf3bench.OperationWrite
	}
}

func captureRF3EvidenceCut(t *testing.T, cluster *multiGroupTransactionRF3Cluster) rf3EvidenceCut {
	t.Helper()
	var cut rf3EvidenceCut
	storage, err := rf3bench.MeasureFootprint(cluster.groups[0].storageRoots[:]...)
	if err != nil {
		t.Fatal(err)
	}
	cut.storageAllocated = storage.AllocatedBytes
	cut.storageApparent = storage.ApparentBytes
	cut.storageFiles = storage.Files
	for local := 0; local < multiGroupRF3Voters; local++ {
		_, progress, found := cluster.progress[local].GroupProgressMetrics(cluster.groups[0].key)
		if !found {
			t.Fatal("RF3 progress metrics omitted benchmark group")
		}
		cut.proposalCommands += progress.ProposalCommands
		cut.proposalBatches += progress.ProposalBatches
		cut.readyPersisted += progress.ReadyPersisted
		cut.appliedEntries += progress.AppliedEntries
		cut.applyBatches += progress.ApplyBatches
		wal := cluster.groups[0].wals[local].Metrics()
		cut.walEntries += wal.Entries
		cut.walLiveBytes += wal.LiveBytes
		cut.walSyncs += wal.Syncs
		inbound := cluster.peers[local].InboundStats()
		cut.inboundAccepted += inbound.Accepted
		cut.inboundRejected += inbound.Rejected
		cut.inboundFailed += inbound.Failed
		for remote := 0; remote < multiGroupRF3Voters; remote++ {
			if local == remote {
				continue
			}
			var node rafttransport.NodeID
			node[0] = byte(remote + 1)
			stats, err := cluster.peers[local].TransportStats(node)
			if err != nil {
				t.Fatal(err)
			}
			cut.transportSentBytes += stats.SentBytes
			cut.transportSentFrames += stats.SentFrames
			cut.transportDials += stats.DialAttempts
			cut.transportFailures += stats.DialFailures + stats.WriteFailures
		}
		resources, err := cluster.groups[0].reads[local].ResourceStats()
		if err != nil {
			t.Fatal(err)
		}
		appendResource := func(stats durable.Stats) {
			cut.storageDeviceBytes += stats.DeviceBytes
			cut.storageFileEnd += stats.FileEnd
			cut.storageReadBytes += stats.ReadBytes
			cut.storagePageReads += stats.PageReads
			cut.storageBatches += stats.CommittedBatches
		}
		appendResource(resources.System)
		appendResource(resources.Capture)
		for relation := uint16(0); relation < resources.RelationCount; relation++ {
			appendResource(resources.Relations[relation])
		}
		durability, err := cluster.groups[0].reads[local].DurabilityStats()
		if err != nil {
			t.Fatal(err)
		}
		cut.checkpointUpdates += durability.Updates
		cut.checkpointSyncs += durability.JournalSyncs + durability.CertificateSyncs +
			durability.MarkerSyncs
		cut.checkpointAppliedSum += durability.CheckpointAppliedIndex
		cut.checkpointCount += durability.Checkpoints
		cut.checkpointExplicit += durability.ExplicitCheckpoints
		cut.checkpointPeriodic += durability.PeriodicCheckpoints
		cut.checkpointPressure += durability.PressureCheckpoints
		cut.checkpointMarker += durability.MarkerCheckpoints
		completion := cluster.groups[0].reads[local].BatchCompletionStats()
		cut.completionBatches += completion.Batches
		cut.completionEntries += completion.Entries
		cut.completionComplete += completion.CompleteBatches
	}
	return cut
}

func evidenceCounters(before, after rf3EvidenceCut, reads, writes, retries, logicalWriteBytes uint64) []rf3bench.Counter {
	counter := func(scope, name string, first, last uint64) rf3bench.Counter {
		return rf3bench.Counter{Scope: []byte(scope), Name: []byte(name), Before: first, After: last}
	}
	return []rf3bench.Counter{
		counter("gateway", "retries", 0, retries),
		counter("network", "dial_attempts", before.transportDials, after.transportDials),
		counter("network", "failures", before.transportFailures, after.transportFailures),
		counter("network", "inbound_accepted", before.inboundAccepted, after.inboundAccepted),
		counter("network", "inbound_failed", before.inboundFailed, after.inboundFailed),
		counter("network", "inbound_rejected", before.inboundRejected, after.inboundRejected),
		counter("network", "sent_bytes", before.transportSentBytes, after.transportSentBytes),
		counter("network", "sent_frames", before.transportSentFrames, after.transportSentFrames),
		counter("raft", "applied_entries", before.appliedEntries, after.appliedEntries),
		counter("raft", "apply_batches", before.applyBatches, after.applyBatches),
		counter("raft", "checkpoint_applied_sum", before.checkpointAppliedSum, after.checkpointAppliedSum),
		counter("raft", "checkpoint_count", before.checkpointCount, after.checkpointCount),
		counter("raft", "checkpoint_explicit", before.checkpointExplicit, after.checkpointExplicit),
		counter("raft", "checkpoint_marker", before.checkpointMarker, after.checkpointMarker),
		counter("raft", "checkpoint_periodic", before.checkpointPeriodic, after.checkpointPeriodic),
		counter("raft", "checkpoint_pressure", before.checkpointPressure, after.checkpointPressure),
		counter("raft", "checkpoint_syncs", before.checkpointSyncs, after.checkpointSyncs),
		counter("raft", "checkpoint_updates", before.checkpointUpdates, after.checkpointUpdates),
		counter("raft", "completion_batches", before.completionBatches, after.completionBatches),
		counter("raft", "completion_complete_batches", before.completionComplete, after.completionComplete),
		counter("raft", "completion_entries", before.completionEntries, after.completionEntries),
		counter("raft", "proposal_batches", before.proposalBatches, after.proposalBatches),
		counter("raft", "proposal_commands", before.proposalCommands, after.proposalCommands),
		counter("raft", "ready_persisted", before.readyPersisted, after.readyPersisted),
		counter("raft", "wal_entries", before.walEntries, after.walEntries),
		counter("raft", "wal_live_bytes", before.walLiveBytes, after.walLiveBytes),
		counter("raft", "wal_syncs", before.walSyncs, after.walSyncs),
		counter("storage", "allocated_bytes_after", 0, after.storageAllocated),
		counter("storage", "allocated_bytes_before", 0, before.storageAllocated),
		counter("storage", "apparent_bytes_after", 0, after.storageApparent),
		counter("storage", "apparent_bytes_before", 0, before.storageApparent),
		counter("storage", "committed_batches", before.storageBatches, after.storageBatches),
		counter("storage", "device_bytes", before.storageDeviceBytes, after.storageDeviceBytes),
		counter("storage", "file_count_after", 0, after.storageFiles),
		counter("storage", "file_count_before", 0, before.storageFiles),
		// FileEnd is a current physical gauge and may fall when a checkpoint
		// reuses/truncates an unreachable suffix. Encode the detached end state
		// from zero instead of misrepresenting it as a monotonic delta counter.
		counter("storage", "file_end", 0, after.storageFileEnd),
		counter("storage", "page_reads", before.storagePageReads, after.storagePageReads),
		counter("storage", "read_bytes", before.storageReadBytes, after.storageReadBytes),
		counter("workload", "logical_write_bytes", 0, logicalWriteBytes),
		counter("workload", "reads", 0, reads),
		counter("workload", "writes", 0, writes),
	}
}

func evidenceMetadata(readLimit uint32) []rf3bench.Metadata {
	revision, modified := rf3bench.BuildProvenance()
	metadata := []rf3bench.Metadata{
		{Key: []byte("counter_cut"), Value: []byte("followers-applied")},
		{Key: []byte("go_version"), Value: []byte(runtime.Version())},
		{Key: []byte("goarch"), Value: []byte(runtime.GOARCH)},
		{Key: []byte("goos"), Value: []byte(runtime.GOOS)},
		{Key: []byte("payload_bytes"), Value: []byte("logical-key-value")},
		{Key: []byte("read_admission"), Value: []byte("owner-bounded-retry-wait-in-latency")},
		{Key: []byte("read_max_value_bytes"), Value: strconv.AppendUint(nil, uint64(readLimit), 10)},
		{Key: []byte("read_retry_backoff_cap_ns"), Value: strconv.AppendUint(nil, uint64(rf3EvidenceReadBackoffCap), 10)},
		{Key: []byte("read_retry_rounds"), Value: strconv.AppendUint(nil, rf3EvidenceReadAttempts, 10)},
		{Key: []byte("vcs_modified"), Value: []byte(modified)},
		{Key: []byte("vcs_revision"), Value: []byte(revision)},
		{Key: []byte("write_path"), Value: []byte("single-participant-one-proposal")},
	}
	return metadata
}

func evidenceEnvU64(t *testing.T, name string, fallback, limit uint64) uint64 {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || parsed == 0 || parsed > limit {
		t.Fatalf("%s=%q is outside 1..%d", name, raw, limit)
	}
	return parsed
}

func evidenceWorkload(t *testing.T, raw string) rf3bench.Workload {
	t.Helper()
	switch raw {
	case "", "mixed":
		return rf3bench.WorkloadMixed
	case "read":
		return rf3bench.WorkloadRead
	case "write":
		return rf3bench.WorkloadWrite
	default:
		t.Fatalf("VIBEDB_RF3_WORKLOAD=%q, want read, write, or mixed", raw)
		return 0
	}
}

func workloadBytes(workload rf3bench.Workload) []byte {
	switch workload {
	case rf3bench.WorkloadRead:
		return []byte("read")
	case rf3bench.WorkloadWrite:
		return []byte("write")
	default:
		return []byte("mixed")
	}
}

func atomicMaximum(value *atomic.Uint64, candidate uint64) {
	for current := value.Load(); candidate > current; current = value.Load() {
		if value.CompareAndSwap(current, candidate) {
			return
		}
	}
}
