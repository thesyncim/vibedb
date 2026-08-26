package raftservice_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3bench"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const rf3EvidenceSeed = uint64(0x5649424544425246)

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
	checkpointUpdates    uint64
	checkpointSyncs      uint64
	checkpointAppliedSum uint64
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
	cluster := newMultiGroupTransactionRF3Cluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := cluster.owners[0].Campaign(ctx, cluster.groups[0].key); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.groups[0].key)
	roundTripper := newMultiGroupRF3RoundTripper(t, cluster)
	executor, err := gateway.NewReplicatedExecutorWithOptions(roundTripper, gateway.ReplicatedExecutorOptions{
		MaxAttempts: 8, AttemptTimeout: 10 * time.Second, LeaderHintCapacity: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	route := cluster.route(0)
	clients := make([]rf3EvidenceClient, config.Clients)
	var minimumApplied uint64
	for index := range clients {
		clients[index] = openRF3EvidenceClient(t, ctx, executor, route,
			cluster.groups[0].bases[leader], uint32(index))
		seed := evidenceValue(uint32(index), clients[index].sequence)
		result := proposeRF3Evidence(t, ctx, executor, route, cluster.groups[0].bases[leader],
			&clients[index], seed)
		minimumApplied = max(minimumApplied, result.Outcome.AppliedIndex)
	}
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.groups[0].key, minimumApplied)

	for ordinal := uint64(0); ordinal < config.Warmup; ordinal++ {
		client := &clients[ordinal%uint64(len(clients))]
		if evidenceOperation(config.Workload, config.Seed, ordinal) == rf3bench.OperationRead {
			readRF3Evidence(t, ctx, executor, route, client.key, minimumApplied)
		} else {
			result := proposeRF3Evidence(t, ctx, executor, route, cluster.groups[0].bases[leader],
				client, evidenceValue(uint32(ordinal%uint64(len(clients))), client.sequence))
			minimumApplied = max(minimumApplied, result.Outcome.AppliedIndex)
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
					result, readErr := executor.ReadPoint(ctx, route, gateway.ReplicatedPointRead{
						Relation: 1, Key: client.key, MinimumApplied: minimumApplied,
						MaxValueBytes: replication.MaxMutationValueBytes, Linearizable: true,
					})
					if readErr != nil {
						t.Errorf("read client=%d ordinal=%d: %v", clientIndex, ordinal+1, readErr)
						return
					}
					sample.Applied, sample.Found = result.Applied, result.Found
					sample.Commit, sample.Checkpoint = result.State.Commit, result.State.CheckpointApplied
					sample.Retries = uint32(result.Retries)
				} else {
					value := evidenceValue(clientIndex, client.sequence)
					command := evidenceCommand(cluster.groups[0].bases[leader], client,
						[]replication.Mutation{{Kind: replication.MutationPut, Key: client.key, Value: value}})
					encoded, appendErr := replication.AppendCommand(nil, command)
					if appendErr != nil {
						t.Errorf("encode client=%d ordinal=%d: %v", clientIndex, ordinal+1, appendErr)
						return
					}
					result, proposeErr := executor.Propose(ctx, route, encoded)
					if proposeErr != nil {
						t.Errorf("write client=%d ordinal=%d: %v", clientIndex, ordinal+1, proposeErr)
						return
					}
					client.sequence++
					sample.Applied = result.Outcome.AppliedIndex
					sample.Commit, sample.Checkpoint = result.State.Commit, result.State.CheckpointApplied
					sample.Retries, sample.PayloadBytes = uint32(result.Retries), uint32(len(encoded))
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
	report := rf3bench.Report{Config: config, Metadata: evidenceMetadata(), Samples: samples,
		Counters: evidenceCounters(before, after, reads, writes, retries, logicalWriteBytes)}
	var encoded bytes.Buffer
	if err := rf3bench.WriteTSV(&encoded, report); err != nil {
		t.Fatal(err)
	}
	return rf3EvidenceRun{report: report, encoded: encoded.Bytes(), readCount: reads, writeCount: writes}
}

func openRF3EvidenceClient(t *testing.T, ctx context.Context, executor *gateway.ReplicatedExecutor,
	route gateway.ReplicatedRoute, base sqldriver.ReplicatedShardStoreIdentity,
	clientIndex uint32,
) rf3EvidenceClient {
	t.Helper()
	var client rf3EvidenceClient
	client.id[0], client.id[1] = 0xe1, byte(clientIndex+1)
	keyJSON := strconv.AppendQuote(nil, "rf3-evidence-"+strconv.FormatUint(uint64(clientIndex), 10))
	var ok bool
	client.key, ok = orderedkey.AppendJSONString(nil, keyJSON, orderedkey.Ascending)
	if !ok {
		t.Fatal("encode evidence key")
	}
	command := evidenceCommand(base, &client, nil)
	command.Kind, command.ClientEpoch, command.ClientSequence = replication.CommandSessionOpen, 0, 1
	command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	command.Fingerprint = sha256.Sum256(append([]byte("rf3-evidence-open"), byte(clientIndex)))
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Propose(ctx, route, encoded)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(result.Completion)
	if err != nil || completion.ResultCode != replicatedstate.ResultApplied || completion.ClientEpoch == 0 {
		t.Fatalf("open completion=%+v err=%v", completion, err)
	}
	client.epoch, client.sequence = completion.ClientEpoch, 2
	return client
}

func evidenceCommand(base sqldriver.ReplicatedShardStoreIdentity, client *rf3EvidenceClient,
	mutations []replication.Mutation,
) replication.Command {
	command := rf3Command(base, replication.CommandMutationBatch, client.epoch, client.sequence, mutations)
	command.ClientID = client.id
	var identity [25]byte
	copy(identity[:16], client.id[:])
	identity[16] = byte(command.Kind)
	for offset := 0; offset < 8; offset++ {
		identity[17+offset] = byte(client.sequence >> (8 * offset))
	}
	command.Fingerprint = sha256.Sum256(identity[:])
	return command
}

func proposeRF3Evidence(t *testing.T, ctx context.Context, executor *gateway.ReplicatedExecutor,
	route gateway.ReplicatedRoute, base sqldriver.ReplicatedShardStoreIdentity,
	client *rf3EvidenceClient, value []byte,
) gateway.ReplicatedResult {
	t.Helper()
	command := evidenceCommand(base, client,
		[]replication.Mutation{{Kind: replication.MutationPut, Key: client.key, Value: value}})
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Propose(ctx, route, encoded)
	if err != nil {
		t.Fatal(err)
	}
	client.sequence++
	return result
}

func readRF3Evidence(t *testing.T, ctx context.Context, executor *gateway.ReplicatedExecutor,
	route gateway.ReplicatedRoute, key []byte, minimumApplied uint64,
) {
	t.Helper()
	result, err := executor.ReadPoint(ctx, route, gateway.ReplicatedPointRead{
		Relation: 1, Key: key, MinimumApplied: minimumApplied,
		MaxValueBytes: replication.MaxMutationValueBytes, Linearizable: true,
	})
	if err != nil || !result.Found {
		t.Fatalf("warmup read found=%t err=%v", result.Found, err)
	}
}

func evidenceValue(client uint32, sequence uint64) []byte {
	raw := append([]byte(`{"client":`), strconv.FormatUint(uint64(client), 10)...)
	raw = append(raw, []byte(`,"sequence":`)...)
	raw = strconv.AppendUint(raw, sequence, 10)
	raw = append(raw, '}')
	canonical, err := vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		panic(err)
	}
	return canonical
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
	for local := 0; local < multiGroupRF3Voters; local++ {
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
		counter("raft", "checkpoint_applied_sum", before.checkpointAppliedSum, after.checkpointAppliedSum),
		counter("raft", "checkpoint_syncs", before.checkpointSyncs, after.checkpointSyncs),
		counter("raft", "checkpoint_updates", before.checkpointUpdates, after.checkpointUpdates),
		counter("storage", "committed_batches", before.storageBatches, after.storageBatches),
		counter("storage", "device_bytes", before.storageDeviceBytes, after.storageDeviceBytes),
		counter("storage", "file_end", before.storageFileEnd, after.storageFileEnd),
		counter("storage", "page_reads", before.storagePageReads, after.storagePageReads),
		counter("storage", "read_bytes", before.storageReadBytes, after.storageReadBytes),
		counter("workload", "logical_write_bytes", 0, logicalWriteBytes),
		counter("workload", "reads", 0, reads),
		counter("workload", "writes", 0, writes),
	}
}

func evidenceMetadata() []rf3bench.Metadata {
	metadata := []rf3bench.Metadata{
		{Key: []byte("counter_cut"), Value: []byte("followers-applied")},
		{Key: []byte("go_version"), Value: []byte(runtime.Version())},
		{Key: []byte("goarch"), Value: []byte(runtime.GOARCH)},
		{Key: []byte("goos"), Value: []byte(runtime.GOOS)},
		{Key: []byte("vcs_modified"), Value: []byte("unknown")},
		{Key: []byte("vcs_revision"), Value: []byte("unknown")},
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				metadata[5].Value = []byte(setting.Value)
			case "vcs.modified":
				metadata[4].Value = []byte(setting.Value)
			}
		}
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
