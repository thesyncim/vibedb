// Command crdb-rf3-kvbench runs the same closed-loop, fixed-key RF3 write
// workload as TestRF3EvidenceMatrix against CockroachDB's public SQL API.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/rf3bench"
)

const (
	evidenceSeed = uint64(0x5649424544425246)
	putName      = "rf3_evidence_put"
	putSQL       = "UPSERT INTO rf3_kv_evidence (k, v) VALUES ($1, $2)"
)

type config struct {
	url          string
	output       string
	storageRoots []string
	clients      uint32
	operations   uint64
	warmup       uint64
	timeout      time.Duration
}

type clientState struct {
	connection *pgx.Conn
	key        []byte
	sequence   uint64
}

func main() {
	if err := run(context.Background(), parseFlags()); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseFlags() config {
	var clients uint64
	var roots string
	var result config
	flag.StringVar(&result.url, "url", "postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable", "CockroachDB SQL URL")
	flag.StringVar(&result.output, "output", "", "new canonical TSV output file")
	flag.StringVar(&roots, "storage-roots", "", "comma-separated CockroachDB store roots")
	flag.Uint64Var(&clients, "clients", 32, "closed-loop clients")
	flag.Uint64Var(&result.operations, "operations", 100000, "measured writes")
	flag.Uint64Var(&result.warmup, "warmup", 128, "unmeasured warmup writes")
	flag.DurationVar(&result.timeout, "timeout", 15*time.Minute, "whole-run timeout")
	flag.Parse()
	if clients <= uint64(^uint32(0)) {
		result.clients = uint32(clients)
	}
	for _, root := range strings.Split(roots, ",") {
		if root = strings.TrimSpace(root); root != "" {
			result.storageRoots = append(result.storageRoots, root)
		}
	}
	return result
}

func run(parent context.Context, config config) error {
	if parent == nil || config.url == "" || config.output == "" || config.clients == 0 ||
		config.clients > rf3bench.MaximumClients || config.operations == 0 ||
		config.operations > rf3bench.MaximumOperations || config.timeout <= 0 ||
		len(config.storageRoots) == 0 {
		return errors.New("crdb-rf3-kvbench: invalid configuration")
	}
	ctx, cancel := context.WithTimeout(parent, config.timeout)
	defer cancel()

	admin, err := pgx.Connect(ctx, config.url)
	if err != nil {
		return fmt.Errorf("connect admin: %w", err)
	}
	defer admin.Close(context.Background())
	var version string
	if err = admin.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("query version: %w", err)
	}
	for _, statement := range []string{
		"DROP TABLE IF EXISTS rf3_kv_evidence",
		"CREATE TABLE rf3_kv_evidence (k BYTES PRIMARY KEY, v BYTES NOT NULL)",
		"ALTER TABLE rf3_kv_evidence CONFIGURE ZONE USING num_replicas = 3",
	} {
		if _, err = admin.Exec(ctx, statement); err != nil {
			return fmt.Errorf("setup %q: %w", statement, err)
		}
	}

	clients := make([]clientState, config.clients)
	defer func() {
		for index := range clients {
			if clients[index].connection != nil {
				_ = clients[index].connection.Close(context.Background())
			}
		}
	}()
	for index := range clients {
		connection, connectErr := pgx.Connect(ctx, config.url)
		if connectErr != nil {
			return fmt.Errorf("connect client %d: %w", index, connectErr)
		}
		clients[index] = clientState{connection: connection, key: evidenceKey(uint32(index)), sequence: 1}
		if _, err = connection.Prepare(ctx, putName, putSQL); err != nil {
			return fmt.Errorf("prepare client %d: %w", index, err)
		}
		if err = put(ctx, &clients[index], uint32(index)); err != nil {
			return fmt.Errorf("initialize client %d: %w", index, err)
		}
	}
	for ordinal := uint64(0); ordinal < config.warmup; ordinal++ {
		index := uint32(ordinal % uint64(config.clients))
		if err = put(ctx, &clients[index], index); err != nil {
			return fmt.Errorf("warmup %d: %w", ordinal+1, err)
		}
	}
	if err = waitForRF3(ctx, admin); err != nil {
		return err
	}
	before, err := rf3bench.MeasureFootprint(config.storageRoots...)
	if err != nil {
		return fmt.Errorf("measure storage before: %w", err)
	}

	samples := make([]rf3bench.Sample, config.operations)
	start := make(chan struct{})
	workCtx, workCancel := context.WithCancel(ctx)
	defer workCancel()
	var workers sync.WaitGroup
	var firstErr error
	var failOnce sync.Once
	for clientIndex := uint32(0); clientIndex < config.clients; clientIndex++ {
		workers.Add(1)
		go func(clientIndex uint32) {
			defer workers.Done()
			client := &clients[clientIndex]
			<-start
			var clientOperation uint64
			for ordinal := uint64(clientIndex); ordinal < config.operations; ordinal += uint64(config.clients) {
				clientOperation++
				value := evidenceValue(clientIndex, client.sequence)
				started := time.Now()
				tag, execErr := client.connection.Exec(workCtx, putName, client.key, value)
				latency := time.Since(started)
				if execErr == nil && tag.RowsAffected() != 1 {
					execErr = fmt.Errorf("affected %d rows", tag.RowsAffected())
				}
				if execErr != nil {
					failOnce.Do(func() {
						firstErr = fmt.Errorf("client %d operation %d: %w", clientIndex, ordinal+1, execErr)
						workCancel()
					})
					return
				}
				client.sequence++
				samples[ordinal] = rf3bench.Sample{
					Ordinal: ordinal + 1, Client: clientIndex, ClientSequence: clientOperation,
					Operation: rf3bench.OperationWrite, LatencyNS: uint64(max(latency, time.Nanosecond)),
					// CockroachDB does not expose its Raft apply index through SQL.
					// Preserve the common evidence schema with the explicitly labelled
					// workload ordinal; it is not used in latency or throughput results.
					Applied: ordinal + 1, Commit: ordinal + 1,
					PayloadBytes: uint32(len(client.key) + len(value)),
				}
			}
		}(clientIndex)
	}
	windowStart := time.Now()
	close(start)
	workers.Wait()
	elapsed := time.Since(windowStart)
	if firstErr != nil {
		return firstErr
	}
	if err = verifyFinalState(ctx, admin, clients); err != nil {
		return err
	}
	after, err := rf3bench.MeasureFootprint(config.storageRoots...)
	if err != nil {
		return fmt.Errorf("measure storage after: %w", err)
	}

	var logicalWriteBytes uint64
	for _, sample := range samples {
		logicalWriteBytes += uint64(sample.PayloadBytes)
	}
	report := rf3bench.Report{
		Config: rf3bench.Config{
			Clients: config.clients, Operations: config.operations, Warmup: config.warmup,
			Seed: evidenceSeed, ElapsedNS: uint64(max(elapsed, time.Nanosecond)),
			Workload: rf3bench.WorkloadWrite,
		},
		Metadata: []rf3bench.Metadata{
			{Key: []byte("api"), Value: []byte("postgres-wire-single-autocommit-upsert")},
			{Key: []byte("cockroach_version"), Value: []byte(strings.ReplaceAll(version, "\n", " "))},
			{Key: []byte("consistency"), Value: []byte("serializable")},
			{Key: []byte("engine"), Value: []byte("cockroachdb")},
			{Key: []byte("payload_bytes"), Value: []byte("logical-key-value")},
			{Key: []byte("replication_proof"), Value: []byte("show-ranges-all-voting-replicas-3")},
			{Key: []byte("sample_index"), Value: []byte("workload-ordinal-not-database-log-index")},
			{Key: []byte("statement"), Value: []byte("prepared-upsert-one-row")},
		},
		Samples: samples,
		Counters: []rf3bench.Counter{
			counter("storage", "allocated_bytes_after", 0, after.AllocatedBytes),
			counter("storage", "allocated_bytes_before", 0, before.AllocatedBytes),
			counter("storage", "apparent_bytes_after", 0, after.ApparentBytes),
			counter("storage", "apparent_bytes_before", 0, before.ApparentBytes),
			counter("storage", "file_count_after", 0, after.Files),
			counter("storage", "file_count_before", 0, before.Files),
			counter("workload", "logical_write_bytes", 0, logicalWriteBytes),
			counter("workload", "reads", 0, 0),
			counter("workload", "writes", 0, config.operations),
		},
	}
	file, err := os.OpenFile(filepath.Clean(config.output), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	if err = rf3bench.WriteTSV(file, report); err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func put(ctx context.Context, client *clientState, clientIndex uint32) error {
	value := evidenceValue(clientIndex, client.sequence)
	tag, err := client.connection.Exec(ctx, putName, client.key, value)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("affected %d rows", tag.RowsAffected())
	}
	client.sequence++
	return nil
}

func verifyFinalState(ctx context.Context, connection *pgx.Conn, clients []clientState) error {
	var count uint64
	if err := connection.QueryRow(ctx, "SELECT count(*) FROM rf3_kv_evidence").Scan(&count); err != nil {
		return fmt.Errorf("verify row count: %w", err)
	}
	if count != uint64(len(clients)) {
		return fmt.Errorf("verify row count: got %d, want %d", count, len(clients))
	}
	for index := range clients {
		var value []byte
		if err := connection.QueryRow(ctx, "SELECT v FROM rf3_kv_evidence WHERE k = $1", clients[index].key).Scan(&value); err != nil {
			return fmt.Errorf("verify client %d: %w", index, err)
		}
		want := evidenceValue(uint32(index), clients[index].sequence-1)
		if !bytes.Equal(value, want) {
			return fmt.Errorf("verify client %d: got %q, want %q", index, value, want)
		}
	}
	return nil
}

func waitForRF3(ctx context.Context, connection *pgx.Conn) error {
	const query = `SELECT count(*), min(array_length(voting_replicas, 1)),
		max(array_length(voting_replicas, 1))
		FROM [SHOW RANGES FROM TABLE rf3_kv_evidence WITH DETAILS]`
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ranges, minimum, maximum int64
		if err := connection.QueryRow(ctx, query).Scan(&ranges, &minimum, &maximum); err == nil &&
			ranges > 0 && minimum == 3 && maximum == 3 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for three voting replicas: %w", context.Cause(ctx))
		case <-ticker.C:
		}
	}
}

func evidenceKey(client uint32) []byte {
	quoted := strconv.AppendQuote(nil, "rf3-evidence-"+strconv.FormatUint(uint64(client), 10))
	key, ok := orderedkey.AppendJSONString(nil, quoted, orderedkey.Ascending)
	if !ok {
		panic("crdb-rf3-kvbench: encode evidence key")
	}
	return key
}

func evidenceValue(client uint32, sequence uint64) []byte {
	value := strconv.AppendUint([]byte(`{"client":`), uint64(client), 10)
	value = append(value, `,"id":"rf3-evidence-`...)
	value = strconv.AppendUint(value, uint64(client), 10)
	value = append(value, `","sequence":`...)
	value = strconv.AppendUint(value, sequence, 10)
	return append(value, '}')
}

func counter(scope, name string, before, after uint64) rf3bench.Counter {
	return rf3bench.Counter{Scope: []byte(scope), Name: []byte(name), Before: before, After: after}
}
