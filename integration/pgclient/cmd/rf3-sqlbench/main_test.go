package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestInvalidEndpointDoesNotLeakCredentials(t *testing.T) {
	for _, endpoint := range []string{"postgresql://alice:swordfish@localhost:70000/db", "https://alice:swordfish@localhost/db", "postgresql://alice:swordfish@localhost:invalid/db"} {
		_, _, err := parseURLs(endpoint, "")
		if err == nil || strings.Contains(err.Error(), "swordfish") || strings.Contains(err.Error(), "alice") {
			t.Fatalf("unsafe endpoint error: %v", err)
		}
	}
	err := fmt.Errorf("connect user=alice password=swordfish postgresql://alice:swordfish@localhost/db")
	if text := safeError(err, []string{"postgresql://alice:swordfish@localhost/db"}); strings.Contains(text, "swordfish") || strings.Contains(text, "alice") {
		t.Fatalf("unsafe connection error: %s", text)
	}
}

func TestClientsRejectDuplicateTrials(t *testing.T) {
	for _, raw := range []string{"1,1", "0", "16", "", "1,8,8"} {
		if _, err := parseClients(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}

func TestEarlyConnectionFailureRetainsStructuredReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	c := config{engine: "vibedb", url: "postgresql://alice:swordfish@127.0.0.1:1/db?sslmode=disable", output: path,
		phase: "run", rows: 64, operations: 2, scans: 1, repetitions: 1, seedBatch: 64, clients: "1",
		tables: defaultTable, workloads: "point_hit", groupDistribution: "uniform", skewPercent: 80}
	if err := run(c); err == nil {
		t.Fatal("connection unexpectedly succeeded")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var r report
	if err := json.Unmarshal(raw, &r); err != nil {
		t.Fatal(err)
	}
	if r.SchemaVersion != 2 || r.Status != "failed" || r.VerificationError == "" || len(r.Results) != 0 || strings.Contains(string(raw), "swordfish") {
		t.Fatalf("invalid failure report: %s", raw)
	}
	before := string(raw)
	if err := run(c); err == nil {
		t.Fatal("reused output path")
	}
	if after, err := os.ReadFile(path); err != nil || string(after) != before {
		t.Fatalf("existing evidence was changed: %v", err)
	}
}

func TestAtomicCheckpointRetainsCompletedTrialDuringNextTrial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "report.json")
	r := report{SchemaVersion: 2, Status: "incomplete", VerificationError: "benchmark did not finish", Results: []result{{Repetition: 1, Verified: true}},
		ActiveTrial: &trialRecord{Workload: "point_hit", Clients: 8, Repetition: 2, Phase: "preparing-or-measuring"}}
	if err := writeReport(path, r); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(raw, &got); err != nil || len(got.Results) != 1 || !got.Results[0].Verified || got.ActiveTrial.Repetition != 2 {
		t.Fatalf("checkpoint lost trial state: %s, %v", raw, err)
	}
}

func TestStreamsDoNotPartitionGroupsOrKeysByClient(t *testing.T) {
	c := config{groupDistribution: "uniform"}
	var counts [8][4][2]int
	var keyBuckets [8][16]int
	for ordinal := 0; ordinal < 40000; ordinal++ {
		client := ordinal % 8
		op := 0
		if mixedRead(ordinal) {
			op = 1
		}
		counts[client][groupFor(c, 4, ordinal)][op]++
		keyBuckets[client][readKeyFor(8192, 1, ordinal)%16]++
	}
	for client := range counts {
		for group := range counts[client] {
			for op, count := range counts[client][group] {
				if count < 450 || count > 800 {
					t.Fatalf("client %d group %d operation %d count %d", client, group, op, count)
				}
			}
		}
		for bucket, count := range keyBuckets[client] {
			if count < 200 || count > 440 {
				t.Fatalf("client %d key bucket %d count %d", client, bucket, count)
			}
		}
	}
}

func TestUniformKeyForUsesDisjointStripesAndDatasetBounds(t *testing.T) {
	for _, tc := range []struct {
		rows, clients int
	}{{64, 1}, {64, 3}, {65, 8}, {127, 15}, {8192, 8}} {
		owners := make(map[int]int)
		for client := 0; client < tc.clients; client++ {
			stripeLength := (tc.rows-1-client)/tc.clients + 1
			if stripeLength < 1 {
				t.Fatalf("rows=%d clients=%d client=%d has empty stripe", tc.rows, tc.clients, client)
			}
			max := client + tc.clients*(stripeLength-1)
			if max >= tc.rows {
				t.Fatalf("rows=%d clients=%d client=%d stripe reaches row %d", tc.rows, tc.clients, client, max)
			}
			for rep := 1; rep <= 3; rep++ {
				for ordinal := 0; ordinal < 2000; ordinal++ {
					id := uniformKeyFor(tc.rows, tc.clients, client, rep, ordinal)
					if id < 0 || id >= tc.rows {
						t.Fatalf("rows=%d clients=%d client=%d produced out-of-bounds id %d", tc.rows, tc.clients, client, id)
					}
					if id%tc.clients != client {
						t.Fatalf("rows=%d clients=%d client=%d produced id %d owned by %d", tc.rows, tc.clients, client, id, id%tc.clients)
					}
					if previous, ok := owners[id]; ok && previous != client {
						t.Fatalf("id %d selected by clients %d and %d", id, previous, client)
					}
					owners[id] = client
				}
			}
		}
	}
}

func TestUniformKeyForDeterministicallySpreadsAcrossWideStripes(t *testing.T) {
	const rows, clients, rep = 4096, 8, 7
	stripeLength := (rows-1)/clients + 1
	for client := 0; client < clients; client++ {
		first, second := make([]int, 256), make([]int, 256)
		seen := make(map[int]struct{})
		for operation := 0; operation < 10000; operation++ {
			ordinal := client + operation*clients
			id := uniformKeyFor(rows, clients, client, rep, ordinal)
			seen[id] = struct{}{}
			if operation < len(first) {
				first[operation] = id
			}
		}
		for operation := 0; operation < len(second); operation++ {
			second[operation] = uniformKeyFor(rows, clients, client, rep, client+operation*clients)
		}
		if len(seen) != stripeLength {
			t.Fatalf("client %d selected only %d of %d stripe keys", client, len(seen), stripeLength)
		}
		for ordinal := range first {
			if first[ordinal] != second[ordinal] {
				t.Fatalf("client %d key stream changed at ordinal %d", client, ordinal)
			}
		}
	}
}

func TestMixedUniformOracleSeesOnlyOwnPriorWrites(t *testing.T) {
	const rows, clients, groups, rep = 127, 8, 3, 5
	c := config{groupDistribution: "uniform"}
	scores := make([][]int, groups)
	updates := make([][]int, groups)
	for group := 0; group < groups; group++ {
		scores[group], updates[group] = make([]int, rows), make([]int, rows)
		for id := 0; id < rows; id++ {
			scores[group][id] = id % 100
		}
	}
	reads, writes, readsAfterWrites := 0, 0, 0
	for client := 0; client < clients; client++ {
		for ordinal := client; ordinal < 12000; ordinal += clients {
			group := groupFor(c, groups, ordinal)
			id := uniformKeyFor(rows, clients, client, rep, ordinal)
			if id%clients != client {
				t.Fatalf("client %d selected id %d", client, id)
			}
			if mixedRead(ordinal) {
				reads++
				if updates[group][id] > 0 {
					readsAfterWrites++
				}
				if scores[group][id] != id%100+updates[group][id] {
					t.Fatalf("group %d id %d oracle score = %d, want %d", group, id, scores[group][id], id%100+updates[group][id])
				}
			} else {
				writes++
				scores[group][id]++
				updates[group][id]++
			}
		}
	}
	if reads == 0 || writes == 0 || readsAfterWrites == 0 {
		t.Fatalf("mixed_uniform reads=%d writes=%d reads-after-writes=%d", reads, writes, readsAfterWrites)
	}
}

func TestUniformWorkloadOperationLabels(t *testing.T) {
	if got := operationFor("update_uniform", 0); got != "update_existing" {
		t.Fatalf("update_uniform operation = %q", got)
	}
	reads, writes := 0, 0
	for ordinal := 0; ordinal < 4000; ordinal++ {
		want := "update_existing"
		if mixedRead(ordinal) {
			want = "point_hit"
			reads++
		} else {
			writes++
		}
		if got := operationFor("mixed_uniform", ordinal); got != want {
			t.Fatalf("mixed_uniform ordinal %d operation = %q, want %q", ordinal, got, want)
		}
	}
	if reads == 0 || writes == 0 {
		t.Fatalf("mixed_uniform reads=%d writes=%d", reads, writes)
	}
}

// Exercise trial's real warmup, statement selection, response checking and
// score updates over the extended protocol. The server maintains independent
// rows, so failing to advance the warmup oracle or accepting a stale read
// cannot be hidden by a test that increments both expected counters together.
func TestUniformTrialWarmupPriorReadsAndFullVerification(t *testing.T) {
	const rows, clients, count, warmup = 67, 3, 259, 83
	tables := []string{"wide_a", "wide_b"}
	for _, engine := range []string{"vibedb", "cockroachdb"} {
		for _, workload := range []string{"update_uniform", "mixed_uniform"} {
			t.Run(engine+"/"+workload, func(t *testing.T) {
				c := config{engine: engine, rows: rows, warmup: warmup, groupDistribution: "uniform"}
				scores, stored := make([][]int, len(tables)), make([][]int, len(tables))
				for group := range tables {
					scores[group], stored[group] = make([]int, rows), make([]int, rows)
					for id := range rows {
						scores[group][id], stored[group][id] = id%100, id%100
					}
				}
				var mu sync.Mutex
				seen := make(map[int]bool)
				readsAfterWrites := 0
				calls := make([]int, clients*2)
				endpoint := uniformTrialServer(t, func(connection int, sql string, params [][]byte) ([][][]byte, string, error) {
					mu.Lock()
					defer mu.Unlock()
					cell := func(value string) []byte {
						if engine == "vibedb" {
							encoded, _ := json.Marshal(value)
							return encoded
						}
						return []byte(value)
					}
					row := func(group, id int) [][]byte {
						return [][]byte{cell(key(id)), []byte(strconv.Itoa(id % 16)), []byte(strconv.Itoa(stored[group][id])), cell(payload)}
					}
					if connection >= len(calls) {
						for group, table := range tables {
							if sql == "SELECT COUNT(*) FROM "+table {
								return [][][]byte{{[]byte(strconv.Itoa(rows))}}, "SELECT 1", nil
							}
							if sql == "SELECT id,bucket,score,payload FROM "+table+" WHERE id >= $1 ORDER BY id LIMIT 512" && len(params) == 1 && string(params[0]) == key(0) {
								result := make([][][]byte, rows)
								for id := range rows {
									result[id] = row(group, id)
								}
								return result, "SELECT " + strconv.Itoa(rows), nil
							}
						}
						return nil, "", fmt.Errorf("unexpected verification query %q", sql)
					}
					client, rep := connection%clients, []int{2, 5}[connection/clients]
					call := calls[connection]
					warmups := (warmup-1-client)/clients + 1
					ordinal := client + call*clients
					if call >= warmups {
						ordinal = client + (call-warmups)*clients
					}
					group := groupFor(c, len(tables), ordinal)
					id := uniformKeyFor(rows, clients, client, rep, ordinal)
					read := workload == "mixed_uniform" && mixedRead(ordinal)
					wantSQL := "UPDATE " + tables[group] + " SET score=score+1 WHERE id=$1"
					if read {
						wantSQL = "SELECT id,bucket,score,payload FROM " + tables[group] + " WHERE id=$1"
					}
					if sql != wantSQL || len(params) != 1 || string(params[0]) != key(id) || id%clients != client {
						return nil, "", fmt.Errorf("connection %d ordinal %d statement/key mismatch: %q %q", connection, ordinal, sql, params)
					}
					calls[connection]++
					seen[id] = true
					if read {
						if stored[group][id] > id%100 {
							readsAfterWrites++
						}
						return [][][]byte{row(group, id)}, "SELECT 1", nil
					}
					stored[group][id]++
					return nil, "UPDATE 1", nil
				})
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				for _, rep := range []int{2, 5} {
					out, err := trial(ctx, c, workload, clients, rep, count, tables, scores, []string{endpoint})
					if err != nil || out.Errors != 0 || out.Repetition != rep || len(out.Samples) != count {
						t.Fatalf("trial repetition %d: errors=%d err=%v samples=%v", rep, out.Errors, err, out.Samples)
					}
					for ordinal, sample := range out.Samples {
						if sample.Ordinal != ordinal || sample.Client != ordinal%clients || sample.Operation != operationFor(workload, ordinal) {
							t.Fatalf("sample identity: %+v", sample)
						}
					}
					mu.Lock()
					for group := range tables {
						for id := range rows {
							if scores[group][id] != stored[group][id] {
								t.Errorf("repetition %d group %d key %d oracle=%d stored=%d", rep, group, id, scores[group][id], stored[group][id])
							}
						}
					}
					mu.Unlock()
				}
				mu.Lock()
				for connection, got := range calls {
					client := connection % clients
					if want := (warmup-1-client)/clients + 1 + (count-1-client)/clients + 1; got != want {
						t.Errorf("connection %d operations=%d want=%d", connection, got, want)
					}
				}
				if len(seen) != rows || workload == "mixed_uniform" && readsAfterWrites == 0 {
					t.Errorf("key coverage=%d/%d reads-after-writes=%d", len(seen), rows, readsAfterWrites)
				}
				mu.Unlock()
				admin, err := pgconn.Connect(ctx, endpoint)
				if err != nil {
					t.Fatal(err)
				}
				defer admin.Close(context.Background())
				if err := verify(ctx, admin, c, tables, scores); err != nil {
					t.Fatal(err)
				}
				mu.Lock()
				stored[1][rows-1]++ // A mismatched tail row must fail full-table verification.
				mu.Unlock()
				if err := verify(ctx, admin, c, tables, scores); err == nil {
					t.Fatal("full-table verification accepted a mismatched tail score")
				}
			})
		}
	}
}

func TestRangeVariantsValidateTailCountOrderAndOperationIdentity(t *testing.T) {
	const rep = 7
	for _, rangeRows := range []int{32, 256} {
		workload := "range_" + strconv.Itoa(rangeRows)
		rows := rangeRows + 7
		tailOrdinal := -1
		for ordinal := 0; ordinal < 10000; ordinal++ {
			id := readKeyFor(rows, rep, ordinal)
			if id/rangeRows*rangeRows == rangeRows {
				tailOrdinal = ordinal
				break
			}
		}
		if tailOrdinal < 0 {
			t.Fatalf("%s deterministic stream never reached the tail", workload)
		}
		for _, corrupt := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/corrupt=%v", workload, corrupt), func(t *testing.T) {
				calls, sawTail := 0, false
				endpoint := uniformTrialServer(t, func(_ int, sql string, params [][]byte) ([][][]byte, string, error) {
					ordinal := calls
					calls++
					id := readKeyFor(rows, rep, ordinal)
					id = id / rangeRows * rangeRows
					wantSQL := "SELECT id,score FROM rf3_sql_bench WHERE id >= $1 ORDER BY id LIMIT " + strconv.Itoa(rangeRows)
					if sql != wantSQL || len(params) != 1 || string(params[0]) != key(id) {
						return nil, "", fmt.Errorf("ordinal %d range statement/key mismatch: %q %q", ordinal, sql, params)
					}
					count := min(rangeRows, rows-id)
					result := make([][][]byte, count)
					for offset := range count {
						rowID := id + offset
						result[offset] = [][]byte{[]byte(key(rowID)), []byte(strconv.Itoa(rowID % 100))}
					}
					if id == rangeRows {
						sawTail = true
						if corrupt && len(result) > 1 {
							result[0], result[1] = result[1], result[0]
						}
					}
					return result, "SELECT " + strconv.Itoa(count), nil
				})
				c := config{engine: "cockroachdb", rows: rows, groupDistribution: "uniform"}
				scores := [][]int{make([]int, rows)}
				for id := range rows {
					scores[0][id] = id % 100
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				out, err := trial(ctx, c, workload, 1, rep, tailOrdinal+1,
					[]string{defaultTable}, scores, []string{endpoint})
				if err != nil || !sawTail || calls != tailOrdinal+1 {
					t.Fatalf("err=%v sawTail=%v calls=%d want=%d", err, sawTail, calls, tailOrdinal+1)
				}
				for ordinal, sample := range out.Samples {
					if sample.Ordinal != ordinal || sample.Operation != workload {
						t.Fatalf("sample %d identity: %+v", ordinal, sample)
					}
				}
				if corrupt && out.Errors == 0 || !corrupt && out.Errors != 0 {
					t.Fatalf("corrupt=%v errors=%d", corrupt, out.Errors)
				}
			})
		}
	}
}

func uniformTrialServer(t *testing.T, execute func(int, string, [][]byte) ([][][]byte, string, error)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		for connection := 0; ; connection++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func(connection int) {
				defer workers.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(20 * time.Second))
				backend := pgproto3.NewBackend(conn, conn)
				if _, err := backend.ReceiveStartupMessage(); err != nil {
					t.Error(err)
					return
				}
				backend.Send(&pgproto3.AuthenticationOk{})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if err := backend.Flush(); err != nil {
					t.Error(err)
					return
				}
				var sql string
				var params [][]byte
				for {
					message, err := backend.Receive()
					if err != nil {
						if !errors.Is(err, io.EOF) {
							t.Error(err)
						}
						return
					}
					switch message := message.(type) {
					case *pgproto3.Parse:
						sql = message.Query
						backend.Send(&pgproto3.ParseComplete{})
					case *pgproto3.Bind:
						params = make([][]byte, len(message.Parameters))
						for i, parameter := range message.Parameters {
							params[i] = append([]byte(nil), parameter...)
						}
						backend.Send(&pgproto3.BindComplete{})
					case *pgproto3.Describe:
						if strings.HasPrefix(sql, "SELECT ") {
							names := []string{"id", "bucket", "score", "payload"}
							if strings.HasPrefix(sql, "SELECT COUNT(") {
								names = []string{"count"}
							}
							fields := make([]pgproto3.FieldDescription, len(names))
							for i, name := range names {
								fields[i] = pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}
							}
							backend.Send(&pgproto3.RowDescription{Fields: fields})
						} else {
							backend.Send(&pgproto3.NoData{})
						}
					case *pgproto3.Execute:
						rows, tag, err := execute(connection, sql, params)
						if err != nil {
							backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: err.Error()})
						} else {
							for _, row := range rows {
								backend.Send(&pgproto3.DataRow{Values: row})
							}
							backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
						}
					case *pgproto3.Sync:
						backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
						if err := backend.Flush(); err != nil {
							t.Error(err)
							return
						}
					case *pgproto3.Terminate:
						return
					default:
						t.Errorf("unexpected extended-protocol message %T", message)
						return
					}
				}
			}(connection)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		workers.Wait()
	})
	return "postgresql://bench@" + listener.Addr().String() + "/bench?sslmode=disable"
}

func TestMixedUniformWarmupRejectsEveryIncorrectReadField(t *testing.T) {
	for _, field := range []int{0, 1, 2, 3} {
		t.Run(strconv.Itoa(field), func(t *testing.T) {
			c := config{engine: "cockroachdb", rows: 64, warmup: 32, groupDistribution: "uniform"}
			scores, stored := make([]int, c.rows), make([]int, c.rows)
			for id := range scores {
				scores[id], stored[id] = id%100, id%100
			}
			endpoint := uniformTrialServer(t, func(_ int, sql string, params [][]byte) ([][][]byte, string, error) {
				id, err := strconv.Atoi(strings.TrimPrefix(string(params[0]), "key-"))
				if err != nil || id < 0 || id >= len(stored) {
					return nil, "", fmt.Errorf("unexpected key %q", params)
				}
				if strings.HasPrefix(sql, "UPDATE ") {
					stored[id]++
					return nil, "UPDATE 1", nil
				}
				row := [][]byte{[]byte(key(id)), []byte(strconv.Itoa(id % 16)), []byte(strconv.Itoa(stored[id])), []byte(payload)}
				row[field] = []byte("incorrect")
				return [][][]byte{row}, "SELECT 1", nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := trial(ctx, c, "mixed_uniform", 1, 1, 8, []string{defaultTable}, [][]int{scores}, []string{endpoint})
			if err == nil || !strings.Contains(err.Error(), "mixed_uniform warmup: uniform mixed read mismatch") || out.MeasurementStartedUTC != "" {
				t.Fatalf("incorrect read field %d did not fail warmup: err=%v", field, err)
			}
		})
	}
}

func TestParseTables(t *testing.T) {
	got, err := parseTables("rf3_sql_bench,orders_01")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "rf3_sql_bench,orders_01" {
		t.Fatalf("tables = %v", got)
	}
	for _, input := range []string{"", "RF3", "rf3_sql_bench,rf3_sql_bench", "table-name"} {
		if _, err := parseTables(input); err == nil {
			t.Fatalf("parseTables(%q) accepted invalid input", input)
		}
	}
}

func TestParseWorkloads(t *testing.T) {
	got, err := parseWorkloads("point_hit,mixed,update_existing")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "point_hit,mixed_read_update,update_existing" {
		t.Fatalf("workloads = %v", got)
	}
	for _, input := range []string{"", "point_hit,point_hit", "delete_all"} {
		if _, err := parseWorkloads(input); err == nil {
			t.Fatalf("parseWorkloads(%q) accepted invalid input", input)
		}
	}
}

func TestParseWorkloadsAcceptsExplicitUniformNames(t *testing.T) {
	got, err := parseWorkloads("update_uniform,mixed_uniform")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "update_uniform,mixed_uniform" {
		t.Fatalf("workloads = %v", got)
	}
	if _, err := parseWorkloads("update_uniform,update_uniform"); err == nil {
		t.Fatal("duplicate update_uniform workload accepted")
	}
}

func TestParseWorkloadsAcceptsOptionalRangeSizes(t *testing.T) {
	got, err := parseWorkloads("range_32,range_64,range_256")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "range_32,range_64,range_256" {
		t.Fatalf("unexpected range workloads: %v", got)
	}
}

func TestParseURLsKeepsCredentialsOutOfLabels(t *testing.T) {
	endpoints, labels, err := parseURLs(
		"postgresql://alice:secret@127.0.0.1:5432/vibedb?sslmode=disable",
		"postgresql://alice:secret@127.0.0.1:5432/vibedb?sslmode=disable,postgresql://alice:secret@127.0.0.1:5433/vibedb?sslmode=disable",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(endpoints) != 2 || labels[0] != "127.0.0.1:5432" || labels[1] != "127.0.0.1:5433" {
		t.Fatalf("endpoints=%v labels=%v", endpoints, labels)
	}
	if strings.Contains(strings.Join(labels, ","), "secret") {
		t.Fatalf("endpoint labels contain credentials: %v", labels)
	}
	for _, raw := range []string{"", "http://127.0.0.1:5432/db", "postgresql://127.0.0.1:70000/db", "postgresql://127.0.0.1:5432/db,postgresql://127.0.0.1:5432/other"} {
		if _, _, err := parseURLs("postgresql://127.0.0.1:5432/db", raw); raw == "" {
			if err != nil {
				t.Fatalf("single base URL rejected: %v", err)
			}
		} else if err == nil {
			t.Fatalf("parseURLs accepted invalid input %q", raw)
		}
	}
}

func TestGroupForUniformAndSkewed(t *testing.T) {
	uniform := config{groupDistribution: "uniform"}
	counts := make([]int, 4)
	for ordinal := 0; ordinal < 4000; ordinal++ {
		counts[groupFor(uniform, len(counts), ordinal)]++
	}
	for group, count := range counts {
		if count < 850 || count > 1150 {
			t.Fatalf("uniform group %d count = %d, want an approximately uniform count", group, count)
		}
	}
	skewed := config{groupDistribution: "skewed", skewPercent: 80}
	counts = make([]int, 4)
	for ordinal := 0; ordinal < 4000; ordinal++ {
		counts[groupFor(skewed, len(counts), ordinal)]++
	}
	if counts[0] < 2800 || counts[0] > 3600 {
		t.Fatalf("skewed first group count = %d, want a hot majority", counts[0])
	}
	if counts[1]+counts[2]+counts[3] < 400 {
		t.Fatalf("skewed tail count = %d, want cold groups to remain active", counts[1]+counts[2]+counts[3])
	}

	// The operation stream is independent of placement: every group must see
	// both halves of mixed read/update traffic over a real trial-sized sample.
	for group := 0; group < 4; group++ {
		reads, writes := 0, 0
		for ordinal := 0; ordinal < 4000; ordinal++ {
			if groupFor(uniform, 4, ordinal) != group {
				continue
			}
			if mixedRead(ordinal) {
				reads++
			} else {
				writes++
			}
		}
		if reads == 0 || writes == 0 {
			t.Fatalf("group %d mixed operations reads=%d writes=%d", group, reads, writes)
		}
	}
}

func TestTimingFieldsUseStableJSONNames(t *testing.T) {
	anchor := time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(result{
		MeasurementStartedUTC: anchor,
		Samples:               []sample{{Ordinal: 0, Client: 0, NS: 10, StartOffsetNS: 2, Group: 1, Table: "orders_01"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	text := string(payload)
	for _, field := range []string{"\"measurement_started_utc\"", "\"start_offset_ns\"", "\"group\"", "\"table\"", "\"endpoint\""} {
		if !strings.Contains(text, field) {
			t.Fatalf("JSON %q missing %s", text, field)
		}
	}
}
