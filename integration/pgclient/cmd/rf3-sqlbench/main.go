// rf3-sqlbench compares public PostgreSQL SQL operations, never internal Raft APIs.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type config struct {
	engine, url, output, phase                   string
	rows, operations, scans, warmup, repetitions int
	clients                                      string
}
type sample struct {
	Client  int    `json:"client"`
	Ordinal int    `json:"ordinal"`
	NS      int64  `json:"ns"`
	Error   string `json:"error,omitempty"`
}
type result struct {
	Engine     string   `json:"engine"`
	Workload   string   `json:"workload"`
	Clients    int      `json:"clients"`
	Repetition int      `json:"repetition"`
	Operations int      `json:"operations"`
	Errors     int      `json:"errors"`
	ElapsedNS  int64    `json:"elapsed_ns"`
	Throughput float64  `json:"successful_ops_per_second"`
	P50        int64    `json:"p50_ns"`
	P95        int64    `json:"p95_ns"`
	P99        int64    `json:"p99_ns"`
	Verified   bool     `json:"verified"`
	Samples    []sample `json:"samples"`
}
type report struct {
	Config            configRecord `json:"config"`
	Version           string       `json:"version"`
	Started           string       `json:"started_utc"`
	Results           []result     `json:"results"`
	VerificationError string       `json:"verification_error,omitempty"`
}
type configRecord struct {
	Engine                                                              string
	Rows, PayloadBytes, Operations, ScanOperations, Warmup, Repetitions int
	Clients                                                             string
	Protocol                                                            string
}

const table = "rf3_sql_bench"
const payload = "abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345"

func main() {
	var c config
	flag.StringVar(&c.engine, "engine", "", "vibedb or cockroachdb")
	flag.StringVar(&c.url, "url", "", "PostgreSQL URL for a dedicated benchmark database")
	flag.StringVar(&c.output, "output", "", "new JSON evidence file")
	flag.StringVar(&c.phase, "phase", "all", "setup, run, or all; setup creates a new table and never drops existing data")
	flag.IntVar(&c.rows, "rows", 8192, "initial rows")
	flag.IntVar(&c.operations, "operations", 2000, "measured point/update operations per trial")
	flag.IntVar(&c.scans, "scans", 200, "measured range/group operations per trial")
	flag.IntVar(&c.warmup, "warmup", 100, "unmeasured operations before each trial")
	flag.IntVar(&c.repetitions, "repetitions", 3, "repetitions per workload and concurrency")
	flag.StringVar(&c.clients, "clients", "1,8", "closed-loop concurrency list (maximum 15)")
	flag.Parse()
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(c config) error {
	if (c.engine != "vibedb" && c.engine != "cockroachdb") || c.url == "" || c.rows < 64 || c.rows > 1000000 || c.operations < 1 || c.operations > 1000000 || c.scans < 1 || c.scans > 100000 || c.warmup < 0 || c.warmup > 100000 || c.repetitions < 1 || c.repetitions > 20 || (c.phase != "all" && c.phase != "setup" && c.phase != "run") {
		return fmt.Errorf("invalid benchmark configuration")
	}
	var concurrencies []int
	for _, s := range strings.Split(c.clients, ",") {
		n, e := strconv.Atoi(s)
		if e != nil || n < 1 || n > 15 {
			return fmt.Errorf("invalid clients")
		}
		concurrencies = append(concurrencies, n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	admin, err := pgconn.Connect(ctx, c.url)
	if err != nil {
		return err
	}
	defer admin.Close(context.Background())
	r := report{Config: configRecord{c.engine, c.rows, len(payload), c.operations, c.scans, c.warmup, c.repetitions, c.clients, "extended unnamed parse/bind/execute; text parameters/results; one autocommit statement per operation"}, Started: time.Now().UTC().Format(time.RFC3339)}
	v := admin.ExecParams(ctx, "SELECT version()", nil, nil, nil, nil).Read()
	if v.Err != nil {
		return v.Err
	}
	if len(v.Rows) > 0 {
		r.Version = string(v.Rows[0][0])
	}
	if c.phase != "run" {
		if err = setup(ctx, admin, c); err != nil {
			return err
		}
	}
	if c.phase == "setup" {
		return nil
	}
	f, err := os.OpenFile(c.output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	scores := make([]int, c.rows)
	for i := range scores {
		scores[i] = i % 100
	}
	if err = verify(ctx, admin, c, scores); err != nil {
		return err
	}
	// No planner tuning or stats commands in the timed region. CockroachDB's
	// normal cost-based optimizer and VibeDB's normal gateway are both enabled.
	for _, workload := range []string{"point_hit", "point_miss", "range_64", "group_16", "update_existing"} {
		for _, n := range concurrencies {
			for rep := 1; rep <= c.repetitions; rep++ {
				count := c.operations
				if workload == "range_64" || workload == "group_16" {
					count = c.scans
				}
				out, e := trial(ctx, c, workload, n, rep, count, scores)
				if e != nil {
					r.VerificationError = e.Error()
					break
				}
				verifyErr := verify(ctx, admin, c, scores)
				out.Verified = verifyErr == nil && out.Errors == 0
				r.Results = append(r.Results, out)
				fmt.Fprintf(os.Stderr, "%s %s c=%d rep=%d %.1f ops/s p99=%.3fms errors=%d verified=%v\n", c.engine, workload, n, rep, out.Throughput, float64(out.P99)/1e6, out.Errors, out.Verified)
				if verifyErr != nil {
					r.VerificationError = verifyErr.Error()
				}
				if out.Errors != 0 && r.VerificationError == "" {
					r.VerificationError = "operation errors; trial is not a valid throughput result"
				}
				if r.VerificationError != "" {
					break
				}
			}
			if r.VerificationError != "" {
				break
			}
		}
		if r.VerificationError != "" {
			break
		}
	}
	if err = json.NewEncoder(f).Encode(r); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if r.VerificationError != "" {
		return fmt.Errorf("benchmark failed: %s", r.VerificationError)
	}
	return nil
}
func setup(ctx context.Context, conn *pgconn.PgConn, c config) error {
	ddl := "CREATE TABLE " + table + " (id TEXT PRIMARY KEY, bucket INTEGER NOT NULL, score INTEGER NOT NULL, payload TEXT NOT NULL)"
	if e := conn.ExecParams(ctx, ddl, nil, nil, nil, nil).Read().Err; e != nil {
		return e
	}
	for first := 0; first < c.rows; first += 64 {
		var sql strings.Builder
		sql.WriteString("INSERT INTO " + table + " (id,bucket,score,payload) VALUES ")
		for i := first; i < min(first+64, c.rows); i++ {
			if i != first {
				sql.WriteByte(',')
			}
			fmt.Fprintf(&sql, "('%s',%d,%d,'%s')", key(i), i%16, i%100, payload)
		}
		res := conn.ExecParams(ctx, sql.String(), nil, nil, nil, nil).Read()
		if res.Err != nil {
			return fmt.Errorf("seed %d: %w", first, res.Err)
		}
		if res.CommandTag.RowsAffected() != int64(min(64, c.rows-first)) {
			return fmt.Errorf("seed affected rows")
		}
	}
	if c.engine == "cockroachdb" {
		for _, sql := range []string{"ALTER TABLE " + table + " CONFIGURE ZONE USING num_replicas = 3", "ANALYZE " + table} {
			if e := conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read().Err; e != nil {
				return e
			}
		}
		deadline := time.Now().Add(time.Minute)
		for {
			res := conn.ExecParams(ctx, "SELECT count(*), min(array_length(voting_replicas,1)), max(array_length(voting_replicas,1)) FROM [SHOW RANGES FROM TABLE "+table+" WITH DETAILS]", nil, nil, nil, nil).Read()
			if res.Err == nil && len(res.Rows) == 1 && string(res.Rows[0][0]) != "0" && string(res.Rows[0][1]) == "3" && string(res.Rows[0][2]) == "3" {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("RF3 proof failed: %v", res.Err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return nil
}
func key(i int) string { return fmt.Sprintf("key-%08d", i) }
func textCell(c config, b []byte) string {
	if c.engine == "vibedb" {
		var s string
		if json.Unmarshal(b, &s) == nil {
			return s
		}
	}
	return string(b)
}
func verify(ctx context.Context, conn *pgconn.PgConn, c config, scores []int) error {
	// Validate every stored field after every trial, outside the timing window.
	count := conn.ExecParams(ctx, "SELECT COUNT(*) FROM "+table, nil, nil, nil, nil).Read()
	if count.Err != nil {
		return count.Err
	}
	if len(count.Rows) != 1 || len(count.Rows[0]) != 1 || string(count.Rows[0][0]) != strconv.Itoa(c.rows) {
		return fmt.Errorf("verification row count mismatch")
	}
	for first := 0; first < c.rows; first += 512 {
		res := conn.ExecParams(ctx, "SELECT id,bucket,score,payload FROM "+table+" WHERE id >= $1 ORDER BY id LIMIT 512", [][]byte{[]byte(key(first))}, []uint32{25}, nil, nil).Read()
		if res.Err != nil {
			return fmt.Errorf("verify: %w", res.Err)
		}
		if len(res.Rows) != min(512, c.rows-first) {
			return fmt.Errorf("verification page count mismatch")
		}
		for j, row := range res.Rows {
			i := first + j
			if len(row) != 4 || textCell(c, row[0]) != key(i) || string(row[1]) != strconv.Itoa(i%16) || string(row[2]) != strconv.Itoa(scores[i]) || textCell(c, row[3]) != payload {
				return fmt.Errorf("row %d differs from oracle", i)
			}
		}
	}
	return nil
}
func trial(ctx context.Context, c config, workload string, clients, rep, count int, scores []int) (result, error) {
	out := result{Engine: c.engine, Workload: workload, Clients: clients, Repetition: rep, Operations: count, Samples: make([]sample, count)}
	connections := make([]*pgconn.PgConn, clients)
	defer func() {
		for _, p := range connections {
			if p != nil {
				p.Close(context.Background())
			}
		}
	}()
	for i := range connections {
		p, e := pgconn.Connect(ctx, c.url)
		if e != nil {
			return out, e
		}
		connections[i] = p
	}
	operation := func(conn *pgconn.PgConn, client, ordinal int) error {
		// A fixed bijective stride visits the same keys for both engines. Updates
		// use disjoint per-client keys so this workload makes no contention claim.
		id := (ordinal*7919 + rep*17) % c.rows
		sql := ""
		var params [][]byte
		switch workload {
		case "point_hit":
			sql = "SELECT id,bucket,score,payload FROM " + table + " WHERE id=$1"
			params = [][]byte{[]byte(key(id))}
		case "point_miss":
			sql = "SELECT id,bucket,score,payload FROM " + table + " WHERE id=$1"
			params = [][]byte{[]byte(key(c.rows + id))}
		case "range_64":
			id = (id / 64) * 64
			sql = "SELECT id,score FROM " + table + " WHERE id >= $1 ORDER BY id LIMIT 64"
			params = [][]byte{[]byte(key(id))}
		case "group_16":
			sql = "SELECT bucket,COUNT(*),SUM(score) FROM " + table + " GROUP BY bucket ORDER BY bucket"
		case "update_existing":
			id = client
			sql = "UPDATE " + table + " SET score=score+1 WHERE id=$1"
			params = [][]byte{[]byte(key(id))}
		}
		types := make([]uint32, len(params))
		for i := range types {
			types[i] = 25
		}
		res := conn.ExecParams(ctx, sql, params, types, nil, nil).Read()
		if res.Err != nil {
			return res.Err
		}
		switch workload {
		case "point_hit":
			if len(res.Rows) != 1 || len(res.Rows[0]) != 4 {
				return fmt.Errorf("point result shape")
			}
			row := res.Rows[0]
			if textCell(c, row[0]) != key(id) || string(row[1]) != strconv.Itoa(id%16) || string(row[2]) != strconv.Itoa(scores[id]) || textCell(c, row[3]) != payload {
				return fmt.Errorf("point mismatch")
			}
		case "point_miss":
			if len(res.Rows) != 0 {
				return fmt.Errorf("miss returned rows")
			}
		case "range_64":
			if len(res.Rows) != min(64, c.rows-id) {
				return fmt.Errorf("range count")
			}
			for j, row := range res.Rows {
				if len(row) != 2 || textCell(c, row[0]) != key(id+j) || string(row[1]) != strconv.Itoa(scores[id+j]) {
					return fmt.Errorf("range mismatch")
				}
			}
		case "group_16":
			if len(res.Rows) != 16 {
				return fmt.Errorf("group count")
			}
			for j, row := range res.Rows {
				sum, n := 0, 0
				for k := j; k < c.rows; k += 16 {
					sum += scores[k]
					n++
				}
				if len(row) != 3 || string(row[0]) != strconv.Itoa(j) || string(row[1]) != strconv.Itoa(n) || string(row[2]) != strconv.Itoa(sum) {
					return fmt.Errorf("group mismatch")
				}
			}
		case "update_existing":
			if res.CommandTag.RowsAffected() != 1 {
				return fmt.Errorf("update affected %d", res.CommandTag.RowsAffected())
			}
			scores[id]++
		}
		return nil
	}
	for i := 0; i < c.warmup; i++ {
		if e := operation(connections[i%clients], i%clients, i); e != nil {
			return out, fmt.Errorf("%s warmup: %w", workload, e)
		}
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for client := 0; client < clients; client++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			<-start
			for ordinal := client; ordinal < count; ordinal += clients {
				now := time.Now()
				err := operation(connections[client], client, ordinal)
				s := sample{Client: client, Ordinal: ordinal, NS: time.Since(now).Nanoseconds()}
				if err != nil {
					s.Error = err.Error()
				}
				out.Samples[ordinal] = s
			}
		}(client)
	}
	now := time.Now()
	close(start)
	wg.Wait()
	out.ElapsedNS = time.Since(now).Nanoseconds()
	latencies := make([]int64, 0, count)
	for _, s := range out.Samples {
		if s.Error != "" {
			out.Errors++
		}
		latencies = append(latencies, s.NS)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(q float64) int64 { return latencies[int(math.Ceil(float64(len(latencies))*q))-1] }
	out.P50, out.P95, out.P99 = percentile(.5), percentile(.95), percentile(.99)
	out.Throughput = float64(count-out.Errors) / (float64(out.ElapsedNS) / 1e9)
	return out, nil
}
