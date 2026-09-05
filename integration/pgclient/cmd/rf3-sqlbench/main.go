// rf3-sqlbench compares public PostgreSQL SQL operations, never internal Raft APIs.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type config struct {
	recoveryOracle                               string
	diagnosticTargets                            string
	diagnostics                                  *diagnosticControl
	engine, url, output, phase                   string
	rows, operations, scans, warmup, repetitions int
	seedBatch                                    int
	clients                                      string
	tables, workloads, groupDistribution         string
	skewPercent                                  int
	physicalNodes                                int
	requireExistingTables, verifyEveryTrial      bool
	urls                                         string
}
type sample struct {
	Client        int    `json:"client"`
	Ordinal       int    `json:"ordinal"`
	NS            int64  `json:"ns"`
	StartOffsetNS int64  `json:"start_offset_ns"`
	Group         int    `json:"group"`
	Table         string `json:"table"`
	Endpoint      int    `json:"endpoint"`
	Operation     string `json:"operation"`
	Error         string `json:"error,omitempty"`
}
type result struct {
	Diagnostics           *diagnosticBracket `json:"diagnostics,omitempty"`
	Engine                string             `json:"engine"`
	Workload              string             `json:"workload"`
	Clients               int                `json:"clients"`
	Repetition            int                `json:"repetition"`
	Operations            int                `json:"operations"`
	Errors                int                `json:"errors"`
	ElapsedNS             int64              `json:"elapsed_ns"`
	MeasurementStartedUTC string             `json:"measurement_started_utc"`
	Throughput            float64            `json:"successful_ops_per_second"`
	P50                   int64              `json:"p50_ns"`
	P95                   int64              `json:"p95_ns"`
	P99                   int64              `json:"p99_ns"`
	Verified              bool               `json:"verified"`
	Samples               []sample           `json:"samples"`
}
type report struct {
	SchemaVersion     int          `json:"schema_version"`
	Status            string       `json:"status"`
	ActiveTrial       *trialRecord `json:"active_trial,omitempty"`
	Config            configRecord `json:"config"`
	Version           string       `json:"version"`
	Started           string       `json:"started_utc"`
	Results           []result     `json:"results"`
	VerificationError string       `json:"verification_error,omitempty"`
}
type trialRecord struct {
	Workload   string `json:"workload"`
	Clients    int    `json:"clients"`
	Repetition int    `json:"repetition"`
	Phase      string `json:"phase"`
}
type configRecord struct {
	KeySelection                                                        string
	DiagnosticMode                                                      string
	SeedBatch                                                           int
	VerifyEveryTrial                                                    bool
	Engine                                                              string
	Rows, PayloadBytes, Operations, ScanOperations, Warmup, Repetitions int
	Clients                                                             string
	Protocol                                                            string
	Tables                                                              []string
	Workloads                                                           []string
	GroupDistribution                                                   string
	SkewPercent                                                         int
	PhysicalNodes                                                       int
	EndpointCount                                                       int
	EndpointRouting                                                     string
}

const defaultTable = "rf3_sql_bench"
const payload = "abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345abcdefghijklmnopqrstuvwxyz012345"

var defaultWorkloads = []string{"point_hit", "point_miss", "range_64", "group_16", "update_existing"}

func main() {
	var c config
	flag.StringVar(&c.engine, "engine", "", "vibedb or cockroachdb")
	flag.StringVar(&c.url, "url", "", "PostgreSQL URL for a dedicated benchmark database")
	flag.StringVar(&c.output, "output", "", "new JSON evidence file")
	flag.StringVar(&c.phase, "phase", "all", "setup, run, all, or recovery; recovery verifies a saved oracle without writing SQL")
	flag.IntVar(&c.rows, "rows", 8192, "initial rows")
	flag.IntVar(&c.operations, "operations", 2000, "measured point/update operations per trial")
	flag.IntVar(&c.scans, "scans", 200, "measured range/group operations per trial")
	flag.IntVar(&c.warmup, "warmup", 100, "unmeasured operations before each trial")
	flag.IntVar(&c.repetitions, "repetitions", 3, "repetitions per workload and concurrency")
	flag.IntVar(&c.seedBatch, "seed-batch", 64, "rows per untimed INSERT (1..1024; subject to engine admission limits)")
	flag.StringVar(&c.clients, "clients", "1,8", "closed-loop concurrency list (maximum 15)")
	flag.StringVar(&c.tables, "tables", defaultTable, "comma-separated lowercase logical table names; group placement requires runtime inventory")
	flag.StringVar(&c.workloads, "workloads", strings.Join(defaultWorkloads, ","), "comma-separated workloads; default is the five-workload C1/C8 matrix")
	flag.StringVar(&c.groupDistribution, "group-distribution", "uniform", "table selection: uniform or skewed")
	flag.IntVar(&c.skewPercent, "skew-percent", 80, "skewed selection percentage assigned to the first table")
	flag.IntVar(&c.physicalNodes, "physical-nodes", 0, "reported physical-node count for matrix metadata; does not alter routing")
	flag.BoolVar(&c.requireExistingTables, "require-existing-tables", false, "fail if a requested table was not provisioned before setup")
	flag.BoolVar(&c.verifyEveryTrial, "verify-every-trial", true, "verify every row after each trial; false verifies before and after the full run (each operation is always checked)")
	flag.StringVar(&c.urls, "urls", "", "comma-separated PostgreSQL URLs; clients use endpoint client index modulo this list")
	flag.StringVar(&c.diagnosticTargets, "diagnostic-targets", "", "ready candidate PID/node/snapshot bindings for untimed acknowledged diagnostic brackets")
	flag.StringVar(&c.recoveryOracle, "recovery-oracle", "", "export final expected rows to a new file; phase recovery reads this file")
	flag.Parse()
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(c config) (runErr error) {
	if c.seedBatch < 1 || c.seedBatch > 1024 {
		return fmt.Errorf("invalid seed batch")
	}
	if (c.engine != "vibedb" && c.engine != "cockroachdb") || c.url == "" || c.rows < 64 || c.rows > 1000000 || c.operations < 1 || c.operations > 1000000 || c.scans < 1 || c.scans > 100000 || c.warmup < 0 || c.warmup > 100000 || c.repetitions < 1 || c.repetitions > 20 || (c.phase != "all" && c.phase != "setup" && c.phase != "run" && c.phase != "recovery") {
		return fmt.Errorf("invalid benchmark configuration")
	}
	tables, err := parseTables(c.tables)
	if err != nil {
		return err
	}
	workloads, err := parseWorkloads(c.workloads)
	if err != nil {
		return err
	}
	if c.groupDistribution != "uniform" && c.groupDistribution != "skewed" || c.skewPercent < 51 || c.skewPercent > 99 || c.physicalNodes < 0 || c.physicalNodes > 64 {
		return fmt.Errorf("invalid group distribution or physical-node count")
	}
	if c.groupDistribution == "skewed" && len(tables) < 2 {
		return fmt.Errorf("skewed group distribution requires at least two tables")
	}
	endpoints, endpointLabels, err := parseURLs(c.url, c.urls)
	if err != nil {
		return err
	}
	concurrencies, err := parseClients(c.clients)
	if err != nil {
		return err
	}
	for _, n := range concurrencies {
		for _, workload := range workloads {
			count := c.operations
			if strings.HasPrefix(workload, "range_") || workload == "group_16" {
				count = c.scans
			}
			if count < n {
				return fmt.Errorf("operation count must cover every configured client")
			}
		}
	}
	r := report{SchemaVersion: 2, Status: "incomplete", Results: []result{}, VerificationError: "benchmark did not finish", Config: configRecord{SeedBatch: c.seedBatch, VerifyEveryTrial: c.verifyEveryTrial, Engine: c.engine, Rows: c.rows, PayloadBytes: len(payload), Operations: c.operations, ScanOperations: c.scans, Warmup: c.warmup, Repetitions: c.repetitions, Clients: c.clients, Protocol: "extended unnamed parse/bind/execute; text parameters/results; one autocommit statement per operation", Tables: tables, Workloads: workloads, GroupDistribution: c.groupDistribution, SkewPercent: c.skewPercent, PhysicalNodes: c.physicalNodes, EndpointCount: len(endpointLabels), EndpointRouting: "round-robin-per-client"}, Started: time.Now().UTC().Format(time.RFC3339Nano)}
	r.Config.DiagnosticMode = "none"
	r.Config.KeySelection = "splitmix64-independent-with-replacement-v1"
	if c.diagnosticTargets != "" {
		r.Config.DiagnosticMode = "signal-acknowledged-snapshots"
	}
	if c.phase != "setup" {
		// Reserve the path before connecting or verifying, then use atomic
		// checkpoints outside every timed region. A killed client leaves an
		// explicit incomplete report and every previously finished trial.
		f, openErr := os.OpenFile(c.output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if openErr != nil {
			return openErr
		}
		if err := f.Close(); err != nil {
			return err
		}
		defer func() {
			if runErr != nil {
				r.Status = "failed"
				r.VerificationError = safeError(runErr, endpoints)
				runErr = fmt.Errorf("%s", r.VerificationError)
			} else {
				r.Status, r.VerificationError, r.ActiveTrial = "complete", "", nil
			}
			if err := writeReport(c.output, r); err != nil {
				runErr = fmt.Errorf("persist benchmark evidence: %w (benchmark error: %v)", err, runErr)
			}
		}()
		if err := writeReport(c.output, r); err != nil {
			return err
		}
	} else {
		defer func() {
			if runErr != nil {
				runErr = fmt.Errorf("%s", safeError(runErr, endpoints))
			}
		}()
	}
	if c.diagnosticTargets != "" {
		if c.engine != "vibedb" || c.phase == "setup" {
			return fmt.Errorf("diagnostic brackets require a ready VibeDB candidate run")
		}
		c.diagnostics, err = loadDiagnosticControl(c.diagnosticTargets, filepath.Join(filepath.Dir(c.output), "diagnostics"))
		if err != nil {
			return err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	admin, err := pgconn.Connect(ctx, endpoints[0])
	if err != nil {
		return err
	}
	defer admin.Close(context.Background())
	v := admin.ExecParams(ctx, "SELECT version()", nil, nil, nil, nil).Read()
	if v.Err != nil {
		return v.Err
	}
	if len(v.Rows) > 0 {
		r.Version = string(v.Rows[0][0])
	}
	if c.phase == "recovery" {
		scores, err := readRecoveryOracle(c, tables)
		if err != nil {
			return err
		}
		return verify(ctx, admin, c, tables, scores)
	}
	if c.phase != "run" {
		if err = setup(ctx, admin, c, tables); err != nil {
			return err
		}
	}
	if c.phase == "setup" {
		return nil
	}
	scores := make([][]int, len(tables))
	for group := range scores {
		scores[group] = make([]int, c.rows)
		for i := range scores[group] {
			scores[group][i] = i % 100
		}
	}
	if err = verify(ctx, admin, c, tables, scores); err != nil {
		return err
	}
	// No planner tuning or stats commands in the timed region. CockroachDB's
	// normal cost-based optimizer and VibeDB's normal gateway are both enabled.
	for _, workload := range workloads {
		for _, n := range concurrencies {
			for rep := 1; rep <= c.repetitions; rep++ {
				count := c.operations
				if strings.HasPrefix(workload, "range_") || workload == "group_16" {
					count = c.scans
				}
				r.ActiveTrial = &trialRecord{Workload: workload, Clients: n, Repetition: rep, Phase: "preparing-or-measuring"}
				if err := writeReport(c.output, r); err != nil {
					return err
				}
				out, e := trial(ctx, c, workload, n, rep, count, tables, scores, endpoints)
				if e != nil {
					if out.MeasurementStartedUTC != "" {
						r.Results = append(r.Results, out)
						r.ActiveTrial.Phase = "diagnostics"
					}
					return e
				}
				r.Results = append(r.Results, out)
				r.ActiveTrial.Phase = "verifying"
				if err := writeReport(c.output, r); err != nil {
					return err
				}
				var verifyErr error
				verifyThisTrial := c.verifyEveryTrial || workload == "update_uniform" || workload == "mixed_uniform"
				if verifyThisTrial {
					verifyErr = verify(ctx, admin, c, tables, scores)
				}
				out.Verified = verifyThisTrial && verifyErr == nil && out.Errors == 0
				r.Results[len(r.Results)-1].Verified = out.Verified
				fmt.Fprintf(os.Stderr, "%s %s c=%d rep=%d %.1f ops/s p99=%.3fms errors=%d verified=%v\n", c.engine, workload, n, rep, out.Throughput, float64(out.P99)/1e6, out.Errors, out.Verified)
				if verifyErr != nil {
					return verifyErr
				}
				if out.Errors != 0 {
					return fmt.Errorf("operation errors; trial is not a valid throughput result")
				}
				r.ActiveTrial = nil
				if err := writeReport(c.output, r); err != nil {
					return err
				}
			}
		}
	}
	if !c.verifyEveryTrial {
		r.ActiveTrial = &trialRecord{Phase: "verifying-full-run"}
		if err := writeReport(c.output, r); err != nil {
			return err
		}
		if err := verify(ctx, admin, c, tables, scores); err != nil {
			return err
		}
		for i := range r.Results {
			r.Results[i].Verified = r.Results[i].Errors == 0
		}
		r.ActiveTrial = nil
		if err := writeReport(c.output, r); err != nil {
			return err
		}
	}
	if c.recoveryOracle != "" {
		return writeRecoveryOracle(c, tables, scores)
	}
	return nil
}

func writeReport(path string, r report) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".rf3-sqlbench-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := json.NewEncoder(f).Encode(r); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

func parseClients(raw string) ([]int, error) {
	var clients []int
	seen := map[int]bool{}
	for _, value := range strings.Split(raw, ",") {
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 15 || seen[n] {
			return nil, fmt.Errorf("clients must be distinct integers from 1 to 15")
		}
		seen[n] = true
		clients = append(clients, n)
	}
	return clients, nil
}

func safeError(err error, endpoints []string) string {
	message := err.Error()
	for _, endpoint := range endpoints {
		message = strings.ReplaceAll(message, endpoint, "[PostgreSQL endpoint]")
		parsed, parseErr := url.Parse(endpoint)
		if parseErr != nil {
			continue
		}
		if parsed.User != nil {
			message = strings.ReplaceAll(message, "user="+parsed.User.Username(), "user=[redacted]")
			if password, ok := parsed.User.Password(); ok && password != "" {
				message = strings.ReplaceAll(message, password, "[redacted]")
				message = strings.ReplaceAll(message, url.QueryEscape(password), "[redacted]")
			}
		}
		for key, values := range parsed.Query() {
			if key != "password" && key != "sslpassword" {
				continue
			}
			for _, value := range values {
				if value != "" {
					message = strings.ReplaceAll(message, value, "[redacted]")
				}
			}
		}
	}
	return message
}
func parseTables(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("tables must not be empty")
	}
	parts := strings.Split(raw, ",")
	if len(parts) > 63 {
		return nil, fmt.Errorf("too many tables: %d (maximum 63)", len(parts))
	}
	tables := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if !validIdentifier(name) {
			return nil, fmt.Errorf("invalid table name %q", name)
		}
		if _, ok := seen[name]; ok {
			return nil, fmt.Errorf("duplicate table name %q", name)
		}
		seen[name] = struct{}{}
		tables = append(tables, name)
	}
	return tables, nil
}

func parseWorkloads(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("workloads must not be empty")
	}
	allowed := map[string]struct{}{
		"point_hit": {}, "point_miss": {}, "range_32": {}, "range_64": {}, "range_256": {}, "group_16": {},
		"update_existing": {}, "mixed_read_update": {}, "update_uniform": {}, "mixed_uniform": {},
	}
	workloads := make([]string, 0, len(strings.Split(raw, ",")))
	seen := make(map[string]struct{}, len(allowed))
	for _, part := range strings.Split(raw, ",") {
		workload := strings.TrimSpace(part)
		if workload == "mixed" {
			workload = "mixed_read_update"
		}
		if _, ok := allowed[workload]; !ok {
			return nil, fmt.Errorf("invalid workload %q", workload)
		}
		if _, ok := seen[workload]; ok {
			return nil, fmt.Errorf("duplicate workload %q", workload)
		}
		seen[workload] = struct{}{}
		workloads = append(workloads, workload)
	}
	return workloads, nil
}

func validIdentifier(name string) bool {
	if len(name) == 0 || len(name) > 63 {
		return false
	}
	for i, c := range name {
		if c >= 'a' && c <= 'z' || c == '_' || i > 0 && c >= '0' && c <= '9' {
			continue
		}
		return false
	}
	return true
}

// parseURLs validates the endpoint list once, before any workers are started.
// The returned labels intentionally contain only host:port information so a
// report can describe ordinary endpoint routing without retaining credentials
// embedded in PostgreSQL URLs.
func parseURLs(base, raw string) ([]string, []string, error) {
	values := []string{base}
	if strings.TrimSpace(raw) != "" {
		values = strings.Split(raw, ",")
	}
	endpoints := make([]string, 0, len(values))
	labels := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || strings.ContainsAny(value, " \t\r\n") {
			return nil, nil, fmt.Errorf("invalid PostgreSQL endpoint URL")
		}
		parsed, err := url.Parse(value)
		if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") || parsed.Hostname() == "" || parsed.Fragment != "" {
			return nil, nil, fmt.Errorf("invalid PostgreSQL endpoint URL")
		}
		if port := parsed.Port(); port != "" {
			n, portErr := strconv.Atoi(port)
			if portErr != nil || n < 1 || n > 65535 {
				return nil, nil, fmt.Errorf("invalid PostgreSQL endpoint port")
			}
		}
		label := parsed.Host
		if _, ok := seen[label]; ok {
			return nil, nil, fmt.Errorf("duplicate PostgreSQL endpoint %q", label)
		}
		seen[label] = struct{}{}
		endpoints = append(endpoints, value)
		labels = append(labels, label)
	}
	if len(endpoints) == 0 {
		return nil, nil, fmt.Errorf("PostgreSQL endpoint list must not be empty")
	}
	return endpoints, labels, nil
}

func groupFor(c config, groups, ordinal int) int {
	if groups <= 1 {
		return 0
	}
	// Use an independent deterministic stream for placement. An ordinal
	// modulo group count would correlate with the mixed workload's read/write
	// stream and can accidentally make even numbered groups read-only or
	// write-only.
	value := mixOrdinal(uint64(ordinal) + 0x9e3779b97f4a7c15)
	if c.groupDistribution == "skewed" {
		bucket := int(value % 100)
		if bucket < c.skewPercent {
			return 0
		}
		return 1 + int((value/100)%uint64(groups-1))
	}
	return int(value % uint64(groups))
}

func mixOrdinal(value uint64) uint64 {
	value += 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func mixedRead(ordinal int) bool {
	return mixOrdinal(uint64(ordinal)+0xd1b54a32d192ed03)&1 == 0
}

func readKeyFor(rows, rep, ordinal int) int {
	return int(mixOrdinal(uint64(ordinal)+uint64(rep)*0xa0761d6478bd642f+0xe7037ed1a0b428db) % uint64(rows))
}

// uniformKeyFor selects a key from the stripe owned by client. Every client
// gets the keys whose ids have id%clients == client, including when rows is
// not divisible by clients. The stream is independent of the operation and
// table/group streams so mixed traffic remains approximately half reads and
// half updates at every placement.
func uniformKeyFor(rows, clients, client, rep, ordinal int) int {
	stripeLength := (rows-1-client)/clients + 1
	value := mixOrdinal(uint64(ordinal) + uint64(rep)*0x8cb92baa5d7e1b43 + 0x4f1bbcdc676f3e2d)
	return client + clients*int(value%uint64(stripeLength))
}

func operationFor(workload string, ordinal int) string {
	if workload == "mixed_read_update" || workload == "mixed_uniform" {
		if mixedRead(ordinal) {
			return "point_hit"
		}
		return "update_existing"
	}
	if workload == "update_uniform" {
		return "update_existing"
	}
	return workload
}

func setup(ctx context.Context, conn *pgconn.PgConn, c config, tables []string) error {
	for _, table := range tables {
		if c.requireExistingTables {
			probe := conn.ExecParams(ctx, "SELECT 1 FROM "+table+" LIMIT 0", nil, nil, nil, nil).Read()
			if probe.Err != nil {
				return fmt.Errorf("required table %s is not provisioned: %w", table, probe.Err)
			}
		}
		ddl := "CREATE TABLE IF NOT EXISTS " + table + " (id TEXT PRIMARY KEY, bucket INTEGER NOT NULL, score INTEGER NOT NULL, payload TEXT NOT NULL)"
		if e := conn.ExecParams(ctx, ddl, nil, nil, nil, nil).Read().Err; e != nil {
			return fmt.Errorf("create %s: %w", table, e)
		}
		for first := 0; first < c.rows; first += c.seedBatch {
			if first%65536 == 0 {
				fmt.Fprintf(os.Stderr, "seeding %s %d/%d rows\n", table, first, c.rows)
			}
			var sql strings.Builder
			sql.WriteString("INSERT INTO " + table + " (id,bucket,score,payload) VALUES ")
			for i := first; i < min(first+c.seedBatch, c.rows); i++ {
				if i != first {
					sql.WriteByte(',')
				}
				fmt.Fprintf(&sql, "('%s',%d,%d,'%s')", key(i), i%16, i%100, payload)
			}
			res := conn.ExecParams(ctx, sql.String(), nil, nil, nil, nil).Read()
			if res.Err != nil {
				return fmt.Errorf("seed %s row %d: %w", table, first, res.Err)
			}
			if res.CommandTag.RowsAffected() != int64(min(c.seedBatch, c.rows-first)) {
				return fmt.Errorf("seed %s affected rows", table)
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
					return fmt.Errorf("RF3 proof failed for %s: %v", table, res.Err)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
				}
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
func verify(ctx context.Context, conn *pgconn.PgConn, c config, tables []string, scores [][]int) error {
	// Validate every stored field after every trial, outside the timing window.
	for group, table := range tables {
		count := conn.ExecParams(ctx, "SELECT COUNT(*) FROM "+table, nil, nil, nil, nil).Read()
		if count.Err != nil {
			return count.Err
		}
		if len(count.Rows) != 1 || len(count.Rows[0]) != 1 || string(count.Rows[0][0]) != strconv.Itoa(c.rows) {
			return fmt.Errorf("verification row count mismatch for %s", table)
		}
		for first := 0; first < c.rows; first += 512 {
			res := conn.ExecParams(ctx, "SELECT id,bucket,score,payload FROM "+table+" WHERE id >= $1 ORDER BY id LIMIT 512", [][]byte{[]byte(key(first))}, []uint32{25}, nil, nil).Read()
			if res.Err != nil {
				return fmt.Errorf("verify %s: %w", table, res.Err)
			}
			if len(res.Rows) != min(512, c.rows-first) {
				return fmt.Errorf("verification page count mismatch for %s", table)
			}
			for j, row := range res.Rows {
				i := first + j
				if len(row) != 4 || textCell(c, row[0]) != key(i) || string(row[1]) != strconv.Itoa(i%16) || string(row[2]) != strconv.Itoa(scores[group][i]) || textCell(c, row[3]) != payload {
					return fmt.Errorf("row %d in %s differs from oracle", i, table)
				}
			}
		}
	}
	return nil
}
func trial(ctx context.Context, c config, workload string, clients, rep, count int, tables []string, scores [][]int, endpoints []string) (result, error) {
	if len(endpoints) == 0 {
		return result{}, fmt.Errorf("PostgreSQL endpoint list must not be empty")
	}
	rangeRows := 64
	if strings.HasPrefix(workload, "range_") {
		rangeRows, _ = strconv.Atoi(strings.TrimPrefix(workload, "range_"))
	}
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
		p, e := pgconn.Connect(ctx, endpoints[i%len(endpoints)])
		if e != nil {
			return out, e
		}
		connections[i] = p
	}
	operation := func(conn *pgconn.PgConn, client, ordinal int) error {
		// Independent deterministic streams select keys, tables and operations.
		// Updates use disjoint per-client keys; this makes no contention claim.
		group := groupFor(c, len(tables), ordinal)
		table := tables[group]
		id := readKeyFor(c.rows, rep, ordinal)
		sql := ""
		var params [][]byte
		switch workload {
		case "point_hit":
			sql = "SELECT id,bucket,score,payload FROM " + table + " WHERE id=$1"
			params = [][]byte{[]byte(key(id))}
		case "point_miss":
			sql = "SELECT id,bucket,score,payload FROM " + table + " WHERE id=$1"
			params = [][]byte{[]byte(key(c.rows + id))}
		case "range_32", "range_64", "range_256":
			id = (id / rangeRows) * rangeRows
			sql = "SELECT id,score FROM " + table + " WHERE id >= $1 ORDER BY id LIMIT " + strconv.Itoa(rangeRows)
			params = [][]byte{[]byte(key(id))}
		case "group_16":
			sql = "SELECT bucket,COUNT(*),SUM(score) FROM " + table + " GROUP BY bucket ORDER BY bucket"
		case "update_existing":
			id = client
			sql = "UPDATE " + table + " SET score=score+1 WHERE id=$1"
			params = [][]byte{[]byte(key(id))}
		case "update_uniform":
			id = uniformKeyFor(c.rows, clients, client, rep, ordinal)
			sql = "UPDATE " + table + " SET score=score+1 WHERE id=$1"
			params = [][]byte{[]byte(key(id))}
		case "mixed_read_update":
			if mixedRead(ordinal) {
				// Read keys and per-client update keys are disjoint. This keeps the
				// mixed workload's verification deterministic without claiming
				// serializability under hot-key contention.
				id = clients + readKeyFor(c.rows-clients, rep, ordinal)
				sql = "SELECT id,bucket,score,payload FROM " + table + " WHERE id=$1"
				params = [][]byte{[]byte(key(id))}
			} else {
				id = client
				sql = "UPDATE " + table + " SET score=score+1 WHERE id=$1"
				params = [][]byte{[]byte(key(id))}
			}
		case "mixed_uniform":
			id = uniformKeyFor(c.rows, clients, client, rep, ordinal)
			if mixedRead(ordinal) {
				sql = "SELECT id,bucket,score,payload FROM " + table + " WHERE id=$1"
				params = [][]byte{[]byte(key(id))}
			} else {
				sql = "UPDATE " + table + " SET score=score+1 WHERE id=$1"
				params = [][]byte{[]byte(key(id))}
			}
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
			if textCell(c, row[0]) != key(id) || string(row[1]) != strconv.Itoa(id%16) || string(row[2]) != strconv.Itoa(scores[group][id]) || textCell(c, row[3]) != payload {
				return fmt.Errorf("point mismatch")
			}
		case "point_miss":
			if len(res.Rows) != 0 {
				return fmt.Errorf("miss returned rows")
			}
		case "range_32", "range_64", "range_256":
			if len(res.Rows) != min(rangeRows, c.rows-id) {
				return fmt.Errorf("range count")
			}
			for j, row := range res.Rows {
				if len(row) != 2 || textCell(c, row[0]) != key(id+j) || string(row[1]) != strconv.Itoa(scores[group][id+j]) {
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
					sum += scores[group][k]
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
			scores[group][id]++
		case "update_uniform":
			if res.CommandTag.RowsAffected() != 1 {
				return fmt.Errorf("uniform update affected %d", res.CommandTag.RowsAffected())
			}
			scores[group][id]++
		case "mixed_read_update":
			if mixedRead(ordinal) {
				if len(res.Rows) != 1 || len(res.Rows[0]) != 4 {
					return fmt.Errorf("mixed read result shape")
				}
				row := res.Rows[0]
				if textCell(c, row[0]) != key(id) || string(row[1]) != strconv.Itoa(id%16) || string(row[2]) != strconv.Itoa(scores[group][id]) || textCell(c, row[3]) != payload {
					return fmt.Errorf("mixed read mismatch")
				}
			} else {
				if res.CommandTag.RowsAffected() != 1 {
					return fmt.Errorf("mixed update affected %d", res.CommandTag.RowsAffected())
				}
				scores[group][id]++
			}
		case "mixed_uniform":
			if mixedRead(ordinal) {
				if len(res.Rows) != 1 || len(res.Rows[0]) != 4 {
					return fmt.Errorf("uniform mixed read result shape")
				}
				row := res.Rows[0]
				if textCell(c, row[0]) != key(id) || string(row[1]) != strconv.Itoa(id%16) || string(row[2]) != strconv.Itoa(scores[group][id]) || textCell(c, row[3]) != payload {
					return fmt.Errorf("uniform mixed read mismatch")
				}
			} else {
				if res.CommandTag.RowsAffected() != 1 {
					return fmt.Errorf("uniform mixed update affected %d", res.CommandTag.RowsAffected())
				}
				scores[group][id]++
			}
		}
		return nil
	}
	for i := 0; i < c.warmup; i++ {
		if e := operation(connections[i%clients], i%clients, i); e != nil {
			return out, fmt.Errorf("%s warmup: %w", workload, e)
		}
	}
	var beforeCompleted time.Time
	if c.diagnostics != nil {
		out.Diagnostics = &diagnosticBracket{}
		before, err := c.diagnostics.capture(ctx, workload, clients, rep, "before")
		out.Diagnostics.Before = before
		if err != nil {
			return out, err
		}
		beforeCompleted = time.Now()
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var measurementStart time.Time
	for client := 0; client < clients; client++ {
		wg.Add(1)
		go func(client int) {
			defer wg.Done()
			<-start
			for ordinal := client; ordinal < count; ordinal += clients {
				operationStart := time.Now()
				err := operation(connections[client], client, ordinal)
				operationEnd := time.Now()
				group := groupFor(c, len(tables), ordinal)
				s := sample{Client: client, Ordinal: ordinal, NS: operationEnd.Sub(operationStart).Nanoseconds(), StartOffsetNS: operationStart.Sub(measurementStart).Nanoseconds(), Group: group, Table: tables[group], Endpoint: client % len(endpoints), Operation: operationFor(workload, ordinal)}
				if err != nil {
					s.Error = safeError(err, endpoints)
				}
				out.Samples[ordinal] = s
			}
		}(client)
	}
	// Publish the anchor immediately before closing the start channel. A close
	// synchronizes the write with every receiver, so worker launch time is
	// outside the measured window while offsets remain monotonic and correlate
	// with the published UTC anchor.
	measurementStart = time.Now()
	close(start)
	wg.Wait()
	out.ElapsedNS = time.Since(measurementStart).Nanoseconds()
	// Formatting the wall-clock anchor after the measured work avoids adding
	// RFC3339 formatting to any operation's critical path. The monotonic
	// component retained by time.Time is used for all offsets and elapsed time.
	out.MeasurementStartedUTC = measurementStart.UTC().Format(time.RFC3339Nano)
	var diagnosticErr error
	if out.Diagnostics != nil {
		out.Diagnostics.BeforeCompletedOffsetNS = beforeCompleted.Sub(measurementStart).Nanoseconds()
		out.Diagnostics.AfterStartedOffsetNS = time.Since(measurementStart).Nanoseconds()
		out.Diagnostics.After, diagnosticErr = c.diagnostics.capture(ctx, workload, clients, rep, "after")
		if diagnosticErr == nil {
			out.Diagnostics.Deltas, diagnosticErr = diagnosticDeltas(out.Diagnostics.Before, out.Diagnostics.After)
		}
	}
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
	return out, diagnosticErr
}
