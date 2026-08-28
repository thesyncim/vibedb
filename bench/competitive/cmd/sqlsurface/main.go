// Command sqlsurface drives one deterministic SQL workload through either
// database/sql or a real loopback PostgreSQL wire connection. Setup and the
// final byte oracle are outside the timed interval.
package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	_ "github.com/thesyncim/vibedb/bench/competitive"
	"github.com/thesyncim/vibedb/bench/competitive/cmd/internal/hostmetrics"
	"github.com/thesyncim/vibedb/pgwire"
	vibedriver "github.com/thesyncim/vibedb/sql/driver"
	vibejson "github.com/thesyncim/vibejson"
)

type config struct {
	engine, surface    string
	corpus, operations int
	maxRSS, maxWrites  int64
	requireWrites      bool
}

type sqlClient interface {
	exec(context.Context, string) error
	query(context.Context, string) ([]byte, error)
	close() error
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "sqlsurface:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "vibedb-sqlsurface-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	client, stop, err := openClient(cfg, dir)
	if err != nil {
		return err
	}
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	docs := make([][]byte, cfg.corpus)
	for i := range docs {
		docs[i] = sqlDocument(i, false)
		if !vibejson.Valid(docs[i]) {
			return fmt.Errorf("generated document %d is invalid", i)
		}
	}
	if err := createSchema(ctx, client, cfg.engine); err != nil {
		return err
	}
	for i, document := range docs {
		if err := client.exec(ctx, insertSQL(cfg.engine, i, document)); err != nil {
			return fmt.Errorf("seed %d: %w", i, err)
		}
	}
	before, known, err := hostmetrics.LinuxPhysicalWriteBytes()
	if err != nil {
		return err
	}
	if cfg.requireWrites && !known {
		return errors.New("Linux process write_bytes is required")
	}
	latencies := make([]int64, cfg.operations)
	updated := make([]bool, cfg.corpus)
	logicalBytes := uint64(0)
	started := time.Now()
	for operation := range cfg.operations {
		i := (operation*7919 + 17) % cfg.corpus
		begin := time.Now()
		if operation&1 == 0 {
			got, err := client.query(ctx, selectSQL(cfg.engine, i))
			if err != nil || !bytes.Equal(got, docs[i]) {
				return fmt.Errorf("read %d bytes=%d want=%d: %w", i, len(got), len(docs[i]), err)
			}
		} else {
			docs[i] = sqlDocument(i, !updated[i])
			updated[i] = !updated[i]
			if err := client.exec(ctx, updateSQL(cfg.engine, i, docs[i])); err != nil {
				return fmt.Errorf("update %d: %w", i, err)
			}
			logicalBytes += uint64(len(sqlKey(i)) + len(docs[i]))
		}
		latencies[operation] = time.Since(begin).Nanoseconds()
	}
	elapsed := time.Since(started)
	after, afterKnown, err := hostmetrics.LinuxPhysicalWriteBytes()
	if err != nil {
		return err
	}
	known = known && afterKnown
	writes := int64(0)
	if known {
		writes = after - before
		if writes < 0 {
			return errors.New("process write counter regressed")
		}
		if cfg.requireWrites && writes == 0 {
			return errors.New("process write counter stayed zero")
		}
		if cfg.maxWrites > 0 && writes > cfg.maxWrites {
			return fmt.Errorf("process writes %d exceed %d", writes, cfg.maxWrites)
		}
	} else if cfg.requireWrites {
		return errors.New("process write counter became unavailable")
	}
	for i, want := range docs {
		got, err := client.query(ctx, selectSQL(cfg.engine, i))
		if err != nil || !bytes.Equal(got, want) {
			return fmt.Errorf("final oracle %d: %w", i, err)
		}
	}
	wantIndexed := sqlKey(0)
	if cfg.surface == "pgwire" {
		wantIndexed = strconv.Quote(wantIndexed)
	}
	if got, err := client.query(ctx, indexedSQL(cfg.engine)); err != nil || string(got) != wantIndexed {
		return fmt.Errorf("indexed oracle got=%q: %v", got, err)
	}
	rss, err := hostmetrics.MaxRSSBytes()
	if err != nil {
		return err
	}
	if cfg.maxRSS > 0 && rss > cfg.maxRSS {
		return fmt.Errorf("peak RSS %d exceeds %d", rss, cfg.maxRSS)
	}
	slices.Sort(latencies)
	p999 := percentile(latencies, 999, 1000)
	maximum := latencies[len(latencies)-1]
	throughput := float64(cfg.operations) / elapsed.Seconds()
	source := "unavailable"
	if known {
		source = "linux-proc-self-io-write_bytes"
	}
	fmt.Fprintln(out, "engine\tinterface\tdurability\texact-indexes\tdocument-shape\tdocs\toperations\treads\twrites\tp99.9-us\tmax-us\ttotal-ops/s\tlogical-write-bytes\tphysical-write-known\tphysical-write-source\tphysical-write-bytes\tmax-physical-write-bytes\tpeak-rss-bytes\tmax-rss-bytes")
	fmt.Fprintf(out, "%s\t%s\tpower-safe\t1\tinline\t%d\t%d\t%d\t%d\t%.3f\t%.3f\t%.0f\t%d\t%t\t%s\t%d\t%d\t%d\t%d\n",
		cfg.engine, cfg.surface, cfg.corpus, cfg.operations, (cfg.operations+1)/2, cfg.operations/2,
		float64(p999)/1000, float64(maximum)/1000, throughput, logicalBytes, known, source,
		writes, cfg.maxWrites, rss, cfg.maxRSS)
	return nil
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("sqlsurface", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cfg config
	fs.StringVar(&cfg.engine, "engine", "vibedb", "vibedb or sqlite")
	fs.StringVar(&cfg.surface, "interface", "database-sql", "database-sql or pgwire")
	fs.IntVar(&cfg.corpus, "corpus", 1000, "seed documents")
	fs.IntVar(&cfg.operations, "operations", 10000, "timed operations")
	fs.Int64Var(&cfg.maxRSS, "max-rss-bytes", 1<<30, "hard peak RSS bound")
	fs.Int64Var(&cfg.maxWrites, "max-physical-write-bytes", 4<<30, "hard Linux process write bound")
	fs.BoolVar(&cfg.requireWrites, "require-physical-write", false, "fail without Linux process write evidence")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 || cfg.corpus < 1 || cfg.operations < 2 || cfg.maxRSS < 0 || cfg.maxWrites < 0 {
		return cfg, errors.New("invalid SQL surface configuration")
	}
	if cfg.surface != "database-sql" && cfg.surface != "pgwire" {
		return cfg, errors.New("unknown interface")
	}
	if cfg.engine != "vibedb" && cfg.engine != "sqlite" {
		return cfg, errors.New("unknown engine")
	}
	if cfg.surface == "pgwire" && cfg.engine != "vibedb" {
		return cfg, errors.New("pgwire is available only for vibedb")
	}
	return cfg, nil
}

type databaseSQLClient struct{ db *sql.DB }

func (client databaseSQLClient) exec(ctx context.Context, statement string) error {
	_, err := client.db.ExecContext(ctx, statement)
	return err
}
func (client databaseSQLClient) query(ctx context.Context, statement string) ([]byte, error) {
	rows, err := client.db.QueryContext(ctx, statement)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil || !rows.Next() {
		return nil, errors.Join(err, rows.Err(), sql.ErrNoRows)
	}
	values := make([][]byte, len(columns))
	destinations := make([]any, len(columns))
	for i := range values {
		destinations[i] = &values[i]
	}
	if err := rows.Scan(destinations...); err != nil {
		return nil, err
	}
	return encodeSQLRow(values)
}
func (client databaseSQLClient) close() error { return client.db.Close() }

func openClient(cfg config, dir string) (sqlClient, func(), error) {
	path := filepath.Join(dir, "catalog.db")
	if cfg.surface == "database-sql" {
		driverName, dsn := "vibedb", path
		if cfg.engine == "sqlite" {
			driverName = "sqlite"
			dsn += "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=fullfsync(1)&_pragma=busy_timeout(5000)"
		}
		db, err := sql.Open(driverName, dsn)
		if err != nil {
			return nil, nil, err
		}
		db.SetMaxOpenConns(1)
		if err := db.Ping(); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
		client := databaseSQLClient{db: db}
		return client, func() { _ = client.close() }, nil
	}
	database, err := vibedriver.Open(path)
	if err != nil {
		return nil, nil, err
	}
	server, err := pgwire.NewServer(database, pgwire.Options{Auth: pgwire.Trust(), Database: "bench"})
	if err != nil {
		_ = database.Close()
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = server.Close()
		_ = database.Close()
		return nil, nil, err
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	wire, err := dialWire(listener.Addr().String())
	if err != nil {
		_ = server.Close()
		_ = listener.Close()
		_ = database.Close()
		return nil, nil, err
	}
	stop := func() { _ = wire.close(); _ = server.Close(); _ = listener.Close(); <-serveDone; _ = database.Close() }
	return wire, stop, nil
}

func createSchema(ctx context.Context, client sqlClient, engine string) error {
	statements := []string{"CREATE TABLE docs (id STRING PRIMARY KEY, grp STRING NOT NULL)", "CREATE INDEX docs_by_grp ON docs(grp)"}
	if engine == "sqlite" {
		statements = []string{"CREATE TABLE docs (id TEXT PRIMARY KEY, grp TEXT NOT NULL, doc BLOB NOT NULL) WITHOUT ROWID", "CREATE INDEX docs_by_grp ON docs(grp)"}
	}
	for _, statement := range statements {
		if err := client.exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func sqlKey(i int) string { return fmt.Sprintf("doc:%06d", i) }
func sqlDocument(i int, updated bool) []byte {
	value := 0
	if updated {
		value = 1
	}
	return []byte(fmt.Sprintf(`{"id":"%s","grp":"g%02d","value":%d,"pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, sqlKey(i), i%17, value))
}
func literal(value []byte) string { return "'" + string(value) + "'" }
func insertSQL(engine string, i int, document []byte) string {
	if engine == "sqlite" {
		return fmt.Sprintf("INSERT INTO docs(id,grp,doc) VALUES('%s','g%02d',%s)", sqlKey(i), i%17, literal(document))
	}
	return "INSERT INTO docs VALUES (" + literal(document) + ")"
}
func selectSQL(engine string, i int) string {
	if engine == "sqlite" {
		return fmt.Sprintf("SELECT doc FROM docs WHERE id = '%s'", sqlKey(i))
	}
	return fmt.Sprintf("SELECT id, grp, value, pad FROM docs WHERE id = '%s'", sqlKey(i))
}
func updateSQL(engine string, i int, document []byte) string {
	if engine == "sqlite" {
		return fmt.Sprintf("UPDATE docs SET doc=%s WHERE id='%s'", literal(document), sqlKey(i))
	}
	return fmt.Sprintf("UPDATE docs SET \"$doc\"=%s WHERE id='%s'", literal(document), sqlKey(i))
}
func indexedSQL(engine string) string {
	if engine == "sqlite" {
		return "SELECT id FROM docs WHERE grp='g00' ORDER BY id LIMIT 1"
	}
	return "SELECT id FROM docs WHERE grp='g00' ORDER BY id LIMIT 1"
}
func percentile(sorted []int64, numerator, denominator int) int64 {
	index := (len(sorted)*numerator+denominator-1)/denominator - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func encodeSQLRow(values [][]byte) ([]byte, error) {
	if len(values) == 1 {
		return bytes.Clone(values[0]), nil
	}
	if len(values) != 4 {
		return nil, fmt.Errorf("SQL row has %d columns, want 1 or 4", len(values))
	}
	result := make([]byte, 0, 64+len(values[0])+len(values[1])+len(values[2])+len(values[3]))
	result = append(result, `{"id":`...)
	result = appendSQLString(result, values[0])
	result = append(result, `,"grp":`...)
	result = appendSQLString(result, values[1])
	result = append(result, `,"value":`...)
	result = append(result, values[2]...)
	result = append(result, `,"pad":`...)
	result = appendSQLString(result, values[3])
	result = append(result, '}')
	if !vibejson.Valid(result) {
		return nil, fmt.Errorf("SQL row reconstruction is invalid vibejson: %q from %q", result, values)
	}
	return result, nil
}

func appendSQLString(destination, value []byte) []byte {
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' && vibejson.Valid(value) {
		return append(destination, value...)
	}
	return strconv.AppendQuote(destination, string(value))
}

type wireClient struct {
	connection net.Conn
	reader     *bufio.Reader
}

func dialWire(address string) (*wireClient, error) {
	connection, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		return nil, err
	}
	client := &wireClient{connection: connection, reader: bufio.NewReader(connection)}
	payload := append([]byte{0, 3, 0, 0}, []byte("user\x00bench\x00database\x00bench\x00\x00")...)
	message := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(message, uint32(len(message)))
	copy(message[4:], payload)
	if _, err := connection.Write(message); err != nil {
		_ = connection.Close()
		return nil, err
	}
	for {
		kind, _, err := client.readMessage()
		if err != nil {
			_ = connection.Close()
			return nil, err
		}
		if kind == 'E' {
			_ = connection.Close()
			return nil, errors.New("pgwire startup error")
		}
		if kind == 'Z' {
			break
		}
	}
	return client, nil
}
func (client *wireClient) exec(ctx context.Context, statement string) error {
	_, err := client.queryMessage(ctx, statement, false)
	return err
}
func (client *wireClient) query(ctx context.Context, statement string) ([]byte, error) {
	return client.queryMessage(ctx, statement, true)
}
func (client *wireClient) close() error {
	message := []byte{'X', 0, 0, 0, 4}
	_, _ = client.connection.Write(message)
	return client.connection.Close()
}
func (client *wireClient) queryMessage(ctx context.Context, statement string, wantRow bool) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		if err := client.connection.SetDeadline(deadline); err != nil {
			return nil, err
		}
		defer client.connection.SetDeadline(time.Time{})
	}
	payload := append([]byte(statement), 0)
	message := make([]byte, 5+len(payload))
	message[0] = 'Q'
	binary.BigEndian.PutUint32(message[1:5], uint32(4+len(payload)))
	copy(message[5:], payload)
	if _, err := client.connection.Write(message); err != nil {
		return nil, err
	}
	var row []byte
	var serverErr bool
	for {
		kind, body, err := client.readMessage()
		if err != nil {
			return nil, err
		}
		switch kind {
		case 'D':
			values, err := parseDataRow(body)
			if err != nil {
				return nil, err
			}
			row, err = encodeSQLRow(values)
			if err != nil {
				return nil, err
			}
		case 'E':
			serverErr = true
		case 'Z':
			if serverErr {
				return nil, errors.New("pgwire query error")
			}
			if wantRow && row == nil {
				return nil, errors.New("pgwire query returned no row")
			}
			return row, nil
		}
	}
}

func parseDataRow(body []byte) ([][]byte, error) {
	if len(body) < 2 {
		return nil, errors.New("short DataRow")
	}
	count := int(binary.BigEndian.Uint16(body[:2]))
	body = body[2:]
	values := make([][]byte, count)
	for i := range values {
		if len(body) < 4 {
			return nil, errors.New("short DataRow length")
		}
		n := int32(binary.BigEndian.Uint32(body[:4]))
		body = body[4:]
		if n < 0 || int(n) > len(body) {
			return nil, errors.New("invalid DataRow value length")
		}
		values[i] = bytes.Clone(body[:n])
		body = body[n:]
	}
	if len(body) != 0 {
		return nil, errors.New("DataRow has trailing bytes")
	}
	return values, nil
}
func (client *wireClient) readMessage() (byte, []byte, error) {
	kind, err := client.reader.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	var lengthRaw [4]byte
	if _, err = io.ReadFull(client.reader, lengthRaw[:]); err != nil {
		return 0, nil, err
	}
	length := int(binary.BigEndian.Uint32(lengthRaw[:]))
	if length < 4 || length > 1<<20 {
		return 0, nil, errors.New("invalid pgwire message length")
	}
	body := make([]byte, length-4)
	_, err = io.ReadFull(client.reader, body)
	return kind, body, err
}
