package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/shardservice"
	vibejson "github.com/thesyncim/vibejson"
)

// maxServeRequestBytes bounds one newline-delimited JSON envelope before JSON
// decoding or SQL parsing. Scanner grows only for an actually large request and
// releases the buffer with the connection.
const maxServeRequestBytes = 1 << 20

// The serve subcommand: a stateless routing front-end. It loads an immutable
// catalog generation, refreshes the atomically replaced catalog file after a
// shard reports stale routing metadata, and accepts newline-delimited JSON
// requests over a connection. Each request routes and dispatches against the
// pinned generation: a bounded distributed read by default, or one colocated
// single-shard write when the envelope's op is exec. The reply is the merged
// result. The wire form is a minimal JSON envelope; a request
// carries SQL, typed parameters, and an operational class. The pinned catalog
// and shared SQL planner derive placement, shard constraints, merge order, and
// the global limit. The envelope itself is decoded and emitted with vibejson.

// serveRequest is one query envelope a client sends. SQL and its typed
// parameters are the only semantic inputs; clients cannot override routing or
// merge metadata independently of the statement.
type serveRequest struct {
	// Op selects the gateway operation: the empty value and "query" are the
	// read path; "exec" is the single-shard write path.
	Op     string       `json:"op,omitempty"`
	SQL    string       `json:"sql"`
	Class  string       `json:"class,omitempty"`
	Params []serveParam `json:"params,omitempty"`
}

// serveParam is one typed bound parameter in placeholder order.
type serveParam struct {
	Kind string `json:"kind"`
	Bool bool   `json:"bool,omitempty"`
	Text string `json:"text,omitempty"`
}

// serveResponse is the merged reply plus the routing metadata a client reads for
// observability. Rows carries each cell as raw JSON (a null cell is the JSON
// literal null); Error is set instead when the operation failed.
type serveResponse struct {
	Kind         string            `json:"kind,omitempty"`
	Columns      []string          `json:"columns,omitempty"`
	Rows         [][]serveRawValue `json:"rows,omitempty"`
	RowsAffected int64             `json:"rows_affected,omitempty"`
	Route        string            `json:"route,omitempty"`
	Generation   uint64            `json:"generation,omitempty"`
	ShardsFanned int               `json:"shards_fanned,omitempty"`
	Retries      int               `json:"retries,omitempty"`
	Error        string            `json:"error,omitempty"`
}

// serveRawValue is one already-encoded JSON cell. The methods preserve test and
// client interoperability with encoding/json without using it in the server;
// production output writes the bytes directly through vibejson.Writer.
type serveRawValue []byte

func (r serveRawValue) MarshalJSON() ([]byte, error) { return r, nil }

func (r *serveRawValue) UnmarshalJSON(src []byte) error {
	*r = append((*r)[:0], src...)
	return nil
}

// runServe loads the catalog, binds the listener, and serves until interrupted.
func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	catalog := fs.String("catalog", "", "path to the persisted catalog generation")
	listen := fs.String("listen", "127.0.0.1:0", "host:port to serve on")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *catalog == "" {
		usage()
		return 2
	}
	if err := requireLoopbackListen(*listen); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		return 2
	}

	exec, holder, err := newGateway(*catalog)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: load catalog %q: %v\n", *catalog, err)
		return 1
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: listen %q: %v\n", *listen, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "vibedb-gateway serving catalog generation %d on %s\n",
		holder.Current().Generation(), listener.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logf := func(format string, a ...any) { fmt.Fprintf(os.Stderr, format+"\n", a...) }
	if err := serveGateway(ctx, listener, exec, logf); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: serve: %v\n", err)
		return 1
	}
	return 0
}

// requireLoopbackListen keeps the unauthenticated development protocol from
// becoming a remotely reachable admin/query endpoint. Remote serving needs an
// authenticated transport, which this newline-delimited JSON protocol does not
// provide yet.
func requireLoopbackListen(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen address %q is invalid: %w", address, err)
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("listen address %q must be loopback; remote unauthenticated serving is refused", address)
	}
	return nil
}

// newGateway loads the initial catalog generation and returns an executor that
// dispatches leader-only strong reads over the default TCP client. A stale
// shard refusal reloads the same crash-safe catalog path, publishing only a
// strictly newer valid generation.
func newGateway(catalogPath string) (*gateway.Executor, *gateway.CatalogHolder, error) {
	snap, err := gateway.LoadSnapshot(catalogPath)
	if err != nil {
		return nil, nil, err
	}
	holder := gateway.NewCatalogHolder(snap)
	refresher := gateway.NewFileCatalogRefresher(catalogPath, holder)
	exec := gateway.NewExecutor(gateway.NewClient(nil), holder, gateway.Options{Refresh: refresher.Refresh})
	return exec, holder, nil
}

// serveGateway accepts connections until ctx is canceled, then closes the
// listener and drains in-flight connections. It returns nil on a signaled
// shutdown and the accept error otherwise.
func serveGateway(ctx context.Context, listener net.Listener, exec *gateway.Executor, logf func(string, ...any)) error {
	// Closing the listener when ctx is done unblocks a blocked Accept, so a
	// signal shuts the loop down without a poll.
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			wg.Wait()
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handleConn(ctx, conn, exec, logf)
		}()
	}
}

// handleConn serves newline-delimited JSON requests on one connection until the
// peer disconnects or the server shuts down. Closing the connection when ctx is
// done unblocks a blocked decode so a signaled shutdown drains promptly.
func handleConn(ctx context.Context, conn net.Conn, exec *gateway.Executor, logf func(string, ...any)) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 4096), maxServeRequestBytes)
	writer := vibejson.NewWriter(conn)
	for scanner.Scan() {
		var req serveRequest
		if err := vibejson.Unmarshal(scanner.Bytes(), &req); err != nil {
			if ctx.Err() == nil {
				logf("gateway: decode request: %v", err)
			}
			return
		}
		if err := writeServeResponse(writer, execRequest(ctx, exec, req)); err != nil {
			if ctx.Err() == nil {
				logf("gateway: encode response: %v", err)
			}
			return
		}
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		logf("gateway: decode request: %v", err)
	}
}

// writeServeResponse emits one NDJSON response without converting raw result
// cells into strings or passing them through a generic JSON tree.
func writeServeResponse(w *vibejson.Writer, resp *serveResponse) error {
	if err := w.BeginObject(); err != nil {
		return err
	}
	stringField := func(name, value string) error {
		if value == "" {
			return nil
		}
		if err := w.Key(name); err != nil {
			return err
		}
		return w.String(value)
	}
	if err := stringField("kind", resp.Kind); err != nil {
		return err
	}
	if len(resp.Columns) != 0 {
		if err := w.Key("columns"); err != nil {
			return err
		}
		if err := w.BeginArray(); err != nil {
			return err
		}
		for _, column := range resp.Columns {
			if err := w.String(column); err != nil {
				return err
			}
		}
		if err := w.EndArray(); err != nil {
			return err
		}
	}
	if len(resp.Rows) != 0 {
		if err := w.Key("rows"); err != nil {
			return err
		}
		if err := w.BeginArray(); err != nil {
			return err
		}
		for _, row := range resp.Rows {
			if err := w.BeginArray(); err != nil {
				return err
			}
			for _, cell := range row {
				if err := w.RawUnchecked(cell); err != nil {
					return err
				}
			}
			if err := w.EndArray(); err != nil {
				return err
			}
		}
		if err := w.EndArray(); err != nil {
			return err
		}
	}
	if resp.RowsAffected != 0 {
		if err := w.Key("rows_affected"); err != nil {
			return err
		}
		if err := w.Int(resp.RowsAffected); err != nil {
			return err
		}
	}
	if err := stringField("route", resp.Route); err != nil {
		return err
	}
	if resp.Generation != 0 {
		if err := w.Key("generation"); err != nil {
			return err
		}
		if err := w.Uint(resp.Generation); err != nil {
			return err
		}
	}
	if resp.ShardsFanned != 0 {
		if err := w.Key("shards_fanned"); err != nil {
			return err
		}
		if err := w.Int(int64(resp.ShardsFanned)); err != nil {
			return err
		}
	}
	if resp.Retries != 0 {
		if err := w.Key("retries"); err != nil {
			return err
		}
		if err := w.Int(int64(resp.Retries)); err != nil {
			return err
		}
	}
	if err := stringField("error", resp.Error); err != nil {
		return err
	}
	if err := w.EndObject(); err != nil {
		return err
	}
	if err := w.Newline(); err != nil {
		return err
	}
	return w.Flush()
}

// execRequest translates one request and dispatches it, mapping any failure into
// an error reply rather than dropping the connection.
func execRequest(ctx context.Context, exec *gateway.Executor, req serveRequest) *serveResponse {
	q, err := buildQuery(req)
	if err != nil {
		return &serveResponse{Error: err.Error()}
	}
	var res *gateway.Result
	if req.Op == "exec" {
		// The write path routes the statement to its single owning shard and
		// refuses every scatter before any dispatch.
		res, err = exec.Exec(ctx, q)
	} else {
		res, err = exec.Query(ctx, q)
	}
	if err != nil {
		return &serveResponse{Error: err.Error()}
	}
	return encodeResult(res)
}

// buildQuery turns a request envelope into a gateway query. Placement, routing,
// ordering, and limiting are deliberately absent here: the executor derives
// them from SQL against its pinned catalog generation.
func buildQuery(req serveRequest) (gateway.Query, error) {
	params, err := buildParams(req.Params)
	if err != nil {
		return gateway.Query{}, err
	}
	class, err := parseClass(req.Class)
	if err != nil {
		return gateway.Query{}, err
	}
	return gateway.Query{
		SQL:    req.SQL,
		Params: params,
		Class:  class,
	}, nil
}

// buildParams maps the typed request parameters onto shard-service parameters in
// placeholder order.
func buildParams(in []serveParam) ([]shardservice.Param, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]shardservice.Param, len(in))
	for i, p := range in {
		switch p.Kind {
		case "null":
			out[i] = shardservice.NullParam()
		case "bool":
			out[i] = shardservice.BoolParam(p.Bool)
		case "number":
			out[i] = shardservice.NumberParam(p.Text)
		case "string":
			out[i] = shardservice.StringParam(p.Text)
		case "document":
			out[i] = shardservice.DocumentParam(p.Text)
		default:
			return nil, fmt.Errorf("unknown parameter kind %q", p.Kind)
		}
	}
	return out, nil
}

// parseClass maps the request's class name onto an operational class, defaulting
// to the interactive profile.
func parseClass(name string) (gateway.OperationClass, error) {
	switch name {
	case "", "interactive":
		return gateway.ClassInteractive, nil
	case "batch":
		return gateway.ClassBatch, nil
	case "admin":
		return gateway.ClassAdmin, nil
	default:
		return 0, fmt.Errorf("unknown class %q", name)
	}
}

// encodeResult renders a merged result as a reply envelope, carrying each cell as
// raw JSON so an already-encoded value is not re-encoded.
func encodeResult(res *gateway.Result) *serveResponse {
	resp := &serveResponse{
		Kind:         res.Kind.String(),
		RowsAffected: res.RowsAffected,
		Route:        res.RouteKind.String(),
		Generation:   res.Generation,
		ShardsFanned: res.ShardsFanned,
		Retries:      res.Retries,
	}
	for _, col := range res.Columns {
		resp.Columns = append(resp.Columns, col.Name)
	}
	if len(res.Rows) > 0 {
		resp.Rows = make([][]serveRawValue, len(res.Rows))
		for i, row := range res.Rows {
			cells := make([]serveRawValue, len(row))
			for j, c := range row {
				if c.Null {
					cells[j] = serveRawValue("null")
				} else {
					cells[j] = serveRawValue(c.Bytes)
				}
			}
			resp.Rows[i] = cells
		}
	}
	return resp
}
