package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/servicetls"
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
// pinned generation: a bounded distributed read by default, one colocated
// single-shard write for exec, or an atomic fixed-participant write batch for
// exec_batch. The reply is the merged
// result. The wire form is a minimal JSON envelope; a request
// carries SQL, typed parameters, and an operational class. The pinned catalog
// and shared SQL planner derive placement, shard constraints, merge order, and
// the global limit. The envelope itself is decoded and emitted with vibejson.

// serveRequest is one query envelope a client sends. SQL and its typed
// parameters are the only semantic inputs; clients cannot override routing or
// merge metadata independently of the statement.
type serveRequest struct {
	// Op selects the gateway operation: the empty value and "query" are the
	// read path; "exec" is the single-shard write path; "exec_batch" uses
	// Statements and applies one Class to the complete atomic batch.
	Op         string           `json:"op,omitempty"`
	SQL        string           `json:"sql"`
	Class      string           `json:"class,omitempty"`
	Params     []serveParam     `json:"params,omitempty"`
	Statements []serveStatement `json:"statements,omitempty"`
}

type serveStatement struct {
	SQL    string       `json:"sql"`
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

type repeatedFlag []string

func (values *repeatedFlag) String() string { return strings.Join(*values, ",") }
func (values *repeatedFlag) Set(value string) error {
	if value == "" || len(*values) >= servicetls.AbsoluteMaxIdentities {
		return servicetls.ErrInvalidProfile
	}
	*values = append(*values, value)
	return nil
}

// runServe loads the catalog, binds the listener, and serves until interrupted.
func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	catalog := fs.String("catalog", "", "path to the persisted catalog generation")
	listen := fs.String("listen", "127.0.0.1:0", "host:port to serve on")
	devPlaintext := fs.Bool("dev-plaintext-loopback", false, "explicitly permit unauthenticated loopback development serving")
	tlsCertificate := fs.String("tls-certificate", "", "PEM gateway certificate chain")
	tlsKey := fs.String("tls-key", "", "PEM gateway private key")
	tlsRoots := fs.String("tls-roots", "", "PEM client trust roots")
	tlsIdentityOID := fs.String("tls-identity-oid", "", "operator VibeDB identity OID")
	tlsHandshakeTimeout := fs.Duration("tls-handshake-timeout", 5*time.Second, "hard TLS handshake deadline")
	maxConnections := fs.Int("max-client-connections", 1024, "hard authenticated client connection bound")
	maxHandshakes := fs.Int("max-client-handshakes", 64, "hard concurrent TLS handshake bound")
	var allowedClients repeatedFlag
	fs.Var(&allowedClients, "allow-client-node", "allowed 32-character hexadecimal client NodeID; repeat for each principal")
	var shardPeers repeatedFlag
	fs.Var(&shardPeers, "shard-peer", "authenticated shard address=32-character-hex-NodeID; repeat for each endpoint")
	maxShardConnections := fs.Int("max-shard-connections", 4096, "hard authenticated gateway-to-shard connection bound")
	maxShardHandshakes := fs.Int("max-shard-handshakes", 64, "hard concurrent gateway-to-shard TLS handshake bound")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *catalog == "" {
		usage()
		return 2
	}
	var clientTLS *gateway.ClientTLS
	var shardTLS *servicetls.Client
	if *devPlaintext {
		if *tlsCertificate != "" || *tlsKey != "" || *tlsRoots != "" || *tlsIdentityOID != "" ||
			len(allowedClients) != 0 || len(shardPeers) != 0 {
			fmt.Fprintln(os.Stderr, "gateway: development plaintext and TLS configuration are mutually exclusive")
			return 2
		}
		if err := requireLoopbackListen(*listen); err != nil {
			fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
			return 2
		}
	} else {
		profile, err := servicetls.LoadProfile(*tlsCertificate, *tlsKey, *tlsRoots, *tlsIdentityOID, time.Now)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gateway: load TLS profile: %v\n", err)
			return 2
		}
		allowed := make([]rafttransport.NodeID, len(allowedClients))
		for index, encoded := range allowedClients {
			allowed[index], err = servicetls.ParseNodeID(encoded)
			if err != nil {
				fmt.Fprintf(os.Stderr, "gateway: allowed client %d: %v\n", index, err)
				return 2
			}
		}
		clientTLS, err = gateway.NewClientTLS(profile, allowed)
		if err != nil || *tlsHandshakeTimeout <= 0 || *maxConnections <= 0 ||
			*maxConnections > servicetls.AbsoluteMaxConnections || *maxHandshakes <= 0 ||
			*maxHandshakes > *maxConnections {
			fmt.Fprintf(os.Stderr, "gateway: invalid authenticated listener profile: %v\n", err)
			return 2
		}
		endpoints := make([]servicetls.Endpoint, len(shardPeers))
		for index, encoded := range shardPeers {
			separator := strings.LastIndexByte(encoded, '=')
			if separator <= 0 || separator == len(encoded)-1 {
				fmt.Fprintf(os.Stderr, "gateway: shard peer %d is not address=node-id\n", index)
				return 2
			}
			endpoints[index].Address = encoded[:separator]
			endpoints[index].Node, err = servicetls.ParseNodeID(encoded[separator+1:])
			if err != nil {
				fmt.Fprintf(os.Stderr, "gateway: shard peer %d: %v\n", index, err)
				return 2
			}
		}
		shardTLS, err = servicetls.NewClient(servicetls.ClientOptions{
			TLS: profile, Class: rafttransport.TrafficShardSQL, Endpoints: endpoints,
			Dial: func(ctx context.Context, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", address)
			},
			HandshakeDeadline: servicetls.FixedDeadline(*tlsHandshakeTimeout),
			MaxConnections:    *maxShardConnections, MaxHandshakes: *maxShardHandshakes,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "gateway: invalid authenticated shard transport: %v\n", err)
			return 2
		}
		defer shardTLS.Close()
	}

	var shardDial gateway.DialFunc
	if shardTLS != nil {
		shardDial = shardTLS.Dial
	}
	exec, holder, err := newGatewayWithDial(*catalog, shardDial)
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
	if clientTLS != nil {
		err = serveAuthenticatedGateway(ctx, listener, exec, clientTLS, gateway.ClientTLSLimits{
			MaxConnections: *maxConnections, MaxHandshakes: *maxHandshakes,
			HandshakeDeadline: servicetls.FixedDeadline(*tlsHandshakeTimeout),
		}, logf)
	} else {
		err = serveGateway(ctx, listener, exec, logf)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "gateway: serve: %v\n", err)
		return 1
	}
	return 0
}

// requireLoopbackListen keeps the explicitly selected unauthenticated
// development protocol from becoming a remotely reachable query endpoint.
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
	return newGatewayWithDial(catalogPath, nil)
}

func newGatewayWithDial(catalogPath string, dial gateway.DialFunc) (*gateway.Executor, *gateway.CatalogHolder, error) {
	snap, err := gateway.LoadSnapshot(catalogPath)
	if err != nil {
		return nil, nil, err
	}
	holder := gateway.NewCatalogHolder(snap)
	refresher := gateway.NewFileCatalogRefresher(catalogPath, holder)
	exec := gateway.NewExecutor(gateway.NewClient(dial), holder, gateway.Options{Refresh: refresher.Refresh})
	return exec, holder, nil
}

// serveGateway accepts connections until ctx is canceled, then closes the
// listener and drains in-flight connections. It returns nil on a signaled
// shutdown and the accept error otherwise.
func serveGateway(ctx context.Context, listener net.Listener, exec *gateway.Executor, logf func(string, ...any)) error {
	startGatewayRecovery(ctx, exec, logf)
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

func startGatewayRecovery(ctx context.Context, exec *gateway.Executor, logf func(string, ...any)) {
	go exec.RunRecovery(ctx, 5*time.Second, func(results []gateway.RecoveryResult, err error) {
		if err != nil {
			logf("gateway: transaction recovery: %v", err)
		}
		if len(results) != 0 {
			logf("gateway: transaction recovery resolved %d coordinator(s)", len(results))
		}
	})
}

func serveAuthenticatedGateway(ctx context.Context, listener net.Listener, exec *gateway.Executor,
	capability *gateway.ClientTLS, limits gateway.ClientTLSLimits, logf func(string, ...any)) error {
	startGatewayRecovery(ctx, exec, logf)
	return capability.ServeAuthenticatedClients(ctx, listener, limits,
		func(ctx context.Context, connection net.Conn) { handleConn(ctx, connection, exec, logf) })
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
	var res *gateway.Result
	var err error
	switch req.Op {
	case "exec_batch":
		queries, buildErr := buildBatchQueries(req)
		if buildErr != nil {
			return &serveResponse{Error: buildErr.Error()}
		}
		res, err = exec.ExecBatch(ctx, queries)
	case "exec":
		// The write path routes the statement to its single owning shard and
		// refuses every scatter before any dispatch.
		q, buildErr := buildQuery(req)
		if buildErr != nil {
			return &serveResponse{Error: buildErr.Error()}
		}
		res, err = exec.Exec(ctx, q)
	case "", "query":
		q, buildErr := buildQuery(req)
		if buildErr != nil {
			return &serveResponse{Error: buildErr.Error()}
		}
		res, err = exec.Query(ctx, q)
	default:
		return &serveResponse{Error: fmt.Sprintf("unknown operation %q", req.Op)}
	}
	if err != nil {
		return &serveResponse{Error: err.Error()}
	}
	return encodeResult(res)
}

func buildBatchQueries(req serveRequest) ([]gateway.Query, error) {
	if req.SQL != "" || len(req.Params) != 0 {
		return nil, errors.New("exec_batch uses statements instead of top-level sql or params")
	}
	if len(req.Statements) == 0 {
		return nil, gateway.ErrBatchEmpty
	}
	class, err := parseClass(req.Class)
	if err != nil {
		return nil, err
	}
	queries := make([]gateway.Query, len(req.Statements))
	for i := range req.Statements {
		params, err := buildParams(req.Statements[i].Params)
		if err != nil {
			return nil, fmt.Errorf("statement %d: %w", i, err)
		}
		queries[i] = gateway.Query{SQL: req.Statements[i].SQL, Params: params, Class: class}
	}
	return queries, nil
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
