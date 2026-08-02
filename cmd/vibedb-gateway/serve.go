package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/shardservice"
)

// The serve subcommand: a stateless routing front-end. It loads one immutable
// catalog generation, then accepts newline-delimited JSON requests over a
// connection, routes and dispatches each as a bounded distributed read against
// the pinned generation, and replies with the merged result. The wire form is a
// minimal, stdlib-only JSON envelope; a request names a distribution, an optional
// shard-key binding (absent means scatter), the SQL and typed parameters each
// shard runs locally, an operational class, and the cross-shard sort keys and
// global limit the gateway merges under.

// serveRequest is one query envelope a client sends. Key is the bound shard-key
// value; a nil Key routes as a scatter across every active shard subject to the
// class's admission.
type serveRequest struct {
	Distribution string          `json:"distribution"`
	Key          *scalarValue    `json:"key,omitempty"`
	SQL          string          `json:"sql"`
	Class        string          `json:"class,omitempty"`
	Params       []serveParam    `json:"params,omitempty"`
	Order        []serveOrderKey `json:"order,omitempty"`
	Limit        int             `json:"limit,omitempty"`
}

// scalarValue is a bound shard-key value in exactly one closed placement scalar
// form: a string or an exact-decimal number spelling.
type scalarValue struct {
	String *string `json:"string,omitempty"`
	Number *string `json:"number,omitempty"`
}

// serveParam is one typed bound parameter in placeholder order.
type serveParam struct {
	Kind string `json:"kind"`
	Bool bool   `json:"bool,omitempty"`
	Text string `json:"text,omitempty"`
}

// serveOrderKey names one cross-shard sort column and its direction.
type serveOrderKey struct {
	Column int  `json:"column"`
	Desc   bool `json:"desc,omitempty"`
}

// serveResponse is the merged reply plus the routing metadata a client reads for
// observability. Rows carries each cell as raw JSON (a null cell is the JSON
// literal null); Error is set instead when the operation failed.
type serveResponse struct {
	Kind         string              `json:"kind,omitempty"`
	Columns      []string            `json:"columns,omitempty"`
	Rows         [][]json.RawMessage `json:"rows,omitempty"`
	RowsAffected int64               `json:"rows_affected,omitempty"`
	Route        string              `json:"route,omitempty"`
	Generation   uint64              `json:"generation,omitempty"`
	ShardsFanned int                 `json:"shards_fanned,omitempty"`
	Retries      int                 `json:"retries,omitempty"`
	Error        string              `json:"error,omitempty"`
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
	if err := serveGateway(ctx, listener, exec, holder, logf); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: serve: %v\n", err)
		return 1
	}
	return 0
}

// newGateway loads and pins one catalog generation and returns an executor that
// dispatches leader-only strong reads over the default TCP client.
func newGateway(catalogPath string) (*gateway.Executor, *gateway.CatalogHolder, error) {
	snap, err := gateway.LoadSnapshot(catalogPath)
	if err != nil {
		return nil, nil, err
	}
	holder := gateway.NewCatalogHolder(snap)
	exec := gateway.NewExecutor(gateway.NewClient(nil), holder, gateway.Options{})
	return exec, holder, nil
}

// serveGateway accepts connections until ctx is canceled, then closes the
// listener and drains in-flight connections. It returns nil on a signaled
// shutdown and the accept error otherwise.
func serveGateway(ctx context.Context, listener net.Listener, exec *gateway.Executor, holder *gateway.CatalogHolder, logf func(string, ...any)) error {
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
			handleConn(ctx, conn, exec, holder, logf)
		}()
	}
}

// handleConn serves newline-delimited JSON requests on one connection until the
// peer disconnects or the server shuts down. Closing the connection when ctx is
// done unblocks a blocked decode so a signaled shutdown drains promptly.
func handleConn(ctx context.Context, conn net.Conn, exec *gateway.Executor, holder *gateway.CatalogHolder, logf func(string, ...any)) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	dec := json.NewDecoder(conn)
	enc := json.NewEncoder(conn)
	for {
		var req serveRequest
		if err := dec.Decode(&req); err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				logf("gateway: decode request: %v", err)
			}
			return
		}
		if err := enc.Encode(execRequest(ctx, exec, holder, req)); err != nil {
			if ctx.Err() == nil {
				logf("gateway: encode response: %v", err)
			}
			return
		}
	}
}

// execRequest translates one request against the pinned generation and dispatches
// it, mapping any failure into an error reply rather than dropping the connection.
func execRequest(ctx context.Context, exec *gateway.Executor, holder *gateway.CatalogHolder, req serveRequest) *serveResponse {
	q, err := buildQuery(holder, req)
	if err != nil {
		return &serveResponse{Error: err.Error()}
	}
	res, err := exec.Query(ctx, q)
	if err != nil {
		return &serveResponse{Error: err.Error()}
	}
	return encodeResult(res)
}

// buildQuery turns a request envelope into a gateway query, resolving the
// distribution's arity from the current catalog to build the mapper and its bound
// constraints. A nil key scatters; a present key binds the leading ordinal.
func buildQuery(holder *gateway.CatalogHolder, req serveRequest) (gateway.Query, error) {
	snap := holder.Current()
	if snap == nil {
		return gateway.Query{}, errors.New("no catalog generation is published")
	}
	dist := distribution.DistributionName(req.Distribution)
	spec, ok := snap.Spec(dist)
	if !ok {
		return gateway.Query{}, fmt.Errorf("distribution %q is not in the catalog", req.Distribution)
	}
	cons, err := buildConstraints(spec.Arity, req.Key)
	if err != nil {
		return gateway.Query{}, err
	}
	params, err := buildParams(req.Params)
	if err != nil {
		return gateway.Query{}, err
	}
	class, err := parseClass(req.Class)
	if err != nil {
		return gateway.Query{}, err
	}
	order := make([]gateway.OrderKey, len(req.Order))
	for i, k := range req.Order {
		order[i] = gateway.OrderKey{Column: k.Column, Desc: k.Desc}
	}
	return gateway.Query{
		Distribution: dist,
		Constraints:  cons,
		Mapper:       distribution.NewNativeMapper(spec.Arity),
		SQL:          req.SQL,
		Params:       params,
		Class:        class,
		Order:        order,
		Limit:        req.Limit,
	}, nil
}

// buildConstraints builds one domain per shard-key ordinal: every ordinal is
// unknown for a scatter, and a present key binds the leading ordinal to that
// finite value while the rest stay unknown.
func buildConstraints(arity int, key *scalarValue) (distribution.BoundConstraints, error) {
	cons := make(distribution.BoundConstraints, arity)
	for i := range cons {
		cons[i] = distribution.UnknownDomain()
	}
	if key == nil {
		return cons, nil
	}
	if arity < 1 {
		return nil, errors.New("distribution has no shard-key ordinal to bind")
	}
	scalar, err := key.scalar()
	if err != nil {
		return nil, err
	}
	finite, err := distribution.FiniteDomain(scalar)
	if err != nil {
		return nil, err
	}
	cons[0] = finite
	return cons, nil
}

// scalar returns the closed placement scalar the bound key names.
func (v scalarValue) scalar() (distribution.Scalar, error) {
	switch {
	case v.String != nil:
		return distribution.NewString(*v.String), nil
	case v.Number != nil:
		return distribution.NewNumber(*v.Number)
	default:
		return distribution.Scalar{}, errors.New("key must set string or number")
	}
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
		resp.Rows = make([][]json.RawMessage, len(res.Rows))
		for i, row := range res.Rows {
			cells := make([]json.RawMessage, len(row))
			for j, c := range row {
				if c.Null {
					cells[j] = json.RawMessage("null")
				} else {
					cells[j] = json.RawMessage(c.Bytes)
				}
			}
			resp.Rows[i] = cells
		}
	}
	return resp
}
