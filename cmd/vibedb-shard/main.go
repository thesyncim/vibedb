// Command vibedb-shard is the leader-only shard service: it opens one local
// vibedb SQL catalog, adopts a static ownership identity (distribution, shard,
// ownership epoch, and routing version), and serves the shard-service wire
// contract over a stdlib length-prefixed transport.
//
// It admits every request against its configured identity before executing, and
// executes the admitted statement locally through the ordinary vibedb parser and
// planner. The wire contract carries SQL text plus typed parameters only; no
// serialized execution plan crosses it.
//
// Usage:
//
//	vibedb-shard serve -store <path> -listen <addr> \
//	    -distribution <name> -shard <id> -epoch <n> -routing-version <n>
//
// It serves until interrupted, then closes the listener, drains in-flight
// connections, and releases the catalog.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	os.Exit(run(os.Args))
}

func run(args []string) int {
	if len(args) < 2 {
		usage()
		return 2
	}
	switch args[1] {
	case "serve":
		return runServe(args[2:])
	default:
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  vibedb-shard serve -store <path> -listen <addr> "+
		"-distribution <name> -shard <id> -epoch <n> -routing-version <n>")
}

func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	var (
		store          = fs.String("store", "", "path to the local vibedb catalog file")
		listen         = fs.String("listen", "127.0.0.1:0", "host:port to serve on")
		distName       = fs.String("distribution", "", "owned distribution group name")
		shard          = fs.String("shard", "", "owned shard identifier")
		epoch          = fs.Uint64("epoch", 0, "static ownership epoch")
		routingVersion = fs.Uint64("routing-version", 0, "routed manifest generation")
		maxConns       = fs.Int("max-connections", 0, "0 selects the default; -1 is unlimited")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *store == "" || *distName == "" || *shard == "" {
		usage()
		return 2
	}

	db, err := sqldriver.Open(*store)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open store=%q: %v\n", *store, err)
		return 1
	}
	defer db.Close()

	cfg := shardservice.Ownership{
		Distribution:   distribution.DistributionName(*distName),
		Shard:          distribution.ShardID(*shard),
		Epoch:          distribution.OwnershipEpoch(*epoch),
		RoutingVersion: distribution.RoutingVersion(*routingVersion),
	}
	srv, err := shardservice.NewServer(db, cfg, shardservice.Options{
		MaxConnections: *maxConns,
		OnError: func(err error) {
			fmt.Fprintf(os.Stderr, "connection error: %v\n", err)
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error configure shard: %v\n", err)
		return 1
	}

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listen=%q: %v\n", *listen, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "vibedb-shard serving distribution=%q shard=%q epoch=%d routing-version=%d on %s\n",
		*distName, *shard, *epoch, *routingVersion, listener.Addr())

	// Close the server when the process is signaled, which unblocks Serve.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	if err := srv.Serve(listener); err != nil && err != shardservice.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "error serve: %v\n", err)
		return 1
	}
	return 0
}
