// Command vibedb-shard serves either a static development shard or one process
// bundle containing 1..64 prepared members of authenticated three-replica Raft
// groups. The RF3 command opens exact retained WAL, SQL, apply, topology, and
// certificate identities; it never initializes or repairs missing replicated
// state.
//
// It admits every request against its configured identity before executing, and
// executes the admitted statement locally through the ordinary vibedb parser and
// planner. The wire contract carries SQL text plus typed parameters only; no
// serialized execution plan crosses it.
//
// serve durably advances the bound store's local ownership-epoch and
// routing-version high-waters before listening. That excludes stale or duplicate
// serving over the same open store, but is not distributed election or lease
// authority and cannot revoke a process serving a copied store.
//
// Usage:
//
//	vibedb-shard init -store <path> -distribution <name> -shard <id> \
//	    -allocation-generation <n>
//
//	vibedb-shard serve -store <path> -listen <addr> \
//	    -distribution <name> -shard <id> -allocation-generation <n> \
//	    -epoch <n> -routing-version <n>
//
//	vibedb-shard serve-rf3 -manifest <path>
//
//	vibedb-shard prepare-rf3 -manifest <path>
//
//	vibedb-shard bootstrap-rf3 -manifest <path>
//
//	vibedb-shard adopt-restored-rf3 -manifest <path>
//
// It serves until interrupted, then closes the listener, drains in-flight
// connections, and releases the catalog.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/processprofile"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func main() {
	os.Exit(runProfiled(os.Args))
}

func runProfiled(args []string) int {
	if len(args) > 1 && args[1] == "serve-rf3" {
		stop, err := processprofile.StartFromEnv("shard")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		defer stop()
	}
	return run(args)
}

func run(args []string) int {
	if len(args) < 2 {
		usage()
		return 2
	}
	switch args[1] {
	case "init":
		return runInit(args[2:])
	case "serve":
		return runServe(args[2:])
	case "serve-rf3":
		return runServeRF3(args[2:])
	case "prepare-node-group-rf3":
		return runPrepareNodeGroupRF3(args[2:])
	case "prepare-node-rf3":
		return runPrepareNodeRF3(args[2:])
	case "prepare-rf3":
		return runPrepareRF3(args[2:])
	case "bootstrap-rf3":
		return runBootstrapRF3(args[2:])
	case "adopt-restored-rf3":
		return runAdoptRestoredRF3(args[2:])
	default:
		usage()
		return 2
	}
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

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  vibedb-shard init -store <path> -distribution <name> "+
		"-shard <id> -allocation-generation <n>")
	fmt.Fprintln(os.Stderr, "  vibedb-shard serve -store <path> -listen <addr> "+
		"-distribution <name> -shard <id> -allocation-generation <n> "+
		"-epoch <n> -routing-version <n>")
	fmt.Fprintln(os.Stderr, "  vibedb-shard serve-rf3 -manifest <path>")
	fmt.Fprintln(os.Stderr, "  vibedb-shard prepare-rf3 -manifest <path>")
	fmt.Fprintln(os.Stderr, "  vibedb-shard prepare-node-rf3 -manifest <path>")
	fmt.Fprintln(os.Stderr, "  vibedb-shard prepare-node-group-rf3 -manifest <path>")
	fmt.Fprintln(os.Stderr, "  vibedb-shard bootstrap-rf3 -manifest <path>")
	fmt.Fprintln(os.Stderr, "  vibedb-shard adopt-restored-rf3 -manifest <path>")
}

func runInit(args []string) int {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	store := fs.String("store", "", "path for the new local vibedb shard catalog file")
	distName := fs.String("distribution", "", "owned distribution group name")
	shard := fs.String("shard", "", "owned shard identifier")
	allocation := fs.Uint64("allocation-generation", 0, "topology shard allocation generation")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *store == "" || *distName == "" || *shard == "" || *allocation == 0 {
		usage()
		return 2
	}
	binding := sqldriver.ShardStoreBinding{
		Distribution:         distribution.DistributionName(*distName),
		Shard:                distribution.ShardID(*shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(*allocation),
	}
	db, err := sqldriver.InitializeShardStore(*store, binding)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error initialize store=%q: %v\n", *store, err)
		return 1
	}
	identity, identityErr := db.ShardStoreIdentity()
	closeErr := db.Close()
	if identityErr != nil || closeErr != nil {
		fmt.Fprintf(os.Stderr, "error finish store=%q: %v\n", *store,
			errors.Join(identityErr, closeErr))
		return 1
	}
	fmt.Fprintf(os.Stderr,
		"vibedb-shard initialized distribution=%q shard=%q allocation-generation=%d log-id=%x store=%q\n",
		identity.Distribution, identity.Shard, identity.AllocationGeneration,
		identity.LogID, *store,
	)
	return 0
}

func runServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	var (
		store               = fs.String("store", "", "path to the local vibedb catalog file")
		listen              = fs.String("listen", "127.0.0.1:0", "host:port to serve on")
		distName            = fs.String("distribution", "", "owned distribution group name")
		shard               = fs.String("shard", "", "owned shard identifier")
		allocation          = fs.Uint64("allocation-generation", 0, "topology shard allocation generation")
		epoch               = fs.Uint64("epoch", 0, "static ownership epoch")
		routingVersion      = fs.Uint64("routing-version", 0, "routed manifest generation")
		maxConns            = fs.Int("max-connections", 0, "0 selects the default; -1 is unlimited")
		devPlaintext        = fs.Bool("dev-plaintext-loopback", false, "explicitly permit unauthenticated loopback development serving")
		tlsCertificate      = fs.String("tls-certificate", "", "PEM shard certificate chain")
		tlsKey              = fs.String("tls-key", "", "PEM shard private key")
		tlsRoots            = fs.String("tls-roots", "", "PEM gateway trust roots")
		tlsIdentityOID      = fs.String("tls-identity-oid", "", "operator VibeDB identity OID")
		tlsTimeout          = fs.Duration("tls-handshake-timeout", 5*time.Second, "hard TLS handshake deadline")
		maxHandshakes       = fs.Int("max-handshakes", 32, "hard concurrent TLS handshake bound")
		authorizationPolicy = fs.String("authorization-policy", "", "bounded vibejson principal/capability policy")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *store == "" || *distName == "" || *shard == "" || *allocation == 0 ||
		*epoch == 0 || *routingVersion == 0 {
		usage()
		return 2
	}
	var authenticated *servicetls.Server
	var authorization *serviceauthz.Gate
	authenticatedConnectionLimit := *maxConns
	if authenticatedConnectionLimit == 0 {
		authenticatedConnectionLimit = shardservice.DefaultMaxConnections
	}
	if *devPlaintext {
		if *tlsCertificate != "" || *tlsKey != "" || *tlsRoots != "" || *tlsIdentityOID != "" || *authorizationPolicy != "" {
			fmt.Fprintln(os.Stderr, "error: development plaintext and TLS configuration are mutually exclusive")
			return 2
		}
		if err := requireLoopbackListen(*listen); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return 2
		}
	} else {
		profile, err := servicetls.LoadProfile(*tlsCertificate, *tlsKey, *tlsRoots, *tlsIdentityOID, time.Now)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error load TLS profile: %v\n", err)
			return 2
		}
		policy, policyErr := serviceauthz.LoadFile(*authorizationPolicy)
		if policyErr != nil {
			fmt.Fprintf(os.Stderr, "error authorization policy: %v\n", policyErr)
			return 2
		}
		authorizer, err := servicetls.NewNodeAuthorizer(policy.NodesWith(serviceauthz.CapabilityDelegate))
		if err != nil {
			fmt.Fprintf(os.Stderr, "error gateway authorization: %v\n", err)
			return 2
		}
		authenticated, err = servicetls.NewServer(profile, rafttransport.TrafficShardSQL, authorizer)
		if err != nil || *tlsTimeout <= 0 || authenticatedConnectionLimit <= 0 ||
			authenticatedConnectionLimit > servicetls.AbsoluteMaxConnections || *maxHandshakes <= 0 ||
			*maxHandshakes > authenticatedConnectionLimit {
			fmt.Fprintf(os.Stderr, "error authenticated listener profile: %v\n", err)
			return 2
		}
		authorization, err = serviceauthz.NewGate(policy)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error authorization gate: %v\n", err)
			return 2
		}
	}

	binding := sqldriver.ShardStoreBinding{
		Distribution:         distribution.DistributionName(*distName),
		Shard:                distribution.ShardID(*shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(*allocation),
	}
	db, err := sqldriver.OpenShardStore(*store, binding)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open store=%q: %v\n", *store, err)
		return 1
	}
	defer db.Close()

	cfg := shardservice.Ownership{
		Distribution:         binding.Distribution,
		Shard:                binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		Epoch:                distribution.OwnershipEpoch(*epoch),
		RoutingVersion:       distribution.RoutingVersion(*routingVersion),
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
	defer srv.Close()

	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error listen=%q: %v\n", *listen, err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "vibedb-shard serving distribution=%q shard=%q allocation-generation=%d epoch=%d routing-version=%d on %s\n",
		*distName, *shard, *allocation, *epoch, *routingVersion, listener.Addr())

	// Close the server when the process is signaled, which unblocks Serve.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	if authenticated != nil {
		err = authenticated.Serve(ctx, listener, servicetls.Limits{
			MaxConnections: authenticatedConnectionLimit, MaxHandshakes: *maxHandshakes,
			HandshakeDeadline: servicetls.FixedDeadline(*tlsTimeout),
		}, func(_ context.Context, connection rafttransport.PeerConnection) {
			srv.ServeAuthorizedConn(connection, authorization, nil)
		})
	} else {
		err = srv.Serve(listener)
	}
	if err != nil && err != shardservice.ErrServerClosed && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "error serve: %v\n", err)
		return 1
	}
	return 0
}

// requireLoopbackListen keeps the unauthenticated shard protocol local until
// an authenticated transport is available. Passing a non-loopback address
// would let a peer issue shard SQL and DDL without credentials.
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
