package gatewayruntime

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
)

// runServe parses the standalone command's compatibility flags and forwards
// the complete configuration to the reusable Runtime lifecycle. It contains
// no gateway assembly of its own.
func runServe(args []string) (exitCode int) {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	catalog := fs.String("catalog", "", "path to the persisted catalog generation")
	var tableCatalogs repeatedFlag
	fs.Var(&tableCatalogs, "register-table-catalog", "authenticated provisioned single-table catalog to register before serving (repeatable)")
	catalogRouteSeed := fs.String("catalog-route-seed", "", "path to the last authenticated crash-safe catalog route seed")
	devStaticCatalog := fs.Bool("dev-static-catalog", false, "explicitly use the local catalog file as development authority")
	catalogBootstrapIfMissing := fs.Bool("catalog-bootstrap-if-missing", false, "atomically publish and attest the immutable generation-one catalog proof when authority is empty")
	catalogRelation := fs.Uint("catalog-relation", 0, "authenticated relation ID storing catalog and operation records")
	catalogAttempts := fs.Int("catalog-attempts", 8, "bounded leader-routing attempts for replicated catalog operations")
	catalogAttemptTimeout := fs.Duration("catalog-attempt-timeout", 5*time.Second, "per-endpoint replicated catalog attempt deadline")
	catalogSessionJournal := fs.String("catalog-session-journal", "", "durable native controller session journal base path")
	durableAckKeyPath := fs.String("durable-ack-key", "", "cluster-shared 64-character lowercase hexadecimal durable ACK key file")
	catalogClientID := fs.String("catalog-client-id", "", "stable 32-hex-character controller client identity")
	catalogRetryHome := fs.String("catalog-retry-home", "", "stable 16-hex-character controller retry-home identity")
	catalogSessionLease := fs.Duration("catalog-session-lease", 24*time.Hour, "monotonic controller session renewal interval")
	controllerInterval := fs.Duration("controller-interval", time.Second, "bounded replicated split reconciliation interval")
	hotShardCapacity := fs.String("hot-shard-capacity", "", "strict canonical vibejson provisioned pressure capacities")
	hotShardInterval := fs.Duration("hot-shard-interval", time.Second, "pressure-window publication cadence; not correctness authority")
	replicaControlManifestPath := fs.String("replica-control-manifest", "", "strict canonical vibejson replica-control topology and bounds")
	backupRepositoryPath := fs.String("backup-repository", "", "absolute server-local durable backup repository directory")
	backupMaxBackups := fs.Int("backup-max-backups", 16, "hard retained certified backup bound")
	backupMaxArtifacts := fs.Int("backup-max-artifacts", 4096, "hard retained backup artifact bound")
	backupMaxArtifactBytes := fs.Uint64("backup-max-artifact-bytes", 64<<30, "hard bytes per backup artifact")
	backupMaxDiskBytes := fs.Uint64("backup-max-disk-bytes", 256<<30, "hard aggregate backup repository bytes")
	schemaRolloutPlan := fs.String("schema-rollout-plan", "", "strict canonical vibejson per-replica schema rollout plan")
	schemaRolloutOnce := fs.Bool("schema-rollout-once", false, "execute the authenticated schema rollout and exit")
	listen := fs.String("listen", "127.0.0.1:0", "host:port to serve on")
	pgDevListen := fs.String("pg-dev-listen", "", "optional loopback-only, trust-authenticated PostgreSQL endpoint with durable auto-commit writes")
	pgDevDDLSocket := fs.String("pg-dev-ddl-socket", "", "private Unix socket of the local development provisioning supervisor")
	devPlaintext := fs.Bool("dev-plaintext-loopback", false, "explicitly permit unauthenticated loopback development serving")
	tlsCertificate := fs.String("tls-certificate", "", "PEM gateway certificate chain")
	tlsKey := fs.String("tls-key", "", "PEM gateway private key")
	tlsRoots := fs.String("tls-roots", "", "PEM client trust roots")
	tlsIdentityOID := fs.String("tls-identity-oid", "", "operator VibeDB identity OID")
	tlsHandshakeTimeout := fs.Duration("tls-handshake-timeout", 5*time.Second, "hard TLS handshake deadline")
	maxConnections := fs.Int("max-client-connections", 1024, "hard authenticated client connection bound")
	maxHandshakes := fs.Int("max-client-handshakes", 64, "hard concurrent TLS handshake bound")
	authorizationPolicy := fs.String("authorization-policy", "", "bounded vibejson principal/capability policy")
	var shardPeers repeatedFlag
	fs.Var(&shardPeers, "shard-peer", "authenticated shard address=32-character-hex-NodeID; repeat for each endpoint")
	maxShardConnections := fs.Int("max-shard-connections-per-pool", 4096, "hard connection bound for each authenticated SQL and RF3 shard pool; transient control pools cap this at 8")
	maxShardHandshakes := fs.Int("max-shard-handshakes-per-pool", 64, "hard concurrent TLS handshake bound for each authenticated SQL and RF3 shard pool; transient control pools cap this at 8")
	maxNativeReadConcurrency := fs.Int("max-native-read-concurrency", gateway.DefaultReplicatedReadConcurrency, "hard concurrent public RF3 point-read bound")
	maxNativeReadBytes := fs.Uint64("max-native-read-bytes", gateway.DefaultReplicatedReadInFlight, "hard aggregate schema-bounded public RF3 response-byte reservation")
	maxNativeScatterConcurrency := fs.Int("max-native-scatter-concurrency", gateway.DefaultReplicatedScatterConcurrency, "hard concurrent RF3 shard-group reads; requests may contain more groups and drain through this bound")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *catalog == "" {
		usage()
		return 2
	}
	if *pgDevDDLSocket != "" && (*pgDevListen == "" || !filepath.IsAbs(*pgDevDDLSocket) || *devStaticCatalog) {
		fmt.Fprintln(os.Stderr, "gateway: local DDL requires RF3 PostgreSQL and an absolute private Unix socket")
		return 2
	}
	if *pgDevListen != "" {
		if err := requireLoopbackListen(*pgDevListen); err != nil || *devStaticCatalog {
			fmt.Fprintln(os.Stderr, "gateway: pg-dev-listen requires a loopback address and RF3 catalog")
			return 2
		}
	}
	if *schemaRolloutOnce && *schemaRolloutPlan == "" ||
		*schemaRolloutPlan != "" && (*devStaticCatalog || *devPlaintext || *replicaControlManifestPath == "") {
		fmt.Fprintln(os.Stderr, "gateway: schema rollout requires replicated catalog and authenticated replica-control manifest")
		return 2
	}
	if *backupRepositoryPath != "" && (!filepath.IsAbs(*backupRepositoryPath) ||
		filepath.Clean(*backupRepositoryPath) != *backupRepositoryPath || *devStaticCatalog ||
		*devPlaintext || *replicaControlManifestPath == "") {
		fmt.Fprintln(os.Stderr, "gateway: backup repository requires an absolute clean path, replicated catalog, TLS, and replica-control manifest")
		return 2
	}
	if err := validateGatewayHotShardServeMode(
		*hotShardCapacity, *replicaControlManifestPath, *devStaticCatalog, *devPlaintext,
	); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	if *devStaticCatalog {
		if !*devPlaintext || *catalogBootstrapIfMissing || *catalogRelation != 0 ||
			*catalogRouteSeed != "" || *hotShardCapacity != "" || *durableAckKeyPath != "" {
			fmt.Fprintln(os.Stderr, "gateway: static catalog is an explicit plaintext development mode")
			return 2
		}
	} else if *catalogRouteSeed == "" || *catalogRouteSeed == *catalog ||
		*catalogRelation == 0 || *catalogRelation > uint(replication.MaxRelationID) || *catalogAttempts <= 0 ||
		*catalogAttemptTimeout <= 0 || *catalogSessionJournal == "" || *durableAckKeyPath == "" ||
		*controllerInterval <= 0 || *catalogSessionLease <= 0 || *hotShardInterval <= 0 ||
		len(*catalogClientID) != 32 || len(*catalogRetryHome) != 16 {
		fmt.Fprintln(os.Stderr, "gateway: replicated catalog relation, identities, journal, and positive bounds are required")
		return 2
	}
	returnServe, handled := runServeThroughRuntime(
		*catalog, tableCatalogs, *catalogRouteSeed, *devStaticCatalog,
		*catalogBootstrapIfMissing, *catalogRelation, *catalogAttempts,
		*catalogAttemptTimeout, *catalogSessionJournal, *durableAckKeyPath,
		*catalogClientID, *catalogRetryHome, *catalogSessionLease, *listen,
		*pgDevListen, *pgDevDDLSocket, *devPlaintext, *tlsCertificate, *tlsKey,
		*tlsRoots, *tlsIdentityOID, *tlsHandshakeTimeout, *authorizationPolicy,
		shardPeers, *maxConnections, *maxHandshakes, *maxShardConnections,
		*maxShardHandshakes, *maxNativeReadConcurrency, *maxNativeReadBytes,
		*maxNativeScatterConcurrency, *hotShardCapacity, *hotShardInterval,
		*replicaControlManifestPath, *backupRepositoryPath, *backupMaxBackups,
		*backupMaxArtifacts, *backupMaxArtifactBytes, *backupMaxDiskBytes,
		*controllerInterval, *schemaRolloutPlan, *schemaRolloutOnce,
	)
	if !handled {
		return 2
	}
	return returnServe
}
