package gatewayruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/servicetls"
)

// runServeThroughRuntime is the sole standalone serving entry point. It keeps
// CLI-only parsing and process signal wiring in the command package while all
// assembly, serving, and shutdown behavior lives on Runtime.
func runServeThroughRuntime(
	catalog string, tableCatalogs []string, routeSeed string, static, bootstrap bool,
	relation uint, attempts int, attemptTimeout time.Duration, sessionJournal, ackPath,
	clientIDText, retryHomeText string, sessionLease time.Duration, listen, pgListen,
	pgDDLSocket string, plaintext bool, certificate, key, roots, identityOID string,
	handshakeTimeout time.Duration, policyPath string, shardPeerTexts []string,
	maxConnections, maxHandshakes, maxShardConnections, maxShardHandshakes int,
	maxNativeReads int, maxNativeBytes uint64, maxNativeScatter int,
	hotShardCapacity string, hotShardInterval time.Duration, replicaControl,
	backupRepository string, backupMaxBackups, backupMaxArtifacts int,
	backupMaxArtifactBytes, backupMaxDiskBytes uint64, controllerInterval time.Duration,
	schemaPlan string, schemaOnce bool, initialDirectory string,
) (code int, handled bool) {
	if catalog == "" {
		return 0, false
	}
	var clientID replication.ID128
	var retryHome replication.RetryHome
	if !static && (decodeFixedHex(clientIDText, clientID[:]) != nil ||
		decodeFixedHex(retryHomeText, retryHome[:]) != nil) {
		fmt.Fprintln(os.Stderr, "gateway: catalog client identity or retry home is not canonical hexadecimal")
		return 2, true
	}
	peers := make([]servicetls.Endpoint, len(shardPeerTexts))
	for index, encoded := range shardPeerTexts {
		separator := strings.LastIndexByte(encoded, '=')
		if separator <= 0 || separator == len(encoded)-1 {
			fmt.Fprintf(os.Stderr, "gateway: shard peer %d is not address=node-id\n", index)
			return 2, true
		}
		node, err := servicetls.ParseNodeID(encoded[separator+1:])
		if err != nil {
			fmt.Fprintf(os.Stderr, "gateway: shard peer %d: %v\n", index, err)
			return 2, true
		}
		peers[index] = servicetls.Endpoint{Address: encoded[:separator], Node: node}
	}
	initialNodes, err := loadInitialNodeDirectory(initialDirectory)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: initial node directory: %v\n", err)
		return 2, true
	}
	config := Config{
		InitialNodeDirectory: initialNodes,
		CatalogPath:          catalog, CatalogRouteSeedPath: routeSeed,
		DevStaticCatalog: static, DevPlaintext: plaintext,
		CatalogBootstrapIfMissing: bootstrap,
		CatalogRelation:           replication.RelationID(relation), CatalogAttempts: attempts,
		CatalogAttemptTimeout: attemptTimeout, CatalogSessionLease: sessionLease,
		CatalogSessionJournal: sessionJournal, CatalogClientID: clientID,
		CatalogRetryHome: retryHome, DurableAckKeyPath: ackPath,
		ListenAddress: listen, TLSCertificate: certificate, TLSKey: key, TLSRoots: roots,
		TLSIdentityOID: identityOID, TLSHandshakeTimeout: handshakeTimeout,
		AuthorizationPolicy: policyPath, ShardPeers: peers,
		MaxConnections: maxConnections, MaxHandshakes: maxHandshakes,
		MaxShardConnections: maxShardConnections, MaxShardHandshakes: maxShardHandshakes,
		MaxNativeReadConcurrency: maxNativeReads, MaxNativeReadBytes: maxNativeBytes,
		MaxNativeScatterConcurrency: maxNativeScatter, PGListenAddress: pgListen,
		PGDDLSocket: pgDDLSocket, TableCatalogs: append([]string(nil), tableCatalogs...),
		HotShardCapacityPath: hotShardCapacity, HotShardInterval: hotShardInterval,
		ReplicaControlManifestPath: replicaControl, BackupRepositoryPath: backupRepository,
		BackupMaxBackups: backupMaxBackups, BackupMaxArtifacts: backupMaxArtifacts,
		BackupMaxArtifactBytes: backupMaxArtifactBytes, BackupMaxDiskBytes: backupMaxDiskBytes,
		ControllerInterval: controllerInterval, SchemaRolloutPlan: schemaPlan,
		SchemaRolloutOnce: schemaOnce,
		Logf:              func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) },
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runtime, err := Open(ctx, config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "gateway: open runtime: %v\n", err)
		if errors.Is(err, ErrInvalidConfig) {
			return 2, true
		}
		return 1, true
	}
	serveErr := runtime.Serve(ctx)
	closeErr := runtime.Close()
	if serveErr != nil || closeErr != nil {
		fmt.Fprintf(os.Stderr, "gateway: serve: %v\n", errors.Join(serveErr, closeErr))
		return 1, true
	}
	return 0, true
}
