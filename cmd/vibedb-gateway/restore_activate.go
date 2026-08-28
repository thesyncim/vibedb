package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/kubeoperator"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibejson"
)

func runRestoreActivate(arguments []string) int {
	flags := flag.NewFlagSet("restore-activate", flag.ContinueOnError)
	path := flags.String("manifest", "", "canonical authenticated restore activation manifest")
	if err := flags.Parse(arguments); err != nil || *path == "" || flags.NArg() != 0 {
		return 2
	}
	manifest, err := loadGatewayRestoreManifest(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "restore activation manifest: %v\n", err)
		return 2
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, time.Duration(manifest.TimeoutMS)*time.Millisecond)
	defer cancel()
	permit, err := executeGatewayRestore(ctx, manifest)
	if err != nil {
		fmt.Fprintf(os.Stderr, "restore activation failed: %v\n", err)
		return 1
	}
	output := struct {
		Operation      string `json:"operation"`
		Groups         uint32 `json:"groups"`
		CatalogWitness string `json:"catalog_witness"`
	}{hex.EncodeToString(permit.Operation[:]), permit.Groups, hex.EncodeToString(permit.CatalogWitness[:])}
	raw, err := vibejson.Marshal(&output)
	if err != nil {
		return 1
	}
	if _, err = os.Stdout.Write(append(raw, '\n')); err != nil {
		return 1
	}
	return 0
}

// executeGatewayRestore never seeds or publishes a generic catalog head. The
// target catalog serves only the restricted restore bootstrap surface until
// the separately observed replicated activation grants client serving.
func executeGatewayRestore(ctx context.Context, manifest gatewayRestoreManifest) (clusterrestore.ServingPermit, error) {
	operationRaw, err := readGatewayRestoreInput(manifest.Operation, 64<<20)
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	operation, err := clusterrestore.OpenOperation(operationRaw)
	if err != nil || len(operation.Targets) != len(manifest.Groups) {
		return clusterrestore.ServingPermit{}, fmt.Errorf("operation group vector: %w", errors.Join(gateway.ErrRestoreActivation, err))
	}
	schemas, err := readGatewayRestoreInput(manifest.SchemaSet, kubeoperator.RestoreTemplateMaxBytes)
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	if err = kubeoperator.ValidateRestoreSchemaSet(schemas, operation, 0); err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	policyRaw, err := readGatewayRestoreInput(manifest.Policy, serviceauthz.AbsoluteMaxPolicyBytes)
	if err != nil || sha256.Sum256(policyRaw) != operation.TargetPolicyDigest {
		return clusterrestore.ServingPermit{}, fmt.Errorf("sealed policy digest: %w", errors.Join(gateway.ErrRestoreActivation, err))
	}
	policy, err := serviceauthz.Load(policyRaw)
	if err != nil || policy.Generation() != operation.PolicyGeneration {
		return clusterrestore.ServingPermit{}, fmt.Errorf("sealed policy generation: %w", errors.Join(gateway.ErrRestoreActivation, err))
	}
	profile, err := servicetls.LoadProfile(manifest.TLS.Certificate, manifest.TLS.Key, manifest.TLS.Roots, manifest.TLS.IdentityOID, time.Now)
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	target := operation.Targets[operation.CatalogOrdinal].Group
	if profile.LocalIdentity().TrustDomain != (rafttransport.TrustDomain{ClusterID: target.ClusterID, ClusterIncarnation: target.ClusterIncarnation}) {
		return clusterrestore.ServingPermit{}, fmt.Errorf("target trust domain: %w", gateway.ErrRestoreActivation)
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	operator := serviceauthz.Authority{Node: profile.LocalIdentity().Node, Generation: policy.Generation()}
	if gate.CheckAuthority(operator, serviceauthz.CapabilityRestoreActivate) != serviceauthz.DecisionAllow ||
		gate.CheckAuthority(operator, serviceauthz.CapabilityTopology) != serviceauthz.DecisionAllow {
		return clusterrestore.ServingPermit{}, fmt.Errorf("operator capabilities: %w", gateway.ErrRestoreActivation)
	}
	snapshot, err := gateway.LoadSnapshot(manifest.TargetCatalog)
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	sealedSnapshot, err := kubeoperator.RestoreTargetCatalog(schemas, operation)
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	actualCatalog, err := gateway.AppendSnapshotDocument(nil, snapshot)
	sealedCatalog, sealedErr := gateway.AppendSnapshotDocument(nil, sealedSnapshot)
	if err != nil || sealedErr != nil || !bytes.Equal(actualCatalog, sealedCatalog) {
		return clusterrestore.ServingPermit{}, fmt.Errorf("sealed target catalog: %w", errors.Join(gateway.ErrRestoreActivation, err, sealedErr))
	}
	for _, path := range []string{manifest.ActivationRoot, filepath.Dir(manifest.Sessions[0].Journal), filepath.Dir(manifest.Sessions[1].Journal)} {
		if err = ensureGatewayRestoreDirectory(path); err != nil {
			return clusterrestore.ServingPermit{}, err
		}
	}
	for _, group := range manifest.Groups {
		if err = ensureGatewayRestoreDirectory(group.Root); err != nil {
			return clusterrestore.ServingPermit{}, err
		}
	}
	staging, err := clusterbackup.OpenRestoreStagingRoot(manifest.StagingRoot, clusterbackup.RepositoryLimits{
		MaxBackups: 1, MaxArtifacts: manifest.Repository.MaxArtifacts,
		MaxArtifactBytes: manifest.Repository.MaxArtifactBytes, MaxDiskBytes: manifest.Repository.MaxDiskBytes,
	})
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	defer staging.Close()
	if staging.Permit != operation.Permit {
		return clusterrestore.ServingPermit{}, fmt.Errorf("verified staging permit: %w", gateway.ErrRestoreActivation)
	}
	catalog, pool, err := newGatewayRestoreCatalog(ctx, manifest, operation, snapshot, profile, gate, operator)
	if err != nil {
		return clusterrestore.ServingPermit{}, fmt.Errorf("target catalog connection: %w", err)
	}
	defer pool.Close()
	endpoints := make([]gateway.ReplicatedEndpoint, 0, len(operation.Targets)*3)
	for ordinal, target := range operation.Targets {
		for replica, identity := range target.Replicas {
			endpoints = append(endpoints, gateway.ReplicatedEndpoint{Member: identity.Member, Node: identity.Node,
				StoreID: identity.Store, NodeIncarnation: identity.NodeIncarnation,
				ControlAddress: manifest.Groups[ordinal].ControlAddresses[replica]})
		}
	}
	deadline := servicetls.FixedDeadline(time.Duration(manifest.AttemptTimeoutMS) * time.Millisecond)
	opener, err := newGatewayShardControlOpener(profile, deadline, dialGatewayRestore, endpoints, manifest.MaxConnections)
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	serving, err := shardservice.NewRestoreServingControlClient(opener, deadline, deadline)
	if err != nil {
		return clusterrestore.ServingPermit{}, err
	}
	_, permit, err := gateway.ActivateRestore(ctx, gateway.RestoreActivationOptions{
		Root: manifest.ActivationRoot, Staging: staging, Operation: operation,
		Installer: gatewayRestoreGroupInstaller{schemas: schemas, groups: manifest.Groups},
		Catalog:   catalog, Serving: serving, Gate: gate, Operator: operator,
	})
	if err != nil {
		return permit, fmt.Errorf("complete activation: %w", err)
	}
	return permit, nil
}

type gatewayRestoreGroupInstaller struct {
	schemas []byte
	groups  []gatewayRestoreGroup
}

func (installer gatewayRestoreGroupInstaller) Install(ctx context.Context, operation clusterrestore.Operation, ordinal uint32, artifact io.Reader) (clusterrestore.RootWitness, error) {
	if int(ordinal) >= len(installer.groups) || installer.groups[ordinal].Ordinal != ordinal {
		return clusterrestore.RootWitness{}, gateway.ErrRestoreActivation
	}
	result, err := kubeoperator.RestoreGroup(ctx, kubeoperator.RestoreGroupConfig{Root: installer.groups[ordinal].Root,
		Template: installer.schemas, Operation: operation, Ordinal: ordinal, Artifact: artifact})
	return result.Witness, err
}

func newGatewayRestoreCatalog(ctx context.Context, manifest gatewayRestoreManifest, operation clusterrestore.Operation,
	snapshot *gateway.Snapshot, profile *rafttransport.PeerTLS, gate *serviceauthz.Gate, operator serviceauthz.Authority,
) (*gateway.ReplicatedRestoreCatalog, *gateway.AuthenticatedReplicatedClient, error) {
	if ctx == nil || snapshot == nil || profile == nil || gate == nil || !operator.Valid() ||
		int(operation.CatalogOrdinal) >= len(operation.Targets) {
		return nil, nil, gateway.ErrRestoreActivation
	}
	var replicas [3]gateway.ReplicatedEndpoint
	route, found := snapshot.ResolveReplicatedRoute(gateway.ReplicatedCatalogDistribution, gateway.ReplicatedCatalogShard, replicas[:0])
	if !found || route.Group != operation.Targets[operation.CatalogOrdinal].Group || len(route.Replicas) != 3 {
		return nil, nil, gateway.ErrRestoreActivation
	}
	for index, target := range operation.Targets[operation.CatalogOrdinal].Replicas {
		if route.Replicas[index].Member != target.Member || route.Replicas[index].Node != target.Node || route.Replicas[index].StoreID != target.Store || route.Replicas[index].NodeIncarnation != target.NodeIncarnation {
			return nil, nil, gateway.ErrRestoreActivation
		}
	}
	timeout := time.Duration(manifest.AttemptTimeoutMS) * time.Millisecond
	route, err := refreshGatewayRestoreCatalogRoute(ctx, profile, operator, route, manifest.Attempts, timeout)
	if err != nil {
		return nil, nil, err
	}
	pool, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: profile, Dial: dialGatewayRestore, HandshakeDeadline: servicetls.FixedDeadline(timeout),
		MaxConnections: manifest.MaxConnections, MaxPerEndpoint: manifest.MaxConnections, MaxIdlePerEndpoint: 2,
		MaxHandshakes: manifest.MaxConnections, MaxWaiters: manifest.MaxConnections, MaxIdleAge: 30 * time.Second, MaxLifetime: 15 * time.Minute,
	})
	if err != nil {
		return nil, nil, err
	}
	fail := func(err error) (*gateway.ReplicatedRestoreCatalog, *gateway.AuthenticatedReplicatedClient, error) {
		return nil, nil, errors.Join(err, pool.Close())
	}
	executor, err := gateway.NewReplicatedExecutor(pool, manifest.Attempts, timeout)
	if err != nil {
		return fail(err)
	}
	bound, err := serviceauthz.WithAuthority(ctx, operator)
	if err != nil {
		return fail(err)
	}
	var sessions [2]*gateway.NativeSession
	for index, config := range manifest.Sessions {
		sessions[index], err = openGatewayRestoreSession(bound, executor, route, config, time.Duration(manifest.SessionLeaseMS)*time.Millisecond)
		if err != nil {
			return fail(err)
		}
	}
	authority, err := gateway.NewReplicatedCatalogAuthority(gateway.ReplicatedCatalogAuthorityOptions{
		Executor: executor, Route: route, Relation: 1, Holder: gateway.NewCatalogHolder(nil), Session: sessions[0], Authority: operator,
	})
	if err != nil {
		return fail(err)
	}
	catalog, err := gateway.NewReplicatedRestoreCatalog(gateway.ReplicatedRestoreCatalogOptions{Catalog: authority, Session: sessions[1], Gate: gate, Operator: operator})
	if err != nil {
		return fail(err)
	}
	return catalog, pool, nil
}

func dialGatewayRestore(ctx context.Context, address string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "tcp", address)
}

func openGatewayRestoreSession(ctx context.Context, executor *gateway.ReplicatedExecutor, route gateway.ReplicatedRoute, config gatewayRestoreSession, lease time.Duration) (*gateway.NativeSession, error) {
	var client replication.ID128
	var retry replication.RetryHome
	if decodeFixedHex(config.ClientID, client[:]) != nil || decodeFixedHex(config.RetryHome, retry[:]) != nil {
		return nil, gateway.ErrRestoreActivation
	}
	tenant := []byte{replicatedCatalogControllerTenant}
	binding, err := gateway.NativeSessionJournalBinding(route, string(gateway.ReplicatedCatalogDistribution), string(gateway.ReplicatedCatalogShard), tenant, 1, serviceauthz.CapabilityTopology)
	if err != nil {
		return nil, err
	}
	journal, err := gateway.OpenNativeSessionJournal(gateway.NativeSessionJournalOptions{Path: config.Journal, ClientID: client, RetryHome: retry, MaxCommandBytes: replication.MaxCommandBytes, Binding: binding})
	if err != nil {
		return nil, err
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{Executor: executor, Route: route,
		Distribution: string(gateway.ReplicatedCatalogDistribution), Shard: string(gateway.ReplicatedCatalogShard), Tenant: tenant,
		ClientID: client, RetryHome: retry, Resolver: gateway.BaseRelationResolver{Relation: 1}, Journal: journal,
		ProposalCapability: serviceauthz.CapabilityTopology, MaxRelationBatches: 1, MaxMutations: 4, InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes})
	if err != nil {
		return nil, err
	}
	if session.Status().Pending {
		if _, err = session.RetryPending(ctx); err != nil {
			return nil, err
		}
	}
	deadline := time.Now().Add(lease).UnixNano()
	if deadline <= 0 {
		return nil, gateway.ErrRestoreActivation
	}
	status := session.Status()
	if !status.Active {
		_, err = session.Open(ctx, deadline)
	} else {
		if deadline <= status.LeaseDeadline {
			if status.LeaseDeadline == math.MaxInt64 {
				return nil, gateway.ErrRestoreActivation
			}
			deadline = status.LeaseDeadline + 1
		}
		_, err = session.Renew(ctx, status.LeaseDeadline, deadline)
	}
	if err != nil {
		return nil, err
	}
	return session, nil
}

func ensureGatewayRestoreDirectory(path string) error {
	if !gatewayRestorePath(path) {
		return gateway.ErrRestoreActivation
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("private restore authority directory: %w", errors.Join(gateway.ErrRestoreActivation, err))
	}
	return nil
}
