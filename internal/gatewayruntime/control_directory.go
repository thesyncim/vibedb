package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var errGatewayControlDirectory = errors.New("vibedb-gateway: invalid live control directory")

type versionedControlDirectoryReader interface {
	ReadNodeDirectory(context.Context) ([]gateway.NodeRecord, uint64, error)
}

// readGatewayControlDirectoryCut obtains one complete metadata cut and its
// authoritative global directory revision. A catalog generation is carried by
// each NodeRecord, so unrelated catalog publications do not force an endpoint
// refresh. An adapter that cannot return the global CAS revision is rejected;
// taking the maximum child revision would make a valid add/remove cut appear
// stale forever.
func readGatewayControlDirectoryCut(
	ctx context.Context, reader gateway.DirectoryReader,
) (gateway.ReplicatedControlDirectorySnapshot, error) {
	if ctx == nil || reader == nil {
		return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
	}
	if cutReader, ok := reader.(gateway.NodeDirectoryCutReader); ok {
		cut, err := cutReader.ReadNodeDirectoryCut(ctx)
		if err != nil {
			return gateway.ReplicatedControlDirectorySnapshot{}, err
		}
		if !cut.Valid() {
			return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
		}
		live := gateway.ReplicatedControlDirectorySnapshot{
			Revision: cut.Revision, CatalogGeneration: cut.CatalogGeneration,
			// The durable cut retains historical incarnations so retirement
			// proofs can address their exact identities.  The live participant
			// directory admits only the newest incarnation of each physical
			// NodeID; otherwise a reincarnated node would create duplicate
			// transport participants and fail closed during startup.
			Nodes: slices.Clone(cut.CurrentNodes()),
		}
		if !live.Valid() {
			return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
		}
		return live, nil
	}
	versioned, ok := reader.(versionedControlDirectoryReader)
	if !ok {
		return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
	}
	nodes, revision, err := versioned.ReadNodeDirectory(ctx)
	if err != nil {
		return gateway.ReplicatedControlDirectorySnapshot{}, err
	}
	if len(nodes) == 0 {
		return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
	}
	// The replicated directory may retain tombstones for older incarnations.
	// Collapse those physical identities to the newest incarnation for the
	// current cut; the control directory keeps each exact historical endpoint
	// separately for immutable drain fences.
	latest := make(map[rafttransport.NodeID]gateway.NodeRecord, len(nodes))
	for _, node := range nodes {
		prior, found := latest[node.NodeID]
		if found && prior.Incarnation > node.Incarnation {
			continue
		}
		if found && prior.Incarnation == node.Incarnation && prior != node {
			return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
		}
		latest[node.NodeID] = node
	}
	nodes = nodes[:0]
	for _, node := range latest {
		nodes = append(nodes, node)
	}
	nodes = slices.Clone(nodes)
	slices.SortFunc(nodes, func(left, right gateway.NodeRecord) int {
		return bytes.Compare(left.NodeID[:], right.NodeID[:])
	})
	var generation uint64
	for _, node := range nodes {
		if node.CatalogGeneration > generation {
			generation = node.CatalogGeneration
		}
	}
	cut := gateway.ReplicatedControlDirectorySnapshot{
		Revision: revision, CatalogGeneration: generation, Nodes: nodes,
	}
	if !cut.Valid() {
		return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
	}
	return cut, nil
}

func controlDirectoryShardEndpoints(
	directory *gateway.ReplicatedControlDirectory,
) []gateway.ReplicatedEndpoint {
	if directory == nil {
		return nil
	}
	return directory.ShardControlEndpoints()
}

func controlDirectoryGatewayEndpoints(
	directory *gateway.ReplicatedControlDirectory,
) []gatewayControlEndpoint {
	if directory == nil {
		return nil
	}
	source := directory.GatewayControlEndpoints()
	result := make([]gatewayControlEndpoint, len(source))
	for index, endpoint := range source {
		result[index] = gatewayControlEndpoint{Member: endpoint.Member, Address: endpoint.Address}
	}
	return result
}

func controlDirectoryGatewayEndpointsWithHistory(
	directory *gateway.ReplicatedControlDirectory,
) []gatewayControlEndpoint {
	if directory == nil {
		return nil
	}
	source := directory.GatewayControlEndpointsWithHistory()
	result := make([]gatewayControlEndpoint, len(source))
	for index, endpoint := range source {
		result[index] = gatewayControlEndpoint{Member: endpoint.Member, Address: endpoint.Address}
	}
	return result
}

func mergeGatewayShardControlEndpoints(
	manifest []gateway.ReplicatedEndpoint,
	directory []gateway.ReplicatedEndpoint,
) []gateway.ReplicatedEndpoint {
	result := slices.Clone(manifest)
	seen := make(map[[24]byte]struct{}, len(result)+len(directory))
	for _, endpoint := range result {
		var key [24]byte
		copy(key[:16], endpoint.Node[:])
		for index := uint(0); index < 8; index++ {
			key[16+index] = byte(endpoint.NodeIncarnation >> (8 * index))
		}
		seen[key] = struct{}{}
	}
	for _, endpoint := range directory {
		var key [24]byte
		copy(key[:16], endpoint.Node[:])
		for index := uint(0); index < 8; index++ {
			key[16+index] = byte(endpoint.NodeIncarnation >> (8 * index))
		}
		if _, found := seen[key]; !found {
			result = append(result, endpoint)
			seen[key] = struct{}{}
		}
	}
	return result
}

func mergeGatewayControlEndpoints(
	manifest []gatewayControlEndpoint,
	directory []gatewayControlEndpoint,
) []gatewayControlEndpoint {
	result := slices.Clone(manifest)
	seen := make(map[gateway.ClusterCatalogDrainMember]struct{}, len(result)+len(directory))
	for _, endpoint := range result {
		seen[endpoint.Member] = struct{}{}
	}
	for _, endpoint := range directory {
		if _, found := seen[endpoint.Member]; !found {
			result = append(result, endpoint)
			seen[endpoint.Member] = struct{}{}
		}
	}
	return result
}

func controlDirectoryNodes(
	directory *gateway.ReplicatedControlDirectory,
) []rafttransport.NodeID {
	if directory == nil {
		return nil
	}
	nodes := directory.Nodes()
	result := make([]rafttransport.NodeID, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, node.NodeID)
	}
	return result
}

func controlDirectoryGatewayNodes(
	directory *gateway.ReplicatedControlDirectory,
) []rafttransport.NodeID {
	if directory == nil {
		return nil
	}
	endpoints := directory.GatewayControlEndpoints()
	result := make([]rafttransport.NodeID, 0, len(endpoints))
	for _, endpoint := range endpoints {
		result = append(result, endpoint.Member.Node)
	}
	return result
}

// openControlDirectory installs the first complete authenticated directory
// before any control listener is opened. A partial cut is never published to
// an opener or an admission authorizer.
func (runtime *Runtime) openControlDirectory() error {
	if runtime == nil {
		return errGatewayControlDirectory
	}
	reader := runtime.config.ControlDirectory
	if reader == nil {
		// The replicated catalog authority is itself the authoritative directory
		// reader for an embedded frontend. An explicit adapter can still be
		// supplied by a supervisor when the control plane is external.
		reader = runtime.authority
	}
	if reader == nil || runtime.config.TLSProfile == nil || runtime.config.Authorization == nil {
		return errGatewayControlDirectory
	}
	cut, err := readGatewayControlDirectoryCut(runtime.ctx, reader)
	if err != nil {
		return fmt.Errorf("read initial control directory: %w", err)
	}
	directory, err := gateway.NewReplicatedControlDirectory(cut)
	if err != nil {
		return fmt.Errorf("validate initial control directory: %w", err)
	}
	runtime.controlDirectory = directory
	serviceCut, err := runtimeServiceDirectoryCut(runtime.ctx, reader, cut,
		runtime.config.TLSProfile, runtime.config.Authorization.Generation())
	if err != nil {
		return fmt.Errorf("read initial service directory: %w", err)
	}
	runtime.serviceDirectory, err = serviceauthz.NewServiceDirectoryGate(serviceCut)
	if err != nil {
		return fmt.Errorf("validate initial service directory: %w", err)
	}
	if err := bindRuntimeServiceDirectory(runtime.config.Transport, runtime.serviceDirectory,
		runtime.config.RequireServiceDirectoryBinding); err != nil {
		return err
	}
	return nil
}

// applyLiveControlDirectory applies one complete newer catalog directory and
// then updates every dependent transport. Historical endpoint identities stay
// in outbound openers; current gateway identities alone expand inbound TLS
// admission. Existing drain machines retain their immutable member fences.
func (runtime *Runtime) applyLiveControlDirectory(
	ctx context.Context, cut gateway.ReplicatedControlDirectorySnapshot,
) error {
	if runtime == nil || runtime.controlDirectory == nil || ctx == nil {
		return errGatewayControlDirectory
	}
	reader := runtime.config.ControlDirectory
	if reader == nil {
		reader = runtime.authority
	}
	if reader == nil || runtime.config.TLSProfile == nil || runtime.config.Authorization == nil {
		return errGatewayControlDirectory
	}
	serviceCut, err := runtimeServiceDirectoryCut(ctx, reader, cut,
		runtime.config.TLSProfile, runtime.config.Authorization.Generation())
	if err != nil {
		return fmt.Errorf("read service directory cut: %w", err)
	}
	if err := runtime.controlDirectory.Apply(cut); err != nil {
		return err
	}
	directory := runtime.controlDirectory
	if runtime.controlOpener != nil {
		if err := runtime.controlOpener.Update(cut.Revision, directory.ShardControlEndpoints()); err != nil {
			return fmt.Errorf("update shard control directory: %w", err)
		}
	}
	currentGateways := controlDirectoryGatewayEndpoints(directory)
	if runtime.clusterControlOpener != nil {
		if err := runtime.clusterControlOpener.Update(
			cut.Revision, controlDirectoryGatewayEndpointsWithHistory(directory),
		); err != nil {
			return fmt.Errorf("update gateway control directory: %w", err)
		}
	}
	if runtime.drainCoordinator != nil {
		members := make([]gateway.ClusterCatalogDrainMember, len(currentGateways))
		for index, endpoint := range currentGateways {
			members[index] = endpoint.Member
		}
		if err := runtime.drainCoordinator.UpdateMembers(members); err != nil {
			return fmt.Errorf("update catalog drain roster: %w", err)
		}
	}
	if runtime.controlAuthorizer != nil {
		if err := runtime.controlAuthorizer.Replace(appendBootstrapControlNodes(controlDirectoryGatewayNodes(directory), serviceCut)); err != nil {
			return fmt.Errorf("update gateway control authorization: %w", err)
		}
	}
	if runtime.serviceDirectory == nil {
		return errGatewayControlDirectory
	}
	if err := runtime.serviceDirectory.ApplyCommittedCut(serviceCut); err != nil {
		return fmt.Errorf("update service directory: %w", err)
	}
	// Preserve old roster entries for active fences, while making current
	// identities available to the request-level envelope authorization.
	runtime.controlRosterMu.Lock()
	if runtime.controlRoster == nil {
		runtime.controlRoster = make(map[rafttransport.NodeID]map[uint64]struct{})
	}
	for _, endpoint := range currentGateways {
		incarnations := runtime.controlRoster[endpoint.Member.Node]
		if incarnations == nil {
			incarnations = make(map[uint64]struct{})
			runtime.controlRoster[endpoint.Member.Node] = incarnations
		}
		incarnations[endpoint.Member.Incarnation] = struct{}{}
	}
	runtime.controlRosterMu.Unlock()
	return nil
}

func (runtime *Runtime) runControlDirectory() {
	if runtime == nil || runtime.config.ControlDirectory == nil && runtime.authority == nil {
		return
	}
	defer close(runtime.controlDirectoryDone)
	interval := runtime.config.ControllerInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case <-ticker.C:
			reader := runtime.config.ControlDirectory
			if reader == nil {
				reader = runtime.authority
			}
			cut, err := readGatewayControlDirectoryCut(runtime.ctx, reader)
			if err != nil {
				runtime.config.Logf("gatewayruntime: read live control directory: %v", err)
				continue
			}
			if err := runtime.applyLiveControlDirectory(runtime.ctx, cut); err != nil {
				runtime.config.Logf("gatewayruntime: apply live control directory: %v", err)
			}
		}
	}
}
