package gatewayruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var errGatewayControlDirectory = errors.New("vibedb-gateway: invalid live control directory")

type versionedControlDirectoryReader interface {
	ReadNodeDirectory(context.Context) ([]gateway.NodeRecord, uint64, error)
}

// readGatewayControlDirectoryCut obtains one complete metadata cut and its
// authoritative global directory revision. A complete runtime-cut reader is
// preferred when available so a catalog-only publication advances the
// effective generation even when no NodeRecord was rewritten. An adapter that
// cannot return the global CAS revision is rejected; taking the maximum child
// revision would make a valid add/remove cut appear stale forever.
func readGatewayControlDirectoryCut(
	ctx context.Context, reader gateway.DirectoryReader,
) (gateway.ReplicatedControlDirectorySnapshot, error) {
	if ctx == nil || reader == nil {
		return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
	}
	if coherent, ok := reader.(frontendDrainRuntimeCutReader); ok {
		source, err := coherent.ReadFrontendDrainRuntimeCut(ctx)
		if err != nil {
			return gateway.ReplicatedControlDirectorySnapshot{}, err
		}
		if !source.Nodes.Valid() || source.Catalog == nil ||
			source.CatalogHeadDigest == (replication.Digest{}) ||
			source.Catalog.Generation() != source.Nodes.CatalogGeneration {
			return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
		}
		live := gateway.ReplicatedControlDirectorySnapshot{
			Revision: source.Nodes.Revision, CatalogGeneration: source.Nodes.CatalogGeneration,
			// The durable cut retains historical incarnations so retirement
			// proofs can address their exact identities. The live participant
			// directory admits only the newest incarnation of each physical
			// NodeID; otherwise a reincarnated node would create duplicate
			// transport participants and fail closed during startup.
			Nodes: slices.Clone(source.Nodes.CurrentNodes()),
		}
		if !live.Valid() {
			return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
		}
		return live, nil
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

func frontendDrainRuntimeCutSnapshot(
	source gateway.FrontendDrainRuntimeCut,
) (gateway.ReplicatedControlDirectorySnapshot, error) {
	if !source.Nodes.Valid() || source.Catalog == nil ||
		source.CatalogHeadDigest == (replication.Digest{}) ||
		source.Catalog.Generation() != source.Nodes.CatalogGeneration {
		return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
	}
	cut := gateway.ReplicatedControlDirectorySnapshot{
		Revision: source.Nodes.Revision, CatalogGeneration: source.Nodes.CatalogGeneration,
		Nodes: slices.Clone(source.Nodes.CurrentNodes()),
	}
	if !cut.Valid() {
		return gateway.ReplicatedControlDirectorySnapshot{}, errGatewayControlDirectory
	}
	return cut, nil
}

func fallbackControlDirectoryError(prior, next error) error {
	if next != nil {
		return next
	}
	return prior
}

type liveControlDirectoryProjection struct {
	cut        gateway.ReplicatedControlDirectorySnapshot
	serviceCut serviceauthz.ServiceDirectoryCut
	fullCut    frontenddrain.PreparedAckCut
	// catalog is the certified catalog image paired with cut/fullCut. Physical
	// receiver discovery must use this exact image rather than rereading the
	// runtime holder after the source epoch has been selected.
	catalog *gateway.Snapshot
}

// readLiveControlDirectoryProjection obtains one complete source cut and
// derives every dependent projection from that cut. Refresh callers serialize
// this operation with the apply path so a catalog-only advance cannot be
// paired with a stale service directory or an older native gate floor.
func (runtime *Runtime) readLiveControlDirectoryProjection(
	ctx context.Context,
) (liveControlDirectoryProjection, error) {
	if runtime == nil || ctx == nil || runtime.config.TLSProfile == nil || runtime.config.Authorization == nil {
		return liveControlDirectoryProjection{}, errGatewayControlDirectory
	}
	reader := runtime.config.ControlDirectory
	if reader == nil {
		reader = runtime.authority
	}
	if reader == nil {
		return liveControlDirectoryProjection{}, errGatewayControlDirectory
	}
	var (
		cut     gateway.ReplicatedControlDirectorySnapshot
		source  *gateway.FrontendDrainRuntimeCut
		err     error
		rowsErr error
	)
	// Local catalog rows are the leader fast path. Followers and a former
	// leader after a transfer keep the same reader bound; a not-leader probe
	// must fall through to the authenticated source or the catalog authority
	// instead of failing CREATE/refresh closed.
	if rows := runtime.config.CanonicalFrontendDrainRuntimeRows; rows != nil {
		loaded, readErr := gateway.ReadFrontendDrainRuntimeCutFromRows(ctx, rows)
		if readErr == nil {
			if snap, snapErr := frontendDrainRuntimeCutSnapshot(loaded); snapErr == nil {
				copied := loaded
				source = &copied
				cut = snap
			} else {
				readErr = snapErr
			}
		}
		if source == nil {
			rowsErr = fmt.Errorf("read canonical frontend drain row cut: %w", readErr)
		}
	}
	if source == nil {
		if physical := runtime.config.CanonicalFrontendDrainRuntimeSource; physical != nil {
			proof, sourceErr := physical.ReadLatestFrontendDrainCut(ctx)
			if sourceErr != nil {
				return liveControlDirectoryProjection{}, fallbackControlDirectoryError(rowsErr, fmt.Errorf("read canonical frontend drain source proof: %w", sourceErr))
			}
			if !proof.Valid() {
				return liveControlDirectoryProjection{}, fallbackControlDirectoryError(rowsErr, fmt.Errorf("%w: canonical frontend drain source proof is invalid", errGatewayControlDirectory))
			}
			loaded, readErr := readCanonicalFrontendDrainRuntimeCutFromSourceProof(
				ctx, proof, runtime.authority, runtime.config.TLSProfile,
				runtime.config.Authorization.Generation(),
			)
			if readErr != nil {
				return liveControlDirectoryProjection{}, fallbackControlDirectoryError(rowsErr, fmt.Errorf("read canonical frontend drain source authority cut: %w", readErr))
			}
			source = &loaded
			cut, err = frontendDrainRuntimeCutSnapshot(loaded)
		} else if _, coherent := reader.(frontendDrainRuntimeCutReader); coherent {
			loaded, readErr := reader.(frontendDrainRuntimeCutReader).ReadFrontendDrainRuntimeCut(ctx)
			if readErr != nil {
				return liveControlDirectoryProjection{}, fallbackControlDirectoryError(rowsErr, fmt.Errorf("read canonical frontend drain cut: %w", readErr))
			}
			source = &loaded
			cut, err = frontendDrainRuntimeCutSnapshot(loaded)
		} else {
			cut, err = readGatewayControlDirectoryCut(ctx, reader)
			if err != nil && rowsErr != nil {
				err = fallbackControlDirectoryError(rowsErr, err)
			}
		}
	}
	if err != nil {
		return liveControlDirectoryProjection{}, err
	}
	// Bind the local admission state as soon as the complete physical cut is
	// authenticated. Service-directory projection may reject a draining cut
	// whose continuation material is still unavailable, but leaving the local
	// listener Active in that interval would admit new work after the durable
	// NodeDraining transition. applyLiveControlDirectoryProjection repeats this
	// idempotently after all dependent projections validate.
	runtime.syncFrontendDrainFromDirectory(cut.Nodes, cut.Revision)
	projection := liveControlDirectoryProjection{cut: cut}
	if source != nil {
		projection.catalog = source.Catalog
		projection.serviceCut, err = runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
			ctx, *source, runtime.config.TLSProfile, runtime.config.Authorization.Generation())
		if err == nil {
			projection.fullCut, err = frontendDrainPreparedAckCutFromRuntimeCut(*source, projection.serviceCut)
		}
	} else {
		projection.serviceCut, err = runtimeServiceDirectoryCut(ctx, reader, cut,
			runtime.config.TLSProfile, runtime.config.Authorization.Generation())
	}
	if err != nil {
		return liveControlDirectoryProjection{}, fmt.Errorf("read service directory cut: %w", err)
	}
	return projection, nil
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

// controlDirectoryMetricEndpoints projects only the current authenticated
// physical-node records into the exact endpoint identity used by metrics.
// Historical shard endpoints remain available for retirement fences, but
// must not contribute a second node aggregate or stale migration budget.
func controlDirectoryMetricEndpoints(
	directory *gateway.ReplicatedControlDirectory,
) []gateway.ReplicatedEndpoint {
	if directory == nil {
		return nil
	}
	nodes := directory.Nodes()
	result := make([]gateway.ReplicatedEndpoint, 0, len(nodes))
	for _, node := range nodes {
		if node.NodeID == (rafttransport.NodeID{}) || node.Incarnation == 0 || node.ControlAddress == "" {
			continue
		}
		result = append(result, gateway.ReplicatedEndpoint{
			Node: node.NodeID, NodeIncarnation: node.Incarnation, ControlAddress: node.ControlAddress,
		})
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
	// Catalog leaders already own a linearizable row reader. Install that cut
	// before dialing a remote source or the semantic catalog authority; both
	// of those routes wait on a gateway that has not opened yet.
	if rows := runtime.config.CanonicalFrontendDrainRuntimeRows; rows != nil {
		if source, readErr := gateway.ReadFrontendDrainRuntimeCutFromRows(runtime.ctx, rows); readErr == nil {
			if err := runtime.installControlDirectoryFromRuntimeCut(source); err == nil {
				return nil
			}
		}
	}
	var sourceProof *frontenddrain.PreparedAckCut
	if source := runtime.config.CanonicalFrontendDrainRuntimeSource; source != nil {
		proof, sourceErr := source.ReadLatestFrontendDrainCut(runtime.ctx)
		if sourceErr != nil {
			return fmt.Errorf("read initial canonical frontend drain source proof: %w", sourceErr)
		}
		if !proof.Valid() {
			return fmt.Errorf("%w: initial canonical frontend drain source proof is invalid", errGatewayControlDirectory)
		}
		bound, bindErr := bindRuntimeServiceDirectory(runtime.ctx, runtime.config.Transport, proof, true)
		if bindErr != nil {
			return fmt.Errorf("install initial canonical frontend drain source proof: %w", bindErr)
		}
		runtime.serviceDirectory = bound
		sourceProof = &proof
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
	var (
		serviceCut serviceauthz.ServiceDirectoryCut
		fullCut    frontenddrain.PreparedAckCut
	)
	if sourceProof != nil {
		source, readErr := readCanonicalFrontendDrainRuntimeCutFromSourceProof(
			runtime.ctx, *sourceProof, runtime.authority, runtime.config.TLSProfile,
			runtime.config.Authorization.Generation(),
		)
		if readErr != nil {
			return fmt.Errorf("read initial canonical frontend drain source authority cut: %w", readErr)
		}
		if source.Nodes.Revision != cut.Revision || source.Nodes.CatalogGeneration != cut.CatalogGeneration ||
			!reflect.DeepEqual(source.Nodes.CurrentNodes(), cut.Nodes) {
			return fmt.Errorf("%w: source authority cut does not match initial control directory", errGatewayControlDirectory)
		}
		serviceCut, err = runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
			runtime.ctx, source, runtime.config.TLSProfile, runtime.config.Authorization.Generation())
		if err == nil {
			fullCut, err = frontendDrainPreparedAckCutFromRuntimeCut(source, serviceCut)
		}
	} else if _, coherent := reader.(frontendDrainRuntimeCutReader); coherent {
		source, readErr := readCanonicalFrontendDrainRuntimeCut(runtime.ctx, reader, cut)
		if readErr != nil {
			return fmt.Errorf("read initial canonical frontend drain cut: %w", readErr)
		}
		serviceCut, err = runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
			runtime.ctx, source, runtime.config.TLSProfile, runtime.config.Authorization.Generation())
		if err == nil {
			fullCut, err = frontendDrainPreparedAckCutFromRuntimeCut(source, serviceCut)
		}
	} else {
		serviceCut, err = runtimeServiceDirectoryCut(runtime.ctx, reader, cut,
			runtime.config.TLSProfile, runtime.config.Authorization.Generation())
	}
	if err != nil {
		return fmt.Errorf("read initial service directory: %w", err)
	}
	runtime.serviceDirectory, err = serviceauthz.NewServiceDirectoryGate(serviceCut)
	if err != nil {
		return fmt.Errorf("validate initial service directory: %w", err)
	}
	if fullCut.Valid() {
		bound, bindErr := bindRuntimeServiceDirectory(runtime.ctx, runtime.config.Transport, fullCut,
			runtime.config.RequireServiceDirectoryBinding)
		if bindErr != nil {
			return bindErr
		}
		if bound != nil {
			runtime.serviceDirectory = bound
		}
	} else if runtime.config.RequireServiceDirectoryBinding {
		return fmt.Errorf("%w: complete canonical frontend drain cut is required for the local semantic transport", errGatewayControlDirectory)
	}
	return nil
}

func (runtime *Runtime) installControlDirectoryFromRuntimeCut(
	source gateway.FrontendDrainRuntimeCut,
) error {
	if runtime == nil || runtime.config.TLSProfile == nil || runtime.config.Authorization == nil {
		return errGatewayControlDirectory
	}
	cut, err := frontendDrainRuntimeCutSnapshot(source)
	if err != nil {
		return err
	}
	directory, err := gateway.NewReplicatedControlDirectory(cut)
	if err != nil {
		return fmt.Errorf("validate initial control directory: %w", err)
	}
	serviceCut, err := runtimeServiceDirectoryCutFromFrontendDrainRuntimeCut(
		runtime.ctx, source, runtime.config.TLSProfile, runtime.config.Authorization.Generation())
	if err != nil {
		return fmt.Errorf("read initial service directory: %w", err)
	}
	fullCut, err := frontendDrainPreparedAckCutFromRuntimeCut(source, serviceCut)
	if err != nil {
		return fmt.Errorf("read initial service directory: %w", err)
	}
	gate, err := serviceauthz.NewServiceDirectoryGate(serviceCut)
	if err != nil {
		return fmt.Errorf("validate initial service directory: %w", err)
	}
	if fullCut.Valid() {
		bound, bindErr := bindRuntimeServiceDirectory(runtime.ctx, runtime.config.Transport, fullCut,
			runtime.config.RequireServiceDirectoryBinding)
		if bindErr != nil {
			return bindErr
		}
		if bound != nil {
			gate = bound
		}
	} else if runtime.config.RequireServiceDirectoryBinding {
		return fmt.Errorf("%w: complete canonical frontend drain cut is required for the local semantic transport", errGatewayControlDirectory)
	}
	runtime.controlDirectory = directory
	runtime.serviceDirectory = gate
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
	projection, err := runtime.readLiveControlDirectoryProjection(ctx)
	if err != nil {
		return err
	}
	if projection.cut.Revision != cut.Revision || projection.cut.CatalogGeneration != cut.CatalogGeneration ||
		!reflect.DeepEqual(projection.cut.Nodes, cut.Nodes) {
		return fmt.Errorf("%w: source cut changed while applying live control directory", errGatewayControlDirectory)
	}
	return runtime.applyLiveControlDirectoryProjection(ctx, projection)
}

func (runtime *Runtime) applyLiveControlDirectoryProjection(
	ctx context.Context, projection liveControlDirectoryProjection,
) error {
	if runtime == nil || runtime.controlDirectory == nil || ctx == nil ||
		runtime.config.TLSProfile == nil || runtime.config.Authorization == nil ||
		!projection.cut.Valid() || !projection.serviceCut.Valid() {
		return errGatewayControlDirectory
	}
	cut := projection.cut
	serviceCut := projection.serviceCut
	fullCut := projection.fullCut
	// Bind the local frontend to the complete physical-node cut before
	// validating dependent service-directory projections.  A draining
	// gateway's service binding may require a committed continuation grant
	// that an older directory adapter cannot materialize; failing that
	// projection must not leave the local drain acknowledgement at the prior
	// revision.  The frontend update is still derived only from this
	// authenticated catalog cut and never relaxes service authorization.
	runtime.syncFrontendDrainFromDirectory(cut.Nodes, cut.Revision)
	if err := runtime.publishFrontendContinuationGrant(ctx, cut); err != nil {
		return fmt.Errorf("publish frontend continuation grant: %w", err)
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
	if runtime.distributedMetrics != nil {
		if err := runtime.distributedMetrics.UpdateNodeAggregates(controlDirectoryMetricEndpoints(directory)); err != nil {
			return fmt.Errorf("update distributed metrics node directory: %w", err)
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
	if fullCut.Valid() {
		bound, bindErr := bindRuntimeServiceDirectory(ctx, runtime.config.Transport, fullCut,
			runtime.config.RequireServiceDirectoryBinding)
		if bindErr != nil {
			return bindErr
		}
		if bound != nil && runtime.serviceDirectory != bound {
			return fmt.Errorf("%w: semantic transport replaced the retained service-directory gate", errGatewayControlDirectory)
		}
	} else if runtime.config.RequireServiceDirectoryBinding {
		return fmt.Errorf("%w: complete canonical frontend drain cut is required for the local semantic transport", errGatewayControlDirectory)
	}
	if err := runtime.serviceDirectory.ApplyCommittedCut(serviceCut); err != nil {
		return fmt.Errorf("update service directory: %w", err)
	}
	runtime.installPublishedFrontendContinuation(serviceCut)
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

// refreshLiveControlDirectory synchronously publishes the latest catalog and
// service-directory projections. Online table provisioning uses this fence
// before reporting CREATE success so an immediately following topology DDL
// cannot race the periodic directory refresh.
func (runtime *Runtime) refreshLiveControlDirectory(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return errGatewayControlDirectory
	}
	if err := runtime.lockControlDirectoryRefresh(ctx); err != nil {
		return err
	}
	defer runtime.controlDirectoryRefreshMu.Unlock()
	projection, err := runtime.readLiveControlDirectoryProjection(ctx)
	if err != nil {
		return fmt.Errorf("read live control directory: %w", err)
	}
	if err := runtime.applyLiveControlDirectoryProjection(ctx, projection); err != nil {
		return err
	}
	if projection.fullCut.Valid() {
		nodeCut := gateway.NodeDirectoryCut{
			Revision: projection.cut.Revision, Digest: projection.fullCut.DirectoryDigest,
			CatalogGeneration: projection.cut.CatalogGeneration, Nodes: slices.Clone(projection.cut.Nodes),
		}
		digest := projection.fullCut.Digest()
		if digest == ([32]byte{}) {
			runtime.publishedFrontendDrainCutValid = false
			return fmt.Errorf("%w: canonical frontend drain cut digest unavailable", gateway.ErrScalingRevision)
		}
		if runtime.publishedFrontendDrainCutValid && runtime.publishedFrontendDrainCutDigest == digest {
			return nil
		}
		// Invalidate before starting an unfinished round. A later source epoch
		// that happens to return to this digest must still complete a fresh
		// receiver barrier.
		runtime.publishedFrontendDrainCutValid = false
		if err := runtime.publishCanonicalFrontendDrainCut(ctx, nodeCut, projection.fullCut, projection.catalog); err != nil {
			return fmt.Errorf("publish canonical frontend drain cut: %w", err)
		}
		runtime.publishedFrontendDrainCutDigest = digest
		runtime.publishedFrontendDrainCutValid = true
	} else {
		runtime.publishedFrontendDrainCutValid = false
	}
	return nil
}

// lockControlDirectoryRefresh keeps cancellation effective while another
// refresh is still waiting on a slow receiver. A plain Mutex.Lock here would
// let a dead peer strand DDL callers until the previous round's transport
// deadline expires.
func (runtime *Runtime) lockControlDirectoryRefresh(ctx context.Context) error {
	if runtime == nil || ctx == nil {
		return errGatewayControlDirectory
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	const poll = 5 * time.Millisecond
	for {
		if runtime.controlDirectoryRefreshMu.TryLock() {
			return nil
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
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
			if err := runtime.refreshLiveControlDirectory(runtime.ctx); err != nil {
				runtime.config.Logf("gatewayruntime: refresh live control directory: %v", err)
			}
		}
	}
}
