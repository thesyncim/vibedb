package gatewayruntime

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

var gatewaySchemaDDLOperationDomain = []byte("vibedb/gateway/pgwire-schema-ddl-operation/2\x00")
var gatewaySchemaDDLReleasedPinsDomain = []byte("vibedb/gateway/pgwire-schema-ddl-released-pins/1\x00")
var gatewayTableDropOperationDomain = []byte("vibedb/gateway/pgwire-table-drop-operation/1\x00")

type gatewaySchemaDDLRuntime struct {
	authority *gateway.ReplicatedCatalogAuthority
	executor  *gateway.ReplicatedExecutor
	client    *schemainstall.Client
	builder   gatewaySchemaDDLBuilder
	resumer   gatewaySchemaDDLBuildResumer
	journal   string
	principal serviceauthz.Authority
	mu        sync.Mutex
}

type gatewaySchemaDDLBuilder interface {
	Build(context.Context, rafttransport.NodeID, schemainstall.BuildRequest, string) (
		sqldriver.ReplicatedSchemaDDLTarget, error,
	)
}

type gatewaySchemaDDLBuildResumer interface {
	ResumeBuild(context.Context, rafttransport.NodeID, [32]byte, raftmember.GroupKey) (
		schemainstall.BuildRequest, string, sqldriver.ReplicatedSchemaDDLTarget, bool, error,
	)
}

type gatewaySchemaDDLGate struct {
	route    gateway.ReplicatedRoute
	identity routegate.Identity
	binding  routegate.Binding
	epoch    uint64
	applied  uint64
}

func (r *gatewaySchemaDDLRuntime) drainProof(ctx context.Context, result gateway.SchemaRolloutResult,
	gates []gatewaySchemaDDLGate, plans []gateway.SchemaRolloutReplicaPlan,
) (schemainstall.DrainProof, error) {
	completed := gateway.ReplicatedOperationDigest(result.Record)
	authorizationDigest := schemainstall.AuthorizationDigest(result.Authorization)
	if result.Record.Kind != gateway.ReplicatedOperationSchema ||
		result.Record.State != gateway.ReplicatedOperationComplete ||
		result.Record.ID != result.Authorization.Operation || completed == ([32]byte{}) ||
		authorizationDigest == ([32]byte{}) || len(gates) == 0 || len(plans) == 0 {
		return schemainstall.DrainProof{}, gateway.ErrSchemaRollout
	}
	targets := make(map[raftmember.GroupKey]schemainstall.Request, len(gates))
	for _, plan := range plans {
		request := plan.Request
		if request.Operation != result.Authorization.Operation {
			return schemainstall.DrainProof{}, gateway.ErrSchemaRolloutConflict
		}
		if prior, found := targets[request.Group]; found &&
			(prior.AllocationGeneration != request.AllocationGeneration ||
				prior.ToSchemaGeneration != request.ToSchemaGeneration ||
				prior.ToRelationManifestDigest != request.ToRelationManifestDigest) {
			return schemainstall.DrainProof{}, gateway.ErrSchemaRolloutConflict
		}
		targets[request.Group] = request
	}
	ordered := slices.Clone(gates)
	latest, err := r.authority.Read(ctx)
	if err != nil || latest.Generation() < result.Authorization.TargetCatalogGeneration {
		return schemainstall.DrainProof{}, errors.Join(err, gateway.ErrSchemaRolloutConflict)
	}
	for index := range ordered {
		var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
		route, found := latest.ResolveReplicatedRoute(
			ordered[index].route.Distribution, ordered[index].route.Shard, replicas[:0],
		)
		target, expected := targets[route.Group]
		if !found || !expected || route.Group != ordered[index].route.Group ||
			route.AllocationGeneration != uint64(target.AllocationGeneration) ||
			route.Command.SchemaGeneration != target.ToSchemaGeneration ||
			route.Command.RelationManifestDigest != target.ToRelationManifestDigest {
			return schemainstall.DrainProof{}, gateway.ErrSchemaRolloutConflict
		}
		// Route-gate state survives schema-generation replacement, but route
		// reads must use the newly committed command carried by the target
		// catalog rather than the fenced predecessor command.
		ordered[index].route = route
	}
	slices.SortFunc(ordered, func(left, right gatewaySchemaDDLGate) int {
		leftGroup, rightGroup := left.route.Group, right.route.Group
		if order := bytes.Compare(leftGroup.ClusterID[:], rightGroup.ClusterID[:]); order != 0 {
			return order
		}
		if order := bytes.Compare(leftGroup.ClusterIncarnation[:], rightGroup.ClusterIncarnation[:]); order != 0 {
			return order
		}
		if order := cmp.Compare(leftGroup.TopologyRecoveryEpoch, rightGroup.TopologyRecoveryEpoch); order != 0 {
			return order
		}
		if order := bytes.Compare(leftGroup.ShardIncarnation[:], rightGroup.ShardIncarnation[:]); order != 0 {
			return order
		}
		return bytes.Compare(leftGroup.GroupID[:], rightGroup.GroupID[:])
	})
	hasher := sha256.New()
	_, _ = hasher.Write(gatewaySchemaDDLReleasedPinsDomain)
	_, _ = hasher.Write(completed[:])
	_, _ = hasher.Write(authorizationDigest[:])
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], uint64(len(ordered)))
	_, _ = hasher.Write(scalar[:])
	for _, gate := range ordered {
		observed, err := r.executor.ReadRouteGate(ctx, gate.route, gate.applied)
		if err != nil {
			return schemainstall.DrainProof{}, err
		}
		drain := observed.Status.Drain
		if drain.State != routegate.DrainActive || drain.Identity != gate.identity ||
			drain.Binding != gate.binding || drain.Epoch != gate.epoch || observed.Status.ActivePins != 0 {
			return schemainstall.DrainProof{}, gateway.ErrSchemaRolloutConflict
		}
		group := gate.route.Group
		_, _ = hasher.Write(group.ClusterID[:])
		_, _ = hasher.Write(group.ClusterIncarnation[:])
		binary.BigEndian.PutUint64(scalar[:], group.TopologyRecoveryEpoch)
		_, _ = hasher.Write(scalar[:])
		_, _ = hasher.Write(group.ShardIncarnation[:])
		_, _ = hasher.Write(group.GroupID[:])
		binary.BigEndian.PutUint64(scalar[:], observed.Applied)
		_, _ = hasher.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], observed.Status.Revision)
		_, _ = hasher.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], observed.Status.Epoch)
		_, _ = hasher.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], observed.Status.ReleasedPins)
		_, _ = hasher.Write(scalar[:])
		binary.BigEndian.PutUint64(scalar[:], observed.Status.RetainedRecords)
		_, _ = hasher.Write(scalar[:])
		_, _ = hasher.Write(drain.Identity[:])
		_, _ = hasher.Write(drain.Binding[:])
	}
	var released [32]byte
	hasher.Sum(released[:0])
	return schemainstall.DrainProof{Operation: result.Authorization.Operation,
		TargetCatalogGeneration:       result.Authorization.TargetCatalogGeneration,
		TargetCatalogDigest:           result.Authorization.TargetCatalogDigest,
		ActivationAuthorizationDigest: authorizationDigest,
		CompletedOperationDigest:      completed, ReleasedExecutionPinRoot: released}, nil
}

func newGatewaySchemaDDLRuntime(authority *gateway.ReplicatedCatalogAuthority,
	executor *gateway.ReplicatedExecutor, opener *gatewayShardControlOpener,
	readDeadline, writeDeadline rafttransport.DeadlineFunc, journal string,
	principal serviceauthz.Authority,
) (*gatewaySchemaDDLRuntime, error) {
	if authority == nil || executor == nil || opener == nil || journal == "" {
		return nil, gateway.ErrSchemaRollout
	}
	client, err := schemainstall.NewClient(schemainstall.ClientOptions{
		Opener: opener, ReadDeadline: readDeadline, WriteDeadline: writeDeadline,
	})
	if err != nil {
		return nil, err
	}
	return &gatewaySchemaDDLRuntime{authority: authority, executor: executor,
		client: client, builder: client, resumer: client, journal: journal, principal: principal}, nil
}

// buildOrResume closes the stateless-coordinator cut between durable replica
// materialization and replicated catalog intent publication. A replacement
// gateway deterministically mints the same operation, but has no local journal
// proving that a shard already retained its build. Build is still the fast
// path. Only a conflict or uncertain response performs the read-only resume
// RPC, so successful DDL and every query retain their existing cost.
func (r *gatewaySchemaDDLRuntime) buildOrResume(ctx context.Context,
	node rafttransport.NodeID, request schemainstall.BuildRequest, sql string,
) (schemainstall.BuildRequest, sqldriver.ReplicatedSchemaDDLTarget, error) {
	var target sqldriver.ReplicatedSchemaDDLTarget
	if r == nil || r.builder == nil || r.resumer == nil {
		return schemainstall.BuildRequest{}, target, gateway.ErrSchemaRollout
	}
	target, err := r.builder.Build(ctx, node, request, sql)
	if err == nil {
		return request, target, nil
	}
	if !errors.Is(err, schemainstall.ErrOutcomeUnknown) && !errors.Is(err, schemainstall.ErrConflict) {
		return schemainstall.BuildRequest{}, target, err
	}
	retained, retainedSQL, retainedTarget, _, resumeErr :=
		r.resumer.ResumeBuild(ctx, node, request.Operation, request.Group)
	if resumeErr != nil {
		return schemainstall.BuildRequest{}, target, errors.Join(err, resumeErr)
	}
	if retained.Operation != request.Operation || retained.Group != request.Group ||
		retained.AllocationGeneration != request.AllocationGeneration ||
		retained.FromSchemaGeneration != request.FromSchemaGeneration ||
		retained.FromRelationManifestDigest != request.FromRelationManifestDigest ||
		retained.SourceApplied == 0 || retained.SourceApplied > request.SourceApplied ||
		retained.SQLBytes != uint64(len(sql)) || retained.SQLDigest != sha256.Sum256([]byte(sql)) ||
		retainedSQL != sql {
		return schemainstall.BuildRequest{}, target, errors.Join(err, gateway.ErrSchemaRolloutConflict)
	}
	return retained, retainedTarget, nil
}

func (r *gatewaySchemaDDLRuntime) buildShadow(ctx context.Context, node rafttransport.NodeID,
	request schemainstall.BuildRequest, sql string,
) (bool, error) {
	if r == nil || r.client == nil || !request.Shadow {
		return false, gateway.ErrSchemaRollout
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		noOp, err := r.client.BuildShadow(ctx, node, request, sql)
		if !errors.Is(err, schemainstall.ErrOutcomeUnknown) {
			return noOp, err
		}
		select {
		case <-ctx.Done():
			return false, errors.Join(err, context.Cause(ctx))
		case <-ticker.C:
		}
	}
}

func gatewaySchemaDDLOperation(snapshot *gateway.Snapshot, table, sql string) ([32]byte, error) {
	placement, found := snapshot.Placement(table)
	if !found {
		return [32]byte{}, gateway.ErrSchemaRollout
	}
	var schemaGeneration uint64
	var logicalSchema [32]byte
	matched := 0
	for _, descriptor := range snapshot.ReplicatedShardDescriptors() {
		if descriptor.Distribution != placement.Distribution {
			continue
		}
		if descriptor.Command.SchemaGeneration == 0 || descriptor.LogicalSchemaDigest == ([32]byte{}) ||
			matched != 0 && (descriptor.Command.SchemaGeneration != schemaGeneration ||
				descriptor.LogicalSchemaDigest != logicalSchema) {
			return [32]byte{}, gateway.ErrSchemaRolloutConflict
		}
		schemaGeneration, logicalSchema = descriptor.Command.SchemaGeneration, descriptor.LogicalSchemaDigest
		matched++
	}
	if matched == 0 {
		return [32]byte{}, gateway.ErrSchemaRollout
	}
	h := sha256.New()
	_, _ = h.Write(gatewaySchemaDDLOperationDomain)
	var generation [8]byte
	binary.LittleEndian.PutUint64(generation[:], schemaGeneration)
	_, _ = h.Write(generation[:])
	_, _ = h.Write(logicalSchema[:])
	_, _ = h.Write([]byte(placement.Distribution))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(table))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(sql))
	var result [32]byte
	h.Sum(result[:0])
	return result, nil
}

func gatewayTableDropOperation(snapshot *gateway.Snapshot, table string) ([32]byte, error) {
	placement, found := snapshot.Placement(table)
	if !found {
		return [32]byte{}, gateway.ErrSchemaRollout
	}
	h := sha256.New()
	_, _ = h.Write(gatewayTableDropOperationDomain)
	_, _ = h.Write([]byte(placement.Distribution))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(table))
	for _, descriptor := range snapshot.ReplicatedShardDescriptors() {
		if descriptor.Distribution != placement.Distribution {
			continue
		}
		_, _ = h.Write(descriptor.Group.ClusterID[:])
		_, _ = h.Write(descriptor.Group.ClusterIncarnation[:])
		_, _ = h.Write(descriptor.Group.ShardIncarnation[:])
		_, _ = h.Write(descriptor.Group.GroupID[:])
	}
	var result [32]byte
	h.Sum(result[:0])
	return result, nil
}

// retainedOperation resolves pre-v2 operation directories created before the
// operation identity was detached from the global catalog generation. It is
// also a bounded recovery index for a gateway restarted after replicas built
// but before the catalog operation row existed. Replica receipts—not directory
// names—authenticate the SQL and exact source schema.
func (r *gatewaySchemaDDLRuntime) retainedOperation(ctx context.Context,
	descriptors []gateway.ReplicatedShardDescriptor, sql string, current [32]byte,
) ([32]byte, error) {
	entries, err := os.ReadDir(r.journal)
	if os.IsNotExist(err) {
		return current, nil
	}
	if err != nil {
		return [32]byte{}, err
	}
	const maxRetainedSchemaOperations = 4096
	if len(entries) > maxRetainedSchemaOperations {
		return [32]byte{}, gateway.ErrSchemaRolloutConflict
	}
	selected := current
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) != sha256.Size*2 {
			continue
		}
		raw, decodeErr := hex.DecodeString(entry.Name())
		if decodeErr != nil || len(raw) != sha256.Size {
			continue
		}
		var candidate [32]byte
		copy(candidate[:], raw)
		if candidate == ([32]byte{}) || candidate == current {
			continue
		}
		retainedSQL, recoveryErr := r.recoverySQL(ctx, descriptors, candidate)
		if recoveryErr != nil || retainedSQL != sql {
			continue
		}
		if selected != current && selected != candidate {
			return [32]byte{}, gateway.ErrSchemaRolloutConflict
		}
		selected = candidate
	}
	return selected, nil
}

func gatewaySchemaDDLGateIdentity(operation [32]byte, group raftmember.GroupKey) (routegate.Identity, routegate.Binding) {
	return schemainstall.SchemaDDLRouteGateIdentity(operation, group)
}

func (r *gatewaySchemaDDLRuntime) openGateSession(ctx context.Context,
	operation [32]byte, route gateway.ReplicatedRoute, suffix string,
) (*gateway.NativeSession, error) {
	identity, binding := gatewaySchemaDDLGateIdentity(operation, route.Group)
	var clientID replication.ID128
	var retryHome replication.RetryHome
	copy(clientID[:], identity[:])
	copy(retryHome[:], binding[:])
	directory := filepath.Join(r.journal, hex.EncodeToString(operation[:]))
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(directory, hex.EncodeToString(route.Group.GroupID[:])+"-"+suffix)
	tenant := []byte("schema-ddl")
	journalBinding, err := gateway.NativeSessionJournalBinding(route,
		string(route.Distribution), string(route.Shard), tenant, 1,
		serviceauthz.CapabilityTopology)
	if err != nil {
		return nil, err
	}
	journal, err := gateway.OpenNativeSessionJournal(gateway.NativeSessionJournalOptions{
		Path: path, ClientID: clientID, RetryHome: retryHome,
		MaxCommandBytes: replication.MaxCommandBytes, Binding: journalBinding,
	})
	if err != nil {
		return nil, err
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{
		Executor: r.executor, Route: route, Distribution: string(route.Distribution),
		Shard: string(route.Shard), Tenant: tenant, ClientID: clientID, RetryHome: retryHome,
		Resolver: gateway.BaseRelationResolver{Relation: 1}, Journal: journal,
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 1,
		InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		return nil, err
	}
	if session.Status().Pending {
		if _, err := session.RetryPending(ctx); err != nil {
			return nil, err
		}
	}
	if !session.Status().Active {
		if _, err := session.Open(ctx, time.Now().Add(2*time.Minute).UnixNano()); err != nil {
			return nil, err
		}
	}
	return session, nil
}

func (r *gatewaySchemaDDLRuntime) acquireGate(ctx context.Context,
	operation [32]byte, route gateway.ReplicatedRoute,
) (gatewaySchemaDDLGate, error) {
	identity, binding := gatewaySchemaDDLGateIdentity(operation, route.Group)
	observed, err := r.executor.ReadRouteGate(ctx, route, 1)
	if err != nil {
		return gatewaySchemaDDLGate{}, err
	}
	drain := observed.Status.Drain
	if drain.State == routegate.DrainActive && (drain.Identity != identity || drain.Binding != binding) {
		released, releaseErr := r.releaseCompletedSchemaGate(ctx, route, observed)
		if releaseErr != nil || !released {
			return gatewaySchemaDDLGate{}, errors.Join(releaseErr, gateway.ErrSchemaRolloutConflict)
		}
		observed, err = r.executor.ReadRouteGate(ctx, route, observed.Applied)
		if err != nil {
			return gatewaySchemaDDLGate{}, err
		}
	}
	session, err := r.openGateSession(ctx, operation, route, "acquire")
	if err != nil {
		return gatewaySchemaDDLGate{}, err
	}
	command := routegate.Command{Operation: routegate.OperationBeginExclusive,
		Epoch: observed.Status.Epoch, Identity: identity, Binding: binding}
	if _, err = session.RouteGate(ctx, command); err != nil {
		return gatewaySchemaDDLGate{}, err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		observed, err = r.executor.ReadRouteGate(ctx, route, 1)
		if err != nil {
			return gatewaySchemaDDLGate{}, err
		}
		drain := observed.Status.Drain
		if drain.Identity != identity || drain.Binding != binding || drain.Epoch != command.Epoch {
			return gatewaySchemaDDLGate{}, gateway.ErrSchemaRolloutConflict
		}
		if drain.State == routegate.DrainActive {
			// Retiring the topology session is itself a replicated publication.
			// Settle it before selecting the immutable source cut, then observe
			// the exact applied index under the still-active route gate.
			if err := session.RetireReleaseAndDestroy(ctx); err != nil {
				return gatewaySchemaDDLGate{}, err
			}
			observed, err = r.executor.ReadRouteGate(ctx, route, observed.Applied)
			if err != nil {
				return gatewaySchemaDDLGate{}, err
			}
			return gatewaySchemaDDLGate{route: route, identity: identity,
				binding: binding, epoch: command.Epoch, applied: observed.Applied}, nil
		}
		if drain.State != routegate.DrainPending {
			return gatewaySchemaDDLGate{}, gateway.ErrSchemaRolloutConflict
		}
		select {
		case <-ctx.Done():
			return gatewaySchemaDDLGate{}, context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

// releaseCompletedSchemaGate reconciles the narrow crash cut where the target
// catalog was committed but the coordinator died before releasing its traffic
// gate. The replicated catalog's Complete record and the gate's exact
// operation-derived identity jointly authorize this cleanup; a running or
// unrelated operation is never displaced.
func (r *gatewaySchemaDDLRuntime) releaseCompletedSchemaGate(ctx context.Context,
	route gateway.ReplicatedRoute, observed gateway.ReplicatedRouteGateReadResult,
) (bool, error) {
	drain := observed.Status.Drain
	if drain.State != routegate.DrainActive {
		return false, nil
	}
	ids, err := r.authority.ReadOperationIDs(ctx)
	if err != nil {
		return false, err
	}
	for _, operation := range ids {
		record, readErr := r.authority.ReadOperation(ctx, operation)
		if readErr != nil {
			return false, readErr
		}
		if record.Kind != gateway.ReplicatedOperationSchema || record.State != gateway.ReplicatedOperationComplete {
			continue
		}
		identity, binding := gatewaySchemaDDLGateIdentity(operation, route.Group)
		if drain.Identity != identity || drain.Binding != binding {
			continue
		}
		session, openErr := r.openGateSession(ctx, operation, route, "release-recovered")
		if openErr != nil {
			return false, openErr
		}
		command := routegate.Command{Operation: routegate.OperationReleaseExclusive,
			Epoch: drain.Epoch, Identity: identity, Binding: binding}
		_, releaseErr := session.RouteGate(ctx, command)
		return true, errors.Join(releaseErr, session.RetireReleaseAndDestroy(ctx))
	}
	return false, nil
}

func (r *gatewaySchemaDDLRuntime) releaseGate(ctx context.Context,
	operation [32]byte, gate gatewaySchemaDDLGate, snapshot *gateway.Snapshot,
) error {
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := snapshot.ResolveReplicatedRoute(gate.route.Distribution, gate.route.Shard, replicas[:0])
	if !ok {
		return gateway.ErrReplicatedRoute
	}
	observed, err := r.executor.ReadRouteGate(ctx, route, 1)
	if err != nil {
		return err
	}
	if observed.Status.Drain.State == routegate.DrainReleased ||
		observed.Status.Drain.State == routegate.DrainNone {
		return nil
	}
	session, err := r.openGateSession(ctx, operation, route, "release")
	if err != nil {
		return err
	}
	command := routegate.Command{Operation: routegate.OperationReleaseExclusive,
		Epoch: observed.Status.Epoch, Identity: gate.identity, Binding: gate.binding}
	_, releaseErr := session.RouteGate(ctx, command)
	return errors.Join(releaseErr, session.RetireReleaseAndDestroy(ctx))
}

// Recover settles every nonterminal schema operation before PostgreSQL can
// route through a stale catalog head. The replicated operation directory is
// the bounded work list; an authenticated shard build receipt supplies the
// exact retained SQL. Execute then revalidates all RF3 receipts and replays the
// ordinary idempotent controller. There is no table scan or query-path branch.
func (r *gatewaySchemaDDLRuntime) Recover(ctx context.Context) error {
	if r == nil || ctx == nil {
		return gateway.ErrSchemaRollout
	}
	ctx, err := serviceauthz.WithAuthority(ctx, r.principal)
	if err != nil {
		return err
	}
	operations, err := r.authority.ReadOperationIDs(ctx)
	if err != nil {
		return fmt.Errorf("read schema recovery directory: %w", err)
	}
	for _, operation := range operations {
		record, readErr := r.authority.ReadOperation(ctx, operation)
		if readErr != nil {
			return fmt.Errorf("read schema recovery operation %x: %w", operation, readErr)
		}
		if record.Kind != gateway.ReplicatedOperationSchema ||
			(record.State != gateway.ReplicatedOperationPlanned &&
				record.State != gateway.ReplicatedOperationRunning) {
			continue
		}
		current, readErr := r.authority.Read(ctx)
		if readErr != nil {
			return fmt.Errorf("read schema recovery catalog: %w", readErr)
		}
		sql, findErr := r.recoverySQL(ctx, current.ReplicatedShardDescriptors(), operation)
		if findErr != nil {
			return fmt.Errorf("recover schema operation %x: %w", operation, findErr)
		}
		table, resolveErr := gateway.ResolveReplicatedSchemaDDLTable(current, sql)
		derived, operationErr := gatewaySchemaDDLOperation(current, table, sql)
		_, legacyJournalErr := os.Stat(filepath.Join(r.journal, hex.EncodeToString(operation[:])))
		legacyBound := legacyJournalErr == nil
		if resolveErr != nil || operationErr != nil || table == "" || derived != operation && !legacyBound {
			return fmt.Errorf("recover schema operation %x SQL binding: %w",
				operation, errors.Join(resolveErr, operationErr, legacyJournalErr, gateway.ErrSchemaRolloutConflict))
		}
		if executeErr := r.Execute(ctx, sql); executeErr != nil {
			return fmt.Errorf("resume schema operation %x: %w", operation, executeErr)
		}
	}
	return r.recoverRetainedTableDrop(ctx)
}

func (r *gatewaySchemaDDLRuntime) recoverRetainedTableDrop(ctx context.Context) error {
	entries, err := os.ReadDir(r.journal)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	const maxRetainedSchemaOperations = 4096
	if len(entries) > maxRetainedSchemaOperations {
		return gateway.ErrSchemaRolloutConflict
	}
	retained := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) == sha256.Size*2 {
			retained[entry.Name()] = struct{}{}
		}
	}
	current, err := r.authority.Read(ctx)
	if err != nil {
		return err
	}
	for _, profile := range current.ReplicatedTableProfiles() {
		operation, operationErr := gatewayTableDropOperation(current, profile.Table)
		if operationErr != nil {
			return operationErr
		}
		if _, found := retained[hex.EncodeToString(operation[:])]; !found {
			continue
		}
		placement, found := current.Placement(profile.Table)
		if !found {
			continue
		}
		fenced := false
		for _, descriptor := range current.ReplicatedShardDescriptors() {
			if descriptor.Distribution != placement.Distribution {
				continue
			}
			var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
			route, ok := current.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard, replicas[:0])
			if !ok {
				return gateway.ErrReplicatedRoute
			}
			observed, readErr := r.executor.ReadRouteGate(ctx, route, 1)
			if readErr != nil {
				return readErr
			}
			identity, binding := gatewaySchemaDDLGateIdentity(operation, route.Group)
			drain := observed.Status.Drain
			if (drain.State == routegate.DrainPending || drain.State == routegate.DrainActive) &&
				drain.Identity == identity && drain.Binding == binding {
				fenced = true
			}
		}
		if !fenced {
			continue
		}
		if dropErr := r.DropTable(ctx, profile.Table, false); dropErr != nil {
			return fmt.Errorf("resume table retirement %q: %w", profile.Table, dropErr)
		}
		return nil
	}
	return nil
}

func (r *gatewaySchemaDDLRuntime) recoverySQL(ctx context.Context,
	descriptors []gateway.ReplicatedShardDescriptor, operation [32]byte,
) (string, error) {
	if r == nil || r.resumer == nil || len(descriptors) == 0 || operation == ([32]byte{}) {
		return "", gateway.ErrSchemaRollout
	}
	var retained string
	for _, descriptor := range descriptors {
		for _, replica := range descriptor.Replicas {
			request, sql, target, _, err := r.resumer.ResumeBuild(ctx, replica.Node, operation, descriptor.Group)
			if errors.Is(err, schemainstall.ErrMissing) {
				continue
			}
			if err != nil {
				return "", err
			}
			sourceMatch := request.FromSchemaGeneration == descriptor.Command.SchemaGeneration &&
				request.FromRelationManifestDigest == descriptor.Command.RelationManifestDigest
			targetMatch := !target.NoOp && request.FromSchemaGeneration+1 == descriptor.Command.SchemaGeneration &&
				target.Proof.Catalog.SchemaGeneration == descriptor.Command.SchemaGeneration &&
				target.Proof.Catalog.RelationManifestDigest == descriptor.Command.RelationManifestDigest
			if sql == "" || request.Operation != operation || request.Group != descriptor.Group ||
				request.AllocationGeneration != descriptor.AllocationGeneration || !sourceMatch && !targetMatch ||
				retained != "" && retained != sql {
				return "", fmt.Errorf("%w: retained member %d binding sql=%t operation=%t group=%t allocation=%t generation=%t(%d/%d) manifest=%t(%x/%x) consistent-sql=%t",
					gateway.ErrSchemaRolloutConflict, replica.Member, sql != "", request.Operation == operation,
					request.Group == descriptor.Group, request.AllocationGeneration == descriptor.AllocationGeneration,
					sourceMatch || targetMatch,
					request.FromSchemaGeneration, descriptor.Command.SchemaGeneration,
					request.FromRelationManifestDigest == descriptor.Command.RelationManifestDigest,
					request.FromRelationManifestDigest, descriptor.Command.RelationManifestDigest,
					retained == "" || retained == sql)
			}
			retained = sql
		}
	}
	if retained == "" {
		return "", errors.Join(schemainstall.ErrMissing, gateway.ErrSchemaRolloutConflict)
	}
	return retained, nil
}

func (r *gatewaySchemaDDLRuntime) Execute(ctx context.Context, sql string) (resultErr error) {
	if r == nil || ctx == nil {
		return gateway.ErrSchemaRollout
	}
	ctx, err := serviceauthz.WithAuthority(ctx, r.principal)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, err := r.authority.Read(ctx)
	if err != nil {
		return fmt.Errorf("read schema catalog head: %w", err)
	}
	table, err := gateway.ResolveReplicatedSchemaDDLTable(current, sql)
	if err != nil {
		return fmt.Errorf("resolve distributed schema DDL: %w", err)
	}
	if table == "" {
		return nil
	}
	placement, found := current.Placement(table)
	if !found {
		return fmt.Errorf("%w: %s", gateway.ErrSchemaRollout, table)
	}
	descriptors := current.ReplicatedShardDescriptors()
	operation, err := gatewaySchemaDDLOperation(current, table, sql)
	if err != nil {
		return fmt.Errorf("derive schema operation: %w", err)
	}
	operation, err = r.retainedOperation(ctx, descriptors, sql, operation)
	if err != nil {
		return fmt.Errorf("resolve retained schema operation: %w", err)
	}
	var gates []gatewaySchemaDDLGate
	defer func() {
		record, operationErr := r.authority.ReadOperation(context.WithoutCancel(ctx), operation)
		if operationErr == nil && record.Kind == gateway.ReplicatedOperationSchema &&
			record.State == gateway.ReplicatedOperationRunning {
			// Running is the no-return boundary: the prepared source cut must
			// remain fenced until exact activation and catalog publication finish.
			// Releasing here would admit a suffix the immutable target did not
			// certify and make crash recovery impossible without weakening proof.
			return
		}
		if operationErr != nil && !errors.Is(operationErr, gateway.ErrReplicatedOperationMissing) {
			resultErr = errors.Join(resultErr, fmt.Errorf("read schema operation for gate release: %w", operationErr))
			return
		}
		latest, readErr := r.authority.Read(context.WithoutCancel(ctx))
		if readErr != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("read schema catalog for gate release: %w", readErr))
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		for i := len(gates) - 1; i >= 0; i-- {
			if releaseErr := r.releaseGate(releaseCtx, operation, gates[i], latest); releaseErr != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("release schema route gate: %w", releaseErr))
			}
		}
	}()
	existing, operationErr := r.authority.ReadOperation(ctx, operation)
	recovering := operationErr == nil && existing.Kind == gateway.ReplicatedOperationSchema &&
		existing.State == gateway.ReplicatedOperationRunning
	if operationErr != nil && !errors.Is(operationErr, gateway.ErrReplicatedOperationMissing) {
		return fmt.Errorf("read durable schema operation: %w", operationErr)
	}
	if !recovering {
		_, statErr := os.Stat(filepath.Join(r.journal, hex.EncodeToString(operation[:])))
		recovering = statErr == nil
		if statErr != nil && !os.IsNotExist(statErr) {
			return fmt.Errorf("inspect local schema operation journal: %w", statErr)
		}
	}
	// A crash may leave every replica on the authenticated target while the
	// gateway catalog still names the source generation. Reading that stale
	// data route cannot work—and must not be used as evidence. First obtain a
	// read-only resume receipt from every control endpoint. Only when all RF3
	// replicas prove this exact operation locally active may the controller
	// replay its idempotent receipts and finish the catalog CAS without opening
	// or reacquiring a traffic gate. If even one replica is not active, fall
	// through to the ordinary gated recovery path.
	if recovering {
		allActive := true
		matched := 0
		resumed := make([]gateway.SchemaDDLReplicaBuild, 0, gateway.ServingReplicaCount)
		for _, descriptor := range descriptors {
			if descriptor.Distribution != placement.Distribution {
				continue
			}
			matched++
			for _, replica := range descriptor.Replicas {
				request, retainedSQL, replicaTarget, active, resumeErr :=
					r.client.ResumeBuild(ctx, replica.Node, operation, descriptor.Group)
				if errors.Is(resumeErr, schemainstall.ErrMissing) {
					allActive = false
					break
				}
				if resumeErr != nil {
					return fmt.Errorf("inspect resumed schema build member %d: %w", replica.Member, resumeErr)
				}
				sourceMatch := request.FromSchemaGeneration == descriptor.Command.SchemaGeneration &&
					request.FromRelationManifestDigest == descriptor.Command.RelationManifestDigest
				targetMatch := !replicaTarget.NoOp && request.FromSchemaGeneration+1 == descriptor.Command.SchemaGeneration &&
					replicaTarget.Proof.Catalog.SchemaGeneration == descriptor.Command.SchemaGeneration &&
					replicaTarget.Proof.Catalog.RelationManifestDigest == descriptor.Command.RelationManifestDigest
				if retainedSQL != sql || request.AllocationGeneration != descriptor.AllocationGeneration ||
					!sourceMatch && !targetMatch {
					return fmt.Errorf("%w: resumed schema build member %d differs", gateway.ErrSchemaRolloutConflict, replica.Member)
				}
				allActive = allActive && active
				resumed = append(resumed, gateway.SchemaDDLReplicaBuild{Node: replica.Node,
					Member: replica.Member, Request: request, Target: replicaTarget})
			}
			if !allActive {
				break
			}
		}
		if allActive && matched != 0 {
			reconciled, applied, reconcileErr := gateway.ReconcileAppliedReplicatedSchemaDDLCatalog(
				current, operation, table, sql, resumed,
			)
			if reconcileErr != nil {
				return fmt.Errorf("reconcile applied schema catalog: %w", reconcileErr)
			}
			if applied {
				for reconciled != current {
					publishErr := r.authority.Publish(ctx, current.Generation(), reconciled)
					if !errors.Is(publishErr, gateway.ErrReplicatedCatalogPending) {
						if publishErr != nil {
							return fmt.Errorf("publish reconciled schema catalog: %w", publishErr)
						}
						break
					}
					if retryErr := r.authority.RetryPending(ctx); retryErr != nil &&
						!errors.Is(retryErr, gateway.ErrReplicatedCatalogPending) {
						return errors.Join(publishErr, retryErr)
					}
					if ctx.Err() != nil {
						return errors.Join(publishErr, context.Cause(ctx))
					}
				}
				latest, readErr := r.authority.Read(ctx)
				if readErr != nil {
					return readErr
				}
				for _, descriptor := range descriptors {
					if descriptor.Distribution != placement.Distribution {
						continue
					}
					identity, binding := gatewaySchemaDDLGateIdentity(operation, descriptor.Group)
					gate := gatewaySchemaDDLGate{route: gateway.ReplicatedRoute{
						Distribution: descriptor.Distribution, Shard: descriptor.Shard, Group: descriptor.Group,
					}, identity: identity, binding: binding}
					if releaseErr := r.releaseGate(ctx, operation, gate, latest); releaseErr != nil {
						return releaseErr
					}
				}
				return nil
			}
			target, plans, planErr := gateway.BuildReplicatedSchemaDDLPlan(current, operation, table, sql, resumed)
			if planErr != nil {
				return fmt.Errorf("reconstruct completed schema plan: %w", planErr)
			}
			controller, controllerErr := gateway.NewSchemaRolloutController(gateway.SchemaRolloutControllerOptions{
				Authority: r.authority, Client: r.client, MaxConcurrent: 16,
			})
			if controllerErr != nil {
				return controllerErr
			}
			_, controllerErr = controller.Execute(ctx, operation, target, plans)
			if controllerErr != nil {
				return fmt.Errorf("publish completed schema plan: %w", controllerErr)
			}
			latest, readErr := r.authority.Read(ctx)
			if readErr != nil {
				return readErr
			}
			for _, descriptor := range descriptors {
				if descriptor.Distribution != placement.Distribution {
					continue
				}
				identity, binding := gatewaySchemaDDLGateIdentity(operation, descriptor.Group)
				gate := gatewaySchemaDDLGate{route: gateway.ReplicatedRoute{
					Distribution: descriptor.Distribution, Shard: descriptor.Shard, Group: descriptor.Group,
				}, identity: identity, binding: binding}
				if releaseErr := r.releaseGate(ctx, operation, gate, latest); releaseErr != nil {
					return releaseErr
				}
			}
			return nil
		}
	}
	if !recovering {
		shadowCount, noOpCount := 0, 0
		for _, descriptor := range descriptors {
			if descriptor.Distribution != placement.Distribution {
				continue
			}
			for _, replica := range descriptor.Replicas {
				request := schemainstall.BuildRequest{Shadow: true, Operation: operation, Group: descriptor.Group,
					AllocationGeneration:       descriptor.AllocationGeneration,
					FromSchemaGeneration:       descriptor.Command.SchemaGeneration,
					FromRelationManifestDigest: descriptor.Command.RelationManifestDigest,
					SQLBytes:                   uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
				noOp, shadowErr := r.buildShadow(ctx, replica.Node, request, sql)
				if shadowErr != nil {
					return fmt.Errorf("build online schema shadow member %d: %w", replica.Member, shadowErr)
				}
				shadowCount++
				if noOp {
					noOpCount++
				}
			}
		}
		if shadowCount == 0 || noOpCount != 0 && noOpCount != shadowCount {
			return fmt.Errorf("online schema shadows disagree no-op=%d replicas=%d: %w",
				noOpCount, shadowCount, gateway.ErrSchemaRolloutConflict)
		}
		if noOpCount == shadowCount {
			return nil
		}
	}
	for _, descriptor := range descriptors {
		if descriptor.Distribution != placement.Distribution {
			continue
		}
		var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
		route, ok := current.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard, replicas[:0])
		if !ok {
			return fmt.Errorf("resolve schema route for %s/%s: %w", descriptor.Distribution, descriptor.Shard, gateway.ErrReplicatedRoute)
		}
		if recovering {
			identity, binding := gatewaySchemaDDLGateIdentity(operation, route.Group)
			observed, observeErr := r.executor.ReadRouteGate(ctx, route, 1)
			if observeErr != nil {
				return fmt.Errorf("observe recovering schema route gate: %w", observeErr)
			}
			drain := observed.Status.Drain
			if drain.State == routegate.DrainActive && (drain.Identity != identity || drain.Binding != binding) {
				released, releaseErr := r.releaseCompletedSchemaGate(ctx, route, observed)
				if releaseErr != nil || !released {
					return fmt.Errorf("recovering schema route gate is owned by another operation: %w",
						errors.Join(releaseErr, gateway.ErrSchemaRolloutConflict))
				}
				observed, observeErr = r.executor.ReadRouteGate(ctx, route, observed.Applied)
				if observeErr != nil {
					return fmt.Errorf("observe released predecessor schema gate: %w", observeErr)
				}
				drain = observed.Status.Drain
			}
			if drain.State == routegate.DrainActive {
				gates = append(gates, gatewaySchemaDDLGate{route: route, identity: identity,
					binding: binding, epoch: drain.Epoch, applied: observed.Applied})
				continue
			}
			if drain.State != routegate.DrainReleased && drain.State != routegate.DrainNone {
				return fmt.Errorf("recovering schema route gate state=%d: %w", drain.State, gateway.ErrSchemaRolloutConflict)
			}
			gate, err := r.acquireGate(ctx, operation, route)
			if err != nil {
				return fmt.Errorf("recover schema route gate for %s/%s: %w", descriptor.Distribution, descriptor.Shard, err)
			}
			gates = append(gates, gate)
		} else {
			gate, err := r.acquireGate(ctx, operation, route)
			if err != nil {
				return fmt.Errorf("acquire schema route gate for %s/%s: %w", descriptor.Distribution, descriptor.Shard, err)
			}
			gates = append(gates, gate)
		}
	}
	if len(gates) == 0 {
		return fmt.Errorf("schema DDL matched no replicated route: %w", gateway.ErrSchemaRollout)
	}
	byGroup := make(map[raftmember.GroupKey]gatewaySchemaDDLGate, len(gates))
	for _, gate := range gates {
		byGroup[gate.route.Group] = gate
	}
	builds := make([]gateway.SchemaDDLReplicaBuild, 0, len(gates)*gateway.ServingReplicaCount)
	for _, descriptor := range descriptors {
		gate, changed := byGroup[descriptor.Group]
		if !changed {
			continue
		}
		for _, replica := range descriptor.Replicas {
			if recovering {
				request, retainedSQL, target, _, err := r.client.ResumeBuild(ctx, replica.Node, operation, descriptor.Group)
				if errors.Is(err, schemainstall.ErrMissing) {
					request = schemainstall.BuildRequest{Operation: operation, Group: descriptor.Group,
						AllocationGeneration:       descriptor.AllocationGeneration,
						FromSchemaGeneration:       descriptor.Command.SchemaGeneration,
						FromRelationManifestDigest: descriptor.Command.RelationManifestDigest,
						SourceApplied:              gate.applied, SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
					request, target, err = r.buildOrResume(ctx, replica.Node, request, sql)
					retainedSQL = sql
				}
				if err != nil {
					return errors.Join(fmt.Errorf("resume schema build member %d: %w", replica.Member, err), gateway.ErrSchemaRolloutConflict)
				}
				if retainedSQL != sql || request.AllocationGeneration != descriptor.AllocationGeneration ||
					request.FromSchemaGeneration != descriptor.Command.SchemaGeneration ||
					request.FromRelationManifestDigest != descriptor.Command.RelationManifestDigest {
					return fmt.Errorf("%w: resumed schema build member %d differs: sql=%t allocation=%t generation=%t manifest=%t",
						gateway.ErrSchemaRolloutConflict, replica.Member, retainedSQL == sql,
						request.AllocationGeneration == descriptor.AllocationGeneration,
						request.FromSchemaGeneration == descriptor.Command.SchemaGeneration,
						request.FromRelationManifestDigest == descriptor.Command.RelationManifestDigest)
				}
				builds = append(builds, gateway.SchemaDDLReplicaBuild{Node: replica.Node,
					Member: replica.Member, Request: request, Target: target})
				continue
			}
			request := schemainstall.BuildRequest{Operation: operation, Group: descriptor.Group,
				AllocationGeneration:       descriptor.AllocationGeneration,
				FromSchemaGeneration:       descriptor.Command.SchemaGeneration,
				FromRelationManifestDigest: descriptor.Command.RelationManifestDigest,
				SourceApplied:              gate.applied, SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
			request, target, err := r.buildOrResume(ctx, replica.Node, request, sql)
			if err != nil {
				return fmt.Errorf("build schema target member %d: %w", replica.Member, err)
			}
			builds = append(builds, gateway.SchemaDDLReplicaBuild{Node: replica.Node,
				Member: replica.Member, Request: request, Target: target})
		}
	}
	target, plans, err := gateway.BuildReplicatedSchemaDDLPlan(current, operation, table, sql, builds)
	if err != nil {
		return fmt.Errorf("construct distributed schema plan: %w", err)
	}
	if len(plans) == 0 {
		return nil
	}
	controller, err := gateway.NewSchemaRolloutController(gateway.SchemaRolloutControllerOptions{
		Authority: r.authority, Client: r.client, MaxConcurrent: 16,
	})
	if err != nil {
		return fmt.Errorf("create schema rollout controller: %w", err)
	}
	result, err := controller.Execute(ctx, operation, target, plans)
	if err != nil {
		return fmt.Errorf("execute distributed schema plan: %w", err)
	}
	proof, err := r.drainProof(ctx, result, gates, plans)
	if err != nil {
		return fmt.Errorf("certify distributed schema drain: %w", err)
	}
	if err = controller.Drain(ctx, plans, result.Authorization, proof); err != nil {
		return fmt.Errorf("drain distributed schema predecessors: %w", err)
	}
	return nil
}

// DropTable cuts one independently provisioned table out of the replicated
// namespace. It reuses the schema traffic gate, but unlike ALTER/TRUNCATE it
// has no target data image: the certified catalog removal is the commit point.
func (r *gatewaySchemaDDLRuntime) DropTable(ctx context.Context, table string, ifExists bool) (resultErr error) {
	if r == nil || ctx == nil || table == "" {
		return gateway.ErrSchemaRollout
	}
	ctx, err := serviceauthz.WithAuthority(ctx, r.principal)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, err := r.authority.Read(ctx)
	if err != nil {
		return err
	}
	placement, found := current.Placement(table)
	if !found {
		if ifExists {
			return nil
		}
		return fmt.Errorf("%w: %s", sqldriver.ErrTableNotFound, table)
	}
	operation, err := gatewayTableDropOperation(current, table)
	if err != nil {
		return err
	}
	gates := make([]gatewaySchemaDDLGate, 0, 1)
	committed := false
	defer func() {
		if committed {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		for index := len(gates) - 1; index >= 0; index-- {
			resultErr = errors.Join(resultErr, r.releaseGate(releaseCtx, operation, gates[index], current))
		}
	}()
	for _, descriptor := range current.ReplicatedShardDescriptors() {
		if descriptor.Distribution != placement.Distribution {
			continue
		}
		var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
		route, ok := current.ResolveReplicatedRoute(descriptor.Distribution, descriptor.Shard, replicas[:0])
		if !ok || route.Group != descriptor.Group {
			return gateway.ErrReplicatedRoute
		}
		gate, acquireErr := r.acquireGate(ctx, operation, route)
		if acquireErr != nil {
			return acquireErr
		}
		gates = append(gates, gate)
	}
	if len(gates) == 0 {
		return gateway.ErrReplicatedRoute
	}
	if err = r.authority.RetireProvisionedTable(ctx, table, operation); err != nil {
		return err
	}
	committed = true
	return nil
}
