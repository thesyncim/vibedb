package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

var gatewaySchemaDDLOperationDomain = []byte("vibedb/gateway/pgwire-schema-ddl-operation\x00")

type gatewaySchemaDDLRuntime struct {
	authority *gateway.ReplicatedCatalogAuthority
	executor  *gateway.ReplicatedExecutor
	client    *schemainstall.Client
	resumer   gatewaySchemaDDLBuildResumer
	journal   string
	principal serviceauthz.Authority
	mu        sync.Mutex
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
		client: client, resumer: client, journal: journal, principal: principal}, nil
}

func gatewaySchemaDDLOperation(snapshot *gateway.Snapshot, table, sql string) [32]byte {
	h := sha256.New()
	_, _ = h.Write(gatewaySchemaDDLOperationDomain)
	var generation [8]byte
	binary.LittleEndian.PutUint64(generation[:], snapshot.Generation())
	_, _ = h.Write(generation[:])
	_, _ = h.Write([]byte(table))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(sql))
	var result [32]byte
	h.Sum(result[:0])
	return result
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
		if resolveErr != nil || table == "" || gatewaySchemaDDLOperation(current, table, sql) != operation {
			return fmt.Errorf("recover schema operation %x SQL binding: %w",
				operation, errors.Join(resolveErr, gateway.ErrSchemaRolloutConflict))
		}
		if executeErr := r.Execute(ctx, sql); executeErr != nil {
			return fmt.Errorf("resume schema operation %x: %w", operation, executeErr)
		}
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
			request, sql, _, _, err := r.resumer.ResumeBuild(ctx, replica.Node, operation, descriptor.Group)
			if errors.Is(err, schemainstall.ErrMissing) {
				continue
			}
			if err != nil {
				return "", err
			}
			if sql == "" || request.Operation != operation || request.Group != descriptor.Group ||
				request.AllocationGeneration != descriptor.AllocationGeneration ||
				request.FromSchemaGeneration != descriptor.Command.SchemaGeneration ||
				request.FromRelationManifestDigest != descriptor.Command.RelationManifestDigest ||
				retained != "" && retained != sql {
				return "", gateway.ErrSchemaRolloutConflict
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
	operation := gatewaySchemaDDLOperation(current, table, sql)
	descriptors := current.ReplicatedShardDescriptors()
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
				if retainedSQL != sql || request.AllocationGeneration != descriptor.AllocationGeneration ||
					request.FromSchemaGeneration != descriptor.Command.SchemaGeneration ||
					request.FromRelationManifestDigest != descriptor.Command.RelationManifestDigest {
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
					target, err = r.client.Build(ctx, replica.Node, request, sql)
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
			target, err := r.client.Build(ctx, replica.Node, request, sql)
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
	_, err = controller.Execute(ctx, operation, target, plans)
	if err != nil {
		return fmt.Errorf("execute distributed schema plan: %w", err)
	}
	return nil
}
