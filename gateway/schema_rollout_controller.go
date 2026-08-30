package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/schemainstall"
)

var schemaRolloutReplicaReceiptDomain = []byte(
	"vibedb/gateway/schema-rollout/rf3-replica-receipts\x00",
)

type SchemaInstallClient interface {
	Prepare(context.Context, rafttransport.NodeID, schemainstall.Request, []byte) (schemainstall.Receipt, error)
	Authorize(context.Context, rafttransport.NodeID, schemainstall.Request, schemainstall.Authorization) (schemainstall.Record, error)
	Commit(context.Context, rafttransport.NodeID, schemainstall.Request, schemainstall.Authorization) (schemainstall.Record, error)
	Activate(context.Context, rafttransport.NodeID, schemainstall.Request, schemainstall.Authorization) (schemainstall.Record, error)
	Drain(context.Context, rafttransport.NodeID, schemainstall.Request, schemainstall.Authorization, schemainstall.DrainProof) (schemainstall.Record, error)
}

// SchemaRolloutReplicaPlan carries one replica-local canonical SQL catalog
// image. Bundle bytes may differ across RF3 replicas because local immutable
// storage identities differ; the controller folds all three authenticated
// receipts into one constant-size group witness.
type SchemaRolloutReplicaPlan struct {
	Member  uint64
	Node    rafttransport.NodeID
	Request schemainstall.Request
	Bundle  []byte
}

// NewSchemaRolloutReplicaPlan derives every portable request field from the
// authenticated base/target catalogs. The admin surface supplies only the
// replica identity, its local canonical bundle, and the target apply contract;
// it cannot restate group epochs or relation manifests independently.
func NewSchemaRolloutReplicaPlan(
	id [32]byte, base, target *Snapshot, node rafttransport.NodeID, member uint64,
	applyContract [32]byte, bundle []byte,
) (SchemaRolloutReplicaPlan, error) {
	if id == ([32]byte{}) || base == nil || target == nil || node == (rafttransport.NodeID{}) ||
		member == 0 || applyContract == ([32]byte{}) || len(bundle) == 0 ||
		len(bundle) > schemainstall.AbsoluteMaxBundleBytes {
		return SchemaRolloutReplicaPlan{}, ErrSchemaRollout
	}
	changes, err := schemaRolloutChanges(base, target)
	if err != nil {
		return SchemaRolloutReplicaPlan{}, err
	}
	descriptors := target.replicatedDescriptors()
	for _, change := range changes {
		for _, descriptor := range descriptors {
			if descriptor.Group != change.group {
				continue
			}
			for _, replica := range descriptor.Replicas {
				if replica.Node != node || replica.Member != member {
					continue
				}
				owned := slices.Clone(bundle)
				return SchemaRolloutReplicaPlan{Member: member, Node: node, Bundle: owned,
					Request: schemainstall.Request{Operation: id, Group: change.group,
						AllocationGeneration:       change.allocation,
						FromSchemaGeneration:       change.fromSchemaGeneration,
						FromRelationManifestDigest: change.fromRelationManifestDigest,
						ToSchemaGeneration:         change.toSchemaGeneration,
						ToRelationManifestDigest:   change.toRelationManifestDigest,
						ApplyContractDigest:        applyContract, BundleDigest: sha256.Sum256(owned),
						BundleBytes: uint64(len(owned))}}, nil
			}
		}
	}
	return SchemaRolloutReplicaPlan{}, ErrSchemaRollout
}

type SchemaRolloutControllerOptions struct {
	Authority     *ReplicatedCatalogAuthority
	Client        SchemaInstallClient
	MaxConcurrent int
}

type SchemaRolloutController struct {
	authority *ReplicatedCatalogAuthority
	client    SchemaInstallClient
	workers   int
}

type SchemaRolloutResult struct {
	Record        ReplicatedOperationRecord
	Authorization schemainstall.Authorization
}

func NewSchemaRolloutController(options SchemaRolloutControllerOptions) (*SchemaRolloutController, error) {
	if options.Authority == nil || options.Client == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > 64 {
		return nil, ErrSchemaRollout
	}
	return &SchemaRolloutController{authority: options.Authority,
		client: options.Client, workers: options.MaxConcurrent}, nil
}

func validateSchemaRolloutReplicaPlans(
	id [32]byte, target *Snapshot, changes []schemaRolloutChange,
	plans []SchemaRolloutReplicaPlan,
) error {
	if id == ([32]byte{}) || target == nil || len(plans) != len(changes)*ServingReplicaCount {
		return ErrSchemaRollout
	}
	changesByGroup := make(map[raftmember.GroupKey]schemaRolloutChange, len(changes))
	for _, change := range changes {
		changesByGroup[change.group] = change
	}
	type replicaIdentity struct {
		group  raftmember.GroupKey
		member uint64
		node   rafttransport.NodeID
	}
	descriptors := target.replicatedDescriptors()
	want := make(map[replicaIdentity]struct{}, len(plans))
	for _, descriptor := range descriptors {
		if _, changed := changesByGroup[descriptor.Group]; !changed {
			continue
		}
		for _, replica := range descriptor.Replicas {
			want[replicaIdentity{group: descriptor.Group, member: replica.Member,
				node: replica.Node}] = struct{}{}
		}
	}
	seen := make(map[replicaIdentity]struct{}, len(plans))
	for _, plan := range plans {
		request := plan.Request
		change, found := changesByGroup[request.Group]
		identity := replicaIdentity{group: request.Group, member: plan.Member, node: plan.Node}
		_, targetReplica := want[identity]
		if !found || !targetReplica ||
			request.Operation != id || request.Group != change.group ||
			request.AllocationGeneration != change.allocation ||
			request.FromSchemaGeneration != change.fromSchemaGeneration ||
			request.FromRelationManifestDigest != change.fromRelationManifestDigest ||
			request.ToSchemaGeneration != change.toSchemaGeneration ||
			request.ToRelationManifestDigest != change.toRelationManifestDigest ||
			uint64(len(plan.Bundle)) != request.BundleBytes ||
			sha256.Sum256(plan.Bundle) != request.BundleDigest {
			return ErrSchemaRollout
		}
		if _, duplicate := seen[identity]; duplicate {
			return ErrSchemaRollout
		}
		seen[identity] = struct{}{}
	}
	return nil
}

func schemaRolloutChangesFromPlans(plans []SchemaRolloutReplicaPlan) ([]schemaRolloutChange, error) {
	byGroup := make(map[raftmember.GroupKey]schemaRolloutChange)
	counts := make(map[raftmember.GroupKey]int)
	for _, plan := range plans {
		request := plan.Request
		change := schemaRolloutChange{group: request.Group,
			allocation:                 request.AllocationGeneration,
			fromSchemaGeneration:       request.FromSchemaGeneration,
			fromRelationManifestDigest: request.FromRelationManifestDigest,
			toSchemaGeneration:         request.ToSchemaGeneration,
			toRelationManifestDigest:   request.ToRelationManifestDigest}
		if prior, found := byGroup[request.Group]; found && prior != change {
			return nil, ErrSchemaRollout
		}
		byGroup[request.Group] = change
		counts[request.Group]++
	}
	result := make([]schemaRolloutChange, 0, len(byGroup))
	for group, change := range byGroup {
		if counts[group] != ServingReplicaCount {
			return nil, ErrSchemaRollout
		}
		result = append(result, change)
	}
	slices.SortFunc(result, func(left, right schemaRolloutChange) int {
		return compareMembershipGrantGroup(left.group, right.group)
	})
	return result, nil
}

func (controller *SchemaRolloutController) parallel(
	ctx context.Context, count int, run func(int) error,
) error {
	if controller == nil || ctx == nil || count <= 0 {
		return ErrSchemaRollout
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(context.Canceled)
	jobs := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	workers := min(controller.workers, count)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for index := range jobs {
				if err := run(index); err != nil {
					once.Do(func() { first = err; cancel(err) })
					return
				}
			}
		}()
	}
	for index := 0; index < count; index++ {
		select {
		case jobs <- index:
		case <-ctx.Done():
			index = count
		}
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		return first
	}
	return context.Cause(ctx)
}

// retrySchemaReplicaOutcome settles only an exact idempotent replica command.
// A schema activation commonly closes the source runtime before the control
// response reaches the coordinator; treating that transport cut as failure
// strands the durable Running operation until process restart. Retrying the
// same request/authorization lets the installer observe its retained record
// and return the already-completed phase. Deterministic conflicts, bounds and
// authorization failures still return immediately.
func retrySchemaReplicaOutcome(ctx context.Context, run func() error) error {
	if ctx == nil || run == nil {
		return ErrSchemaRollout
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := run()
		if !errors.Is(err, schemainstall.ErrOutcomeUnknown) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, context.Cause(ctx))
		case <-ticker.C:
		}
	}
}

func writeSchemaReplicaReceipt(
	h hash.Hash, scratch *[8]byte, plan SchemaRolloutReplicaPlan,
	receipt schemainstall.Receipt,
) {
	binary.BigEndian.PutUint64(scratch[:], plan.Member)
	_, _ = h.Write(scratch[:])
	_, _ = h.Write(plan.Node[:])
	_, _ = h.Write(receipt.InstallationDigest[:])
}

func aggregateSchemaReplicaReceipts(
	changes []schemaRolloutChange, plans []SchemaRolloutReplicaPlan,
	receipts []schemainstall.Receipt,
) ([]SchemaRolloutPreparedGroup, error) {
	result := make([]SchemaRolloutPreparedGroup, len(changes))
	for index, change := range changes {
		members := make([]int, 0, ServingReplicaCount)
		for planIndex := range plans {
			if plans[planIndex].Request.Group == change.group {
				members = append(members, planIndex)
			}
		}
		if len(members) != ServingReplicaCount {
			return nil, ErrSchemaRollout
		}
		slices.SortFunc(members, func(left, right int) int {
			if plans[left].Member < plans[right].Member {
				return -1
			}
			if plans[left].Member > plans[right].Member {
				return 1
			}
			return 0
		})
		h := sha256.New()
		_, _ = h.Write(schemaRolloutReplicaReceiptDomain)
		var scratch [8]byte
		writeSchemaRolloutGroup(h, &scratch, change.group)
		binary.BigEndian.PutUint64(scratch[:], ServingReplicaCount)
		_, _ = h.Write(scratch[:])
		for _, planIndex := range members {
			receipt := receipts[planIndex]
			if receipt.Group != change.group || receipt.AllocationGeneration != change.allocation ||
				receipt.FromSchemaGeneration != change.fromSchemaGeneration ||
				receipt.FromRelationManifestDigest != change.fromRelationManifestDigest ||
				receipt.ToSchemaGeneration != change.toSchemaGeneration ||
				receipt.ToRelationManifestDigest != change.toRelationManifestDigest ||
				receipt.ContractDigest != SchemaRolloutContractDigest() {
				return nil, ErrSchemaRollout
			}
			writeSchemaReplicaReceipt(h, &scratch, plans[planIndex], receipt)
		}
		prepared := SchemaRolloutPreparedGroup{Group: change.group,
			AllocationGeneration:       change.allocation,
			FromSchemaGeneration:       change.fromSchemaGeneration,
			FromRelationManifestDigest: change.fromRelationManifestDigest,
			ToSchemaGeneration:         change.toSchemaGeneration,
			ToRelationManifestDigest:   change.toRelationManifestDigest,
			ContractDigest:             SchemaRolloutContractDigest()}
		h.Sum(prepared.InstallationDigest[:0])
		result[index] = prepared
	}
	return result, nil
}

// Execute prepares every RF3 replica, durably authorizes the no-return cut,
// installs the target locally, and only then publishes the gateway catalog.
// Replaying the identical plan resumes every boundary idempotently.
func (controller *SchemaRolloutController) Execute(
	ctx context.Context, id [32]byte, target *Snapshot, plans []SchemaRolloutReplicaPlan,
) (SchemaRolloutResult, error) {
	if controller == nil || ctx == nil {
		return SchemaRolloutResult{}, ErrSchemaRollout
	}
	existing, operationErr := controller.authority.ReadOperation(ctx, id)
	var changes []schemaRolloutChange
	var err error
	if errors.Is(operationErr, ErrReplicatedOperationMissing) {
		current, readErr := controller.authority.Read(ctx)
		if readErr != nil {
			return SchemaRolloutResult{}, readErr
		}
		changes, err = schemaRolloutChanges(current, target)
	} else if operationErr != nil {
		return SchemaRolloutResult{}, operationErr
	} else {
		intent, openErr := openSchemaRolloutOperation(existing)
		targetRaw, targetErr := schemaRolloutCatalogDocument(target)
		if openErr != nil || targetErr != nil || !schemaRolloutHeadMatches(
			intent.TargetCatalogGeneration, intent.TargetHeadBytes,
			intent.TargetHeadDigest, target, targetRaw,
		) || existing.State == ReplicatedOperationCancelled {
			return SchemaRolloutResult{}, errors.Join(openErr, targetErr, ErrSchemaRolloutConflict)
		}
		changes, err = schemaRolloutChangesFromPlans(plans)
	}
	if err != nil || validateSchemaRolloutReplicaPlans(id, target, changes, plans) != nil {
		return SchemaRolloutResult{}, errors.Join(err, ErrSchemaRollout)
	}
	receipts := make([]schemainstall.Receipt, len(plans))
	if err = controller.parallel(ctx, len(plans), func(index int) error {
		plan := plans[index]
		var prepareErr error
		prepareErr = retrySchemaReplicaOutcome(ctx, func() error {
			receipts[index], prepareErr = controller.client.Prepare(ctx, plan.Node, plan.Request, plan.Bundle)
			return prepareErr
		})
		if prepareErr != nil {
			return fmt.Errorf("schema rollout prepare member %d: %w", plan.Member, prepareErr)
		}
		return nil
	}); err != nil {
		return SchemaRolloutResult{}, err
	}
	groups, err := aggregateSchemaReplicaReceipts(changes, plans, receipts)
	if err != nil {
		return SchemaRolloutResult{}, fmt.Errorf("schema rollout aggregate receipts: %w", err)
	}
	planned := existing
	if errors.Is(operationErr, ErrReplicatedOperationMissing) {
		planned, err = controller.settleCatalogPublication(ctx, func() (ReplicatedOperationRecord, error) {
			return controller.authority.PrepareSchemaRollout(ctx, id, target, groups)
		})
		if err != nil {
			return SchemaRolloutResult{}, fmt.Errorf("schema rollout publish prepared intent: %w", err)
		}
	} else {
		preparedRoot, rootErr := schemaRolloutPreparedRoot(changes, groups)
		intent, openErr := openSchemaRolloutOperation(planned)
		if rootErr != nil || openErr != nil || preparedRoot != intent.PreparedGroupRoot {
			return SchemaRolloutResult{}, errors.Join(rootErr, openErr, ErrSchemaRolloutConflict)
		}
	}
	running, err := controller.settleCatalogPublication(ctx, func() (ReplicatedOperationRecord, error) {
		return controller.authority.AuthorizeSchemaRollout(ctx, id, target)
	})
	if err != nil {
		return SchemaRolloutResult{}, fmt.Errorf("schema rollout authorize intent: %w", err)
	}
	intent, err := openSchemaRolloutOperation(running)
	if err != nil || planned.Proof != intent.PreparedGroupRoot {
		return SchemaRolloutResult{}, errors.Join(err, ErrSchemaRolloutConflict)
	}
	authorization := schemainstall.Authorization{Operation: id,
		TargetCatalogGeneration: intent.TargetCatalogGeneration,
		TargetCatalogDigest:     intent.TargetHeadDigest,
		PreparedGroupCount:      intent.PreparedGroupCount,
		PreparedGroupRoot:       intent.PreparedGroupRoot,
		ContractDigest:          SchemaRolloutContractDigest()}
	if err = controller.parallel(ctx, len(plans), func(index int) error {
		plan := plans[index]
		phaseErr := retrySchemaReplicaOutcome(ctx, func() error {
			_, retryErr := controller.client.Authorize(ctx, plan.Node, plan.Request, authorization)
			return retryErr
		})
		if phaseErr != nil {
			return fmt.Errorf("schema rollout authorize member %d: %w", plan.Member, phaseErr)
		}
		return nil
	}); err != nil {
		return SchemaRolloutResult{Record: running, Authorization: authorization}, err
	}
	if err = controller.parallel(ctx, len(plans), func(index int) error {
		plan := plans[index]
		phaseErr := retrySchemaReplicaOutcome(ctx, func() error {
			_, retryErr := controller.client.Commit(ctx, plan.Node, plan.Request, authorization)
			return retryErr
		})
		if phaseErr != nil {
			return fmt.Errorf("schema rollout commit transition member %d: %w", plan.Member, phaseErr)
		}
		return nil
	}); err != nil {
		return SchemaRolloutResult{Record: running, Authorization: authorization}, err
	}
	if err = controller.activateReplicaGroups(ctx, plans, authorization); err != nil {
		return SchemaRolloutResult{Record: running, Authorization: authorization}, err
	}
	complete, err := controller.settleCatalogPublication(ctx, func() (ReplicatedOperationRecord, error) {
		return controller.authority.CommitSchemaRollout(ctx, id, target)
	})
	if err != nil {
		err = fmt.Errorf("schema rollout commit catalog: %w", err)
	}
	return SchemaRolloutResult{Record: complete, Authorization: authorization}, err
}

// activateReplicaGroups performs a rolling replica replacement inside each
// Raft group while preserving parallelism across independent groups. Replacing
// multiple voters in one group concurrently can remove quorum or let a leader
// election append a neutral entry to an old-generation state machine.
func (controller *SchemaRolloutController) activateReplicaGroups(
	ctx context.Context, plans []SchemaRolloutReplicaPlan,
	authorization schemainstall.Authorization,
) error {
	groupIndexes := make(map[raftmember.GroupKey]int, len(plans)/ServingReplicaCount)
	groups := make([][]int, 0, len(plans)/ServingReplicaCount)
	for index := range plans {
		group := plans[index].Request.Group
		groupIndex, found := groupIndexes[group]
		if !found {
			groupIndex = len(groups)
			groupIndexes[group] = groupIndex
			groups = append(groups, make([]int, 0, ServingReplicaCount))
		}
		groups[groupIndex] = append(groups[groupIndex], index)
	}
	return controller.parallel(ctx, len(groups), func(groupIndex int) error {
		for _, index := range groups[groupIndex] {
			plan := plans[index]
			phaseErr := retrySchemaReplicaOutcome(ctx, func() error {
				_, retryErr := controller.client.Activate(ctx, plan.Node, plan.Request, authorization)
				return retryErr
			})
			if phaseErr != nil {
				return fmt.Errorf("schema rollout activate member %d: %w", plan.Member, phaseErr)
			}
		}
		return nil
	})
}

// settleCatalogPublication resolves an outcome-unknown catalog command inside
// the controller that owns its exact immutable plan. RetryPending can only
// resend the session-journaled bytes; re-entering action then observes the
// resulting operation/catalog state and advances idempotently. SQL clients
// therefore receive success when the durable effect is provably committed,
// without a reconnect and without weakening the authority's fail-closed API.
func (controller *SchemaRolloutController) settleCatalogPublication(
	ctx context.Context, action func() (ReplicatedOperationRecord, error),
) (ReplicatedOperationRecord, error) {
	for {
		record, err := action()
		if !errors.Is(err, ErrReplicatedCatalogPending) {
			return record, err
		}
		if retryErr := controller.authority.RetryPending(ctx); retryErr != nil &&
			!errors.Is(retryErr, ErrReplicatedCatalogPending) {
			return ReplicatedOperationRecord{}, errors.Join(err, retryErr)
		}
		if err := ctx.Err(); err != nil {
			return ReplicatedOperationRecord{}, errors.Join(ErrReplicatedCatalogPending, err)
		}
	}
}

func (controller *SchemaRolloutController) Drain(
	ctx context.Context, plans []SchemaRolloutReplicaPlan,
	authorization schemainstall.Authorization, proof schemainstall.DrainProof,
) error {
	if controller == nil || ctx == nil || len(plans) == 0 {
		return ErrSchemaRollout
	}
	return controller.parallel(ctx, len(plans), func(index int) error {
		plan := plans[index]
		_, err := controller.client.Drain(ctx, plan.Node, plan.Request, authorization, proof)
		return err
	})
}
