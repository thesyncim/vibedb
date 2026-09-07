package raftservice

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type authorityTestHost struct {
	ownerHost
	mu               sync.Mutex
	status           raftmember.RuntimeStatus
	token            raftauthority.AuthorityToken
	tokenErr         error
	validationErr    []error
	validationTokens []raftauthority.AuthorityToken
	readIndexes      int
	starts           int
	validations      int
}

func (host *authorityTestHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.status, nil
}

func (host *authorityTestHost) StartReadAuthorityRound(raftmember.GroupKey) error {
	host.mu.Lock()
	host.starts++
	host.mu.Unlock()
	return nil
}

func (host *authorityTestHost) ReadAuthorityToken(raftmember.GroupKey) (raftauthority.AuthorityToken, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.token, host.tokenErr
}

func (host *authorityTestHost) ValidateReadAuthorityToken(
	_ raftmember.GroupKey, token raftauthority.AuthorityToken,
) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.validations++
	host.validationTokens = append(host.validationTokens, token)
	if len(host.validationErr) == 0 {
		return nil
	}
	err := host.validationErr[0]
	host.validationErr = host.validationErr[1:]
	return err
}

func (host *authorityTestHost) ReadIndex(raftmember.GroupKey, []byte) error {
	host.mu.Lock()
	host.readIndexes++
	host.mu.Unlock()
	return nil
}

func (host *authorityTestHost) Close() error { return nil }

// concurrentAuthorityTestHost models the optional ExecutionLane capability
// without changing the established serialized fixture. Its Try method never
// reads Owner state; tests use it to prove busy fallback and direct success
// preserve the exact authority token.
type concurrentAuthorityTestHost struct {
	*authorityTestHost
	tryMu      sync.Mutex
	tryAttempt bool
	tryErr     error
	tryCalls   int
	tryTokens  []raftauthority.AuthorityToken
	tryHook    func()
}

type quiesceAuthorityTestHost struct {
	*authorityTestHost
	quiesceCalls int
}

func (host *quiesceAuthorityTestHost) ObserveSchemaTransition(
	raftmember.GroupKey, []byte,
) (uint64, bool, error) {
	return 19, true, nil
}

func (host *quiesceAuthorityTestHost) QuiesceSQLGeneration(raftmember.GroupKey) error {
	host.quiesceCalls++
	return nil
}

func (host *concurrentAuthorityTestHost) TryValidateReadAuthorityToken(
	_ raftmember.GroupKey, _ uint64, _ uint64, _ uint64,
	token raftauthority.AuthorityToken,
) (bool, error) {
	host.tryMu.Lock()
	host.tryCalls++
	host.tryTokens = append(host.tryTokens, token)
	hook := host.tryHook
	attempt, tryErr := host.tryAttempt, host.tryErr
	host.tryMu.Unlock()
	if hook != nil {
		hook()
	}
	if !attempt {
		return false, nil
	}
	return true, tryErr
}

func (fixture readAuthorityOwnerFixture) concurrentHost() *concurrentAuthorityTestHost {
	host := &concurrentAuthorityTestHost{authorityTestHost: fixture.host}
	fixture.owner.host = host
	return host
}

type readAuthorityPointSource struct {
	point      replicatedstate.PointReadResult
	batch      replicatedstate.PointReadBatchResult
	hook       func()
	mu         sync.Mutex
	mins       []uint64
	pointCalls int
	batchCalls int
}

func (source *readAuthorityPointSource) PointReadInto(
	_ replication.RelationID, _ []byte, minimumApplied uint64, _ int, dst []byte,
) (replicatedstate.PointReadResult, error) {
	source.mu.Lock()
	source.pointCalls++
	source.mins = append(source.mins, minimumApplied)
	hook := source.hook
	result := source.point
	source.mu.Unlock()
	if hook != nil {
		hook()
	}
	// Match the production point source: copy into the caller's buffer and
	// return a result that borrows that storage. Authority reads pass a nil
	// destination until their serialized final validation succeeds.
	result.Value = append(dst[:0], result.Value...)
	return result, nil
}

func (source *readAuthorityPointSource) PointReadBatchInto(
	_ []byte, minimumApplied uint64, _ int, _ []byte,
) (replicatedstate.PointReadBatchResult, error) {
	source.mu.Lock()
	source.batchCalls++
	source.mins = append(source.mins, minimumApplied)
	source.mu.Unlock()
	return source.batch, nil
}

type readAuthorityOwnerFixture struct {
	owner      *Owner
	host       *authorityTestHost
	source     *readAuthorityPointSource
	group      raftmember.GroupKey
	fence      ServingFence
	generation *ownerGeneration
	metrics    *ProgressMetrics
	done       chan struct{}
	handled    chan struct{}
}

func newReadAuthorityOwnerFixture(t testing.TB) readAuthorityOwnerFixture {
	t.Helper()
	group := peerServerTestGroup()
	fence := ServingFence{Group: group, AllocationGeneration: 3,
		Command: CommandFence{ReplicaSetVersion: 7, ActivePolicyGeneration: 5,
			ProtectionEpoch: 6, OwnershipEpoch: 8, SchemaGeneration: 9,
			RelationManifestDigest: [32]byte{4}, RoutingVersion: 10, RouteGeneration: 11},
		MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, Term: 5}
	status := raftmember.RuntimeStatus{MemberID: 2, LeaderID: 2, Term: 5, Commit: 19, Applied: 19}
	source := &readAuthorityPointSource{}
	source.point = replicatedstate.PointReadResult{
		Fence: testLinearizablePointSnapshotFence(fence, 19), Found: true, Value: []byte("value"),
	}
	value := []byte{1, 0, 0, 0, 1, 0, 0, 0, 0}
	source.batch = replicatedstate.PointReadBatchResult{
		Fence: testLinearizablePointSnapshotFence(fence, 19), Data: value,
	}
	host := &authorityTestHost{status: status, token: readAuthorityTestToken(group)}
	generation := &ownerGeneration{}
	metrics := new(ProgressMetrics)
	owner := &Owner{host: host, started: true, ingress: make(chan ownerRequest, 4),
		members: map[raftmember.GroupKey]ownerMember{group: {
			identity: raftmember.RuntimeIdentity{Group: group, AllocationGeneration: 3,
				MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4,
				RelationManifestDigest: [32]byte{4}},
			command: fence.Command, read: source, generation: generation,
		}},
		limits: Limits{MaxIngressItems: 4, MaxIngressBytes: 1 << 20,
			MaxPendingReadItems: 2, MaxPendingReadBytes: 1 << 30},
		pendingReads: make(map[[16]byte]*readDelivery),
		metrics:      metrics,
	}
	done := make(chan struct{})
	handled := make(chan struct{}, 32)
	go func() {
		defer close(done)
		for request := range owner.ingress {
			err := owner.handle(request)
			owner.release(request.bytes)
			if err != nil && !errors.Is(err, raftmodel.ErrReadyPending) {
				// The public call receives the same error through its reply. The
				// fixture only needs to keep servicing a possible bounded retry.
			}
			if request.kind == requestReadLinear && request.read.delivery != nil {
				if _, pending := owner.pendingReads[request.read.delivery.context]; !pending {
					handled <- struct{}{}
					continue
				}
				owner.finishReadOutcomes([]raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
					Context: request.read.delivery.context[:], Index: 19, Term: 5, Incarnation: 4,
				}}})
			}
			handled <- struct{}{}
		}
	}()
	return readAuthorityOwnerFixture{owner: owner, host: host, source: source,
		group: group, fence: fence, generation: generation, metrics: metrics,
		done: done, handled: handled}
}

func TestOwnerPointReadAuthorityUnavailableEnsuresRoundThenUsesReadIndex(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	fixture.host.mu.Lock()
	fixture.host.tokenErr = raftauthority.ErrNotQuorum
	fixture.host.mu.Unlock()
	result, lease, err := fixture.owner.ReadPoint(t.Context(), PointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Relation: 1, Key: []byte("key"), MinimumApplied: 7,
		MaxValueBytes: 64, Linearizable: true,
	})
	if err != nil || lease == nil || !result.Found {
		t.Fatalf("result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
	fixture.host.mu.Lock()
	starts, readIndexes, validations := fixture.host.starts, fixture.host.readIndexes, fixture.host.validations
	fixture.host.mu.Unlock()
	if starts != 1 || readIndexes != 1 || validations != 0 {
		t.Fatalf("starts=%d readIndexes=%d validations=%d", starts, readIndexes, validations)
	}
	if got := fixture.metrics.Snapshot(); got.AuthorityReadHits != 0 ||
		got.AuthorityReadIndexFallbacks != 1 || got.AuthorityRoundAttempts != 1 {
		t.Fatalf("authority metrics=%+v", got)
	}
}

func (fixture readAuthorityOwnerFixture) close() {
	close(fixture.owner.ingress)
	<-fixture.done
}

func readAuthorityTestToken(group raftmember.GroupKey) raftauthority.AuthorityToken {
	return raftauthority.AuthorityToken{
		Group: raftauthority.GroupIdentity{ClusterID: group.ClusterID,
			ClusterIncarnation: group.ClusterIncarnation, TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
			ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID},
		Config: raftauthority.ConfigIdentity{AppliedVersion: 7, Digest: [32]byte{9}},
		Term:   5, Holder: 2, HolderIncarnation: 4, PolicyVersion: 1, PolicyDigest: [32]byte{8},
		Nonce: 1, StartedAt: 1, ExpiresAt: 100,
	}
}

func TestServingFencePermitCachesOneServingEpochAndRevokesOneWay(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	member := fixture.owner.members[fixture.group]
	serving := ServingState{
		Identity: member.identity, Command: member.command, Status: fixture.host.status,
	}
	first := fixture.owner.ensureServingFencePermit(serving)
	second := fixture.owner.ensureServingFencePermit(serving)
	if first == nil || first != second || !first.valid(fixture.fence, fixture.generation) {
		t.Fatalf("cached permit first=%p second=%p valid=%t", first, second,
			first != nil && first.valid(fixture.fence, fixture.generation))
	}
	fixture.owner.revokeServingFencePermit(fixture.group)
	if first.valid(fixture.fence, fixture.generation) || fixture.owner.members[fixture.group].permit != nil {
		t.Fatal("revocation left the cached serving permit usable")
	}
	third := fixture.owner.ensureServingFencePermit(serving)
	if third == nil || third == first || !third.valid(fixture.fence, fixture.generation) {
		t.Fatalf("replacement permit first=%p third=%p valid=%t", first, third,
			third != nil && third.valid(fixture.fence, fixture.generation))
	}
}

func TestServingFencePermitPublicationAndQuiesceEdgesDoNotRevive(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	member := fixture.owner.members[fixture.group]
	serving := ServingState{
		Identity: member.identity, Command: member.command, Status: fixture.host.status,
	}
	old := fixture.owner.ensureServingFencePermit(serving)
	if old == nil {
		t.Fatal("initial serving permit was not created")
	}

	// A metadata-only publication keeps the cached epoch capability. A changed
	// command fence revokes it before the replacement is published.
	fixture.owner.storeOwnerMember(fixture.group, member)
	if fixture.owner.members[fixture.group].permit != old {
		t.Fatal("unchanged member publication replaced its permit")
	}
	member.command.RouteGeneration++
	fixture.owner.storeOwnerMember(fixture.group, member)
	if old.valid(fixture.fence, fixture.generation) || fixture.owner.members[fixture.group].permit != nil {
		t.Fatal("command-fence publication revived the old permit")
	}

	serving.Command = member.command
	current := fixture.owner.ensureServingFencePermit(serving)
	if current == nil || current == old {
		t.Fatalf("replacement serving permit old=%p current=%p", old, current)
	}
	fixture.owner.revokeServingFencePermit(fixture.group)
	if !fixture.generation.quiesce() {
		t.Fatal("generation did not enter quiescing state")
	}
	fixture.generation.resume()
	if current.valid(serving.Fence(), fixture.generation) {
		t.Fatal("resuming a quiesced generation revived its permit")
	}
	resumed := fixture.owner.ensureServingFencePermit(serving)
	if resumed == nil || resumed == current {
		t.Fatalf("resume did not mint a fresh permit current=%p resumed=%p", current, resumed)
	}
	fixture.owner.revokeAllServingFencePermits()
	if resumed.valid(serving.Fence(), fixture.generation) {
		t.Fatal("shutdown revoke left a serving permit usable")
	}
}

func TestOwnerQuiesceFailedPinsRevokesServingPermitBeforeTransition(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	member := fixture.owner.members[fixture.group]
	serving := ServingState{
		Identity: member.identity, Command: member.command, Status: fixture.host.status,
	}
	old := fixture.owner.ensureServingFencePermit(serving)
	if old == nil {
		t.Fatal("initial serving permit was not created")
	}
	transition := replicatedstate.SchemaTransition{
		From: replicatedstate.Binding{
			ClusterID:              fixture.group.ClusterID,
			ClusterIncarnation:     fixture.group.ClusterIncarnation,
			TopologyRecoveryEpoch:  fixture.group.TopologyRecoveryEpoch,
			Distribution:           "docs",
			Shard:                  "0000-ffff",
			AllocationGeneration:   fixture.fence.AllocationGeneration,
			ShardIncarnation:       fixture.group.ShardIncarnation,
			GroupID:                fixture.group.GroupID,
			ActivePolicyGeneration: fixture.fence.Command.ActivePolicyGeneration,
			ProtectionEpoch:        fixture.fence.Command.ProtectionEpoch,
			OwnershipEpoch:         fixture.fence.Command.OwnershipEpoch,
			SchemaGeneration:       fixture.fence.Command.SchemaGeneration,
			RoutingVersion:         fixture.fence.Command.RoutingVersion,
			RouteGeneration:        fixture.fence.Command.RouteGeneration,
			OwnedRange:             distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
		},
		ToSchemaGeneration:        fixture.fence.Command.SchemaGeneration + 1,
		ExpectedReplicaSetVersion: fixture.fence.Command.ReplicaSetVersion,
		MembershipSequence:        1,
		MembershipSource:          [32]byte{1},
		MembershipTarget:          [32]byte{2},
		FromManifest:              [32]byte{4},
		FromApplyContract:         [32]byte{5},
		ToManifest:                [32]byte{6},
		ToApplyContract:           [32]byte{7},
		RequestDigest:             [32]byte{8},
		AuthorizationDigest:       [32]byte{9},
		CatalogCASDigest:          [32]byte{10},
	}
	encoded, err := replicatedstate.AppendSchemaTransition(nil, transition)
	if err != nil {
		t.Fatal(err)
	}
	host := &quiesceAuthorityTestHost{authorityTestHost: fixture.host}
	fixture.owner.host = host
	if !fixture.generation.acquire() {
		t.Fatal("generation pin was refused")
	}
	err = fixture.owner.quiesceSchemaGeneration(ownerRequest{
		group: fixture.group, fence: fixture.fence, data: encoded,
	})
	if !errors.Is(err, ErrServingFence) {
		t.Fatalf("quiesce with live pin = %v, want ErrServingFence", err)
	}
	if fixture.generation.pins.Load() != 1 || fixture.generation.quiescing.Load() {
		t.Fatalf("failed quiesce changed generation state: pins=%d quiescing=%v",
			fixture.generation.pins.Load(), fixture.generation.quiescing.Load())
	}
	if old.valid(fixture.fence, fixture.generation) ||
		fixture.owner.members[fixture.group].permit != nil {
		t.Fatal("failed quiesce revived or retained the old serving permit")
	}
	if host.quiesceCalls != 0 {
		t.Fatalf("host quiesced despite live pin: calls=%d", host.quiesceCalls)
	}
	fixture.generation.release()
	fresh := fixture.owner.ensureServingFencePermit(serving)
	if fresh == nil || fresh == old || !fresh.valid(fixture.fence, fixture.generation) {
		t.Fatalf("released generation did not mint a fresh permit: old=%p fresh=%p", old, fresh)
	}
}

func TestOwnerAuthorityValidationBusyLaneQueuesExactToken(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	host := fixture.concurrentHost()
	result, lease, err := fixture.owner.ReadPoint(t.Context(), PointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Relation: 1, Key: []byte("key"), MinimumApplied: 7,
		MaxValueBytes: 64, Linearizable: true,
	})
	if err != nil || lease == nil || !result.Found {
		t.Fatalf("result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
	host.tryMu.Lock()
	tryCalls := host.tryCalls
	tryTokens := append([]raftauthority.AuthorityToken(nil), host.tryTokens...)
	host.tryMu.Unlock()
	fixture.host.mu.Lock()
	validations := fixture.host.validations
	validationTokens := append([]raftauthority.AuthorityToken(nil), fixture.host.validationTokens...)
	readIndexes := fixture.host.readIndexes
	fixture.host.mu.Unlock()
	if tryCalls != 1 || validations != 1 || readIndexes != 0 || len(tryTokens) != 1 ||
		len(validationTokens) != 1 || tryTokens[0] != fixture.host.token ||
		validationTokens[0] != fixture.host.token {
		t.Fatalf("try=%d/%v queued=%d/%v readIndexes=%d token=%+v/%+v", tryCalls,
			tryTokens, validations, validationTokens, readIndexes, tryTokens,
			validationTokens)
	}
}

func TestOwnerAuthorityValidationUsesConcurrentPermitAndSkipsQueue(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	host := fixture.concurrentHost()
	host.tryMu.Lock()
	host.tryAttempt = true
	host.tryMu.Unlock()
	result, lease, err := fixture.owner.ReadPoint(t.Context(), PointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Relation: 1, Key: []byte("key"), MinimumApplied: 7,
		MaxValueBytes: 64, Linearizable: true,
	})
	if err != nil || lease == nil || !result.Found {
		t.Fatalf("result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
	host.tryMu.Lock()
	tryCalls, tryTokens := host.tryCalls, append([]raftauthority.AuthorityToken(nil), host.tryTokens...)
	host.tryMu.Unlock()
	fixture.host.mu.Lock()
	validations, readIndexes := fixture.host.validations, fixture.host.readIndexes
	fixture.host.mu.Unlock()
	if tryCalls != 1 || len(tryTokens) != 1 || tryTokens[0] != fixture.host.token ||
		validations != 0 || readIndexes != 0 {
		t.Fatalf("try=%d tokens=%v queued=%d readIndexes=%d", tryCalls, tryTokens,
			validations, readIndexes)
	}
}

func TestOwnerAuthorityPermitRevocationStopsConcurrentValidation(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	host := fixture.concurrentHost()
	host.tryMu.Lock()
	host.tryAttempt = true
	host.tryMu.Unlock()
	var cut LinearizablePointReadCut
	if err := fixture.owner.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
	}, &cut); err != nil {
		t.Fatalf("point cut admission: %v", err)
	}
	<-fixture.handled
	permit := cut.authorityPermit
	fixture.owner.revokeServingFencePermit(fixture.group)
	result, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, nil)
	if err != nil || !result.Found {
		t.Fatalf("revoked cut result=%+v err=%v", result, err)
	}
	if permit == nil || permit.valid(fixture.fence, fixture.generation) {
		t.Fatal("revoked cut retained a usable permit")
	}
	if err := cut.Close(); err != nil {
		t.Fatalf("cut close: %v", err)
	}
	host.tryMu.Lock()
	tryCalls := host.tryCalls
	host.tryMu.Unlock()
	fixture.host.mu.Lock()
	readIndexes := fixture.host.readIndexes
	fixture.host.mu.Unlock()
	if tryCalls != 0 || readIndexes != 1 {
		t.Fatalf("revoked cut tryCalls=%d readIndexes=%d", tryCalls, readIndexes)
	}
}

func TestOwnerAuthorityPermitRevocationDuringConcurrentValidationFallsBack(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	host := fixture.concurrentHost()
	host.tryMu.Lock()
	host.tryAttempt = true
	host.tryHook = func() { fixture.owner.revokeServingFencePermit(fixture.group) }
	host.tryMu.Unlock()
	result, lease, err := fixture.owner.ReadPoint(t.Context(), PointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Relation: 1, Key: []byte("key"), MinimumApplied: 7,
		MaxValueBytes: 64, Linearizable: true,
	})
	if err != nil || lease == nil || !result.Found {
		t.Fatalf("result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
	host.tryMu.Lock()
	tryCalls := host.tryCalls
	host.tryMu.Unlock()
	fixture.host.mu.Lock()
	readIndexes, validations := fixture.host.readIndexes, fixture.host.validations
	fixture.host.mu.Unlock()
	if tryCalls != 1 || readIndexes != 1 || validations != 0 {
		t.Fatalf("during validation tryCalls=%d readIndexes=%d queuedValidations=%d",
			tryCalls, readIndexes, validations)
	}
}

func TestOwnerPointReadAuthorityFastPathUsesCommitFloorAndSkipsReadIndex(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	result, lease, err := fixture.owner.ReadPoint(t.Context(), PointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Relation: 1, Key: []byte("key"), MinimumApplied: 7,
		MaxValueBytes: 64, Linearizable: true,
	})
	if err != nil || lease == nil || !result.Found || string(result.Value) != "value" {
		t.Fatalf("result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
	fixture.host.mu.Lock()
	readIndexes, validations := fixture.host.readIndexes, fixture.host.validations
	fixture.host.mu.Unlock()
	fixture.source.mu.Lock()
	mins := append([]uint64(nil), fixture.source.mins...)
	fixture.source.mu.Unlock()
	if readIndexes != 0 || validations != 1 || len(mins) != 1 || mins[0] != 19 {
		t.Fatalf("readIndexes=%d validations=%d mins=%v", readIndexes, validations, mins)
	}
	if got := fixture.metrics.Snapshot(); got.AuthorityReadHits != 1 ||
		got.AuthorityReadIndexFallbacks != 0 || got.AuthorityReadValidationFailures != 0 {
		t.Fatalf("authority metrics=%+v", got)
	}
	if fixture.owner.pendingReadItems != 0 || fixture.owner.pendingReadBytes != 0 ||
		fixture.generation.pins.Load() != 0 {
		t.Fatalf("retained resources reads=%d/%d pins=%d", fixture.owner.pendingReadItems,
			fixture.owner.pendingReadBytes, fixture.generation.pins.Load())
	}
}

func TestOwnerPointReadAuthorityRequiresExactDataCapability(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	result, lease, err := fixture.owner.ReadPoint(t.Context(), PointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityTopology,
		Relation: 1, Key: []byte("key"), MinimumApplied: 7,
		MaxValueBytes: 64, Linearizable: true,
	})
	if err != nil || lease == nil || !result.Found {
		t.Fatalf("result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
	fixture.host.mu.Lock()
	readIndexes, validations, starts := fixture.host.readIndexes, fixture.host.validations, fixture.host.starts
	fixture.host.mu.Unlock()
	if readIndexes != 1 || validations != 0 || starts != 0 {
		t.Fatalf("readIndexes=%d validations=%d starts=%d", readIndexes, validations, starts)
	}
	if got := fixture.metrics.Snapshot(); got.AuthorityReadHits != 0 ||
		got.AuthorityReadIndexFallbacks != 0 || got.AuthorityRoundAttempts != 0 {
		t.Fatalf("authority metrics=%+v", got)
	}
}

func TestOwnerLinearizablePointCutAuthorityPathSkipsReadIndex(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	var cut LinearizablePointReadCut
	if err := fixture.owner.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
	}, &cut); err != nil {
		t.Fatalf("point cut admission: %v", err)
	}
	if cut.Source() != fixture.source {
		t.Fatal("point cut did not retain the authority-selected source")
	}
	if got := cut.State().Fence(); got != fixture.fence {
		t.Fatalf("point cut state fence=%+v, want=%+v", got, fixture.fence)
	}
	result, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, nil)
	if err != nil || !result.Found || string(result.Value) != "value" {
		t.Fatalf("point cut result=%+v err=%v", result, err)
	}
	if got := cut.State().Fence(); got != fixture.fence {
		t.Fatalf("post-read state fence=%+v, want=%+v", got, fixture.fence)
	}
	if err := cut.Close(); err != nil {
		t.Fatalf("point cut close: %v", err)
	}
	fixture.host.mu.Lock()
	readIndexes, validations := fixture.host.readIndexes, fixture.host.validations
	fixture.host.mu.Unlock()
	if readIndexes != 0 || validations != 1 {
		t.Fatalf("readIndexes=%d validations=%d", readIndexes, validations)
	}
	if got := fixture.metrics.Snapshot(); got.AuthorityReadHits != 1 ||
		got.AuthorityReadIndexFallbacks != 0 {
		t.Fatalf("authority metrics=%+v", got)
	}
	if fixture.owner.pendingReadItems != 0 || fixture.owner.pendingReadBytes != 0 ||
		fixture.generation.pins.Load() != 0 {
		t.Fatalf("retained resources reads=%d/%d pins=%d", fixture.owner.pendingReadItems,
			fixture.owner.pendingReadBytes, fixture.generation.pins.Load())
	}
}

func TestOwnerLinearizablePointCutRetriesAfterAuthorityValidation(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	fixture.host.validationErr = []error{raftauthority.ErrObservationStale}
	var cut LinearizablePointReadCut
	if err := fixture.owner.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
	}, &cut); err != nil {
		t.Fatalf("point cut admission: %v", err)
	}
	result, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, nil)
	if err != nil || !result.Found || string(result.Value) != "value" {
		t.Fatalf("point cut result=%+v err=%v", result, err)
	}
	if got := cut.State().Fence(); got != fixture.fence {
		t.Fatalf("post-fallback state fence=%+v, want=%+v", got, fixture.fence)
	}
	if err := cut.Close(); err != nil {
		t.Fatalf("point cut close: %v", err)
	}
	fixture.host.mu.Lock()
	readIndexes, validations := fixture.host.readIndexes, fixture.host.validations
	fixture.host.mu.Unlock()
	fixture.source.mu.Lock()
	calls, mins := fixture.source.pointCalls, append([]uint64(nil), fixture.source.mins...)
	fixture.source.mu.Unlock()
	if readIndexes != 1 || validations != 1 || calls != 2 || len(mins) != 2 || mins[0] != 19 || mins[1] != 19 {
		t.Fatalf("readIndexes=%d validations=%d calls=%d mins=%v", readIndexes, validations, calls, mins)
	}
	if got := fixture.metrics.Snapshot(); got.AuthorityReadHits != 0 ||
		got.AuthorityReadIndexFallbacks != 1 || got.AuthorityReadValidationRetries != 1 ||
		got.AuthorityReadValidationFailures != 1 {
		t.Fatalf("authority metrics=%+v", got)
	}
	if fixture.owner.pendingReadItems != 0 || fixture.owner.pendingReadBytes != 0 ||
		fixture.generation.pins.Load() != 0 {
		t.Fatalf("retained resources reads=%d/%d pins=%d", fixture.owner.pendingReadItems,
			fixture.owner.pendingReadBytes, fixture.generation.pins.Load())
	}
}

func TestOwnerLinearizablePointCutRejectsGenerationFenceChanges(t *testing.T) {
	tests := []struct {
		name            string
		mutate          func(readAuthorityOwnerFixture)
		wantErr         error
		wantValue       string
		wantReadIndexes int
		wantCalls       int
	}{
		{name: "transition fenced", mutate: func(fixture readAuthorityOwnerFixture) {
			fixture.generation.transitionFenced.Store(true)
		}, wantErr: ErrServingFence, wantReadIndexes: 1, wantCalls: 1},
		{name: "command fence changed", mutate: func(fixture readAuthorityOwnerFixture) {
			member := fixture.owner.members[fixture.group]
			member.command.RouteGeneration++
			fixture.owner.members[fixture.group] = member
		}, wantErr: ErrServingFence, wantReadIndexes: 0, wantCalls: 1},
		{name: "generation replaced", mutate: func(fixture readAuthorityOwnerFixture) {
			member := fixture.owner.members[fixture.group]
			member.generation = &ownerGeneration{}
			fixture.owner.members[fixture.group] = member
		}, wantValue: "value", wantReadIndexes: 1, wantCalls: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newReadAuthorityOwnerFixture(t)
			defer fixture.close()
			var cut LinearizablePointReadCut
			if err := fixture.owner.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
				Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
			}, &cut); err != nil {
				t.Fatalf("point cut admission: %v", err)
			}
			<-fixture.handled
			test.mutate(fixture)
			dst := []byte("sentinel")
			result, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, dst)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("point cut error=%v, want %v", err, test.wantErr)
				}
				if string(dst) != "sentinel" {
					t.Fatalf("failed authority read mutated caller bytes=%q", dst)
				}
			} else if err != nil || !result.Found || string(result.Value) != test.wantValue ||
				len(result.Value) > len(dst) || string(dst[:len(result.Value)]) != test.wantValue {
				t.Fatalf("point cut result=%+v err=%v dst=%q", result, err, dst)
			}
			_ = cut.Close()
			fixture.host.mu.Lock()
			readIndexes, validations := fixture.host.readIndexes, fixture.host.validations
			fixture.host.mu.Unlock()
			fixture.source.mu.Lock()
			calls := fixture.source.pointCalls
			fixture.source.mu.Unlock()
			if readIndexes != test.wantReadIndexes || validations != 0 || calls != test.wantCalls {
				t.Fatalf("readIndexes=%d validations=%d sourceCalls=%d", readIndexes, validations, calls)
			}
			currentGeneration := fixture.owner.members[fixture.group].generation
			if fixture.owner.pendingReadItems != 0 || fixture.owner.pendingReadBytes != 0 ||
				fixture.generation.pins.Load() != 0 || currentGeneration == nil ||
				currentGeneration.pins.Load() != 0 {
				t.Fatalf("retained resources reads=%d/%d pins=%d", fixture.owner.pendingReadItems,
					fixture.owner.pendingReadBytes, fixture.generation.pins.Load())
			}
		})
	}
}

func TestOwnerPointReadAuthorityExpiryFallsBackToReadIndexOnce(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	fixture.host.validationErr = []error{raftauthority.ErrRoundExpired}
	result, lease, err := fixture.owner.ReadPoint(t.Context(), PointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Relation: 1, Key: []byte("key"), MinimumApplied: 7,
		MaxValueBytes: 64, Linearizable: true,
	})
	if err != nil || lease == nil || !result.Found {
		t.Fatalf("result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
	fixture.host.mu.Lock()
	readIndexes, validations := fixture.host.readIndexes, fixture.host.validations
	fixture.host.mu.Unlock()
	fixture.source.mu.Lock()
	calls, mins := fixture.source.pointCalls, append([]uint64(nil), fixture.source.mins...)
	fixture.source.mu.Unlock()
	if readIndexes != 1 || validations != 1 || calls != 2 || len(mins) != 2 || mins[0] != 19 || mins[1] != 19 {
		t.Fatalf("readIndexes=%d validations=%d calls=%d mins=%v", readIndexes, validations, calls, mins)
	}
	if got := fixture.metrics.Snapshot(); got.AuthorityReadHits != 0 ||
		got.AuthorityReadIndexFallbacks != 1 || got.AuthorityReadValidationRetries != 1 ||
		got.AuthorityReadValidationFailures != 1 {
		t.Fatalf("authority metrics=%+v", got)
	}
	if fixture.owner.pendingReadItems != 0 || fixture.owner.pendingReadBytes != 0 ||
		fixture.generation.pins.Load() != 0 {
		t.Fatalf("retained resources reads=%d/%d pins=%d", fixture.owner.pendingReadItems,
			fixture.owner.pendingReadBytes, fixture.generation.pins.Load())
	}
}

func TestOwnerPointBatchAuthorityFastPathFinalValidationAndCancelCleanup(t *testing.T) {
	fixture := newReadAuthorityOwnerFixture(t)
	defer fixture.close()
	packed, err := replicatedstate.AppendPointReadBatch(nil, []replicatedstate.PointRead{{Relation: 1, Key: []byte("a")}})
	if err != nil {
		t.Fatal(err)
	}
	result, lease, err := fixture.owner.ReadPointBatch(t.Context(), PointReadBatchRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Packed: packed, MinimumApplied: 7, MaxResultBytes: 64,
	})
	if err != nil || lease == nil || len(result.Data) == 0 {
		t.Fatalf("batch result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
	fixture.source.mu.Lock()
	batchCalls := fixture.source.batchCalls
	fixture.source.mu.Unlock()
	fixture.host.mu.Lock()
	readIndexes, validations := fixture.host.readIndexes, fixture.host.validations
	fixture.host.mu.Unlock()
	if batchCalls != 1 || readIndexes != 0 || validations != 1 {
		t.Fatalf("batchCalls=%d readIndexes=%d validations=%d", batchCalls, readIndexes, validations)
	}

	ctx, cancel := context.WithCancel(t.Context())
	fixture.source.hook = cancel
	if _, lease, err := fixture.owner.ReadPoint(ctx, PointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Relation: 1, Key: []byte("key"), MinimumApplied: 7,
		MaxValueBytes: 64, Linearizable: true,
	}); !errors.Is(err, context.Canceled) || lease != nil {
		t.Fatalf("canceled fast read lease=%T err=%v", lease, err)
	}
	fixture.host.mu.Lock()
	validations = fixture.host.validations
	readIndexes = fixture.host.readIndexes
	fixture.host.mu.Unlock()
	if validations != 1 || readIndexes != 0 || fixture.owner.pendingReadItems != 0 ||
		fixture.owner.pendingReadBytes != 0 || fixture.generation.pins.Load() != 0 {
		t.Fatalf("canceled cleanup validations=%d readIndexes=%d reads=%d/%d pins=%d",
			validations, readIndexes, fixture.owner.pendingReadItems, fixture.owner.pendingReadBytes,
			fixture.generation.pins.Load())
	}
}
