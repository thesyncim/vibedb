package raftmember

import (
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type readAuthorityRuntimeClock struct {
	now time.Duration
	err error
}

func (clock *readAuthorityRuntimeClock) Now() (time.Duration, error) {
	if clock.err != nil {
		return 0, clock.err
	}
	return clock.now, nil
}

type readAuthorityFollowerFixture struct {
	fixture runtimeFixture
	local   uint64
	peer    uint64
	clock   *readAuthorityRuntimeClock
	checked *raftauthority.CheckedClock
	policy  raftauthority.ReadAuthorityPolicy
}

func newReadAuthorityFollowerFixture(t testing.TB, seed byte) readAuthorityFollowerFixture {
	t.Helper()
	identity := testWALIdentity(seed)
	peer := identity.MemberID + 1
	fixture := newRuntimeFixture(t, seed, []uint64{identity.MemberID, peer})
	runtime := fixture.runtime
	drainRuntime(t, runtime, nil)
	status, err := runtime.Status()
	if err != nil {
		t.Fatalf("initial Runtime.Status: %v", err)
	}
	term := status.Term + 1
	if err := runtime.StepMessage(&pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: &term,
		Commit: runtimeUint64Ptr(status.Commit),
	}); err != nil {
		t.Fatalf("establish follower leader: %v", err)
	}
	drainRuntime(t, runtime, nil)
	status, err = runtime.Status()
	if err != nil {
		t.Fatalf("follower Runtime.Status: %v", err)
	}
	previousIndex := status.Commit
	previousTerm, err := fixture.wal.Term(previousIndex)
	if err != nil {
		t.Fatalf("previous log term: %v", err)
	}
	entryIndex := previousIndex + 1
	entryTerm := status.Term
	if err := runtime.StepMessage(&pb.Message{
		Type: pb.MsgApp.Enum(), From: runtimeUint64Ptr(peer),
		To: runtimeUint64Ptr(identity.MemberID), Term: &entryTerm,
		Index: &previousIndex, LogTerm: &previousTerm, Commit: &entryIndex,
		Entries: []*pb.Entry{{Type: pb.EntryNormal.Enum(), Index: &entryIndex, Term: &entryTerm}},
	}); err != nil {
		t.Fatalf("establish current-term commit: %v", err)
	}
	drainRuntime(t, runtime, nil)
	status, err = runtime.Status()
	if err != nil {
		t.Fatalf("committed follower Runtime.Status: %v", err)
	}
	if status.LeaderID != peer || status.Commit < entryIndex || status.Applied < entryIndex {
		t.Fatalf("follower status = %+v, want leader %d applied commit >= %d", status, peer, entryIndex)
	}

	return readAuthorityFollowerFixture{
		fixture: fixture, local: identity.MemberID, peer: peer,
		clock: &readAuthorityRuntimeClock{},
	}
}

func readAuthorityRuntimePolicy(voters []uint64, maxGrant time.Duration) raftauthority.ReadAuthorityPolicy {
	policy := raftauthority.ReadAuthorityPolicy{
		Enabled: true, PolicyVersion: 71, MaxGrant: maxGrant,
		ClockRatePPM: 100_000, RoundingMargin: time.Millisecond,
		Voters:       append([]uint64(nil), voters...),
		Capabilities: make([]raftauthority.VoterCapability, len(voters)),
	}
	for index, voter := range voters {
		policy.Capabilities[index] = raftauthority.VoterCapability{
			MemberID: voter, PolicyVersion: policy.PolicyVersion, Enabled: true,
		}
	}
	return policy
}

func (fixture *readAuthorityFollowerFixture) configure(
	t testing.TB,
	leaderIncarnation func(uint64) (uint64, bool, error),
) {
	t.Helper()
	fixture.policy = readAuthorityRuntimePolicy([]uint64{fixture.local, fixture.peer}, time.Second)
	fixture.checked = raftauthority.NewCheckedClock(fixture.clock)
	if err := fixture.fixture.runtime.ConfigureReadAuthority(ReadAuthorityOptions{
		Policy: fixture.policy, Clock: fixture.checked, LeaderIncarnation: leaderIncarnation,
	}); err != nil {
		t.Fatalf("ConfigureReadAuthority: %v", err)
	}
	quarantine, err := fixture.policy.QuarantineDuration()
	if err != nil {
		t.Fatalf("QuarantineDuration: %v", err)
	}
	fixture.clock.now = quarantine
}

func (fixture *readAuthorityFollowerFixture) request(t testing.TB, nonce uint64) raftauthority.AuthorityRequest {
	t.Helper()
	observation, err := fixture.fixture.runtime.ReadAuthorityObservation()
	if err != nil {
		t.Fatalf("ReadAuthorityObservation: %v", err)
	}
	if !observation.CurrentTermCommitted || !observation.Stable || observation.Leader != fixture.peer {
		t.Fatalf("authority observation = %+v, want stable committed follower view", observation)
	}
	return raftauthority.AuthorityRequest{
		Group: observation.Group, Term: observation.Term, Holder: observation.Leader,
		HolderIncarnation: observation.LeaderIncarnation, Config: observation.Config,
		PolicyVersion: fixture.policy.PolicyVersion, PolicyDigest: fixture.policy.PolicyDigest(),
		Nonce: nonce, StartAt: fixture.clock.now,
	}
}

func (fixture *readAuthorityFollowerFixture) grant(t testing.TB) raftauthority.AuthorityGrant {
	t.Helper()
	request := fixture.request(t, 1)
	outbound, produced, err := fixture.fixture.runtime.StepAuthorityMessageFrom(fixture.peer,
		&raftauthority.Message{Kind: raftauthority.MessageRequest, Request: request},
	)
	if err != nil {
		t.Fatalf("StepAuthorityMessage(request): %v", err)
	}
	if !produced || outbound.Authority == nil || outbound.Authority.Kind != raftauthority.MessageGrant {
		t.Fatalf("authority response = %+v produced=%t, want grant", outbound, produced)
	}
	return outbound.Authority.Grant
}

func TestRuntimeReadAuthorityFollowerPromiseGatesElectionEdgesUntilExpiry(t *testing.T) {
	fixture := newReadAuthorityFollowerFixture(t, 160)
	fixture.configure(t, func(memberID uint64) (uint64, bool, error) {
		if memberID == fixture.peer {
			return 77, true, nil
		}
		return 0, false, nil
	})
	grant := fixture.grant(t)
	status, err := fixture.fixture.runtime.Status()
	if err != nil {
		t.Fatal(err)
	}
	blocked := []struct {
		name string
		call func() error
	}{
		{name: "tick", call: fixture.fixture.runtime.Tick},
		{name: "campaign", call: fixture.fixture.runtime.Campaign},
		{name: "vote", call: func() error {
			return fixture.fixture.runtime.StepMessage(&pb.Message{
				Type: pb.MsgVote.Enum(), From: runtimeUint64Ptr(fixture.peer),
				To: runtimeUint64Ptr(fixture.local), Term: runtimeUint64Ptr(status.Term),
			})
		}},
		{name: "pre-vote", call: func() error {
			return fixture.fixture.runtime.StepMessage(&pb.Message{
				Type: pb.MsgPreVote.Enum(), From: runtimeUint64Ptr(fixture.peer),
				To: runtimeUint64Ptr(fixture.local), Term: runtimeUint64Ptr(status.Term),
			})
		}},
		{name: "timeout-now", call: func() error {
			return fixture.fixture.runtime.StepMessage(&pb.Message{
				Type: pb.MsgTimeoutNow.Enum(), From: runtimeUint64Ptr(fixture.peer),
				To: runtimeUint64Ptr(fixture.local), Term: runtimeUint64Ptr(status.Term),
			})
		}},
	}
	for _, test := range blocked {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); !errors.Is(err, ErrAuthorityElectionBlocked) {
				t.Fatalf("%s error = %v, want ErrAuthorityElectionBlocked", test.name, err)
			}
		})
	}
	fixture.clock.now = grant.PromiseUntil
	if err := fixture.fixture.runtime.Tick(); err != nil {
		t.Fatalf("tick at promise expiry = %v", err)
	}
	drainRuntime(t, fixture.fixture.runtime, nil)
}

func newReadAuthorityLeaderFixture(t testing.TB, seed byte) readAuthorityFollowerFixture {
	t.Helper()
	identity := testWALIdentity(seed)
	peer := identity.MemberID + 1
	fixture := newRuntimeFixture(t, seed, []uint64{identity.MemberID, peer})
	drainRuntime(t, fixture.runtime, nil)
	electRuntimeWithPeer(t, fixture.runtime, identity.MemberID, peer)
	state := readAuthorityFollowerFixture{
		fixture: fixture, local: identity.MemberID, peer: peer,
		clock:  &readAuthorityRuntimeClock{},
		policy: readAuthorityRuntimePolicy([]uint64{identity.MemberID, peer}, time.Second),
	}
	state.checked = raftauthority.NewCheckedClock(state.clock)
	if err := fixture.runtime.ConfigureReadAuthority(ReadAuthorityOptions{
		Policy: state.policy, Clock: state.checked,
		LeaderIncarnation: func(memberID uint64) (uint64, bool, error) {
			if memberID == peer {
				return 77, true, nil
			}
			return 0, false, nil
		},
	}); err != nil {
		t.Fatalf("ConfigureReadAuthority leader: %v", err)
	}
	quarantine, err := state.policy.QuarantineDuration()
	if err != nil {
		t.Fatal(err)
	}
	state.clock.now = quarantine
	status, err := fixture.runtime.Status()
	if err != nil || status.RaftState != raft.StateLeader || status.LeaderID != identity.MemberID {
		t.Fatalf("leader status = %+v err=%v", status, err)
	}
	return state
}

func TestRuntimeReadAuthorityLeaderPromiseAllowsTickButBlocksTransfer(t *testing.T) {
	fixture := newReadAuthorityLeaderFixture(t, 161)
	runtime := fixture.fixture.runtime
	if err := runtime.StartReadAuthorityRound(); err != nil {
		t.Fatalf("StartReadAuthorityRound: %v", err)
	}
	until, held, err := runtime.authority.promise.PromiseUntil()
	if err != nil || !held {
		t.Fatalf("local promise = %v held=%t err=%v", until, held, err)
	}
	if err := runtime.Tick(); err != nil {
		t.Fatalf("leader Tick while promise held = %v", err)
	}
	drainRuntime(t, runtime, nil)
	if err := runtime.TransferLeader(fixture.peer); !errors.Is(err, ErrAuthorityElectionBlocked) {
		t.Fatalf("leader TransferLeader while promise held = %v, want ErrAuthorityElectionBlocked", err)
	}
	fixture.clock.now = until
	if err := runtime.TransferLeader(fixture.peer); err != nil {
		t.Fatalf("leader TransferLeader at promise expiry = %v", err)
	}
	drainRuntime(t, runtime, nil)
}

func TestRuntimeReadAuthorityClockFaultFailsClosedAfterGrant(t *testing.T) {
	fixture := newReadAuthorityFollowerFixture(t, 162)
	fixture.configure(t, func(memberID uint64) (uint64, bool, error) {
		if memberID == fixture.peer {
			return 88, true, nil
		}
		return 0, false, nil
	})
	_ = fixture.grant(t)
	clockErr := errors.New("injected elapsed clock fault")
	fixture.clock.err = clockErr
	for _, call := range []struct {
		name string
		call func() error
	}{
		{name: "tick", call: fixture.fixture.runtime.Tick},
		{name: "campaign", call: fixture.fixture.runtime.Campaign},
	} {
		t.Run(call.name, func(t *testing.T) {
			err := call.call()
			if !errors.Is(err, raftauthority.ErrClockFault) || !errors.Is(err, clockErr) {
				t.Fatalf("%s error = %v, want persistent clock fault", call.name, err)
			}
		})
	}
	fixture.clock.err = nil
	if err := fixture.fixture.runtime.Tick(); !errors.Is(err, raftauthority.ErrClockFault) {
		t.Fatalf("clock recovery unexpectedly reopened election edge: %v", err)
	}
}

func TestRuntimeReadAuthorityRequiresIndependentLeaderIncarnation(t *testing.T) {
	fixture := newReadAuthorityFollowerFixture(t, 163)
	fixture.configure(t, func(memberID uint64) (uint64, bool, error) {
		if memberID == fixture.peer {
			return 99, true, nil
		}
		return 0, false, nil
	})
	observation, err := fixture.fixture.runtime.ReadAuthorityObservation()
	if err != nil {
		t.Fatal(err)
	}
	if observation.LeaderIncarnation == fixture.fixture.runtime.identity.NodeIncarnation || observation.LeaderIncarnation != 99 {
		t.Fatalf("leader incarnation = %d, local = %d, want independent 99",
			observation.LeaderIncarnation, fixture.fixture.runtime.identity.NodeIncarnation)
	}
	bad := fixture.request(t, 1)
	bad.HolderIncarnation++
	outbound, produced, err := fixture.fixture.runtime.StepAuthorityMessageFrom(fixture.peer,
		&raftauthority.Message{Kind: raftauthority.MessageRequest, Request: bad},
	)
	if err != nil || produced || outbound.Authority != nil {
		t.Fatalf("mismatched incarnation response = %+v produced=%t err=%v, want dropped", outbound, produced, err)
	}
	if grant := fixture.grant(t); grant.Request.HolderIncarnation != 99 {
		t.Fatalf("grant holder incarnation = %d, want 99", grant.Request.HolderIncarnation)
	}

	missing := newReadAuthorityFollowerFixture(t, 164)
	missing.configure(t, nil)
	if _, err := missing.fixture.runtime.ReadAuthorityObservation(); !errors.Is(err, ErrAuthorityLeaderIncarnationUnavailable) {
		t.Fatalf("missing leader incarnation error = %v, want ErrAuthorityLeaderIncarnationUnavailable", err)
	}
}

func startReadAuthorityRoundWithQuorum(t testing.TB, fixture readAuthorityFollowerFixture) raftauthority.AuthorityToken {
	t.Helper()
	runtime := fixture.fixture.runtime
	if err := runtime.StartReadAuthorityRound(); err != nil {
		t.Fatalf("StartReadAuthorityRound: %v", err)
	}
	requestOutbound, ok := runtime.DrainAuthorityOutbound()
	if !ok || requestOutbound.Authority == nil || requestOutbound.Authority.Kind != raftauthority.MessageRequest {
		t.Fatalf("DrainAuthorityOutbound = %+v ok=%t, want request", requestOutbound, ok)
	}
	request := requestOutbound.Authority.Request
	peerClock := raftauthority.NewCheckedClock(&readAuthorityRuntimeClock{now: fixture.clock.now})
	peerBook, err := raftauthority.NewPromiseBook(peerClock, request.Group, fixture.peer, fixture.policy)
	if err != nil {
		t.Fatalf("peer NewPromiseBook: %v", err)
	}
	observation, err := runtime.ReadAuthorityObservation()
	if err != nil {
		t.Fatalf("leader observation: %v", err)
	}
	grant, err := peerBook.Grant(request, observation)
	if err != nil {
		t.Fatalf("peer Grant: %v", err)
	}
	if _, produced, err := runtime.StepAuthorityMessageFrom(fixture.peer, &raftauthority.Message{
		Kind: raftauthority.MessageGrant, Request: grant.Request, Grant: grant,
	}); err != nil || produced {
		t.Fatalf("StepAuthorityMessage(grant) produced=%t err=%v, want accepted no response", produced, err)
	}
	token, err := runtime.ReadAuthorityToken()
	if err != nil {
		t.Fatalf("ReadAuthorityToken: %v", err)
	}
	return token
}

func TestRuntimeReadAuthorityNoQuorumExpiresAndAllocatesNewNonce(t *testing.T) {
	fixture := newReadAuthorityLeaderFixture(t, 170)
	runtime := fixture.fixture.runtime
	if err := runtime.StartReadAuthorityRound(); err != nil {
		t.Fatalf("StartReadAuthorityRound: %v", err)
	}
	firstNonce := runtime.authority.round.Request().Nonce
	if _, ok := runtime.DrainAuthorityOutbound(); !ok {
		t.Fatal("initial authority request was not queued")
	}
	if err := runtime.StartReadAuthorityRound(); !errors.Is(err, ErrAuthorityRoundActive) {
		t.Fatalf("StartReadAuthorityRound before no-quorum expiry = %v, want ErrAuthorityRoundActive", err)
	}
	promiseUntil, held, err := runtime.authority.promise.PromiseUntil()
	if err != nil || !held {
		t.Fatalf("initial self promise = %v held=%t err=%v", promiseUntil, held, err)
	}
	fixture.clock.now = promiseUntil
	if err := runtime.StartReadAuthorityRound(); err != nil {
		t.Fatalf("StartReadAuthorityRound after no-quorum expiry: %v", err)
	}
	if runtime.authority.round == nil {
		t.Fatal("new no-quorum round was not retained")
	}
	secondNonce := runtime.authority.round.Request().Nonce
	if secondNonce != firstNonce+1 {
		t.Fatalf("new round nonce = %d, first = %d, want one increment", secondNonce, firstNonce)
	}
	request, ok := runtime.DrainAuthorityOutbound()
	if !ok || request.Authority == nil || request.Authority.Request.Nonce != secondNonce {
		t.Fatalf("new authority request = %+v ok=%t, want nonce %d", request, ok, secondNonce)
	}
}

func TestRuntimeReadAuthorityRenewalRetainsOldTokenUntilQuorum(t *testing.T) {
	fixture := newReadAuthorityLeaderFixture(t, 171)
	runtime := fixture.fixture.runtime
	oldToken := startReadAuthorityRoundWithQuorum(t, fixture)
	fixture.clock.now = oldToken.StartedAt + time.Nanosecond
	if fixture.clock.now >= oldToken.ExpiresAt {
		t.Fatalf("renewal test clock %v already reached old expiry %v", fixture.clock.now, oldToken.ExpiresAt)
	}
	if err := runtime.StartReadAuthorityRound(); err != nil {
		t.Fatalf("StartReadAuthorityRound renewal: %v", err)
	}
	if runtime.authority.renewal == nil {
		t.Fatal("renewal round was not retained")
	}
	requestOutbound, ok := runtime.DrainAuthorityOutbound()
	if !ok || requestOutbound.Authority == nil || requestOutbound.Authority.Kind != raftauthority.MessageRequest {
		t.Fatalf("renewal outbound = %+v ok=%t, want request", requestOutbound, ok)
	}
	if requestOutbound.Authority.Request.Nonce <= oldToken.Nonce {
		t.Fatalf("renewal nonce = %d, old = %d, want newer nonce", requestOutbound.Authority.Request.Nonce, oldToken.Nonce)
	}
	current, err := runtime.ReadAuthorityToken()
	if err != nil || current.Nonce != oldToken.Nonce {
		t.Fatalf("token during incomplete renewal = %+v err=%v, want old nonce %d", current, err, oldToken.Nonce)
	}
	if err := runtime.ValidateReadAuthorityToken(oldToken); err != nil {
		t.Fatalf("old token validation during renewal: %v", err)
	}

	peerClock := raftauthority.NewCheckedClock(&readAuthorityRuntimeClock{now: fixture.clock.now})
	peerBook, err := raftauthority.NewPromiseBook(peerClock, requestOutbound.Authority.Request.Group,
		fixture.peer, fixture.policy)
	if err != nil {
		t.Fatalf("peer NewPromiseBook: %v", err)
	}
	observation, err := runtime.ReadAuthorityObservation()
	if err != nil {
		t.Fatalf("renewal observation: %v", err)
	}
	grant, err := peerBook.Grant(requestOutbound.Authority.Request, observation)
	if err != nil {
		t.Fatalf("peer renewal Grant: %v", err)
	}
	if _, produced, err := runtime.StepAuthorityMessageFrom(fixture.peer, &raftauthority.Message{
		Kind: raftauthority.MessageGrant, Request: grant.Request, Grant: grant,
	}); err != nil || produced {
		t.Fatalf("renewal grant step produced=%t err=%v, want accepted no response", produced, err)
	}
	newToken, err := runtime.ReadAuthorityToken()
	if err != nil {
		t.Fatalf("ReadAuthorityToken after renewal quorum: %v", err)
	}
	if newToken.Nonce != requestOutbound.Authority.Request.Nonce || newToken.Nonce <= oldToken.Nonce {
		t.Fatalf("renewed token = %+v, old nonce=%d request nonce=%d", newToken, oldToken.Nonce, requestOutbound.Authority.Request.Nonce)
	}
	if err := runtime.ValidateReadAuthorityToken(newToken); err != nil {
		t.Fatalf("renewed token validation: %v", err)
	}
	if err := runtime.ValidateReadAuthorityToken(oldToken); err != nil {
		t.Fatalf("old token validation after renewal quorum: %v", err)
	}
}

func TestRuntimeReadAuthorityConfigurationPendingDoesNotLatch(t *testing.T) {
	t.Run("ordinary-normal-apply-lag", func(t *testing.T) {
		identity := testWALIdentity(172)
		peer := identity.MemberID + 1
		fixture := newRuntimeFixture(t, 172, []uint64{identity.MemberID, peer})
		runtime := fixture.runtime
		drainRuntime(t, runtime, nil)
		electRuntimeWithPeer(t, runtime, identity.MemberID, peer)
		command := testApplySessionOpen(fixture.base)
		if err := runtime.Propose(command); err != nil {
			t.Fatalf("normal proposal: %v", err)
		}
		var appendMessage *pb.Message
		drainRuntime(t, runtime, func(outbound OutboundMessage) error {
			if outbound.Message.GetType() == pb.MsgApp && outbound.To == peer &&
				len(outbound.Message.GetEntries()) != 0 {
				appendMessage = proto.Clone(outbound.Message).(*pb.Message)
			}
			return nil
		})
		if appendMessage == nil {
			t.Fatal("normal proposal produced no append")
		}
		last := appendMessage.GetEntries()[len(appendMessage.GetEntries())-1].GetIndex()
		if err := runtime.StepMessage(&pb.Message{
			Type: pb.MsgAppResp.Enum(), From: runtimeUint64Ptr(peer), To: runtimeUint64Ptr(identity.MemberID),
			Term: runtimeUint64Ptr(appendMessage.GetTerm()), Index: runtimeUint64Ptr(last),
		}); err != nil {
			t.Fatalf("normal proposal commit response: %v", err)
		}
		if runtime.node.PendingConfiguration() {
			t.Fatal("normal apply lag set PendingConfiguration")
		}
		workspace := new(ReadyWorkspace)
		captured, err := runtime.DriveReady(workspace, func(OutboundMessage) error { return nil }, settleTestApplied)
		if err != nil || captured.Kind != DriveCaptured {
			t.Fatalf("capture committed normal Ready = %+v err=%v", captured, err)
		}
		if runtime.node.PendingConfiguration() {
			t.Fatal("captured normal apply lag set PendingConfiguration")
		}
		drainRuntime(t, runtime, nil)
		if runtime.node.PendingConfiguration() {
			t.Fatal("normal apply left PendingConfiguration latched")
		}
	})

	t.Run("rejected-stale-config-append", func(t *testing.T) {
		fixture := newReadAuthorityFollowerFixture(t, 173)
		runtime := fixture.fixture.runtime
		status, err := runtime.Status()
		if err != nil {
			t.Fatal(err)
		}
		_, encoded, err := pb.MarshalConfChange(&pb.ConfChange{
			Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: runtimeUint64Ptr(fixture.peer + 1),
		})
		if err != nil {
			t.Fatalf("encode stale configuration: %v", err)
		}
		staleTerm := status.Term + 1
		staleIndex := status.Commit
		previousIndex := staleIndex - 1
		previousTerm, err := fixture.fixture.wal.Term(previousIndex)
		if err != nil {
			t.Fatalf("stale append previous term: %v", err)
		}
		if err := runtime.StepMessage(&pb.Message{
			Type: pb.MsgApp.Enum(), From: runtimeUint64Ptr(fixture.peer), To: runtimeUint64Ptr(fixture.local),
			Term: &staleTerm, Index: &previousIndex, LogTerm: &previousTerm, Commit: &staleIndex,
			Entries: []*pb.Entry{{Type: pb.EntryConfChange.Enum(), Term: &staleTerm, Index: &staleIndex, Data: encoded}},
		}); err != nil {
			t.Fatalf("rejected stale configuration append: %v", err)
		}
		if runtime.node.PendingConfiguration() {
			t.Fatal("rejected stale configuration append set PendingConfiguration")
		}
		drainRuntime(t, runtime, nil)
		if runtime.node.PendingConfiguration() {
			t.Fatal("rejected stale configuration append latched PendingConfiguration")
		}
		heartbeatTerm := staleTerm
		if err := runtime.StepMessage(&pb.Message{
			Type: pb.MsgHeartbeat.Enum(), From: runtimeUint64Ptr(fixture.peer), To: runtimeUint64Ptr(fixture.local),
			Term: &heartbeatTerm, Commit: &staleIndex,
		}); err != nil {
			t.Fatalf("post-rejection heartbeat: %v", err)
		}
		drainRuntime(t, runtime, nil)
		if runtime.node.PendingConfiguration() {
			t.Fatal("post-rejection heartbeat left PendingConfiguration latched")
		}
	})
}

func TestRuntimeReadAuthorityRoundInvalidatesOnTermAndConfigChange(t *testing.T) {
	t.Run("term", func(t *testing.T) {
		fixture := newReadAuthorityLeaderFixture(t, 165)
		token := startReadAuthorityRoundWithQuorum(t, fixture)
		status, err := fixture.fixture.runtime.Status()
		if err != nil {
			t.Fatal(err)
		}
		newTerm := status.Term + 1
		if err := fixture.fixture.runtime.StepMessage(&pb.Message{
			Type: pb.MsgHeartbeat.Enum(), From: runtimeUint64Ptr(fixture.peer),
			To: runtimeUint64Ptr(fixture.local), Term: &newTerm,
			Commit: runtimeUint64Ptr(status.Commit),
		}); err != nil {
			t.Fatalf("higher-term heartbeat: %v", err)
		}
		drainRuntime(t, fixture.fixture.runtime, nil)
		if err := fixture.fixture.runtime.ValidateReadAuthorityToken(token); err == nil {
			t.Fatal("token survived term change")
		}
	})

	t.Run("configuration", func(t *testing.T) {
		fixture := newReadAuthorityLeaderFixture(t, 166)
		_ = startReadAuthorityRoundWithQuorum(t, fixture)
		change := &pb.ConfChange{
			Type: pb.ConfChangeAddLearnerNode.Enum(), NodeId: runtimeUint64Ptr(fixture.peer + 1),
			Context: make([]byte, MembershipTransitionDigestBytes),
		}
		if err := fixture.fixture.runtime.ProposeConfChange(change); err != nil {
			t.Fatalf("ProposeConfChange: %v", err)
		}
		if _, err := fixture.fixture.runtime.ReadAuthorityToken(); err == nil {
			t.Fatal("token survived configuration proposal")
		}
		drainRuntime(t, fixture.fixture.runtime, nil)
	})
}

func TestRuntimeReadAuthorityPolicyChecksAndDisableReenableSafety(t *testing.T) {
	t.Run("applied-voter-set-and-disabled-fallback", func(t *testing.T) {
		identity := testWALIdentity(167)
		peer := identity.MemberID + 1
		fixture := newRuntimeFixture(t, 167, []uint64{identity.MemberID, peer})
		drainRuntime(t, fixture.runtime, nil)
		clock := &readAuthorityRuntimeClock{}
		policy := readAuthorityRuntimePolicy([]uint64{identity.MemberID}, time.Second)
		if err := fixture.runtime.ConfigureReadAuthority(ReadAuthorityOptions{
			Policy: policy, Clock: raftauthority.NewCheckedClock(clock),
		}); !errors.Is(err, ErrAuthorityConfigurationMismatch) {
			t.Fatalf("mismatched applied voter set error = %v, want ErrAuthorityConfigurationMismatch", err)
		}
		if fixture.runtime.ReadAuthorityEnabled() {
			t.Fatal("mismatched policy enabled authority")
		}
		if err := fixture.runtime.ConfigureReadAuthority(ReadAuthorityOptions{}); err != nil {
			t.Fatalf("disabled authority configuration: %v", err)
		}
		if err := fixture.runtime.Campaign(); err != nil {
			t.Fatalf("disabled policy Campaign: %v", err)
		}
		drainRuntime(t, fixture.runtime, nil)
	})

	t.Run("disable-reenable-preserves-immutable-policy", func(t *testing.T) {
		fixture := newReadAuthorityFollowerFixture(t, 168)
		fixture.configure(t, func(memberID uint64) (uint64, bool, error) {
			if memberID == fixture.peer {
				return 101, true, nil
			}
			return 0, false, nil
		})
		grant := fixture.grant(t)
		if err := fixture.fixture.runtime.ConfigureReadAuthority(ReadAuthorityOptions{}); err != nil {
			t.Fatalf("disable authority: %v", err)
		}
		if fixture.fixture.runtime.ReadAuthorityEnabled() {
			t.Fatal("disabled authority still reported enabled")
		}
		short := fixture.policy
		short.Capabilities = append([]raftauthority.VoterCapability(nil), fixture.policy.Capabilities...)
		short.PolicyVersion++
		short.MaxGrant = 500 * time.Millisecond
		for index := range short.Capabilities {
			short.Capabilities[index].PolicyVersion = short.PolicyVersion
		}
		shortOptions := ReadAuthorityOptions{
			Policy: short, Clock: fixture.checked,
			LeaderIncarnation: func(memberID uint64) (uint64, bool, error) {
				if memberID == fixture.peer {
					return 101, true, nil
				}
				return 0, false, nil
			},
		}
		if err := fixture.fixture.runtime.ConfigureReadAuthority(shortOptions); !errors.Is(err, ErrAuthorityReconfiguration) {
			t.Fatalf("reenable authority while old promise is live = %v, want ErrAuthorityReconfiguration", err)
		}
		fixture.clock.now = grant.PromiseUntil - time.Nanosecond
		if err := fixture.fixture.runtime.Campaign(); !errors.Is(err, ErrAuthorityElectionBlocked) {
			t.Fatalf("Campaign before original promise expiry = %v, want ErrAuthorityElectionBlocked", err)
		}
		fixture.clock.now = grant.PromiseUntil
		if err := fixture.fixture.runtime.ConfigureReadAuthority(shortOptions); !errors.Is(err, ErrAuthorityReconfiguration) {
			t.Fatalf("reenable changed authority after old promise expiry = %v, want ErrAuthorityReconfiguration", err)
		}
		until, held, err := fixture.fixture.runtime.authority.promise.PromiseUntil()
		if err != nil || !held || until != grant.PromiseUntil {
			t.Fatalf("old promise after rejected reconfiguration = %v held=%t err=%v, want %v retained", until, held, err, grant.PromiseUntil)
		}
		if err := fixture.fixture.runtime.ConfigureReadAuthority(ReadAuthorityOptions{
			Policy: fixture.policy, Clock: fixture.checked,
			LeaderIncarnation: shortOptions.LeaderIncarnation,
		}); err != nil {
			t.Fatalf("same-policy reenable after old promise expiry: %v", err)
		}
		if !fixture.fixture.runtime.ReadAuthorityEnabled() {
			t.Fatal("same-policy reenable left authority disabled")
		}
		if err := fixture.fixture.runtime.Campaign(); err != nil {
			t.Fatalf("Campaign after same-policy reenable at expired promise: %v", err)
		}
		drainRuntime(t, fixture.fixture.runtime, nil)
	})

	t.Run("disabled-clock-fault-remains-closed", func(t *testing.T) {
		fixture := newReadAuthorityFollowerFixture(t, 169)
		fixture.configure(t, func(memberID uint64) (uint64, bool, error) {
			if memberID == fixture.peer {
				return 102, true, nil
			}
			return 0, false, nil
		})
		_ = fixture.grant(t)
		if err := fixture.fixture.runtime.ConfigureReadAuthority(ReadAuthorityOptions{}); err != nil {
			t.Fatalf("disable authority: %v", err)
		}
		clockErr := errors.New("injected disabled-authority clock fault")
		fixture.clock.err = clockErr
		for _, test := range []struct {
			name string
			call func() error
		}{
			{name: "tick", call: fixture.fixture.runtime.Tick},
			{name: "campaign", call: fixture.fixture.runtime.Campaign},
		} {
			t.Run(test.name, func(t *testing.T) {
				err := test.call()
				if !errors.Is(err, raftauthority.ErrClockFault) || !errors.Is(err, clockErr) {
					t.Fatalf("%s after disable with clock fault = %v, want persistent ErrClockFault", test.name, err)
				}
			})
		}
	})
}
