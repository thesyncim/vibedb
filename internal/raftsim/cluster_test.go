package raftsim

import (
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

type traceScript struct {
	t       *testing.T
	cluster *Cluster
	trace   *Trace
	time    uint64
	ready   map[uint64]uint64
}

func newTraceScript(t *testing.T, scenario *Scenario, seed uint64) *traceScript {
	t.Helper()
	cluster, err := NewCluster(scenario)
	if err != nil {
		t.Fatal(err)
	}
	return &traceScript{
		t: t, cluster: cluster, trace: NewTrace(seed, scenario.Digest()),
		ready: make(map[uint64]uint64),
	}
}

func (s *traceScript) execute(event Event) {
	s.t.Helper()
	event.Time = s.time
	s.time++
	if err := s.trace.Append(event); err != nil {
		s.t.Fatalf("append %s: %v", event.Kind, err)
	}
	canonical, _ := s.trace.Event(s.trace.Len() - 1)
	if err := s.cluster.Execute(canonical); err != nil {
		s.t.Fatalf("execute %s: %v", event.Kind, err)
	}
	if event.Kind == EventRestart {
		s.ready[event.Node] = 0
	}
}

func (s *traceScript) driveMember(id uint64) bool {
	s.t.Helper()
	changed := false
	for steps := 0; steps < 256; steps++ {
		member, err := s.cluster.member(id)
		if err != nil || member.node == nil {
			return changed
		}
		switch member.node.Phase() {
		case raftmodel.PhaseIdle:
			has, err := member.node.HasReady()
			if err != nil {
				s.t.Fatal(err)
			}
			if !has {
				return changed
			}
			s.ready[id]++
			s.execute(Event{Kind: EventCaptureReady, Node: id, Ref: s.ready[id]})
		case raftmodel.PhaseCaptured:
			s.execute(Event{Kind: EventPersistReady, Node: id, Ref: member.node.ReadyID()})
		case raftmodel.PhasePersisted:
			progress, ok := member.node.CurrentReady()
			if !ok {
				s.t.Fatal("missing Ready progress")
			}
			if progress.MessagesSent < progress.MessageCount {
				s.execute(Event{
					Kind: EventSendMessage, Node: id, Ref: s.cluster.nextMessageID + 1,
				})
			} else {
				s.execute(Event{Kind: EventFinishMessages, Node: id, Ref: member.node.ReadyID()})
			}
		case raftmodel.PhaseMessagesDrained:
			s.execute(Event{Kind: EventInstallSnapshot, Node: id, Ref: member.node.ReadyID()})
		case raftmodel.PhaseSnapshotInstalled:
			progress, ok := member.node.CurrentReady()
			if !ok {
				s.t.Fatal("missing apply progress")
			}
			expected := uint64(0)
			if progress.CommittedApplied < progress.CommittedCount {
				expected = member.machine.Applied() + 1
			}
			s.execute(Event{Kind: EventApplyEntry, Node: id, Ref: member.node.ReadyID(), Value: expected})
		case raftmodel.PhaseEntriesApplied:
			progress, ok := member.node.CurrentReady()
			if !ok {
				s.t.Fatal("missing read progress")
			}
			if progress.ReadStatesRecorded < progress.ReadStateCount {
				s.execute(Event{Kind: EventRecordReadState, Node: id, Ref: member.node.ReadyID()})
			} else {
				s.execute(Event{Kind: EventFinishReadStates, Node: id, Ref: member.node.ReadyID()})
			}
		case raftmodel.PhaseReadStatesRecorded:
			s.execute(Event{Kind: EventAdvanceReady, Node: id, Ref: member.node.ReadyID()})
		case raftmodel.PhaseFailed:
			s.t.Fatalf("member %d failed: %v", id, member.node.Failure())
		default:
			s.t.Fatalf("member %d unexpected phase %s", id, member.node.Phase())
		}
		changed = true
	}
	s.t.Fatalf("member %d did not quiesce", id)
	return false
}

func (s *traceScript) quiesce() {
	s.t.Helper()
	for iteration := 0; iteration < 1024; iteration++ {
		progress := false
		for _, member := range s.cluster.members {
			if s.driveMember(member.id) {
				progress = true
			}
		}
		for _, message := range s.cluster.ActiveMessages() {
			from, _ := s.cluster.memberOrdinal(message.From)
			to, _ := s.cluster.memberOrdinal(message.To)
			target, _ := s.cluster.member(message.To)
			if s.cluster.partitioned[from][to] || target.node == nil {
				continue
			}
			s.execute(Event{
				Kind: EventDeliverMessage, Node: message.To, Peer: message.From, Ref: message.ID,
			})
			progress = true
		}
		if !progress {
			return
		}
	}
	s.t.Fatal("cluster did not quiesce")
}

func (s *traceScript) quiesceWithRNG(rng *RNG) {
	s.t.Helper()
	for iteration := 0; iteration < 1024; iteration++ {
		progress := false
		for _, member := range s.cluster.members {
			if s.driveMember(member.id) {
				progress = true
			}
		}
		eligible := make([]MessageInfo, 0, len(s.cluster.messages))
		for _, message := range s.cluster.ActiveMessages() {
			from, _ := s.cluster.memberOrdinal(message.From)
			to, _ := s.cluster.memberOrdinal(message.To)
			target, _ := s.cluster.member(message.To)
			if !s.cluster.partitioned[from][to] && target.node != nil {
				eligible = append(eligible, message)
			}
		}
		if len(eligible) != 0 {
			choice, _ := rng.Choose(uint64(len(eligible)))
			message := eligible[choice]
			s.execute(Event{
				Kind: EventDeliverMessage, Node: message.To, Peer: message.From, Ref: message.ID,
			})
			progress = true
		}
		if !progress {
			return
		}
	}
	s.t.Fatal("randomized cluster did not quiesce")
}

func TestScenarioOwnsCanonicalInputsAndFencesReplayIdentity(t *testing.T) {
	voters := []uint64{3, 1, 2}
	payload := []byte("owned")
	context := []byte("read-context")
	scenario, err := NewScenario(voters, []Proposal{{Reference: 9, Data: payload}}, []ReadRequest{{Reference: 7, Context: context}})
	if err != nil {
		t.Fatal(err)
	}
	digest := scenario.Digest()
	voters[0], payload[0], context[0] = 99, 'X', 'Y'
	if scenario.Digest() != digest || !slices.Equal(scenario.voters, []uint64{1, 2, 3}) {
		t.Fatal("scenario retained caller backing or noncanonical voters")
	}
	cluster, err := NewCluster(scenario)
	if err != nil {
		t.Fatal(err)
	}
	if cluster.ReplayIdentity().ScenarioDigest != digest {
		t.Fatal("cluster replay identity differs from scenario")
	}
}

func TestSingleVoterCrashAfterDurableCommitReplaysBeforeResponse(t *testing.T) {
	scenario, err := NewScenario(
		[]uint64{1},
		[]Proposal{{Reference: 41, Data: []byte("survives-crash")}},
		[]ReadRequest{{Reference: 51, Context: []byte("linearizable-read")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	script := newTraceScript(t, scenario, 0x51)
	script.execute(Event{Kind: EventCampaign, Node: 1})
	script.quiesce()
	state, _ := script.cluster.MemberState(1)
	if state.Status.RaftState != raft.StateLeader {
		t.Fatalf("campaign state = %s", state.Status.RaftState)
	}

	script.execute(Event{Kind: EventPropose, Node: 1, Ref: 41})
	// Stop at the first Ready that contains the committed proposal, after its
	// complete durability and message stages but before ordered application.
	for cuts := 0; cuts < 16; cuts++ {
		member, _ := script.cluster.member(1)
		if member.node.Phase() == raftmodel.PhaseIdle {
			has, hasErr := member.node.HasReady()
			if hasErr != nil || !has {
				t.Fatalf("proposal produced no Ready: %v", hasErr)
			}
			script.ready[1]++
			script.execute(Event{Kind: EventCaptureReady, Node: 1, Ref: script.ready[1]})
		}
		progress, _ := member.node.CurrentReady()
		if member.node.Phase() == raftmodel.PhaseCaptured {
			script.execute(Event{Kind: EventFailPersistAmbiguous, Node: 1, Ref: member.node.ReadyID()})
			// The exact retry settles the ambiguous durable outcome.
			script.execute(Event{Kind: EventPersistReady, Node: 1, Ref: member.node.ReadyID()})
		}
		for {
			progress, _ = member.node.CurrentReady()
			if progress.MessagesSent == progress.MessageCount {
				break
			}
			script.execute(Event{Kind: EventSendMessage, Node: 1, Ref: script.cluster.nextMessageID + 1})
		}
		script.execute(Event{Kind: EventFinishMessages, Node: 1, Ref: member.node.ReadyID()})
		script.execute(Event{Kind: EventInstallSnapshot, Node: 1, Ref: member.node.ReadyID()})
		progress, _ = member.node.CurrentReady()
		if progress.CommittedCount != 0 {
			break
		}
		script.execute(Event{Kind: EventApplyEntry, Node: 1, Ref: member.node.ReadyID()})
		script.execute(Event{Kind: EventFinishReadStates, Node: 1, Ref: member.node.ReadyID()})
		script.execute(Event{Kind: EventAdvanceReady, Node: 1, Ref: member.node.ReadyID()})
	}
	if _, completed := script.cluster.ProposalCompleted(1, 41); completed || script.cluster.ProposalResponded(41) {
		t.Fatal("proposal completed or responded before apply")
	}
	script.execute(Event{Kind: EventCrash, Node: 1})
	script.execute(Event{Kind: EventRestart, Node: 1, Value: 2})
	script.quiesce()
	if _, completed := script.cluster.ProposalCompleted(1, 41); !completed {
		t.Fatal("restart did not replay durable committed proposal")
	}
	script.execute(Event{Kind: EventRespondProposal, Node: 1, Ref: 41})
	script.execute(Event{Kind: EventCampaign, Node: 1})
	script.quiesce()
	script.execute(Event{Kind: EventRequestRead, Node: 1, Ref: 51})
	script.quiesce()
	script.execute(Event{Kind: EventServeRead, Node: 1, Ref: 51})

	encoded, err := script.trace.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenTrace(encoded)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := NewCluster(scenario)
	if err != nil {
		t.Fatal(err)
	}
	if steps, err := Replay(opened, replayed); err != nil || steps != opened.Len() {
		t.Fatalf("Replay() = %d/%d, %v", steps, opened.Len(), err)
	}
	if !replayed.ProposalResponded(41) {
		t.Fatal("replay omitted proposal response")
	}
}

func TestAmbiguousPersistCrashWithoutRetryRecoversDurableProposal(t *testing.T) {
	scenario, err := NewScenario([]uint64{1}, []Proposal{{Reference: 61, Data: []byte("ambiguous")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	script := newTraceScript(t, scenario, 0x6161)
	script.execute(Event{Kind: EventCampaign, Node: 1})
	script.quiesce()
	script.execute(Event{Kind: EventPropose, Node: 1, Ref: 61})
	member, _ := script.cluster.member(1)
	script.ready[1]++
	script.execute(Event{Kind: EventCaptureReady, Node: 1, Ref: script.ready[1]})
	script.execute(Event{Kind: EventFailPersistAmbiguous, Node: 1, Ref: member.node.ReadyID()})
	script.execute(Event{Kind: EventCrash, Node: 1})
	script.execute(Event{Kind: EventRestart, Node: 1, Value: 2})
	script.execute(Event{Kind: EventCampaign, Node: 1})
	script.quiesce()
	if _, completed := script.cluster.ProposalCompleted(1, 61); !completed {
		t.Fatal("ambiguous durable proposal was not replayed after crash")
	}
	script.execute(Event{Kind: EventRespondProposal, Node: 1, Ref: 61})
}

func TestDefinitePersistFailureCrashDoesNotInventProposal(t *testing.T) {
	scenario, err := NewScenario([]uint64{1}, []Proposal{{Reference: 62, Data: []byte("definite")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	script := newTraceScript(t, scenario, 0x6262)
	script.execute(Event{Kind: EventCampaign, Node: 1})
	script.quiesce()
	script.execute(Event{Kind: EventPropose, Node: 1, Ref: 62})
	member, _ := script.cluster.member(1)
	script.ready[1]++
	script.execute(Event{Kind: EventCaptureReady, Node: 1, Ref: script.ready[1]})
	script.execute(Event{Kind: EventFailPersistDefinite, Node: 1, Ref: member.node.ReadyID()})
	script.execute(Event{Kind: EventCrash, Node: 1})
	script.execute(Event{Kind: EventRestart, Node: 1, Value: 2})
	script.execute(Event{Kind: EventCampaign, Node: 1})
	script.quiesce()
	if _, completed := script.cluster.ProposalCompleted(1, 62); completed {
		t.Fatal("definitely failed persistence invented proposal after restart")
	}
	if err := script.cluster.Execute(Event{
		Kind: EventRespondProposal, Node: 1, Ref: 62, Time: script.time,
	}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("missing completion response error = %v", err)
	}
}

func TestThreeVoterPartitionElectsLeaderAndConvergesPrefix(t *testing.T) {
	scenario, err := NewScenario([]uint64{1, 2, 3}, []Proposal{
		{Reference: 1, Data: []byte("before-partition")},
		{Reference: 2, Data: []byte("during-partition")},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	script := newTraceScript(t, scenario, 0x12345678)
	script.execute(Event{Kind: EventPartitionLink, Node: 1, Peer: 2})
	script.execute(Event{Kind: EventPartitionLink, Node: 1, Peer: 3})
	script.execute(Event{Kind: EventCampaign, Node: 2})
	script.quiesce()
	state2, _ := script.cluster.MemberState(2)
	if state2.Status.RaftState != raft.StateLeader || state2.Status.GetTerm() <= 1 {
		t.Fatalf("member 2 status = %+v after partition", state2.Status)
	}
	script.execute(Event{Kind: EventPropose, Node: 2, Ref: 2})
	script.quiesce()
	script.execute(Event{Kind: EventRespondProposal, Node: 2, Ref: 2})
	if _, completed := script.cluster.ProposalCompleted(1, 2); completed {
		t.Fatal("isolated old leader applied partitioned proposal")
	}

	script.execute(Event{Kind: EventHealLink, Node: 1, Peer: 2})
	script.execute(Event{Kind: EventHealLink, Node: 1, Peer: 3})
	script.quiesce()
	for _, id := range []uint64{1, 2, 3} {
		if _, completed := script.cluster.ProposalCompleted(id, 2); !completed {
			t.Fatalf("member %d did not converge to proposal 2", id)
		}
	}
	script.execute(Event{Kind: EventPropose, Node: 2, Ref: 1})
	script.quiesce()
	script.execute(Event{Kind: EventRespondProposal, Node: 2, Ref: 1})
	for _, id := range []uint64{1, 2, 3} {
		if _, completed := script.cluster.ProposalCompleted(id, 1); !completed {
			t.Fatalf("member %d did not converge to proposal 1", id)
		}
	}
	if err := script.cluster.CheckInvariants(); err != nil {
		t.Fatal(err)
	}

	replayed, err := NewCluster(scenario)
	if err != nil {
		t.Fatal(err)
	}
	if steps, err := Replay(script.trace, replayed); err != nil || steps != script.trace.Len() {
		t.Fatalf("partition Replay() = %d/%d, %v", steps, script.trace.Len(), err)
	}
	for _, id := range []uint64{1, 2, 3} {
		left, _ := script.cluster.MemberState(id)
		right, _ := replayed.MemberState(id)
		if left.Applied != right.Applied || left.DurableCommit != right.DurableCommit ||
			left.Status.GetTerm() != right.Status.GetTerm() {
			t.Fatalf("member %d replay state differs: %+v vs %+v", id, left, right)
		}
	}
}

func TestPartitionedLeaderTicksThroughDeterministicCheckQuorum(t *testing.T) {
	scenario, err := NewScenario([]uint64{1, 2, 3}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	script := newTraceScript(t, scenario, 0x7171)
	script.execute(Event{Kind: EventCampaign, Node: 1})
	script.quiesce()
	state, _ := script.cluster.MemberState(1)
	if state.Status.RaftState != raft.StateLeader {
		t.Fatalf("initial state = %s", state.Status.RaftState)
	}
	script.execute(Event{Kind: EventPartitionLink, Node: 1, Peer: 2})
	script.execute(Event{Kind: EventPartitionLink, Node: 1, Peer: 3})
	// The first check clears peer activity observed during the election; the
	// second full interval proves no quorum has responded since the partition.
	for range 2 * raftmodel.ElectionTick {
		script.execute(Event{Kind: EventLeaderTick, Node: 1})
		script.quiesce()
	}
	state, _ = script.cluster.MemberState(1)
	if state.Status.RaftState == raft.StateLeader {
		t.Fatal("partitioned leader did not step down at fixed check-quorum threshold")
	}
	script.execute(Event{Kind: EventHealLink, Node: 1, Peer: 2})
	script.execute(Event{Kind: EventHealLink, Node: 1, Peer: 3})
	script.quiesce()
	script.execute(Event{Kind: EventCampaign, Node: 2})
	script.quiesce()
	state, _ = script.cluster.MemberState(2)
	if state.Status.RaftState != raft.StateLeader {
		t.Fatalf("healed state = %s", state.Status.RaftState)
	}
}

func TestSameSeedProducesByteExactScheduleAndFinalState(t *testing.T) {
	scenario, err := NewScenario([]uint64{1, 2, 3}, []Proposal{{Reference: 7, Data: []byte("seeded")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	run := func(seed uint64) (*Trace, *Cluster) {
		script := newTraceScript(t, scenario, seed)
		rng := NewRNG(seed)
		script.execute(Event{Kind: EventPartitionLink, Node: 1, Peer: 2})
		script.execute(Event{Kind: EventPartitionLink, Node: 1, Peer: 3})
		script.execute(Event{Kind: EventCampaign, Node: 2})
		script.quiesceWithRNG(&rng)
		script.execute(Event{Kind: EventPropose, Node: 2, Ref: 7})
		script.quiesceWithRNG(&rng)
		script.execute(Event{Kind: EventHealLink, Node: 1, Peer: 2})
		script.execute(Event{Kind: EventHealLink, Node: 1, Peer: 3})
		script.quiesceWithRNG(&rng)
		return script.trace, script.cluster
	}
	leftTrace, leftCluster := run(0x5eed)
	rightTrace, rightCluster := run(0x5eed)
	leftBytes, err := leftTrace.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	rightBytes, err := rightTrace.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(leftBytes, rightBytes) {
		t.Fatal("same seed produced different canonical traces")
	}
	for _, id := range []uint64{1, 2, 3} {
		left, _ := leftCluster.MemberState(id)
		right, _ := rightCluster.MemberState(id)
		if left.Applied != right.Applied || left.DurableCommit != right.DurableCommit ||
			left.Status.GetTerm() != right.Status.GetTerm() {
			t.Fatalf("member %d same-seed state differs", id)
		}
	}
}

func TestClusterRefusesResponseBeforePublicationAndPastTraceTime(t *testing.T) {
	scenario, err := NewScenario([]uint64{1}, []Proposal{{Reference: 1, Data: []byte("x")}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := NewCluster(scenario)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.Execute(Event{Kind: EventRespondProposal, Node: 1, Ref: 1}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("early response error = %v", err)
	}
	if err := cluster.Execute(Event{Kind: EventCampaign, Node: 1, Time: 2}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Execute(Event{Kind: EventCrash, Node: 1, Time: 1}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("time regression error = %v", err)
	}
}

func TestNetworkDuplicateDropAndDefiniteDiskFailureAreReplayable(t *testing.T) {
	scenario, err := NewScenario([]uint64{1, 2, 3}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	script := newTraceScript(t, scenario, 77)
	script.execute(Event{Kind: EventCampaign, Node: 1})
	if !script.driveMember(1) {
		t.Fatal("campaign produced no Ready work")
	}
	messages := script.cluster.ActiveMessages()
	if len(messages) < 2 {
		t.Fatalf("campaign messages = %d, want at least two", len(messages))
	}
	original := messages[0]
	duplicateID := script.cluster.nextMessageID + 1
	script.execute(Event{
		Kind: EventDuplicateMessage, Node: original.To, Peer: original.From,
		Ref: original.ID, Value: duplicateID,
	})
	script.execute(Event{
		Kind: EventDropMessage, Node: original.To, Peer: original.From, Ref: original.ID,
	})
	script.quiesce()
	state, _ := script.cluster.MemberState(1)
	if state.Status.RaftState != raft.StateLeader {
		t.Fatalf("duplicate/drop election state = %s", state.Status.RaftState)
	}
	script.execute(Event{Kind: EventLeaderTick, Node: 1})
	script.quiesce()

	replayed, err := NewCluster(scenario)
	if err != nil {
		t.Fatal(err)
	}
	if steps, err := Replay(script.trace, replayed); err != nil || steps != script.trace.Len() {
		t.Fatalf("network Replay() = %d/%d, %v", steps, script.trace.Len(), err)
	}

	single, err := NewScenario([]uint64{1}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	disk := newTraceScript(t, single, 88)
	disk.execute(Event{Kind: EventCampaign, Node: 1})
	disk.ready[1] = 1
	disk.execute(Event{Kind: EventCaptureReady, Node: 1, Ref: 1})
	disk.execute(Event{Kind: EventFailPersistDefinite, Node: 1, Ref: 1})
	member, _ := disk.cluster.member(1)
	if member.store.PersistCount() != 0 || member.node.Phase() != raftmodel.PhaseCaptured {
		t.Fatalf("definite failure persisted=%d phase=%s", member.store.PersistCount(), member.node.Phase())
	}
	disk.quiesce()
	state, _ = disk.cluster.MemberState(1)
	if state.Status.RaftState != raft.StateLeader {
		t.Fatalf("definite retry state = %s", state.Status.RaftState)
	}
}

func TestReadOutcomeCannotCrossProcessIncarnation(t *testing.T) {
	scenario, err := NewScenario([]uint64{1}, nil, []ReadRequest{{Reference: 1, Context: []byte("read")}})
	if err != nil {
		t.Fatal(err)
	}
	script := newTraceScript(t, scenario, 91)
	script.execute(Event{Kind: EventCampaign, Node: 1})
	script.quiesce()
	script.execute(Event{Kind: EventRequestRead, Node: 1, Ref: 1})
	script.quiesce()
	script.execute(Event{Kind: EventCrash, Node: 1})
	if err := script.cluster.Execute(Event{Kind: EventServeRead, Node: 1, Ref: 1, Time: script.time}); !errors.Is(err, ErrClusterStopped) {
		t.Fatalf("crashed read response error = %v", err)
	}
	script.execute(Event{Kind: EventRestart, Node: 1, Value: 2})
	if err := script.cluster.Execute(Event{Kind: EventServeRead, Node: 1, Ref: 1, Time: script.time}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("stale-incarnation read response error = %v", err)
	}
}

func TestLeaderObservationPersistsAcrossProcessCuts(t *testing.T) {
	cluster := new(Cluster)
	if err := cluster.observeLeader(7, 1); err != nil {
		t.Fatal(err)
	}
	if err := cluster.observeLeader(9, 2); err != nil {
		t.Fatal(err)
	}
	if err := cluster.observeLeader(7, 1); err != nil {
		t.Fatalf("same leader observation error = %v", err)
	}
	if err := cluster.observeLeader(7, 2); !errors.Is(err, ErrInvariant) {
		t.Fatalf("different historical leader error = %v", err)
	}
}

func BenchmarkClusterInvariantAtAppliedPrefixCeiling(b *testing.B) {
	voters := make([]uint64, MaxMembers)
	for i := range voters {
		voters[i] = uint64(i + 1)
	}
	entries := make([]*pb.Entry, MaxAppliedEntries-1)
	term := uint64(2)
	for i := range entries {
		index := uint64(i + 2)
		entries[i] = &pb.Entry{Term: &term, Index: &index}
	}
	commit := uint64(MaxAppliedEntries)
	cluster := &Cluster{members: make([]clusterMember, MaxMembers)}
	for i, id := range voters {
		store, err := NewMemoryStore(voters)
		if err != nil {
			b.Fatal(err)
		}
		if err := store.Persist(raftmodel.PersistBatch{
			NodeIncarnation: 1, ReadyID: 1, Entries: entries,
			HardState: &pb.HardState{Term: &term, Commit: &commit},
		}); err != nil {
			b.Fatal(err)
		}
		machine, err := NewMemoryMachine(voters)
		if err != nil {
			b.Fatal(err)
		}
		for index := uint64(2); index <= commit; index++ {
			if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{
				Index: index, Term: term, Type: pb.EntryNormal,
			}, nil); err != nil {
				b.Fatal(err)
			}
		}
		cluster.members[i] = clusterMember{id: id, incarnation: 1, store: store, machine: machine}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := cluster.CheckInvariants(); err != nil {
			b.Fatal(err)
		}
	}
}
