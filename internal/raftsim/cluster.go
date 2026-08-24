package raftsim

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	MaxNetworkMessages = 1 << 16
	MaxNetworkBytes    = 128 << 20
)

var (
	ErrInvalidEvent   = errors.New("raftsim: event is not enabled")
	ErrClusterStopped = errors.New("raftsim: member is stopped")
	ErrNetworkFull    = errors.New("raftsim: network message limit reached")
	ErrInvariant      = errors.New("raftsim: cluster invariant violated")
)

type clusterMember struct {
	id          uint64
	incarnation uint64
	node        *raftmodel.Node
	store       *MemoryStore
	machine     *MemoryMachine
}

type networkMessage struct {
	id      uint64
	message *pb.Message
	bytes   int
	active  bool
}

type proposalResponse struct {
	node      uint64
	reference uint64
	index     uint64
	digest    [32]byte
}

type readResult struct {
	node      uint64
	reference uint64
	outcome   raftmodel.ReadOutcome
	served    bool
}

type issuedReference struct {
	reference uint64
}

type termLeader struct{ term, id uint64 }

// MemberState is a detached observation of one simulated member.
type MemberState struct {
	ID            uint64
	Incarnation   uint64
	Up            bool
	Phase         raftmodel.Phase
	ReadyID       uint64
	Applied       uint64
	DurableCommit uint64
	Status        raft.BasicStatus
}

// MessageInfo is a detached observation of one retained network message.
type MessageInfo struct {
	ID     uint64
	From   uint64
	To     uint64
	Type   pb.MessageType
	Active bool
}

// Cluster is a serialized, deterministic executor around actual RawNode
// instances. It models logical stable storage, process crashes, message
// delivery, and partitions; it deliberately does not model a production WAL,
// snapshots, transport framing, or wall-clock election scheduling.
type Cluster struct {
	scenario *Scenario
	members  []clusterMember

	messages      []networkMessage
	nextMessageID uint64
	networkBytes  int
	partitioned   [MaxMembers][MaxMembers]bool

	proposed  []issuedReference
	readIssue []issuedReference
	responses []proposalResponse
	reads     []readResult
	leaders   []termLeader
	now       uint64
}

// NewCluster creates an independently crash-persistent store and state
// machine for every scenario voter, then recovers one RawNode per member.
func NewCluster(scenario *Scenario) (*Cluster, error) {
	if scenario == nil {
		return nil, ErrInvalidScenario
	}
	cluster := &Cluster{scenario: scenario, members: make([]clusterMember, len(scenario.voters))}
	for i, id := range scenario.voters {
		store, err := NewMemoryStore(scenario.voters)
		if err != nil {
			return nil, err
		}
		machine, err := NewMemoryMachine(scenario.voters)
		if err != nil {
			return nil, err
		}
		node, err := raftmodel.NewNode(id, 1, store, machine)
		if err != nil {
			return nil, fmt.Errorf("raftsim: construct member %d: %w", id, err)
		}
		cluster.members[i] = clusterMember{
			id: id, incarnation: 1, node: node, store: store, machine: machine,
		}
	}
	if err := cluster.checkInvariants(); err != nil {
		return nil, err
	}
	return cluster, nil
}

func (c *Cluster) ReplayIdentity() ReplayIdentity {
	if c == nil || c.scenario == nil {
		return ReplayIdentity{}
	}
	return ReplayIdentity{SimulatorVersion: SimulatorVersion, ScenarioDigest: c.scenario.digest}
}

// Execute applies one exact trace decision and checks safety invariants before
// accepting the step. Expected injected persistence failures are successful
// simulator events; every other error stops replay at this event.
func (c *Cluster) Execute(event Event) error {
	if c == nil || !event.Valid() || event.Time < c.now {
		return ErrInvalidEvent
	}
	c.now = event.Time
	member, err := c.member(event.Node)
	if err != nil {
		return c.eventError(event, err)
	}

	switch event.Kind {
	case EventCampaign:
		err = c.withNode(member, func(node *raftmodel.Node) error { return node.Campaign() })
	case EventLeaderTick:
		err = c.withNode(member, func(node *raftmodel.Node) error {
			status := node.Status()
			if status.RaftState != raft.StateLeader || status.Lead != member.id {
				return fmt.Errorf("%w: member is not leader", ErrInvalidEvent)
			}
			return node.Tick()
		})
	case EventPropose:
		err = c.propose(member, event.Ref)
	case EventRequestRead:
		err = c.requestRead(member, event.Ref)
	case EventCaptureReady:
		err = c.captureReady(member, event.Ref)
	case EventPersistReady:
		err = c.persistReady(member, event.Ref)
	case EventFailPersistDefinite:
		err = c.failPersist(member, event.Ref, PersistFailBefore)
	case EventFailPersistAmbiguous:
		err = c.failPersist(member, event.Ref, PersistThenError)
	case EventSendMessage:
		err = c.sendMessage(member, event.Ref)
	case EventFinishMessages:
		err = c.readyOperation(member, event.Ref, func(node *raftmodel.Node) error { return node.FinishMessages() })
	case EventInstallSnapshot:
		err = c.readyOperation(member, event.Ref, func(node *raftmodel.Node) error { return node.InstallSnapshot() })
	case EventApplyEntry:
		err = c.applyEntry(member, event.Ref, event.Value)
	case EventRecordReadState:
		err = c.recordReadState(member, event.Ref)
	case EventFinishReadStates:
		err = c.finishReadStates(member, event.Ref)
	case EventServeRead:
		err = c.serveRead(member, event.Ref)
	case EventRespondProposal:
		err = c.respondProposal(member, event.Ref)
	case EventAdvanceReady:
		err = c.readyOperation(member, event.Ref, func(node *raftmodel.Node) error { return node.AdvanceReady() })
	case EventDeliverMessage:
		err = c.deliverMessage(member, event.Peer, event.Ref)
	case EventDropMessage:
		err = c.dropMessage(member, event.Peer, event.Ref)
	case EventDuplicateMessage:
		err = c.duplicateMessage(member, event.Peer, event.Ref, event.Value)
	case EventCrash:
		err = c.crash(member)
	case EventRestart:
		err = c.restart(member, event.Value)
	case EventPartitionLink:
		err = c.setPartition(member, event.Peer, true)
	case EventHealLink:
		err = c.setPartition(member, event.Peer, false)
	default:
		err = ErrInvalidEvent
	}
	if err != nil {
		return c.eventError(event, err)
	}
	if err := c.checkInvariants(); err != nil {
		return c.eventError(event, err)
	}
	return nil
}

func (c *Cluster) eventError(event Event, err error) error {
	return fmt.Errorf("raftsim: step %d %s member=%d ref=%d: %w", event.Step, event.Kind, event.Node, event.Ref, err)
}

func (c *Cluster) member(id uint64) (*clusterMember, error) {
	i, found := slices.BinarySearchFunc(c.members, id, func(member clusterMember, target uint64) int {
		switch {
		case member.id < target:
			return -1
		case member.id > target:
			return 1
		default:
			return 0
		}
	})
	if !found {
		return nil, fmt.Errorf("%w: unknown member %d", ErrInvalidEvent, id)
	}
	return &c.members[i], nil
}

func (c *Cluster) memberOrdinal(id uint64) (int, error) {
	for i := range c.members {
		if c.members[i].id == id {
			return i, nil
		}
	}
	return 0, fmt.Errorf("%w: unknown member %d", ErrInvalidEvent, id)
}

func (c *Cluster) withNode(member *clusterMember, operation func(*raftmodel.Node) error) error {
	if member.node == nil {
		return ErrClusterStopped
	}
	return operation(member.node)
}

func (c *Cluster) propose(member *clusterMember, reference uint64) error {
	proposal, ok := c.scenario.proposal(reference)
	if !ok || hasIssued(c.proposed, reference) {
		return ErrInvalidEvent
	}
	if member.node == nil {
		return ErrClusterStopped
	}
	payload := appendModelCommand(make([]byte, 0, modelCommandHeaderBytes+len(proposal.Data)), reference, proposal.Data)
	if err := member.node.Propose(payload); err != nil {
		return err
	}
	c.proposed = append(c.proposed, issuedReference{reference: reference})
	return nil
}

func (c *Cluster) requestRead(member *clusterMember, reference uint64) error {
	request, ok := c.scenario.read(reference)
	if !ok || hasIssued(c.readIssue, reference) {
		return ErrInvalidEvent
	}
	if member.node == nil {
		return ErrClusterStopped
	}
	if err := member.node.ReadIndex(request.Context); err != nil {
		return err
	}
	c.readIssue = append(c.readIssue, issuedReference{reference: reference})
	return nil
}

func hasIssued(items []issuedReference, reference uint64) bool {
	for _, item := range items {
		if item.reference == reference {
			return true
		}
	}
	return false
}

func (c *Cluster) captureReady(member *clusterMember, reference uint64) error {
	if member.node == nil {
		return ErrClusterStopped
	}
	captured, err := member.node.CaptureReady()
	if err != nil {
		return err
	}
	if !captured || member.node.ReadyID() != reference {
		return fmt.Errorf("%w: captured=%v ReadyID=%d", ErrInvalidEvent, captured, member.node.ReadyID())
	}
	return nil
}

func (c *Cluster) persistReady(member *clusterMember, reference uint64) error {
	return c.readyOperation(member, reference, func(node *raftmodel.Node) error { return node.PersistReady() })
}

func (c *Cluster) failPersist(member *clusterMember, reference uint64, fault PersistFault) error {
	if member.node == nil || member.node.ReadyID() != reference {
		return ErrInvalidEvent
	}
	member.store.SetNextPersistFault(fault)
	err := member.node.PersistReady()
	if !errors.Is(err, ErrPersistInjected) || member.node.Phase() != raftmodel.PhaseCaptured {
		return fmt.Errorf("%w: injected persist result %v phase %s", ErrInvalidEvent, err, member.node.Phase())
	}
	return nil
}

func (c *Cluster) readyOperation(member *clusterMember, reference uint64, operation func(*raftmodel.Node) error) error {
	if member.node == nil {
		return ErrClusterStopped
	}
	if member.node.ReadyID() != reference {
		return fmt.Errorf("%w: current ReadyID=%d", ErrInvalidEvent, member.node.ReadyID())
	}
	return operation(member.node)
}

func (c *Cluster) sendMessage(member *clusterMember, reference uint64) error {
	if member.node == nil || reference == 0 || reference != c.nextMessageID+1 || len(c.messages) >= MaxNetworkMessages {
		return ErrNetworkFull
	}
	var retained *pb.Message
	retainedBytes := 0
	sent, err := member.node.SendNextMessage(func(message *pb.Message) error {
		if message.GetFrom() != member.id || message.GetTo() == 0 || message.GetTo() == member.id {
			return ErrInvalidEvent
		}
		retainedBytes = proto.Size(message)
		if retainedBytes <= 0 || retainedBytes > MaxNetworkBytes-c.networkBytes {
			return ErrNetworkFull
		}
		retained = proto.Clone(message).(*pb.Message)
		return nil
	})
	if err != nil {
		return err
	}
	if !sent || retained == nil {
		return ErrInvalidEvent
	}
	c.nextMessageID = reference
	c.networkBytes += retainedBytes
	c.messages = append(c.messages, networkMessage{
		id: reference, message: retained, bytes: retainedBytes, active: true,
	})
	return nil
}

func (c *Cluster) applyEntry(member *clusterMember, readyID, expectedIndex uint64) error {
	if member.node == nil || member.node.ReadyID() != readyID {
		return ErrInvalidEvent
	}
	result, err := member.node.ApplyNextBatch(nil)
	if err != nil {
		return err
	}
	applied := result.Applied != 0
	publication := member.node.Published()
	if result.Normal.Len() != 0 {
		publication = result.Normal.FinalPublication()
		if err := member.node.SettleAppliedNormalBatch(result.Normal); err != nil {
			return err
		}
	}
	if expectedIndex == 0 {
		if applied {
			return fmt.Errorf("%w: applied unexpected index %d", ErrInvalidEvent, publication.Applied)
		}
		return nil
	}
	if !applied || publication.Applied != expectedIndex {
		return fmt.Errorf("%w: applied=%v index=%d want=%d", ErrInvalidEvent, applied, publication.Applied, expectedIndex)
	}
	return nil
}

func (c *Cluster) recordReadState(member *clusterMember, readyID uint64) error {
	if member.node == nil || member.node.ReadyID() != readyID {
		return ErrInvalidEvent
	}
	recorded, err := member.node.RecordNextReadState()
	if err != nil {
		return err
	}
	if !recorded {
		return ErrInvalidEvent
	}
	return nil
}

func (c *Cluster) finishReadStates(member *clusterMember, readyID uint64) error {
	if member.node == nil || member.node.ReadyID() != readyID {
		return ErrInvalidEvent
	}
	outcomes, err := member.node.FinishReadStates()
	if err != nil {
		return err
	}
	for _, outcome := range outcomes {
		reference, ok := c.readReference(outcome.Barrier.Context)
		if !ok || len(c.reads) >= raftmodel.MaxPendingReads || c.hasReadResult(member.id, reference) {
			return ErrInvariant
		}
		outcome.Barrier.Context = slices.Clone(outcome.Barrier.Context)
		c.reads = append(c.reads, readResult{node: member.id, reference: reference, outcome: outcome})
	}
	return nil
}

func (c *Cluster) readReference(context []byte) (uint64, bool) {
	for _, read := range c.scenario.reads {
		if slices.Equal(context, read.Context) {
			return read.Reference, true
		}
	}
	return 0, false
}

func (c *Cluster) hasReadResult(node, reference uint64) bool {
	for _, result := range c.reads {
		if result.node == node && result.reference == reference {
			return true
		}
	}
	return false
}

func (c *Cluster) serveRead(member *clusterMember, reference uint64) error {
	if member.node == nil {
		return ErrClusterStopped
	}
	for i := range c.reads {
		result := &c.reads[i]
		if result.node != member.id || result.reference != reference {
			continue
		}
		if result.served || result.outcome.Err != nil ||
			result.outcome.Barrier.Incarnation != member.incarnation ||
			member.machine.Applied() < result.outcome.Barrier.Index {
			return ErrInvalidEvent
		}
		result.served = true
		return nil
	}
	return ErrInvalidEvent
}

func (c *Cluster) respondProposal(member *clusterMember, reference uint64) error {
	if member.node == nil {
		return ErrClusterStopped
	}
	if len(c.responses) >= MaxAppliedEntries {
		return ErrMachineFull
	}
	for _, response := range c.responses {
		if response.reference == reference {
			return ErrInvalidEvent
		}
	}
	// This event models an idempotent completion lookup, not survival of the
	// original RPC. Any live replica that has applied the exact replicated
	// reference may return it after retry, including after process restart.
	if !hasIssued(c.proposed, reference) {
		return ErrInvalidEvent
	}
	index, completed := member.machine.Completed(reference)
	if !completed {
		return ErrInvalidEvent
	}
	record, ok := member.machine.Entry(index)
	if !ok {
		return ErrInvariant
	}
	c.responses = append(c.responses, proposalResponse{
		node: member.id, reference: reference, index: index, digest: record.Digest,
	})
	return nil
}

func (c *Cluster) message(reference uint64) (*networkMessage, error) {
	for i := range c.messages {
		if c.messages[i].id == reference {
			return &c.messages[i], nil
		}
	}
	return nil, ErrInvalidEvent
}

func (c *Cluster) deliverMessage(member *clusterMember, peer, reference uint64) error {
	envelope, err := c.message(reference)
	if err != nil || !envelope.active || envelope.message.GetFrom() != peer || envelope.message.GetTo() != member.id {
		return ErrInvalidEvent
	}
	fromOrdinal, err := c.memberOrdinal(peer)
	if err != nil {
		return err
	}
	toOrdinal, _ := c.memberOrdinal(member.id)
	if c.partitioned[fromOrdinal][toOrdinal] {
		return ErrInvalidEvent
	}
	if member.node == nil {
		return ErrClusterStopped
	}
	if err := member.node.Step(envelope.message); err != nil {
		return err
	}
	envelope.active = false
	c.networkBytes -= envelope.bytes
	envelope.message = nil
	envelope.bytes = 0
	return nil
}

func (c *Cluster) dropMessage(member *clusterMember, peer, reference uint64) error {
	envelope, err := c.message(reference)
	if err != nil || !envelope.active || envelope.message.GetFrom() != peer || envelope.message.GetTo() != member.id {
		return ErrInvalidEvent
	}
	envelope.active = false
	c.networkBytes -= envelope.bytes
	envelope.message = nil
	envelope.bytes = 0
	return nil
}

func (c *Cluster) duplicateMessage(member *clusterMember, peer, reference, duplicateID uint64) error {
	envelope, err := c.message(reference)
	if err != nil || !envelope.active || envelope.message.GetFrom() != peer || envelope.message.GetTo() != member.id ||
		duplicateID == 0 || duplicateID != c.nextMessageID+1 || len(c.messages) >= MaxNetworkMessages ||
		envelope.bytes > MaxNetworkBytes-c.networkBytes {
		return ErrInvalidEvent
	}
	c.nextMessageID = duplicateID
	c.networkBytes += envelope.bytes
	c.messages = append(c.messages, networkMessage{
		id: duplicateID, message: proto.Clone(envelope.message).(*pb.Message), bytes: envelope.bytes, active: true,
	})
	return nil
}

func (c *Cluster) crash(member *clusterMember) error {
	if member.node == nil {
		return ErrClusterStopped
	}
	member.node = nil
	return nil
}

func (c *Cluster) restart(member *clusterMember, incarnation uint64) error {
	if member.node != nil || member.incarnation == math.MaxUint64 || incarnation != member.incarnation+1 {
		return ErrInvalidEvent
	}
	node, err := raftmodel.NewNode(member.id, incarnation, member.store, member.machine)
	if err != nil {
		return err
	}
	member.incarnation = incarnation
	member.node = node
	return nil
}

func (c *Cluster) setPartition(member *clusterMember, peer uint64, value bool) error {
	left, err := c.memberOrdinal(member.id)
	if err != nil {
		return err
	}
	right, err := c.memberOrdinal(peer)
	if err != nil || left == right {
		return ErrInvalidEvent
	}
	if c.partitioned[left][right] == value {
		return ErrInvalidEvent
	}
	c.partitioned[left][right] = value
	c.partitioned[right][left] = value
	return nil
}

// MemberState returns a detached member observation.
func (c *Cluster) MemberState(id uint64) (MemberState, bool) {
	if c == nil {
		return MemberState{}, false
	}
	member, err := c.member(id)
	if err != nil {
		return MemberState{}, false
	}
	hard, _, err := member.store.InitialState()
	if err != nil {
		return MemberState{}, false
	}
	state := MemberState{
		ID: member.id, Incarnation: member.incarnation, Up: member.node != nil,
		Applied: member.machine.Applied(), DurableCommit: hard.GetCommit(),
	}
	if member.node != nil {
		state.Phase = member.node.Phase()
		state.ReadyID = member.node.ReadyID()
		state.Status = member.node.Status()
	}
	return state, true
}

// ReadyProgress returns allocation-free counts for the member's captured
// Ready, allowing a trace producer to schedule every micro-step explicitly.
func (c *Cluster) ReadyProgress(id uint64) (raftmodel.ReadyProgress, bool) {
	if c == nil {
		return raftmodel.ReadyProgress{}, false
	}
	member, err := c.member(id)
	if err != nil || member.node == nil {
		return raftmodel.ReadyProgress{}, false
	}
	return member.node.CurrentReady()
}

// ActiveMessages returns active envelope identities in creation order.
func (c *Cluster) ActiveMessages() []MessageInfo {
	if c == nil {
		return nil
	}
	result := make([]MessageInfo, 0, len(c.messages))
	for _, envelope := range c.messages {
		if !envelope.active {
			continue
		}
		result = append(result, MessageInfo{
			ID: envelope.id, From: envelope.message.GetFrom(), To: envelope.message.GetTo(),
			Type: envelope.message.GetType(), Active: true,
		})
	}
	return result
}

// ProposalCompleted reports whether one member durably published a proposal.
func (c *Cluster) ProposalCompleted(memberID, reference uint64) (uint64, bool) {
	if c == nil {
		return 0, false
	}
	member, err := c.member(memberID)
	if err != nil {
		return 0, false
	}
	return member.machine.Completed(reference)
}

// ProposalResponded reports whether an explicit response event occurred after
// publication.
func (c *Cluster) ProposalResponded(reference uint64) bool {
	if c == nil {
		return false
	}
	for _, response := range c.responses {
		if response.reference == reference {
			return true
		}
	}
	return false
}

// CheckInvariants reruns all cluster safety assertions.
func (c *Cluster) CheckInvariants() error { return c.checkInvariants() }

func (c *Cluster) checkInvariants() error {
	if c == nil {
		return ErrInvariant
	}
	for i := range c.members {
		member := &c.members[i]
		hard, _, err := member.store.InitialState()
		if err != nil || member.machine.Applied() > hard.GetCommit() {
			return fmt.Errorf("%w: member %d applied=%d durable-commit=%d", ErrInvariant, member.id, member.machine.Applied(), hard.GetCommit())
		}
		if member.node == nil {
			continue
		}
		status := member.node.Status()
		if status.RaftState == raft.StateLeader {
			if err := c.observeLeader(status.GetTerm(), member.id); err != nil {
				return err
			}
		}
	}

	for left := 0; left < len(c.members); left++ {
		for right := left + 1; right < len(c.members); right++ {
			leftMachine, rightMachine := c.members[left].machine, c.members[right].machine
			common := min(leftMachine.Applied(), rightMachine.Applied())
			// AppliedEntry.Digest chains the entire exact prefix, so the common
			// cut is sufficient evidence and avoids rewalking every older entry
			// after every simulator micro-step.
			if common > 1 {
				leftEntry, leftOK := leftMachine.Entry(common)
				rightEntry, rightOK := rightMachine.Entry(common)
				if !leftOK || !rightOK || leftEntry.Digest != rightEntry.Digest {
					return fmt.Errorf("%w: members %d/%d differ at applied index %d", ErrInvariant, c.members[left].id, c.members[right].id, common)
				}
			}
			if leftMachine.Applied() == rightMachine.Applied() {
				leftPub, rightPub := leftMachine.Published(), rightMachine.Published()
				if leftPub.DataChainDigest != rightPub.DataChainDigest ||
					leftPub.ReplicaSetVersion != rightPub.ReplicaSetVersion ||
					!proto.Equal(leftPub.ConfState, rightPub.ConfState) {
					return fmt.Errorf("%w: equal applied cuts differ for members %d/%d", ErrInvariant, c.members[left].id, c.members[right].id)
				}
			}
		}
	}

	for _, response := range c.responses {
		member, err := c.member(response.node)
		if err != nil {
			return ErrInvariant
		}
		index, completed := member.machine.Completed(response.reference)
		record, ok := member.machine.Entry(index)
		if !completed || !ok || index != response.index || record.Digest != response.digest {
			return fmt.Errorf("%w: response %d lacks durable publication", ErrInvariant, response.reference)
		}
	}
	retainedNetworkBytes := 0
	for _, envelope := range c.messages {
		if envelope.active {
			if envelope.message == nil || envelope.bytes <= 0 {
				return fmt.Errorf("%w: active network envelope is empty", ErrInvariant)
			}
			retainedNetworkBytes += envelope.bytes
		} else if envelope.message != nil || envelope.bytes != 0 {
			return fmt.Errorf("%w: inactive network envelope retains payload", ErrInvariant)
		}
	}
	if retainedNetworkBytes != c.networkBytes || retainedNetworkBytes > MaxNetworkBytes {
		return fmt.Errorf("%w: network bytes %d/%d", ErrInvariant, retainedNetworkBytes, c.networkBytes)
	}
	for _, result := range c.reads {
		if !result.served {
			continue
		}
		member, err := c.member(result.node)
		if err != nil || result.outcome.Err != nil || member.machine.Applied() < result.outcome.Barrier.Index {
			return fmt.Errorf("%w: read %d served before publication", ErrInvariant, result.reference)
		}
	}
	return nil
}

func (c *Cluster) observeLeader(term, id uint64) error {
	index, found := slices.BinarySearchFunc(c.leaders, term, func(item termLeader, target uint64) int {
		switch {
		case item.term < target:
			return -1
		case item.term > target:
			return 1
		default:
			return 0
		}
	})
	if found {
		if c.leaders[index].id != id {
			return fmt.Errorf("%w: leaders %d and %d observed in term %d", ErrInvariant, c.leaders[index].id, id, term)
		}
		return nil
	}
	if term == 0 || len(c.leaders) >= MaxTraceEvents {
		return fmt.Errorf("%w: leader observation bound", ErrInvariant)
	}
	c.leaders = append(c.leaders, termLeader{})
	copy(c.leaders[index+1:], c.leaders[index:])
	c.leaders[index] = termLeader{term: term, id: id}
	return nil
}
