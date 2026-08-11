package raftsim

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
)

const (
	// MaxMembers bounds one simulated Raft group. Production Multi-Raft scale
	// is expressed as many independently bounded groups, not enormous groups.
	MaxMembers = 9
	// MaxScenarioPayloadBytes bounds all caller-owned proposal payloads retained
	// by one replayable scenario.
	MaxScenarioPayloadBytes = 64 << 20
)

var ErrInvalidScenario = errors.New("raftsim: invalid scenario")

// Proposal names one immutable simulator-local payload. Reference is the
// trace-visible correlation identity and must be unique within Proposals.
type Proposal struct {
	Reference uint64
	Data      []byte
}

// ReadRequest names one immutable ReadIndex correlation context.
type ReadRequest struct {
	Reference uint64
	Context   []byte
}

// Scenario owns the complete non-event input needed for byte-exact replay.
// Its digest is carried by Trace and checked before the first event executes.
type Scenario struct {
	voters    []uint64
	proposals []Proposal
	reads     []ReadRequest
	digest    [32]byte
}

// NewScenario validates, canonicalizes, owns, and hashes one static Phase-0
// group. Voter, proposal, and read order supplied by the caller is irrelevant;
// the retained form is sorted by identity.
func NewScenario(voters []uint64, proposals []Proposal, reads []ReadRequest) (*Scenario, error) {
	if len(voters) == 0 || len(voters) > MaxMembers {
		return nil, fmt.Errorf("%w: voter count %d", ErrInvalidScenario, len(voters))
	}
	ownedVoters := slices.Clone(voters)
	slices.Sort(ownedVoters)
	for i, id := range ownedVoters {
		if id == 0 || raft.IsLocalMsgTarget(id) || (i != 0 && id == ownedVoters[i-1]) {
			return nil, fmt.Errorf("%w: voter identity", ErrInvalidScenario)
		}
	}

	if len(proposals) > MaxAppliedEntries {
		return nil, fmt.Errorf("%w: proposal count %d", ErrInvalidScenario, len(proposals))
	}
	ownedProposals := make([]Proposal, len(proposals))
	totalPayload := 0
	for i, proposal := range proposals {
		if proposal.Reference == 0 || len(proposal.Data) > raftmodel.MaxProposalBytes-modelCommandHeaderBytes ||
			len(proposal.Data) > MaxScenarioPayloadBytes-totalPayload {
			return nil, fmt.Errorf("%w: proposal %d", ErrInvalidScenario, proposal.Reference)
		}
		totalPayload += len(proposal.Data)
		ownedProposals[i] = Proposal{Reference: proposal.Reference, Data: slices.Clone(proposal.Data)}
	}
	slices.SortFunc(ownedProposals, func(left, right Proposal) int {
		return cmp.Compare(left.Reference, right.Reference)
	})
	for i := 1; i < len(ownedProposals); i++ {
		if ownedProposals[i].Reference == ownedProposals[i-1].Reference {
			return nil, fmt.Errorf("%w: duplicate proposal %d", ErrInvalidScenario, ownedProposals[i].Reference)
		}
	}

	if len(reads) > raftmodel.MaxPendingReads {
		return nil, fmt.Errorf("%w: read count %d", ErrInvalidScenario, len(reads))
	}
	ownedReads := make([]ReadRequest, len(reads))
	totalReadBytes := 0
	for i, read := range reads {
		if read.Reference == 0 || len(read.Context) == 0 || len(read.Context) > raftmodel.MaxReadContextBytes ||
			len(read.Context) > raftmodel.MaxPendingReadBytes-totalReadBytes {
			return nil, fmt.Errorf("%w: read %d", ErrInvalidScenario, read.Reference)
		}
		totalReadBytes += len(read.Context)
		ownedReads[i] = ReadRequest{Reference: read.Reference, Context: slices.Clone(read.Context)}
	}
	slices.SortFunc(ownedReads, func(left, right ReadRequest) int {
		return cmp.Compare(left.Reference, right.Reference)
	})
	for i := 1; i < len(ownedReads); i++ {
		if ownedReads[i].Reference == ownedReads[i-1].Reference {
			return nil, fmt.Errorf("%w: duplicate read %d", ErrInvalidScenario, ownedReads[i].Reference)
		}
	}
	contexts := make(map[string]struct{}, len(ownedReads))
	for _, read := range ownedReads {
		key := string(read.Context)
		if _, duplicate := contexts[key]; duplicate {
			return nil, fmt.Errorf("%w: duplicate read context", ErrInvalidScenario)
		}
		contexts[key] = struct{}{}
	}

	scenario := &Scenario{voters: ownedVoters, proposals: ownedProposals, reads: ownedReads}
	scenario.digest = scenarioDigest(scenario)
	return scenario, nil
}

// Digest returns the exact immutable scenario identity used by Trace.
func (s *Scenario) Digest() [32]byte {
	if s == nil {
		return [32]byte{}
	}
	return s.digest
}

func (s *Scenario) proposal(reference uint64) (Proposal, bool) {
	if s == nil {
		return Proposal{}, false
	}
	i, found := slices.BinarySearchFunc(s.proposals, reference, func(item Proposal, target uint64) int {
		return cmp.Compare(item.Reference, target)
	})
	if !found {
		return Proposal{}, false
	}
	return s.proposals[i], true
}

func (s *Scenario) read(reference uint64) (ReadRequest, bool) {
	if s == nil {
		return ReadRequest{}, false
	}
	i, found := slices.BinarySearchFunc(s.reads, reference, func(item ReadRequest, target uint64) int {
		return cmp.Compare(item.Reference, target)
	})
	if !found {
		return ReadRequest{}, false
	}
	return s.reads[i], true
}

func scenarioDigest(s *Scenario) [32]byte {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("VDBSCENARIO/v1\x00"))
	var fixed [16]byte
	binary.LittleEndian.PutUint64(fixed[:8], uint64(len(s.voters)))
	binary.LittleEndian.PutUint64(fixed[8:], uint64(len(s.proposals)))
	_, _ = hasher.Write(fixed[:])
	for _, voter := range s.voters {
		binary.LittleEndian.PutUint64(fixed[:8], voter)
		_, _ = hasher.Write(fixed[:8])
	}
	for _, proposal := range s.proposals {
		binary.LittleEndian.PutUint64(fixed[:8], proposal.Reference)
		binary.LittleEndian.PutUint64(fixed[8:], uint64(len(proposal.Data)))
		_, _ = hasher.Write(fixed[:])
		_, _ = hasher.Write(proposal.Data)
	}
	binary.LittleEndian.PutUint64(fixed[:8], uint64(len(s.reads)))
	_, _ = hasher.Write(fixed[:8])
	for _, read := range s.reads {
		binary.LittleEndian.PutUint64(fixed[:8], read.Reference)
		binary.LittleEndian.PutUint64(fixed[8:], uint64(len(read.Context)))
		_, _ = hasher.Write(fixed[:])
		_, _ = hasher.Write(read.Context)
	}
	var digest [32]byte
	_ = hasher.Sum(digest[:0])
	return digest
}
