package rafttransport

import (
	"crypto/sha256"

	"github.com/thesyncim/vibedb/internal/raftmember"
	pb "go.etcd.io/raft/v3/raftpb"
)

// prospectiveConfigurationSequence is request-local validation evidence for one
// grant's three adjacent changes. It is never installed into committed runtime
// authority and cannot certify retained WAL or grant historical sender roles.
type prospectiveConfigurationSequence struct {
	count   int
	entries [3]prospectiveConfigurationEntry
}

type prospectiveConfigurationEntry struct {
	index, term uint64
	kind        pb.EntryType
	digest      [sha256.Size]byte
}

func prospectiveGrantedAppend(current *authorityView, version uint64, message *pb.Message) (*authorityView, bool) {
	if current == nil || message == nil || message.GetType() != pb.MsgApp ||
		version <= current.version || message.GetCommit() < version ||
		!current.grant.Valid() || !grantFitsCommittedCut(current, current.grant) {
		return nil, false
	}
	if _, err := raftmember.MeasureOrdinaryMessage(message); err != nil {
		return nil, false
	}
	roles := make(map[uint64]MemberRole, len(current.roles)+1)
	for member, role := range current.roles {
		roles[member] = role
	}
	projected := &authorityView{version: current.version, roles: roles, grant: current.grant,
		previous: current, replay: current.replay}
	sequence := new(prospectiveConfigurationSequence)
	for _, entry := range message.GetEntries() {
		if entry.GetType() == pb.EntryNormal || entry.GetIndex() <= current.version {
			continue // Historical entries still require the ordinary replay checks.
		}
		if sequence.count == len(sequence.entries) || entry.GetIndex() > version {
			return nil, false
		}
		change, member, digest, err := openSingleConfChange(entry)
		if err != nil || !authorizedConfChange(projected, change, member, digest) {
			return nil, false
		}
		switch change {
		case pb.ConfChangeAddLearnerNode:
			if roles[member] != MemberEnrolled {
				return nil, false
			}
			roles[member] = MemberLearner
		case pb.ConfChangeAddNode:
			if roles[member] != MemberLearner {
				return nil, false
			}
			roles[member] = MemberVoter
		case pb.ConfChangeRemoveNode:
			if roles[member] != MemberVoter {
				return nil, false
			}
			delete(roles, member)
		default:
			return nil, false
		}
		projected.version = entry.GetIndex()
		sequence.entries[sequence.count] = prospectiveConfigurationEntry{
			index: entry.GetIndex(), term: entry.GetTerm(), kind: entry.GetType(), digest: sha256.Sum256(entry.GetData()),
		}
		sequence.count++
	}
	if sequence.count == 0 || projected.version != version {
		return nil, false
	}
	projected.prospective = sequence
	return projected, true
}

func authorizedProspectiveConfiguration(view *authorityView, entry *pb.Entry) bool {
	if view == nil || view.prospective == nil || entry == nil {
		return false
	}
	for index := range view.prospective.count {
		expected := view.prospective.entries[index]
		if expected.index == entry.GetIndex() {
			return expected.term == entry.GetTerm() && expected.kind == entry.GetType() &&
				expected.digest == sha256.Sum256(entry.GetData())
		}
	}
	return false
}
