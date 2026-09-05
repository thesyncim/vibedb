package rafttransport

import (
	"errors"

	pb "go.etcd.io/raft/v3/raftpb"
)

// errRetiredOutboundDestination is an outbound-only refusal. Committed source
// removal can overtake ordinary messages already emitted by Raft. They must
// never be framed or sent, but discarding them must not stop the retained
// sender. Inbound removed-member traffic remains an authorization failure.
var errRetiredOutboundDestination = errors.New("rafttransport: outbound destination was removed")

// retiredOutboundDestination runs only after preflight has established local
// source identity, stable destination enrollment and ordinary message bounds.
// The exact committed removal and current sender role are both mandatory;
// an enrolled learner, unknown destination or unauthorized configuration is
// not an expected stale message.
func retiredOutboundDestination(view *authorityView, message *pb.Message) bool {
	if view == nil || message == nil || view.retiredVersion == 0 ||
		view.grant.SourceMember == 0 || message.GetTo() != view.grant.SourceMember {
		return false
	}
	if _, stillMember := view.roles[message.GetTo()]; stillMember {
		return false
	}
	if _, err := validateAuthorizedConfiguration(view, message.GetEntries()); err != nil {
		return false
	}
	from := view.roles[message.GetFrom()]
	switch message.GetType() {
	case pb.MsgApp, pb.MsgHeartbeat, pb.MsgVote, pb.MsgVoteResp,
		pb.MsgPreVote, pb.MsgPreVoteResp, pb.MsgTimeoutNow:
		return from == MemberVoter
	case pb.MsgAppResp, pb.MsgHeartbeatResp:
		return from == MemberVoter || from == MemberLearner
	default:
		return false
	}
}
