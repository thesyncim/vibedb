// Package raftsim contains the deterministic scheduling and trace substrate
// used to exercise the Raft integration boundary under replayable faults.
//
// The package does not implement consensus. Production protocol transitions
// come from the pinned Raft core through internal/raftmodel. This package owns
// only caller-controlled event order, logical time, bounded queues, and a
// canonical trace that can be replayed without consulting wall time.
//
// The pinned core samples follower and candidate election timeout jitter from
// crypto/rand. A deterministic executor therefore maps EventCampaign to an
// explicit RawNode.Campaign call and never drives follower or candidate Tick.
// EventLeaderTick is admitted only while BasicStatus identifies the local node
// as leader; leader heartbeat/check-quorum uses the fixed configured election
// threshold. It does not claim production ticker liveness.
package raftsim
