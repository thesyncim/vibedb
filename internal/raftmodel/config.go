// Package raftmodel defines Vibedb's executable Raft integration model.
package raftmodel

import raft "go.etcd.io/raft/v3"

// These limits are deliberately finite per Raft group. They are admission and
// batching limits, not wire-format validation: the command codec and transport
// must independently reject oversized input before it reaches the Raft core.
const (
	HeartbeatTick = 1
	ElectionTick  = 10

	MaxSizePerMsg             = 1 << 20
	MaxCommittedSizePerReady  = 4 << 20
	MaxUncommittedEntriesSize = 64 << 20
	MaxInflightMsgs           = 64
	MaxInflightBytes          = 8 << 20
)

// NewConfig returns the audited baseline configuration for one Raft member.
// ID, storage, and the durable applied index are member state; changing any
// other field is a core-selection change and must update the field-locking
// tests and docs/design/raft-core-selection.md together.
//
// raft.NewRawNode validates ID, storage, and the relationships between these
// settings. Keeping validation at that boundary avoids duplicating upstream's
// reserved-ID and configuration rules here.
func NewConfig(id uint64, storage raft.Storage, applied uint64) raft.Config {
	return raft.Config{
		ID:                          id,
		ElectionTick:                ElectionTick,
		HeartbeatTick:               HeartbeatTick,
		Storage:                     storage,
		Applied:                     applied,
		AsyncStorageWrites:          false,
		MaxSizePerMsg:               MaxSizePerMsg,
		MaxCommittedSizePerReady:    MaxCommittedSizePerReady,
		MaxUncommittedEntriesSize:   MaxUncommittedEntriesSize,
		MaxInflightMsgs:             MaxInflightMsgs,
		MaxInflightBytes:            MaxInflightBytes,
		CheckQuorum:                 true,
		PreVote:                     true,
		ReadOnlyOption:              raft.ReadOnlySafe,
		Logger:                      nil,
		DisableProposalForwarding:   true,
		DisableConfChangeValidation: false,
		StepDownOnRemoval:           true,
		TraceLogger:                 nil,
	}
}
