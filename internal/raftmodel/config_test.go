package raftmodel

import (
	"reflect"
	"slices"
	"testing"

	raft "go.etcd.io/raft/v3"
)

func TestNewConfigPinsEveryField(t *testing.T) {
	storage := raft.NewMemoryStorage()
	want := raft.Config{
		ID:                          7,
		ElectionTick:                10,
		HeartbeatTick:               1,
		Storage:                     storage,
		Applied:                     23,
		AsyncStorageWrites:          false,
		MaxSizePerMsg:               1 << 20,
		MaxCommittedSizePerReady:    4 << 20,
		MaxUncommittedEntriesSize:   64 << 20,
		MaxInflightMsgs:             64,
		MaxInflightBytes:            8 << 20,
		CheckQuorum:                 true,
		PreVote:                     true,
		ReadOnlyOption:              raft.ReadOnlySafe,
		Logger:                      nil,
		DisableProposalForwarding:   true,
		DisableConfChangeValidation: false,
		StepDownOnRemoval:           true,
		TraceLogger:                 nil,
	}

	if got := NewConfig(7, storage, 23); !reflect.DeepEqual(got, want) {
		t.Fatalf("NewConfig() = %#v, want %#v", got, want)
	}
}

// This field inventory makes an upstream Config addition an explicit audit
// event instead of silently inheriting a new zero-value behavior.
func TestRaftConfigFieldSetIsAudited(t *testing.T) {
	want := []string{
		"ID",
		"ElectionTick",
		"HeartbeatTick",
		"Storage",
		"Applied",
		"AsyncStorageWrites",
		"MaxSizePerMsg",
		"MaxCommittedSizePerReady",
		"MaxUncommittedEntriesSize",
		"MaxInflightMsgs",
		"MaxInflightBytes",
		"CheckQuorum",
		"PreVote",
		"ReadOnlyOption",
		"Logger",
		"DisableProposalForwarding",
		"DisableConfChangeValidation",
		"StepDownOnRemoval",
		"TraceLogger",
	}
	typ := reflect.TypeFor[raft.Config]()
	got := make([]string, typ.NumField())
	for i := range typ.NumField() {
		got[i] = typ.Field(i).Name
	}
	if !slices.Equal(got, want) {
		t.Fatalf("raft.Config fields = %v, want audited set %v", got, want)
	}
}

func TestNewConfigPassesUpstreamValidation(t *testing.T) {
	cfg := NewConfig(1, raft.NewMemoryStorage(), 0)
	if _, err := raft.NewRawNode(&cfg); err != nil {
		t.Fatalf("raft.NewRawNode(NewConfig()) error = %v", err)
	}
}

func TestLimitsRemainFiniteAndConsistent(t *testing.T) {
	if HeartbeatTick <= 0 || ElectionTick <= HeartbeatTick {
		t.Fatalf("ticks heartbeat=%d election=%d", HeartbeatTick, ElectionTick)
	}
	if MaxSizePerMsg <= 0 || MaxCommittedSizePerReady < MaxSizePerMsg {
		t.Fatalf("message=%d committed-ready=%d", MaxSizePerMsg, MaxCommittedSizePerReady)
	}
	if MaxUncommittedEntriesSize < MaxCommittedSizePerReady {
		t.Fatalf("uncommitted=%d committed-ready=%d", MaxUncommittedEntriesSize, MaxCommittedSizePerReady)
	}
	if MaxInflightMsgs <= 0 || MaxInflightBytes < MaxSizePerMsg {
		t.Fatalf("inflight messages=%d bytes=%d message=%d", MaxInflightMsgs, MaxInflightBytes, MaxSizePerMsg)
	}
}
