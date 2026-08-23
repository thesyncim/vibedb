package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestSessionHashCollisionIsCorruptionNotTupleConflict(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	command := encodeCommand(t, commandValue(fixture.binding, 1))
	view, _ := replication.OpenCommand(command)
	digest := SessionKey(view.Tenant, view.ClientID)
	key := SessionStorageKey(digest)
	foreign, err := AppendSessionRecord(nil, SessionRecord{
		Tenant: []byte("foreign"), ClientID: id128(91), ClientEpoch: 1,
		RetryHome: view.RetryHome, HighSequence: 1, Status: SessionActive,
		RetryWindow: fixture.machine.options.RetryWindow, PhysicalSlotCount: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(key[:], foreign)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), command); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("collision apply error = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); !errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("post-collision error = %v", err)
	}
}

func TestSessionIdentityCapacityRefusesWithoutPoisoning(t *testing.T) {
	fixture := newMachineFixture(t)
	fixture.machine.options.MaxSessions = 1
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	secondValue := commandValue(fixture.binding, 1)
	secondValue.ClientID = id128(99)
	secondValue.Fingerprint = replication.Digest{99}
	second := encodeCommand(t, sessionOpenFor(secondValue))
	if err := fixture.machine.AdmitCommand(second); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("cap admission error = %v", err)
	}
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), second)
	if err != nil || publication.Applied != 3 {
		t.Fatalf("committed refusal = %+v,%v", publication, err)
	}
	if fixture.machine.state.SessionCount != 1 || fixture.machine.state.SessionSlotCount != 1 {
		t.Fatalf("cap failure publication=%+v state=%+v", fixture.machine.Published(), fixture.machine.state)
	}
	if _, err := fixture.machine.LookupCompletion(second); !errors.Is(err, ErrCompletionNotFound) {
		t.Fatalf("refused lookup error = %v", err)
	}
}

func TestPointPathsRejectSessionRetirementMismatch(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Machine, []byte) error
	}{
		{name: "lookup", run: func(machine *Machine, command []byte) error {
			_, err := machine.LookupCompletion(command)
			return err
		}},
		{name: "admission", run: func(machine *Machine, command []byte) error {
			return machine.AdmitCommand(command)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMachineFixture(t)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			command := encodeCommand(t, commandValue(fixture.binding, 1))
			view, err := replication.OpenCommand(command)
			if err != nil {
				t.Fatal(err)
			}
			applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
			if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
				t.Fatal(err)
			}
			digest := SessionKey(view.Tenant, view.ClientID)
			rewriteSessionSlot(t, fixture, digest, 1, func(raw []byte) {
				binary.LittleEndian.PutUint32(raw[140:144], ResultSessionRetired)
			})
			if err := test.run(fixture.machine, command); !errors.Is(err, ErrSessionCorrupt) {
				t.Fatalf("point-path error = %v, want ErrSessionCorrupt", err)
			}
			if _, err := fixture.machine.SessionCapacityState(); !errors.Is(err, ErrApplyPoisoned) {
				t.Fatalf("post-corruption health = %v, want ErrApplyPoisoned", err)
			}
		})
	}
}

func TestFutureReplicaSetVersionStaleCompletionReopens(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	command := commandValue(fixture.binding, 1)
	command.ReplicaSetVersion = 100
	encoded := encodeCommand(t, command)
	applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), encoded); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("reopen future-version stale completion: %v", err)
	}
	lookup, err := reopened.LookupCompletion(encoded)
	if err != nil {
		t.Fatal(err)
	}
	completion, _ := replication.OpenCompletion(lookup.Bytes)
	if completion.ResultCode != ResultStaleFence || completion.ReplicaSetVersion != 100 {
		t.Fatalf("stale completion = %+v", completion)
	}
}

func TestOpenRequiresExactCollectionAndTransactionBounds(t *testing.T) {
	fixture := newMachineFixture(t)
	base := fixture.machine.options

	t.Run("collection limit mismatch", func(t *testing.T) {
		user := fixture.user
		user.Limits.MaxDistinctMutations--
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: user}, fixture.log, base)
		if !errors.Is(err, ErrInvalidCollection) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("transaction documents", func(t *testing.T) {
		options := base
		options.TxnLimits.MaxDocuments = fixture.user.Limits.MaxDistinctMutations + 2
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, options)
		if !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("transaction bytes", func(t *testing.T) {
		maxSystem := len(stateKey) + MaxStateEnvelopeBytes +
			33 + MaxSessionRecordBytes + 35 + MaxSessionSlotRecordBytes
		options := base
		options.TxnLimits.MaxBytes = int64(min(fixture.user.Limits.MaxBatchBytes, replication.MaxCommandBytes)+maxSystem) - 1
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, options)
		if !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("Open error = %v", err)
		}
	})
}

func TestBootstrapExactnessAndBounds(t *testing.T) {
	t.Run("unknown snapshot field", func(t *testing.T) {
		fixture := newMachineFixture(t)
		bootstrap := proto.Clone(fixture.bootstrap).(*pb.Snapshot)
		bootstrap.ProtoReflect().SetUnknown([]byte{0x20, 1})
		_, err := Open(fixture.binding, bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStaticSnapshotOnly) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("oversize data", func(t *testing.T) {
		bootstrap := testBootstrap()
		bootstrap.Data = make([]byte, MaxStaticBootstrapBytes+1)
		if _, _, err := validateBootstrap(bootstrap); !errors.Is(err, ErrStaticSnapshotOnly) {
			t.Fatalf("validateBootstrap error = %v", err)
		}
	})
	t.Run("joint configuration", func(t *testing.T) {
		bootstrap := testBootstrap()
		bootstrap.Metadata.ConfState = &pb.ConfState{
			Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1},
		}
		if _, _, err := validateBootstrap(bootstrap); !errors.Is(err, ErrStaticSnapshotOnly) {
			t.Fatalf("validateBootstrap error = %v", err)
		}
	})
	t.Run("static member ceiling", func(t *testing.T) {
		bootstrap := testBootstrap()
		bootstrap.Metadata.ConfState.Voters = make([]uint64, MaxStaticBootstrapMembers)
		for i := range bootstrap.Metadata.ConfState.Voters {
			bootstrap.Metadata.ConfState.Voters[i] = uint64(i + 1)
		}
		if _, _, err := validateBootstrap(bootstrap); err != nil {
			t.Fatalf("exact member ceiling: %v", err)
		}
		bootstrap.Metadata.ConfState.Voters = append(
			bootstrap.Metadata.ConfState.Voters, MaxStaticBootstrapMembers+1,
		)
		if _, _, err := validateBootstrap(bootstrap); !errors.Is(err, ErrStaticSnapshotOnly) {
			t.Fatalf("one-over member ceiling error = %v", err)
		}
	})
	t.Run("different ConfState install", func(t *testing.T) {
		fixture := newMachineFixture(t)
		bootstrap := proto.Clone(fixture.bootstrap).(*pb.Snapshot)
		bootstrap.Metadata.ConfState = &pb.ConfState{Voters: []uint64{2}}
		if _, err := fixture.machine.InstallSnapshot(bootstrap); !errors.Is(err, ErrStaticSnapshotOnly) {
			t.Fatalf("InstallSnapshot error = %v", err)
		}
	})
	t.Run("different static snapshot on reopen", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		bootstrap := proto.Clone(fixture.bootstrap).(*pb.Snapshot)
		bootstrap.Data = bytes.Clone(bootstrap.Data)
		bootstrap.Data[0] ^= 1
		_, err := Open(fixture.binding, bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
}

func TestOpenDetectsStateLogicalCountAndSessionCorruption(t *testing.T) {
	t.Run("static last-entry digest", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		envelope, err := AppendState(nil, fixture.machine.state)
		if err != nil {
			t.Fatal(err)
		}
		envelope[184] ^= 1
		sealRecord(envelope, stateChecksumDomain)
		if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
			return batch.Put(stateKey, envelope)
		}); err != nil {
			t.Fatal(err)
		}
		_, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("static ConfState", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		state := cloneState(fixture.machine.state)
		state.ConfState = &pb.ConfState{Voters: []uint64{2}}
		putStateDocument(t, fixture.system.Collection, state, nil, nil)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("state bytes", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
			return batch.Put(stateKey, []byte(`"00"`))
		}); err != nil {
			t.Fatal(err)
		}
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("apply contract", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		state := cloneState(fixture.machine.state)
		state.ApplyContractDigest[0] ^= 1
		putStateDocument(t, fixture.system.Collection, state, nil, nil)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("static data-chain seed", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		state := cloneState(fixture.machine.state)
		state.DataChainDigest[0] ^= 1
		putStateDocument(t, fixture.system.Collection, state, nil, nil)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("static user image", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.user.Collection.Put([]byte("direct"), []byte(`{"n":1}`)); err != nil {
			t.Fatal(err)
		}
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("session row counts", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		command := encodeCommand(t, commandValue(fixture.binding, 1))
		applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
			t.Fatal(err)
		}
		state := cloneState(fixture.machine.state)
		state.SessionSlotCount = 0
		putStateDocument(t, fixture.system.Collection, state, nil, nil)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("retired header needs retire result", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		command := encodeCommand(t, commandValue(fixture.binding, 1))
		view, _ := replication.OpenCommand(command)
		applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
			t.Fatal(err)
		}
		digest := SessionKey(view.Tenant, view.ClientID)
		key := SessionStorageKey(digest)
		raw, found, err := fixture.system.Collection.AppendRaw(nil, key[:])
		if err != nil || !found {
			t.Fatalf("session read = %v,%v", found, err)
		}
		header, err := OpenSessionRecord(raw)
		if err != nil {
			t.Fatal(err)
		}
		next := sessionRecord(header)
		next.Status = SessionRetired
		next.AckThrough = next.HighSequence - 1
		record, err := AppendSessionRecord(nil, next)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.system.Collection.Put(key[:], record); err != nil {
			t.Fatal(err)
		}
		_, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("current epoch retry slot", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		for sequence := uint64(1); sequence <= 9; sequence++ {
			if _, err := fixture.machine.ApplyNormal(
				normalMeta(sequence+2), encodeCommand(t, commandValue(fixture.binding, sequence)),
			); err != nil {
				t.Fatal(err)
			}
		}
		digest := SessionKey([]byte("tenant"), id128(77))
		rewriteSessionSlot(t, fixture, digest, 0, func(raw []byte) {
			binary.LittleEndian.PutUint64(raw[60:68], 1)
		})
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("session result apply order", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		for sequence := uint64(1); sequence <= 2; sequence++ {
			if _, err := fixture.machine.ApplyNormal(
				normalMeta(sequence+2), encodeCommand(t, commandValue(fixture.binding, sequence)),
			); err != nil {
				t.Fatal(err)
			}
		}
		digest := SessionKey([]byte("tenant"), id128(77))
		rewriteSessionSlot(t, fixture, digest, 1, func(raw []byte) {
			binary.LittleEndian.PutUint64(raw[68:76], 4)
		})
		rewriteSessionSlot(t, fixture, digest, 2, func(raw []byte) {
			binary.LittleEndian.PutUint64(raw[68:76], 3)
		})
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("mixed routing coordinates", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		if _, err := fixture.machine.ApplyNormal(
			normalMeta(3), encodeCommand(t, commandValue(fixture.binding, 1)),
		); err != nil {
			t.Fatal(err)
		}
		configuration := normalMeta(4)
		configuration.Type = pb.EntryConfChange
		if _, err := fixture.machine.ApplyConfiguration(
			configuration, &pb.ConfState{Voters: []uint64{1, 2}},
		); err != nil {
			t.Fatal(err)
		}
		transition := testOwnershipTransition(fixture.binding, 4)
		encoded, err := AppendOwnershipTransition(nil, transition)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(5), encoded); err != nil {
			t.Fatal(err)
		}
		digest := SessionKey([]byte("tenant"), id128(77))
		rewriteSessionSlot(t, fixture, digest, 1, func(raw []byte) {
			binary.LittleEndian.PutUint64(raw[176:184], transition.ToRouteGeneration)
		})
		_, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("early retirement result", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		applySessionOpen(t, fixture.machine, 2, commandValue(fixture.binding, 1))
		for sequence := uint64(1); sequence <= 2; sequence++ {
			if _, err := fixture.machine.ApplyNormal(
				normalMeta(sequence+2), encodeCommand(t, commandValue(fixture.binding, sequence)),
			); err != nil {
				t.Fatal(err)
			}
		}
		digest := SessionKey([]byte("tenant"), id128(77))
		rewriteSessionSlot(t, fixture, digest, 1, func(raw []byte) {
			binary.LittleEndian.PutUint32(raw[140:144], ResultSessionRetired)
		})
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("completion before first command index", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(3), nil); err != nil {
			t.Fatal(err)
		}
		command := encodeCommand(t, commandValue(fixture.binding, 1))
		view, _ := replication.OpenCommand(command)
		state := cloneState(fixture.machine.state)
		putCraftedSession(t, fixture, state, view, 1, ResultApplied)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("non-stale completion cannot use configuration index", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		configuration := normalMeta(2)
		configuration.Type = pb.EntryConfChange
		if _, err := fixture.machine.ApplyConfiguration(
			configuration, &pb.ConfState{Voters: []uint64{1}},
		); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(3), nil); err != nil {
			t.Fatal(err)
		}
		command := commandValue(fixture.binding, 1)
		command.ReplicaSetVersion = 2
		encoded := encodeCommand(t, command)
		view, _ := replication.OpenCommand(encoded)
		state := cloneState(fixture.machine.state)
		putCraftedSession(t, fixture, state, view, 2, ResultApplied)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("non-stale completion cannot name a future replica-set version", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		for index := uint64(2); index <= 4; index++ {
			if _, err := fixture.machine.ApplyNormal(normalMeta(index), nil); err != nil {
				t.Fatal(err)
			}
		}
		command := commandValue(fixture.binding, 1)
		command.ReplicaSetVersion = 2
		encoded := encodeCommand(t, command)
		view, _ := replication.OpenCommand(encoded)
		state := cloneState(fixture.machine.state)
		putCraftedSession(t, fixture, state, view, 3, ResultApplied)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("stale completion cannot occupy final configuration index", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		meta := normalMeta(2)
		meta.Type = pb.EntryConfChange
		if _, err := fixture.machine.ApplyConfiguration(meta, &pb.ConfState{Voters: []uint64{1}}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(3), nil); err != nil {
			t.Fatal(err)
		}
		command := commandValue(fixture.binding, 1)
		command.ReplicaSetVersion = 100
		encoded := encodeCommand(t, command)
		view, _ := replication.OpenCommand(encoded)
		state := cloneState(fixture.machine.state)
		putCraftedSession(t, fixture, state, view, 2, ResultStaleFence)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
}

func rewriteSessionSlot(
	t testing.TB,
	fixture machineFixture,
	digest [32]byte,
	slot uint16,
	mutate func([]byte),
) {
	t.Helper()
	key, err := SessionSlotStorageKey(digest, slot)
	if err != nil {
		t.Fatal(err)
	}
	raw, found, err := fixture.system.Collection.AppendRaw(nil, key[:])
	if err != nil || !found {
		t.Fatalf("session slot read = %v,%v", found, err)
	}
	mutate(raw)
	sealRecord(raw, sessionSlotChecksumDomain)
	if _, err := fixture.system.Collection.Put(key[:], raw); err != nil {
		t.Fatal(err)
	}
}

func putStateDocument(t testing.TB, collection *durable.Collection, state State, extraKey, extraDocument []byte) {
	t.Helper()
	envelope, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := collection.Update(func(batch *durable.WriteBatch) error {
		if err := batch.Put(stateKey, envelope); err != nil {
			return err
		}
		if len(extraDocument) != 0 {
			return batch.Put(extraKey, extraDocument)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func putCraftedSession(
	t testing.TB,
	fixture machineFixture,
	state State,
	command replication.CommandView,
	applied uint64,
	result uint32,
) {
	t.Helper()
	digest := SessionKey(command.Tenant, command.ClientID)
	sessionKey := SessionStorageKey(digest)
	openSlotKey, err := SessionSlotStorageKey(digest, 0)
	if err != nil {
		t.Fatal(err)
	}
	slotIndex := uint16((command.ClientSequence - 1) % uint64(fixture.machine.options.RetryWindow))
	slotKey, err := SessionSlotStorageKey(digest, slotIndex)
	if err != nil {
		t.Fatal(err)
	}
	header, err := AppendSessionRecord(nil, SessionRecord{
		Tenant: command.Tenant, ClientID: command.ClientID,
		ClientEpoch: command.ClientEpoch, RetryHome: command.RetryHome,
		HighSequence: command.ClientSequence, Status: SessionActive,
		RetryWindow: fixture.machine.options.RetryWindow, PhysicalSlotCount: slotIndex + 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	encodedApplied := applied
	if encodedApplied <= command.ClientEpoch {
		// The codec rejects impossible apply indexes. Encode a structurally valid
		// slot, then reseal the deliberately corrupted fixture below so Open's
		// persisted-image validation remains covered independently.
		encodedApplied = command.ClientEpoch + 1
	}
	openSlot, err := AppendSessionSlot(nil, SessionSlot{
		Slot:                   0,
		SessionDigest:          digest,
		ClientEpoch:            command.ClientEpoch,
		ClientSequence:         1,
		AppliedSequence:        command.ClientEpoch,
		Fingerprint:            replication.Digest{0x6f, 0x70, 0x65, 0x6e},
		LogicalCommandDigest:   [32]byte{0x6f, 0x70, 0x65, 0x6e},
		ResultCode:             ResultSessionOpened,
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: fixture.binding.ActivePolicyGeneration,
		ProtectionEpoch:        fixture.binding.ProtectionEpoch,
		RoutingVersion:         fixture.binding.RoutingVersion,
		RouteGeneration:        fixture.binding.RouteGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	slot, err := AppendSessionSlot(nil, SessionSlot{
		Slot:                   slotIndex,
		SessionDigest:          digest,
		ClientEpoch:            command.ClientEpoch,
		ClientSequence:         command.ClientSequence,
		AppliedSequence:        encodedApplied,
		Fingerprint:            command.Fingerprint,
		LogicalCommandDigest:   LogicalCommandDigest(command),
		ResultCode:             result,
		ReplicaSetVersion:      command.ReplicaSetVersion,
		ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch:        command.ProtectionEpoch,
		RoutingVersion:         command.RoutingVersion,
		RouteGeneration:        command.RouteGeneration,
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied != encodedApplied {
		binary.LittleEndian.PutUint64(slot[68:76], applied)
		sealRecord(slot, sessionSlotChecksumDomain)
	}
	state.SessionCount, state.SessionSlotCount = 1, uint64(slotIndex)+1
	state.SessionEpochHighWater = command.ClientEpoch
	stateEnvelope, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		if err := batch.Put(stateKey, stateEnvelope); err != nil {
			return err
		}
		if err := batch.Put(sessionKey[:], header); err != nil {
			return err
		}
		if err := batch.Put(openSlotKey[:], openSlot); err != nil {
			return err
		}
		return batch.Put(slotKey[:], slot)
	}); err != nil {
		t.Fatal(err)
	}
}
