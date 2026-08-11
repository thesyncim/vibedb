package replicatedstate

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestCompletionHashCollisionIsCorruptionNotTupleConflict(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	a := encodeCommand(t, commandValue(fixture.binding, 1))
	b := encodeCommand(t, commandValue(fixture.binding, 2))
	viewA, _ := replication.OpenCommandV1(a)
	viewB, _ := replication.OpenCommandV1(b)
	document, err := fixture.machine.makeCompletionDocument(viewB, 2, ResultApplied)
	if err != nil {
		t.Fatal(err)
	}
	digestA := CompletionKeyV1(viewA.Tenant, viewA.ClientID, viewA.ClientEpoch, viewA.ClientSequence)
	keyA := completionStorageKey(digestA)
	if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
		return batch.Put(keyA[:], document)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), a); !errors.Is(err, ErrCompletionCorrupt) {
		t.Fatalf("collision apply error = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); !errors.Is(err, ErrApplyPoisoned) {
		t.Fatalf("post-collision error = %v", err)
	}
}

func TestCompletionRetentionCapFailsClosed(t *testing.T) {
	fixture := newMachineFixture(t)
	fixture.machine.options.MaxCompletions = 1
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	first := encodeCommand(t, commandValue(fixture.binding, 1))
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), first); err != nil {
		t.Fatal(err)
	}
	second := encodeCommand(t, commandValue(fixture.binding, 2))
	if err := fixture.machine.AdmitCommand(second); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("cap admission error = %v", err)
	}
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), second); !errors.Is(err, ErrAdmissionBound) {
		t.Fatalf("cap apply error = %v", err)
	}
	if fixture.machine.Applied() != 2 || fixture.machine.state.CompletionCount != 1 {
		t.Fatalf("cap failure publication=%+v state=%+v", fixture.machine.Published(), fixture.machine.state)
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
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), encoded); err != nil {
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
	completion, _ := replication.OpenCompletionV1(lookup.Bytes)
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
		options.TxnLimits.MaxDocuments = fixture.user.Limits.MaxDistinctMutations + 1
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, options)
		if !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("transaction bytes", func(t *testing.T) {
		maxStateDocument := 2*MaxStateEnvelopeBytes + 2
		maxCompletionDocument := 2*MaxCompletionRecordBytes + 2
		maxSystem := len(stateKey) + maxStateDocument + 33 + maxCompletionDocument
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
		bootstrap.Metadata.ConfState.Voters = make([]uint64, MaxStaticBootstrapMembersV1)
		for i := range bootstrap.Metadata.ConfState.Voters {
			bootstrap.Metadata.ConfState.Voters[i] = uint64(i + 1)
		}
		if _, _, err := validateBootstrap(bootstrap); err != nil {
			t.Fatalf("exact member ceiling: %v", err)
		}
		bootstrap.Metadata.ConfState.Voters = append(
			bootstrap.Metadata.ConfState.Voters, MaxStaticBootstrapMembersV1+1,
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

func TestOpenDetectsStateLogicalCountAndCompletionCorruption(t *testing.T) {
	t.Run("static last-entry digest", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		envelope, err := AppendStateV1(nil, fixture.machine.state)
		if err != nil {
			t.Fatal(err)
		}
		envelope[184] ^= 1
		sealRecord(envelope, stateChecksumDomain)
		if err := fixture.system.Collection.Update(func(batch *durable.WriteBatch) error {
			return batch.Put(stateKey, wrapJSONHex(nil, envelope))
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
	t.Run("logical image", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		if err := fixture.user.Collection.Update(func(batch *durable.WriteBatch) error {
			return batch.Put([]byte("outside"), []byte("null"))
		}); err != nil {
			t.Fatal(err)
		}
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("completion count", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		command := encodeCommand(t, commandValue(fixture.binding, 1))
		if _, err := fixture.machine.ApplyNormal(normalMeta(2), command); err != nil {
			t.Fatal(err)
		}
		state := cloneState(fixture.machine.state)
		state.CompletionCount = 0
		putStateDocument(t, fixture.system.Collection, state, nil, nil)
		_, err := Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("duplicate completion applied sequence", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		first := encodeCommand(t, commandValue(fixture.binding, 1))
		if _, err := fixture.machine.ApplyNormal(normalMeta(2), first); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(3), nil); err != nil {
			t.Fatal(err)
		}
		second := encodeCommand(t, commandValue(fixture.binding, 2))
		view, _ := replication.OpenCommandV1(second)
		document, err := fixture.machine.makeCompletionDocument(view, 2, ResultApplied)
		if err != nil {
			t.Fatal(err)
		}
		digest := CompletionKeyV1(view.Tenant, view.ClientID, view.ClientEpoch, view.ClientSequence)
		key := completionStorageKey(digest)
		state := cloneState(fixture.machine.state)
		state.CompletionCount = 2
		putStateDocument(t, fixture.system.Collection, state, key[:], document)
		_, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrCompletionCorrupt) {
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
		command := encodeCommand(t, commandValue(fixture.binding, 1))
		view, _ := replication.OpenCommandV1(command)
		document, err := fixture.machine.makeCompletionDocument(view, 1, ResultApplied)
		if err != nil {
			t.Fatal(err)
		}
		digest := CompletionKeyV1(view.Tenant, view.ClientID, view.ClientEpoch, view.ClientSequence)
		key := completionStorageKey(digest)
		state := cloneState(fixture.machine.state)
		state.CompletionCount = 1
		putStateDocument(t, fixture.system.Collection, state, key[:], document)
		_, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrCompletionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
	t.Run("non-stale completion cannot use configuration index", func(t *testing.T) {
		fixture := newMachineFixture(t)
		if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.machine.ApplyNormal(normalMeta(2), nil); err != nil {
			t.Fatal(err)
		}
		command := commandValue(fixture.binding, 1)
		command.ReplicaSetVersion = 2
		encoded := encodeCommand(t, command)
		view, _ := replication.OpenCommandV1(encoded)
		document, err := fixture.machine.makeCompletionDocument(view, 2, ResultApplied)
		if err != nil {
			t.Fatal(err)
		}
		digest := CompletionKeyV1(view.Tenant, view.ClientID, view.ClientEpoch, view.ClientSequence)
		key := completionStorageKey(digest)
		state := cloneState(fixture.machine.state)
		state.CompletionCount = 1
		putStateDocument(t, fixture.system.Collection, state, key[:], document)
		_, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrCompletionCorrupt) {
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
		view, _ := replication.OpenCommandV1(encoded)
		document, err := fixture.machine.makeCompletionDocument(view, 3, ResultApplied)
		if err != nil {
			t.Fatal(err)
		}
		digest := CompletionKeyV1(view.Tenant, view.ClientID, view.ClientEpoch, view.ClientSequence)
		key := completionStorageKey(digest)
		state := cloneState(fixture.machine.state)
		state.CompletionCount = 1
		putStateDocument(t, fixture.system.Collection, state, key[:], document)
		_, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrCompletionCorrupt) {
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
		command := commandValue(fixture.binding, 1)
		command.ReplicaSetVersion = 100
		encoded := encodeCommand(t, command)
		view, _ := replication.OpenCommandV1(encoded)
		document, err := fixture.machine.makeCompletionDocument(view, 2, ResultStaleFence)
		if err != nil {
			t.Fatal(err)
		}
		digest := CompletionKeyV1(view.Tenant, view.ClientID, view.ClientEpoch, view.ClientSequence)
		key := completionStorageKey(digest)
		state := cloneState(fixture.machine.state)
		state.CompletionCount = 1
		putStateDocument(t, fixture.system.Collection, state, key[:], document)
		_, err = Open(fixture.binding, fixture.bootstrap, fixture.system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options)
		if !errors.Is(err, ErrCompletionCorrupt) {
			t.Fatalf("Open error = %v", err)
		}
	})
}

func putStateDocument(t testing.TB, collection *durable.Collection, state StateV1, extraKey, extraDocument []byte) {
	t.Helper()
	envelope, err := AppendStateV1(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	document := wrapJSONHex(nil, envelope)
	if err := collection.Update(func(batch *durable.WriteBatch) error {
		if err := batch.Put(stateKey, document); err != nil {
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
