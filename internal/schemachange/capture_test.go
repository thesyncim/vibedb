package schemachange

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

type acceptingValidator struct{}

func (acceptingValidator) ValidatePut(_, _ []byte) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}
func (acceptingValidator) ValidateDelete(_, _ []byte, _ bool) replicatedstate.MutationValidation {
	return replicatedstate.MutationValidationAccept
}

type captureFixture struct {
	dir       string
	machine   *replicatedstate.Machine
	binding   replicatedstate.Binding
	bootstrap *pb.Snapshot
	system    replicatedstate.CollectionTarget
	relations []replicatedstate.RelationCollection
	target    replicatedstate.TransitionCaptureTarget
	log       *durable.TxnLog
	options   replicatedstate.Options
	config    CaptureConfig
}

func fixtureTarget(collection *durable.Collection, system bool) replicatedstate.CollectionTarget {
	t := replicatedstate.CollectionTarget{Collection: collection, Validation: replicatedstate.ValidationDeterministicMutation,
		ValidationDigest: sha256.Sum256([]byte("schema-capture-test")), Validator: acceptingValidator{},
		Limits: replicatedstate.CollectionLimits{MaxKeyBytes: collection.MaxKeyBytes(), MaxDocumentBytes: collection.MaxDocumentBytes(),
			MaxDistinctMutations: collection.MaxBatchDocuments(), MaxBatchBytes: collection.MaxBatchBytes()}}
	if system {
		t.Validation, t.ValidationDigest, t.Validator = replicatedstate.ValidationOpaqueBinary, [32]byte{}, nil
	}
	return t
}

func newCaptureFixture(t testing.TB, checkpoint bool) *captureFixture {
	t.Helper()
	return newCaptureFixtureWithLimits(t, checkpoint, replicatedstate.MaxTransitionCaptureRecordBytes, replicatedstate.MaxTransitionCaptureRecordBytes+8)
}

func newCaptureFixtureWithLimits(t testing.TB, checkpoint bool, documentBytes, batchBytes int) *captureFixture {
	t.Helper()
	dir := t.TempDir()
	create := func(name string, options durable.Options) *durable.Collection {
		file, err := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		collection, err := durable.Create(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		return collection
	}
	f := &captureFixture{dir: dir}
	f.system = fixtureTarget(create("system", durable.Options{OpaqueValues: true, MaxBatchDocuments: 32}), true)
	f.target = replicatedstate.TransitionCaptureTarget{Name: "capture", Collection: create("capture", durable.Options{
		OpaqueValues: true, MaxKeyBytes: 8, MaxDocumentBytes: documentBytes,
		MaxBatchDocuments: 1, MaxBatchBytes: batchBytes,
	})}
	members := []durable.NamedCollection{{Name: replicatedstate.SystemCollectionName, Collection: f.system.Collection}}
	for i, name := range []string{"base", "other"} {
		collection := create(name, durable.Options{MaxDocumentBytes: 1024, MaxBatchDocuments: 64, MaxBatchBytes: 128 << 10})
		f.relations = append(f.relations, replicatedstate.RelationCollection{Relation: replication.RelationID(i + 1), Kind: replicatedstate.RelationJSON,
			Name: name, Target: fixtureTarget(collection, false)})
		members = append(members, durable.NamedCollection{Name: name, Collection: collection})
	}
	members = append(members, durable.NamedCollection{Name: f.target.Name, Collection: f.target.Collection})
	var err error
	f.log, err = durable.NewTxnLog(dir, durable.TxnLogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.log.Close() })
	f.options = replicatedstate.Options{MaxSessions: 128, RetryWindow: 8, TransitionCaptureTarget: f.target,
		TxnLimits: durable.TxnLimits{MaxCollections: 4, MaxDocuments: 133, MaxBytes: 64 << 20}}
	if checkpoint {
		f.options.CheckpointGroup, err = durable.NewCheckpointGroup(f.log, members, durable.CheckpointGroupOptions{CheckpointEvery: 1024})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.options.CheckpointGroup.Close() })
	}
	f.binding = replicatedstate.Binding{ClusterID: replication.ID128{1}, ClusterIncarnation: replication.ID128{2}, TopologyRecoveryEpoch: 1,
		Distribution: "dist", Shard: "shard", AllocationGeneration: 1, ShardIncarnation: replication.ID128{3}, GroupID: replication.ID128{4},
		ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		OwnedRange: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}
	index, term := uint64(1), uint64(1)
	f.bootstrap = &pb.Snapshot{Data: []byte("schema-capture-bootstrap"), Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1}}}}
	f.machine, err = replicatedstate.OpenBundle(f.binding, f.bootstrap, f.system, f.relations, f.log, f.options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.InstallSnapshot(f.bootstrap); err != nil {
		t.Fatal(err)
	}
	command := f.command(1, nil)
	command.Kind, command.ClientEpoch, command.NextDeadlineUnixNano = replication.CommandSessionOpen, 0, 2_000_000_000_000_000_000
	f.apply(t, 2, command)
	snapshot, err := f.machine.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	f.config = CaptureConfig{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, BindingDigest: replicatedstate.SplitCaptureBindingDigest(f.binding),
		ManifestDigest: snapshot.Fence().RelationManifestDigest, SchemaGeneration: 1, MaxRecords: 1000, MaxBytes: 16 << 20}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *captureFixture) command(sequence uint64, batches []replication.RelationMutationBatch) replication.Command {
	b := f.binding
	return replication.Command{Kind: replication.CommandMutationBatch, ClusterID: b.ClusterID, ClusterIncarnation: b.ClusterIncarnation,
		TopologyRecoveryEpoch: b.TopologyRecoveryEpoch, Distribution: b.Distribution, Shard: b.Shard, AllocationGeneration: b.AllocationGeneration,
		ShardIncarnation: b.ShardIncarnation, GroupID: b.GroupID, ReplicaSetVersion: 1, ActivePolicyGeneration: b.ActivePolicyGeneration,
		ProtectionEpoch: b.ProtectionEpoch, OwnershipEpoch: b.OwnershipEpoch, SchemaGeneration: b.SchemaGeneration, RoutingVersion: b.RoutingVersion,
		RouteGeneration: b.RouteGeneration, Tenant: []byte("tenant"), ClientID: replication.ID128{20}, ClientEpoch: 2, ClientSequence: sequence,
		Fingerprint: sha256.Sum256([]byte(fmt.Sprintf("command-%d", sequence))), Batches: batches}
}

func (f *captureFixture) apply(t testing.TB, applied uint64, command replication.Command) []byte {
	t.Helper()
	raw, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.machine.AdmitCommand(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.ApplyNormal(raftmodel.ApplyMeta{Index: applied, Term: 1, Type: pb.EntryNormal}, raw); err != nil {
		t.Fatal(err)
	}
	lookup, err := f.machine.LookupCompletion(raw)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != replicatedstate.ResultApplied && completion.ResultCode != replicatedstate.ResultSessionOpened {
		t.Fatalf("unexpected completion %+v err=%v", completion, err)
	}
	return raw
}

func (f *captureFixture) begin(t testing.TB) *SourceCapture {
	t.Helper()
	c, err := NewSourceCapture(f.config, f.target)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.machine.BeginTransitionCapture(c); err != nil {
		t.Fatal(err)
	}
	return c
}

func (f *captureFixture) reopen(t testing.TB) *SourceCapture {
	t.Helper()
	c, err := NewSourceCapture(f.config, f.target)
	if err != nil {
		t.Fatal(err)
	}
	options := f.options
	options.TransitionCapture = c
	f.machine, err = replicatedstate.OpenBundle(f.binding, f.bootstrap, f.system, f.relations, f.log, options)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func singlePut(sequence uint64) []replication.RelationMutationBatch {
	return []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("row"), Value: []byte(fmt.Sprintf(`{"n":%d}`, sequence))}}}}
}

func TestCaptureEveryRelationAndRestart(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprint(checkpoint), func(t *testing.T) {
			f := newCaptureFixture(t, checkpoint)
			c := f.begin(t)
			d, _ := c.Descriptor()
			cursor := d.Base
			batches := []replication.RelationMutationBatch{{Relation: 1}, {Relation: 2}}
			for i := range batches {
				for key := 63; key >= 0; key-- {
					batches[i].Mutations = append(batches[i].Mutations, replication.Mutation{Kind: replication.MutationPut,
						Key: []byte(fmt.Sprintf("key-%02d", key)), Value: []byte(fmt.Sprintf(` {"relation": %d, "n": %d} `, i+1, key))})
				}
			}
			f.apply(t, 3, f.command(2, batches))
			var w CaptureWorkspace
			entry, found, err := c.Next(cursor, &w)
			if err != nil || !found || len(entry.Mutations) != 128 || entry.Abort != NotAborted {
				t.Fatalf("entry count=%d found=%v err=%v", len(entry.Mutations), found, err)
			}
			for i, m := range entry.Mutations {
				if int(m.Relation) != i/64+1 || string(m.Key) != fmt.Sprintf("key-%02d", i%64) || m.BeforePresent || !m.AfterPresent ||
					string(m.After) != fmt.Sprintf(`{"n":%d,"relation":%d}`, i%64, i/64+1) {
					t.Fatalf("mutation %d: %+v", i, m)
				}
			}
			cursor = Cursor{entry.After, entry.Digest}
			c = f.reopen(t)
			batches = []replication.RelationMutationBatch{
				{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("key-00"), Value: []byte(`{"n":99}`)}}},
				{Relation: 2, Mutations: []replication.Mutation{{Kind: replication.MutationDelete, Key: []byte("key-00")}}},
			}
			raw := f.apply(t, 4, f.command(3, batches))
			entry, found, err = c.Next(cursor, &w)
			if err != nil || !found || len(entry.Mutations) != 2 {
				t.Fatalf("update found=%v err=%v", found, err)
			}
			for i, m := range entry.Mutations {
				if !m.MatchesBefore([]byte(fmt.Sprintf(`{"n":0,"relation":%d}`, i+1)), true) || m.MatchesBefore([]byte(`{"wrong":1}`), true) {
					t.Fatal("before witness mismatch")
				}
			}
			if !entry.Mutations[0].AfterPresent || entry.Mutations[1].AfterPresent {
				t.Fatal("update/delete presence mismatch")
			}
			cursor = Cursor{entry.After, entry.Digest}
			if _, err := f.machine.ApplyNormal(raftmodel.ApplyMeta{Index: 5, Term: 1, Type: pb.EntryNormal}, raw); err != nil {
				t.Fatal(err)
			}
			entry, found, err = c.Next(cursor, &w)
			if err != nil || !found || len(entry.Mutations) != 0 {
				t.Fatal("retry duplicated mutations")
			}
			cursor = Cursor{entry.After, entry.Digest}
			if _, found, err := c.Next(cursor, &w); err != nil || found {
				t.Fatal("read past committed head")
			}
			wrong := cursor
			wrong.Digest[0]++
			if _, _, err := c.Next(wrong, &w); err == nil {
				t.Fatal("substituted cursor accepted")
			}
		})
	}
}

func TestCaptureExhaustionDoesNotStopWritesOrReopen(t *testing.T) {
	for _, bound := range []string{"records", "bytes"} {
		t.Run(bound, func(t *testing.T) {
			f := newCaptureFixture(t, true)
			if bound == "records" {
				f.config.MaxRecords = 2
			} else {
				f.config.MaxBytes = uint64(headerBytes + entryBytes*2 + mutationBytes + len("row") + len(`{"n":2}`))
			}
			c := f.begin(t)
			d, _ := c.Descriptor()
			cursor := d.Base
			for applied := uint64(3); applied <= 12; applied++ {
				f.apply(t, applied, f.command(applied-1, singlePut(applied-1)))
			}
			d, err := c.Descriptor()
			if err != nil || d.Abort != AbortCapacity || d.Head.Publication.Applied != 4 || d.Records != 2 || d.Bytes > f.config.MaxBytes || f.target.Collection.Len() != 3 {
				t.Fatalf("unbounded capture %+v err=%v", d, err)
			}
			var w CaptureWorkspace
			first, found, err := c.Next(cursor, &w)
			if err != nil || !found || first.Abort != NotAborted {
				t.Fatal("lost pre-abort entry")
			}
			terminal, found, err := c.Next(Cursor{first.After, first.Digest}, &w)
			if err != nil || !found || terminal.Abort != AbortCapacity || len(terminal.Mutations) != 0 {
				t.Fatal("missing bounded terminal record")
			}
			c = f.reopen(t)
			if !c.CaptureStopped() {
				t.Fatal("reopen forgot terminal capture")
			}
			f.apply(t, 13, f.command(12, singlePut(12)))
			if f.target.Collection.Len() != 3 {
				t.Fatal("reopen resumed aborted capture")
			}
			value, found, err := f.relations[0].Target.Collection.AppendRaw(nil, []byte("row"))
			if err != nil || !found || string(value) != `{"n":12}` {
				t.Fatalf("write after capture abort=%q found=%v err=%v", value, found, err)
			}
		})
	}
}

func TestCaptureFrozenStorageLimitsAbortBuildNotSource(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		for _, bound := range []string{"compact_batch", "wide_batch"} {
			t.Run(fmt.Sprintf("%s/checkpoint_%t", bound, checkpoint), func(t *testing.T) {
				documentBytes, batchBytes := 1024, 1032
				if bound == "wide_batch" {
					batchBytes = 4096
				}
				f := newCaptureFixtureWithLimits(t, checkpoint, documentBytes, batchBytes)
				c := f.begin(t)
				value := []byte(`{"value":"` + strings.Repeat("x", 800) + `"}`)
				batches := []replication.RelationMutationBatch{
					{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("row"), Value: value}}},
					{Relation: 2, Mutations: []replication.Mutation{{Kind: replication.MutationPut, Key: []byte("row"), Value: value}}},
				}
				f.apply(t, 3, f.command(2, batches))
				d, err := c.Descriptor()
				if err != nil || d.Abort != AbortCapacity || d.Head.Publication.Applied != 3 || f.target.Collection.Len() != 2 {
					t.Fatalf("oversized capture did not abort safely: %+v err=%v", d, err)
				}
				for _, relation := range f.relations {
					got, found, err := relation.Target.Collection.AppendRaw(nil, []byte("row"))
					if err != nil || !found || !bytes.Equal(got, value) {
						t.Fatalf("source write lost: %q found=%v err=%v", got, found, err)
					}
				}
				c = f.reopen(t)
				f.apply(t, 4, f.command(3, singlePut(3)))
				if got, err := c.Descriptor(); err != nil || got != d || !c.CaptureStopped() {
					t.Fatal("reopen changed the terminal capture")
				}
				if f.target.Collection.MaxDocumentBytes() != documentBytes || f.target.Collection.MaxBatchBytes() != batchBytes {
					t.Fatal("frozen storage limits changed")
				}
			})
		}
	}
}

type failingBeginCapture struct {
	*SourceCapture
	afterPublish bool
	failure      error
}

func (c *failingBeginCapture) Begin(state replicatedstate.State, publish func([]byte, []byte) error) error {
	if c.afterPublish {
		if err := c.SourceCapture.Begin(state, publish); err != nil {
			return err
		}
	}
	return c.failure
}

func TestCaptureBeginFailureCannotAdvancePastPublishedHeader(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		for _, published := range []bool{false, true} {
			t.Run(fmt.Sprintf("checkpoint_%t/published_%t", checkpoint, published), func(t *testing.T) {
				f := newCaptureFixture(t, checkpoint)
				capture, err := NewSourceCapture(f.config, f.target)
				if err != nil {
					t.Fatal(err)
				}
				failure := errors.New("injected capture startup failure")
				err = f.machine.BeginTransitionCapture(&failingBeginCapture{capture, published, failure})
				if !errors.Is(err, failure) || !errors.Is(err, replicatedstate.ErrTransitionCapture) {
					t.Fatalf("startup lost error identity: %v", err)
				}
				_, err = f.machine.ApplyNormal(raftmodel.ApplyMeta{Index: 3, Term: 1, Type: pb.EntryNormal}, nil)
				if published {
					if err == nil || f.machine.Applied() != 2 {
						t.Fatal("source advanced without retaining its published capture")
					}
					f.reopen(t)
					f.apply(t, 3, f.command(2, singlePut(2)))
				} else if err != nil || f.machine.Applied() != 3 {
					t.Fatalf("pre-publication error disabled usable source: %v", err)
				}
			})
		}
	}
}

func TestCaptureDecoderRejectsNoncanonicalAndReleasesBorrowedRows(t *testing.T) {
	f := newCaptureFixture(t, false)
	f.begin(t)
	f.apply(t, 3, f.command(2, singlePut(2)))
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 3)
	raw, _, err := f.target.Collection.AppendRaw(nil, key[:])
	if err != nil {
		t.Fatal(err)
	}
	var w CaptureWorkspace
	if _, err := openEntry(raw, &w); err != nil {
		t.Fatal(err)
	}
	for length := range len(raw) {
		if _, err := openEntry(raw[:length], &w); err == nil {
			t.Fatalf("accepted truncation %d", length)
		}
	}
	for _, offset := range []int{8, 9, 10, 11, 12, 20, 24, 128, 264, 266, 267, 268, 272, 280} {
		bad := bytes.Clone(raw)
		bad[offset] = 255
		digest := recordDigest(bad[:len(bad)-32])
		copy(bad[len(bad)-32:], digest[:])
		if _, err := openEntry(bad, &w); err == nil {
			t.Fatalf("accepted noncanonical byte %d", offset)
		}
	}
	for _, m := range w.mutations[:cap(w.mutations)] {
		if m.Key != nil || m.After != nil {
			t.Fatal("decoder retained borrowed rows after failure")
		}
	}
	if _, err := openEntry(raw, &w); err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(100, func() {
		if _, err := openEntry(raw, &w); err != nil {
			panic(err)
		}
	}); got != 0 {
		t.Fatalf("warm decode allocations=%g", got)
	}
}

func TestCaptureBeforeImagesAreFixedWitnessesAndBoundMatchesSnapshot(t *testing.T) {
	maximum, err := recordBytes(replicatedstate.TransitionCaptureBounds{
		Transitions: maxMutations, KeyBytes: maxMutations * replication.MaxMutationKeyBytes,
		BeforeBytes: maxMutations * replication.MaxMutationValueBytes,
		AfterBytes:  replication.MaxCommandBytes - maxMutations*replication.MaxMutationKeyBytes,
	})
	if err != nil || maximum != replicatedstate.MaxTransitionCaptureRecordBytes {
		t.Fatalf("maximum=%d err=%v", maximum, err)
	}
	f := newCaptureFixture(t, false)
	f.begin(t)
	before := []byte(`{"body":"` + strings.Repeat("x", 950) + `"}`)
	batches := singlePut(2)
	batches[0].Mutations[0].Value = before
	f.apply(t, 3, f.command(2, batches))
	f.apply(t, 4, f.command(3, singlePut(3)))
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 4)
	raw, _, err := f.target.Collection.AppendRaw(nil, key[:])
	if err != nil || len(raw) != entryBytes+mutationBytes+len("row")+len(`{"n":3}`) {
		t.Fatalf("before image inflated log: bytes=%d err=%v", len(raw), err)
	}
	var w CaptureWorkspace
	entry, err := openEntry(raw, &w)
	if err != nil || !entry.Mutations[0].MatchesBefore(before, true) {
		t.Fatal("compact before witness mismatch")
	}
}

func TestCaptureConcurrentTailReader(t *testing.T) {
	f := newCaptureFixture(t, true)
	c := f.begin(t)
	d, _ := c.Descriptor()
	cursor := d.Base
	done := make(chan error, 1)
	go func() {
		for applied := uint64(3); applied < 35; applied++ {
			raw, err := replication.AppendCommand(nil, f.command(applied-1, singlePut(applied-1)))
			if err == nil {
				_, err = f.machine.ApplyNormal(raftmodel.ApplyMeta{Index: applied, Term: 1, Type: pb.EntryNormal}, raw)
			}
			if err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	var w CaptureWorkspace
	finished := false
	for cursor.Publication.Applied < 34 {
		entry, found, err := c.Next(cursor, &w)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if entry.Abort != NotAborted || len(entry.Mutations) != 1 {
				t.Fatal("invalid concurrent tail")
			}
			cursor = Cursor{entry.After, entry.Digest}
			continue
		}
		if finished {
			t.Fatal("writer finished without its complete tail")
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			finished = true
		default:
			runtime.Gosched()
		}
	}
	if !finished {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func reopenCaptureCrashImage(t *testing.T, f *captureFixture) (*SourceCapture, *replicatedstate.Machine) {
	t.Helper()
	dir := t.TempDir()
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		input, err := os.Open(filepath.Join(f.dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		output, err := os.OpenFile(filepath.Join(dir, entry.Name()), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			_ = input.Close()
			t.Fatal(err)
		}
		_, copyErr := io.Copy(output, input)
		_ = input.Close()
		syncErr, closeErr := output.Sync(), output.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil {
			t.Fatalf("copy crash image: %v %v %v", copyErr, syncErr, closeErr)
		}
	}
	options := []durable.Options{
		{OpaqueValues: true, MaxBatchDocuments: 32},
		{MaxDocumentBytes: 1024, MaxBatchDocuments: 64, MaxBatchBytes: 128 << 10},
		{MaxDocumentBytes: 1024, MaxBatchDocuments: 64, MaxBatchBytes: 128 << 10},
		{OpaqueValues: true, MaxKeyBytes: 8, MaxDocumentBytes: replicatedstate.MaxTransitionCaptureRecordBytes, MaxBatchDocuments: 1, MaxBatchBytes: replicatedstate.MaxTransitionCaptureRecordBytes + 8},
	}
	var opens []durable.TransactionCollectionOpen
	for i, name := range []string{"system", "base", "other", "capture"} {
		file, err := os.OpenFile(filepath.Join(dir, name+".vdb"), os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = file.Close() })
		opens = append(opens, durable.TransactionCollectionOpen{File: file, Options: options[i]})
	}
	collections, log, group, err := durable.OpenCollectionsWithCheckpointGroup(dir, durable.TxnLogOptions{}, opens,
		[]string{replicatedstate.SystemCollectionName, "base", "other", "capture"}, durable.CheckpointGroupOptions{CheckpointEvery: 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = group.Close()
		for _, collection := range collections {
			_ = collection.Close()
		}
		_ = log.Close()
	})
	config := f.options
	config.CheckpointGroup = group
	config.TransitionCaptureTarget.Collection = collections[3]
	c, err := NewSourceCapture(f.config, config.TransitionCaptureTarget)
	if err != nil {
		t.Fatal(err)
	}
	config.TransitionCapture = c
	relations := append([]replicatedstate.RelationCollection(nil), f.relations...)
	for i := range relations {
		relations[i].Target = fixtureTarget(collections[i+1], false)
	}
	machine, err := replicatedstate.OpenBundle(f.binding, f.bootstrap, fixtureTarget(collections[0], true), relations, log, config)
	if err != nil {
		t.Fatal(err)
	}
	return c, machine
}

func TestCaptureAbortCrashRecoveryNeverPoisonsNextWrite(t *testing.T) {
	for _, test := range []struct {
		name        string
		phase       durable.CheckpointGroupFaultPhaseForFacadeTest
		duringApply bool
	}{
		{"prepare", durable.CheckpointGroupFaultAfterPrepareAppendForFacadeTest, true},
		{"decision", durable.CheckpointGroupFaultAfterDecisionAppendForFacadeTest, true},
		{"journal_sync", durable.CheckpointGroupFaultAfterJournalSyncForFacadeTest, false},
		{"physical", durable.CheckpointGroupFaultAfterPhysicalCheckpointForFacadeTest, false},
		{"certificate_write", durable.CheckpointGroupFaultAfterCertificateWriteForFacadeTest, false},
		{"certificate_sync", durable.CheckpointGroupFaultAfterCertificateSyncForFacadeTest, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newCaptureFixture(t, true)
			f.config.MaxRecords = 1
			f.begin(t)
			if err := f.options.CheckpointGroup.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			command := f.command(2, singlePut(2))
			raw, err := replication.AppendCommand(nil, command)
			if err != nil {
				t.Fatal(err)
			}
			if !test.duringApply {
				// Do not look up the completion before injecting a checkpoint
				// fault: a read barrier may checkpoint the pending publication.
				if _, err := f.machine.ApplyNormal(raftmodel.ApplyMeta{Index: 3, Term: 1, Type: pb.EntryNormal}, raw); err != nil {
					t.Fatal(err)
				}
			}
			fired, restore := durable.InstallCheckpointGroupFaultForFacadeTest(test.phase)
			if test.duringApply {
				_, err = f.machine.ApplyNormal(raftmodel.ApplyMeta{Index: 3, Term: 1, Type: pb.EntryNormal}, raw)
			} else {
				err = f.options.CheckpointGroup.Checkpoint()
			}
			restore()
			if !fired() || err == nil {
				t.Fatalf("fault fired=%v err=%v", fired(), err)
			}
			c, machine := reopenCaptureCrashImage(t, f)
			d, err := c.Descriptor()
			if err != nil || d.Head.Publication.Applied != machine.Applied() || (d.Abort == AbortCapacity) != (machine.Applied() == 3) {
				t.Fatalf("skew after crash %+v applied=%d err=%v", d, machine.Applied(), err)
			}
			applied := machine.Applied() + 1
			if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{Index: applied, Term: 1, Type: pb.EntryNormal}, raw); err != nil {
				t.Fatal(err)
			}
			lookup, err := machine.LookupCompletion(raw)
			if err != nil {
				t.Fatal(err)
			}
			completion, err := replication.OpenCompletion(lookup.Bytes)
			if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
				t.Fatal("retry lost exact successful write")
			}
			if !c.CaptureStopped() || c.target.Collection.Len() != 2 {
				t.Fatal("terminal capture grew after crash/retry")
			}
			fresh, err := replication.AppendCommand(nil, f.command(3, singlePut(3)))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{Index: applied + 1, Term: 1, Type: pb.EntryNormal}, fresh); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCaptureStopsOnSourceOwnershipChange(t *testing.T) {
	f := newCaptureFixture(t, true)
	c := f.begin(t)
	if _, err := f.machine.ApplyConfiguration(raftmodel.ApplyMeta{Index: 3, Term: 1, Type: pb.EntryConfChange}, &pb.ConfState{Voters: []uint64{1, 2}}); err != nil {
		t.Fatal(err)
	}
	d, _ := c.Descriptor()
	if d.Abort != NotAborted {
		t.Fatal("membership change unnecessarily aborted capture")
	}
	raw, err := replicatedstate.AppendOwnershipTransition(nil, replicatedstate.OwnershipTransition{
		From: f.binding, ExpectedReplicaSetVersion: 3, SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch: 2, ToRoutingVersion: 2, ToRouteGeneration: 2, ToOwnedRange: f.binding.OwnedRange,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.machine.ApplyNormal(raftmodel.ApplyMeta{Index: 4, Term: 1, Type: pb.EntryNormal}, raw); err != nil {
		t.Fatal(err)
	}
	var w CaptureWorkspace
	entry, found, err := c.Next(d.Head, &w)
	if err != nil || !found || entry.Abort != AbortSourceChanged || len(entry.Mutations) != 0 || !c.CaptureStopped() {
		t.Fatalf("ownership terminal=%+v found=%v err=%v", entry, found, err)
	}
	fence, err := f.machine.SnapshotAuthorizationFence()
	if err != nil {
		t.Fatal(err)
	}
	f.binding = fence.Binding
	c = f.reopen(t)
	if !c.CaptureStopped() {
		t.Fatal("reopen resumed capture across changed ownership")
	}
}

func TestCaptureFinishExactCutAndReopen(t *testing.T) {
	for _, checkpoint := range []bool{false, true} {
		t.Run(fmt.Sprint(checkpoint), func(t *testing.T) {
			f := newCaptureFixture(t, checkpoint)
			c := f.begin(t)
			f.apply(t, 3, f.command(2, singlePut(2)))
			before, _ := c.Descriptor()
			if err := f.machine.FinishTransitionCapture(c, 2); err == nil {
				t.Fatal("stale finish accepted")
			}
			if after, _ := c.Descriptor(); after != before {
				t.Fatal("stale finish changed capture")
			}
			if err := f.machine.FinishTransitionCapture(c, 3); err != nil {
				t.Fatal(err)
			}
			sealed, err := c.Descriptor()
			if err != nil || sealed.SealDigest == [32]byte{} || sealed.Abort != NotAborted || sealed.Head != before.Head ||
				sealed.Records != before.Records+1 || sealed.Bytes != before.Bytes+entryBytes || !c.CaptureStopped() || f.machine.Applied() != 3 {
				t.Fatalf("invalid seal %+v err=%v", sealed, err)
			}
			f.apply(t, 4, f.command(3, singlePut(3)))
			if f.target.Collection.Len() != 3 {
				t.Fatal("write after finish extended sealed stream")
			}
			c = f.reopen(t)
			if got, err := c.Descriptor(); err != nil || got != sealed || !c.CaptureStopped() {
				t.Fatalf("recovered seal %+v err=%v", got, err)
			}
			f.apply(t, 5, f.command(4, singlePut(4)))
			if f.target.Collection.Len() != 3 {
				t.Fatal("reopen restarted sealed capture")
			}
		})
	}
}

func TestCaptureFinishCrashRecovery(t *testing.T) {
	for _, phase := range []durable.CheckpointGroupFaultPhaseForFacadeTest{
		durable.CheckpointGroupFaultAfterPrepareAppendForFacadeTest,
		durable.CheckpointGroupFaultAfterDecisionAppendForFacadeTest,
	} {
		t.Run(fmt.Sprint(phase), func(t *testing.T) {
			f := newCaptureFixture(t, true)
			c := f.begin(t)
			f.apply(t, 3, f.command(2, singlePut(2)))
			if err := f.options.CheckpointGroup.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			fired, restore := durable.InstallCheckpointGroupFaultForFacadeTest(phase)
			err := f.machine.FinishTransitionCapture(c, 3)
			restore()
			if err == nil || !fired() {
				t.Fatalf("seal fault fired=%v err=%v", fired(), err)
			}
			c, machine := reopenCaptureCrashImage(t, f)
			d, err := c.Descriptor()
			if err != nil || d.Abort != NotAborted || d.Head.Publication.Applied != 3 || machine.Applied() != 3 {
				t.Fatalf("skewed recovered seal %+v err=%v", d, err)
			}
			raw, err := replication.AppendCommand(nil, f.command(3, singlePut(3)))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := machine.ApplyNormal(raftmodel.ApplyMeta{Index: 4, Term: 1, Type: pb.EntryNormal}, raw); err != nil {
				t.Fatal(err)
			}
			after, err := c.Descriptor()
			if err != nil {
				t.Fatal(err)
			}
			if d.SealDigest != [32]byte{} {
				if after != d {
					t.Fatal("recovered closed stream changed")
				}
			} else if after.Head.Publication.Applied != 4 || after.SealDigest != [32]byte{} {
				t.Fatal("uncommitted seal stopped recovery capture")
			}
		})
	}
}

func TestCaptureRecoveryRejectsMissingOrForeignEntries(t *testing.T) {
	for _, mode := range []string{"missing", "chain", "operation", "after_terminal"} {
		t.Run(mode, func(t *testing.T) {
			f := newCaptureFixture(t, false)
			if mode == "after_terminal" {
				f.config.MaxRecords = 1
			}
			f.begin(t)
			f.apply(t, 3, f.command(2, singlePut(2)))
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], 3)
			if mode == "operation" {
				f.config.Operation[0]++
			} else {
				raw, _, err := f.target.Collection.AppendRaw(nil, key[:])
				if err != nil {
					t.Fatal(err)
				}
				if mode == "chain" {
					raw[232]++
					digest := recordDigest(raw[:len(raw)-32])
					copy(raw[len(raw)-32:], digest[:])
				}
				if mode == "after_terminal" {
					binary.BigEndian.PutUint64(key[:], 4)
				}
				if err := f.target.Collection.Update(func(b *durable.WriteBatch) error {
					if mode == "missing" {
						return b.Delete(key[:])
					}
					return b.Put(key[:], raw)
				}); err != nil {
					t.Fatal(err)
				}
			}
			c, err := NewSourceCapture(f.config, f.target)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := f.machine.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			state := snapshot.State()
			if err := snapshot.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c.Begin(state, nil); err == nil {
				t.Fatal("accepted invalid recovered capture")
			}
		})
	}
}

func FuzzCaptureHeader(f *testing.F) {
	config := CaptureConfig{Operation: [32]byte{1}, PlanDigest: [32]byte{2}, BindingDigest: [32]byte{3}, ManifestDigest: [32]byte{4}, SchemaGeneration: 1, MaxRecords: 100, MaxBytes: 1 << 20}
	base := Publication{Applied: 1, Term: 1, Ownership: 1, Routing: 1, Route: 1, EntryDigest: [32]byte{5}, DataDigest: [32]byte{6}}
	raw, err := appendHeader(nil, config, base)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add(raw[:40])
	f.Fuzz(func(t *testing.T, raw []byte) {
		config, base, _, err := openHeader(raw)
		if err != nil {
			return
		}
		encoded, err := appendHeader(nil, config, base)
		if err != nil || !bytes.Equal(encoded, raw) {
			t.Fatal("header accepted noncanonical encoding")
		}
	})
}

func FuzzCaptureEntry(f *testing.F) {
	fixture := newCaptureFixture(f, false)
	fixture.begin(f)
	fixture.apply(f, 3, fixture.command(2, singlePut(2)))
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], 3)
	raw, _, err := fixture.target.Collection.AppendRaw(nil, key[:])
	if err != nil {
		f.Fatal(err)
	}
	f.Add(raw)
	f.Add(raw[:40])
	f.Fuzz(func(t *testing.T, raw []byte) {
		var w CaptureWorkspace
		entry, err := openEntry(raw, &w)
		if err != nil {
			return
		}
		if len(entry.Mutations) > maxMutations || entry.After.Applied != entry.Before.Applied+1 ||
			entry.Abort != NotAborted && len(entry.Mutations) != 0 {
			t.Fatal("invalid entry accepted")
		}
		for _, m := range entry.Mutations {
			if len(m.Key) == 0 || cap(m.Key) != len(m.Key) || cap(m.After) != len(m.After) {
				t.Fatal("unbounded borrowed mutation")
			}
		}
	})
}
