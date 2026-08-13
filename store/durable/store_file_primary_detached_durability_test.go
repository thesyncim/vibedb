package durable

import (
	"errors"
	"fmt"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestPrimaryDetachedDurabilityPutDeleteParity(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
		CheckpointStrength: CheckpointFilesystem,
	}
	immediate, immediateKeys, _ := openPreparedPrimaryTestCollection(
		t, "detached-parity-immediate.vibe", options,
	)
	detached, detachedKeys, _ := openPreparedPrimaryTestCollection(
		t, "detached-parity-apply.vibe", options,
	)
	raw := []byte(`{ "detached": true, "n": 7 }`)

	immediateCreated, immediateErr := immediate.putPrimaryWithSplit(
		[]byte(immediateKeys[31]), raw,
	)
	detachedCreated, continuation, applyErr :=
		detached.putPrimaryWithSplitDetached([]byte(detachedKeys[31]), raw)
	if !continuation.pending() {
		t.Fatal("detached Put did not register journal durability")
	}
	detachedErr := detached.awaitPrimaryMutationDurability(
		continuation, applyErr,
	)
	if immediateCreated != detachedCreated ||
		!samePrimaryDetachedError(immediateErr, detachedErr) {
		t.Fatalf(
			"Put immediate=(%v,%v) detached=(%v,%v)",
			immediateCreated, immediateErr, detachedCreated, detachedErr,
		)
	}

	immediateDeleted, immediateErr := immediate.deletePrimaryWithEmptyReclaim(
		[]byte(immediateKeys[31]),
	)
	detachedDeleted, continuation, applyErr := detached.deletePrimaryDetached(
		[]byte(detachedKeys[31]),
	)
	if !continuation.pending() {
		t.Fatal("detached Delete did not register journal durability")
	}
	detachedDeleted, detachedErr = detached.awaitDeletePrimaryWithEmptyReclaim(
		[]byte(detachedKeys[31]), detachedDeleted, continuation, applyErr,
	)
	if immediateDeleted != detachedDeleted ||
		!samePrimaryDetachedError(immediateErr, detachedErr) {
		t.Fatalf(
			"Delete immediate=(%v,%v) detached=(%v,%v)",
			immediateDeleted, immediateErr, detachedDeleted, detachedErr,
		)
	}
	if immediate.Stats().JournalAcks != detached.Stats().JournalAcks ||
		immediate.Stats().JournalSyncs != detached.Stats().JournalSyncs {
		t.Fatalf(
			"durability telemetry immediate acks/syncs=%d/%d detached=%d/%d",
			immediate.Stats().JournalAcks, immediate.Stats().JournalSyncs,
			detached.Stats().JournalAcks, detached.Stats().JournalSyncs,
		)
	}
}

func samePrimaryDetachedError(left, right error) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Error() == right.Error()
}

func TestPrimaryDetachedDurabilityRegistersBeforeCloseWait(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
		CheckpointStrength: CheckpointFilesystem,
	}
	collection, _, _ := openPreparedPrimaryTestCollection(
		t, "detached-close-add.vibe", options,
	)
	created, continuation, applyErr := collection.putPrimaryWithSplitDetached(
		[]byte("detached-close-add"), []byte(`{"v":1}`),
	)
	if applyErr != nil || !created || !continuation.pending() {
		t.Fatalf(
			"detached apply = created %v continuation %+v err %v",
			created, continuation, applyErr,
		)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- collection.Close() }()
	deadline := time.Now().Add(concurrentPrimaryTestTimeout)
	for {
		collection.writer.Lock()
		closed := collection.closed
		collection.writer.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close never acquired writer after detached apply")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close passed durabilityWait before continuation await: %v", err)
	default:
	}
	if err := collection.awaitPrimaryMutationDurability(
		continuation, applyErr,
	); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("Close remained blocked after continuation Done")
	}
}

func TestPrimaryDetachedPublishedDurabilityParityAndCloseWait(t *testing.T) {
	openChainFence := func(name string) (*Collection, []string) {
		t.Helper()
		built, keys, _ := buildFilePrimaryCorpus(t, 256)
		options := Options{
			Backend: BackendPortable, ResidentBytes: 32 << 20,
			Durability: DurabilityAsyncVisible,
		}
		file := createPrimaryPointFile(t, built, options, name)
		options.Durability = DurabilitySync
		collection, err := Open(file, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = collection.Close() })
		if !collection.chainFenceSync() || collection.journalEnabled() {
			t.Fatal("async primary did not reopen on physical chain-fence sync")
		}
		return collection, keys
	}

	immediate, immediateKeys := openChainFence("detached-published-immediate.vibe")
	detached, detachedKeys := openChainFence("detached-published-apply.vibe")
	raw := []byte(`{ "published": true, "n": 11 }`)
	immediateCreated, immediateErr := immediate.putPrimaryWithSplit(
		[]byte(immediateKeys[31]), raw,
	)
	detachedKey := []byte(detachedKeys[31])
	detachedCreated, continuation, applyErr :=
		detached.putPrimaryWithSplitDetached(detachedKey, raw)
	detachedOwned := continuation.pending()
	t.Cleanup(func() {
		if detachedOwned {
			_ = detached.awaitPrimaryMutationDurability(continuation, applyErr)
		}
	})
	if continuation.kind != primaryMutationDurabilityPublished ||
		continuation.target == 0 {
		t.Fatalf("detached physical continuation = %+v", continuation)
	}
	published := detached.committer.PublishedGeneration()
	if published < continuation.target {
		t.Fatalf("published generation %d < target %d", published, continuation.target)
	}
	if durable := detached.committer.DurableGeneration(); durable > published {
		t.Fatalf("durable generation %d > published %d", durable, published)
	}
	detachedErr := detached.awaitPrimaryMutationDurability(
		continuation, applyErr,
	)
	detachedOwned = false
	if durable := detached.committer.DurableGeneration(); durable < continuation.target {
		t.Fatalf("durable generation %d < target %d", durable, continuation.target)
	}
	if got, found, err := detached.AppendRaw(nil, detachedKey); err != nil ||
		!found || string(got) != `{"n":11,"published":true}` {
		t.Fatalf("durable detached value = (%s,%v,%v)", got, found, err)
	}
	if immediateCreated != detachedCreated ||
		!samePrimaryDetachedError(immediateErr, detachedErr) {
		t.Fatalf(
			"physical Put immediate=(%v,%v) detached=(%v,%v)",
			immediateCreated, immediateErr, detachedCreated, detachedErr,
		)
	}

	closeCollection, closeKeys := openChainFence("detached-published-close.vibe")
	_, closeContinuation, closeApplyErr :=
		closeCollection.putPrimaryWithSplitDetached([]byte(closeKeys[32]), raw)
	closeOwned := closeContinuation.pending()
	t.Cleanup(func() {
		if closeOwned {
			_ = closeCollection.awaitPrimaryMutationDurability(
				closeContinuation, closeApplyErr,
			)
		}
	})
	if closeContinuation.kind != primaryMutationDurabilityPublished {
		t.Fatalf("detached Close continuation = %+v", closeContinuation)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- closeCollection.Close() }()
	deadline := time.Now().Add(concurrentPrimaryTestTimeout)
	for {
		closeCollection.writer.Lock()
		closed := closeCollection.closed
		closeCollection.writer.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close never acquired writer after detached physical apply")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close passed physical durability continuation: %v", err)
	default:
	}
	closeErr := closeCollection.awaitPrimaryMutationDurability(
		closeContinuation, closeApplyErr,
	)
	closeOwned = false
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("Close remained blocked after physical continuation Done")
	}
}

func TestPrimaryDetachedDeleteReclaimRunsAfterAcknowledgement(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
		CheckpointStrength: CheckpointFilesystem,
	}
	collection, _, _ := openPreparedPrimaryTestCollection(
		t, "detached-delete-reclaim.vibe", options,
	)
	// Remove every row except one using the compatibility path so the final
	// detached Delete marks exactly one empty route.
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var keys [][]byte
	err = snapshot.RangeRaw(func(key, _ []byte) error {
		keys = append(keys, append([]byte(nil), key...))
		return nil
	})
	_ = snapshot.Close()
	if err != nil || len(keys) < 2 {
		t.Fatalf("seed keys=%d err=%v", len(keys), err)
	}
	for _, key := range keys[1:] {
		if _, err := collection.deletePrimaryWithEmptyReclaim(key); err != nil {
			t.Fatal(err)
		}
	}

	entered, release := blockNextRecoveryJournalGroupSync(t)
	deleted, continuation, applyErr := collection.deletePrimaryDetached(keys[0])
	if applyErr != nil || !deleted || !continuation.pending() {
		t.Fatalf(
			"final Delete apply = deleted %v continuation %+v err %v",
			deleted, continuation, applyErr,
		)
	}
	beforeRoutes := collection.primaryRouter.Load().Len()
	finishDone := make(chan error, 1)
	go func() {
		_, finishErr := collection.awaitDeletePrimaryWithEmptyReclaim(
			keys[0], deleted, continuation, applyErr,
		)
		finishDone <- finishErr
	}()
	select {
	case <-entered:
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("detached Delete never entered journal Sync")
	}
	if got := collection.primaryRouter.Load().Len(); got != beforeRoutes {
		t.Fatalf("empty reclaim ran before acknowledgement: routes %d -> %d", beforeRoutes, got)
	}
	release()
	select {
	case err := <-finishDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(concurrentPrimaryTestTimeout):
		t.Fatal("detached Delete did not finish after acknowledgement")
	}
}

func TestPrimaryDetachedSplitSignalHasNoContinuation(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible,
	}
	collection, _, _ := openPreparedPrimaryTestCollection(
		t, "detached-split-invariant.vibe", options,
	)
	for at := 0; at < 4096; at++ {
		key := []byte(fmt.Sprintf("detached-split-%06d", at))
		created, continuation, err := collection.putPrimaryDetached(
			key, primaryStructuralSplitValue(at),
		)
		if errors.Is(err, ErrPrimaryLeafSplitRequired) {
			if continuation.pending() {
				t.Fatalf("split signal carried continuation %+v", continuation)
			}
			return
		}
		if err := collection.awaitPrimaryMutationDurability(
			continuation, err,
		); err != nil || !created {
			t.Fatalf("detached direct Put %d = created %v err %v", at, created, err)
		}
	}
	t.Fatal("direct Put never reached split pressure")
}

func TestPrimaryDetachedGroupTicketCoverage(t *testing.T) {
	for _, failSync := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync-failure-%t", failSync), func(t *testing.T) {
			options := journalTestOptions(CheckpointFilesystem)
			collection, _, fault := openFaultJournalCollection(t, options)
			entered, release := blockNextRecoveryJournalGroupSync(t)

			_, continuation, applyErr := collection.putPrimaryWithSplitDetached(
				[]byte("detached-covered-ticket"), journalValue(401),
			)
			if applyErr != nil || !continuation.pending() {
				t.Fatalf("covered apply continuation=%+v err=%v", continuation, applyErr)
			}
			coveredDone := make(chan error, 1)
			go func() {
				coveredDone <- collection.awaitPrimaryMutationDurability(
					continuation, applyErr,
				)
			}()
			select {
			case <-entered:
			case <-time.After(concurrentPrimaryTestTimeout):
				t.Fatal("covered detached ticket did not reach Sync")
			}

			fault.Program(storeio.JournalFaultPlan{
				Phase:       storeio.JournalFaultENOSPCAppend,
				AppendIndex: fault.Appends(),
			})
			_, appendErr := collection.Put(
				[]byte("detached-later-append"), journalValue(402),
			)
			if appendErr == nil || !errors.Is(appendErr, syscall.ENOSPC) ||
				errors.Is(appendErr, ErrCommitOutcomeUnknown) {
				t.Fatalf("later append error = %v, want definite ENOSPC", appendErr)
			}
			select {
			case err := <-coveredDone:
				t.Fatalf("covered ticket returned before Sync release: %v", err)
			default:
			}
			if failSync {
				fault.Program(storeio.JournalFaultPlan{
					Phase:     storeio.JournalFaultSyncError,
					SyncIndex: fault.Syncs(),
				})
			}
			release()
			select {
			case coveredErr := <-coveredDone:
				if failSync {
					if !errors.Is(coveredErr, ErrCommitOutcomeUnknown) ||
						!errors.Is(coveredErr, syscall.EIO) ||
						errors.Is(coveredErr, syscall.ENOSPC) {
						t.Fatalf("failed covered ticket = %v", coveredErr)
					}
				} else if coveredErr != nil {
					t.Fatalf("successful covered ticket = %v", coveredErr)
				}
			case <-time.After(concurrentPrimaryTestTimeout):
				t.Fatal("covered detached ticket did not resolve")
			}
		})
	}
}

func TestPrimaryDetachedInvalidHasZeroContinuation(t *testing.T) {
	options := Options{
		Backend: BackendPortable, ResidentBytes: 32 << 20,
		Durability: DurabilityBufferedVisible, RecoveryJournal: true,
	}
	collection, _, _ := openPreparedPrimaryTestCollection(
		t, "detached-invalid.vibe", options,
	)
	for _, test := range []struct {
		name string
		key  []byte
		raw  []byte
		want error
	}{
		{"key", nil, []byte(`{}`), ErrKeyTooLarge},
		{"document", []byte("key"), nil, ErrDocumentTooLarge},
		{"json", []byte("key"), []byte(`{`), io.ErrUnexpectedEOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, continuation, err := collection.putPrimaryDetached(test.key, test.raw)
			if continuation.pending() {
				t.Fatalf("invalid Put carried continuation %+v", continuation)
			}
			if test.name == "json" {
				if err == nil {
					t.Fatal("invalid JSON returned nil")
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("invalid Put error=%v want=%v", err, test.want)
			}
		})
	}
}
