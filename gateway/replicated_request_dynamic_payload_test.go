package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

var errDynamicPayloadFault = errors.New("dynamic payload fault")

type dynamicPayloadLedger struct {
	head   requestledger.HeadRecord
	build  requestledger.PayloadBuildRecord
	chunks map[uint64]requestledger.PayloadChunkRecord
	fault  requestledger.Operation
}

func (ledger *dynamicPayloadLedger) ApplyCAS(
	_ context.Context,
	_ DurableRequestLedgerHome,
	_ requestledger.RequestKey,
	cas DurableRequestLifecycleCAS,
) (DurableRequestLifecycleCASResult, error) {
	var err error
	switch cas.Operation {
	case requestledger.OperationBeginPayloadBuild:
		if ledger.build != (requestledger.PayloadBuildRecord{}) {
			if ledger.build.BuildDigest != cas.PayloadBuild.BuildDigest {
				return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
			}
		} else {
			ledger.build = cas.PayloadBuild
		}
	case requestledger.OperationStagePayloadChunk:
		if cas.ExpectedRevision != ledger.build.Revision ||
			cas.PayloadChunk.Ordinal != ledger.build.NextChunkOrdinal {
			return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
		}
		var next requestledger.PayloadBuildRecord
		next, err = requestledger.AdvancePayloadBuild(
			ledger.build, cas.PayloadChunk, cas.Revision,
		)
		if err == nil {
			chunk := cas.PayloadChunk
			chunk.Data = bytes.Clone(chunk.Data)
			ledger.chunks[chunk.Ordinal] = chunk
			ledger.build = next
		}
	case requestledger.OperationSealPayload:
		if cas.ExpectedRevision != ledger.build.Revision {
			return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
		}
		var next requestledger.PayloadBuildRecord
		next, err = requestledger.SealPayloadBuild(ledger.build, cas.Revision)
		if err == nil && next != cas.PayloadBuild {
			err = ErrDurableRequestConflict
		}
		if err == nil {
			ledger.build = next
		}
	case requestledger.OperationCleanupPayload:
		if cas.ExpectedRevision != ledger.head.Revision {
			return DurableRequestLifecycleCASResult{}, ErrDurableRequestConflict
		}
		var chunk requestledger.PayloadCleanupChunk
		chunk, err = requestledger.PlanPayloadCleanup(ledger.head, cas.PayloadCleanup)
		if err == nil {
			for ordinal := chunk.FirstOrdinal; ordinal < chunk.FirstOrdinal+chunk.ChunkCount; ordinal++ {
				delete(ledger.chunks, ordinal)
			}
			ledger.head, err = requestledger.AdvancePayloadCleanup(
				ledger.head, cas.PayloadCleanup, chunk, cas.Revision,
			)
			if chunk.Final {
				ledger.build = requestledger.PayloadBuildRecord{}
			}
		}
	default:
		err = fmt.Errorf("unexpected operation %d", cas.Operation)
	}
	if err != nil {
		return DurableRequestLifecycleCASResult{}, err
	}
	result := DurableRequestLifecycleCASResult{
		Ledger: replicatedstate.RequestLedgerCompletionResult{
			ResultCode: replicatedstate.ResultApplied,
		},
		Applied: max(ledger.head.Revision, ledger.build.Revision),
	}
	if ledger.fault == cas.Operation {
		ledger.fault = requestledger.OperationInvalid
		return DurableRequestLifecycleCASResult{}, errDynamicPayloadFault
	}
	return result, nil
}

func (ledger *dynamicPayloadLedger) ReadRow(
	_ context.Context,
	_ DurableRequestLedgerHome,
	read DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	applied := max(uint64(1), max(ledger.head.Revision, ledger.build.Revision))
	switch read.Kind {
	case replicatedstate.RequestLedgerReadHead:
		return DurableRequestLifecycleRow{
			Applied: applied, Found: true, Kind: read.Kind, Head: ledger.head,
		}, nil
	case replicatedstate.RequestLedgerReadPayloadBuild:
		return DurableRequestLifecycleRow{
			Applied: applied, Found: ledger.build != (requestledger.PayloadBuildRecord{}),
			Kind: read.Kind, PayloadBuild: ledger.build,
		}, nil
	case replicatedstate.RequestLedgerReadPayloadChunk:
		chunk, found := ledger.chunks[read.Ordinal]
		if found && chunk.ContentRoot != read.ContentRoot {
			found = false
		}
		return DurableRequestLifecycleRow{
			Applied: applied, Found: found, Kind: read.Kind, PayloadChunk: chunk,
		}, nil
	default:
		return DurableRequestLifecycleRow{}, ErrDurableRequestConflict
	}
}

func newDynamicPayloadFixture(t testing.TB) (
	*DurableRequestDynamicPayloadStore,
	*dynamicPayloadLedger,
	DurableRequestLedgerHome,
	requestledger.RequestKey,
	[]byte,
	[]byte,
) {
	t.Helper()
	wave, head, _ := lifecycleRunnerFixture(t)
	ledger := &dynamicPayloadLedger{
		head: head, chunks: make(map[uint64]requestledger.PayloadChunkRecord),
	}
	store, err := NewDurableRequestDynamicPayloadStore(ledger)
	if err != nil {
		t.Fatal(err)
	}
	target := bytes.Repeat([]byte{0x71}, 64)
	command := bytes.Repeat([]byte{0x42}, requestledger.MaxPlanPageBytes+31)
	return store, ledger, wave.Home, wave.Key, target, command
}

func TestDurableRequestDynamicPayloadResumesEveryStageBoundary(t *testing.T) {
	for _, operation := range []requestledger.Operation{
		requestledger.OperationBeginPayloadBuild,
		requestledger.OperationStagePayloadChunk,
		requestledger.OperationSealPayload,
	} {
		t.Run(fmt.Sprintf("operation_%d", operation), func(t *testing.T) {
			store, ledger, home, key, target, command := newDynamicPayloadFixture(t)
			ledger.fault = operation
			if _, err := store.Stage(context.Background(), home, key, target, command); !errors.Is(err, errDynamicPayloadFault) {
				t.Fatalf("first stage err=%v", err)
			}
			payload, err := store.Stage(context.Background(), home, key, target, command)
			if err != nil {
				t.Fatal(err)
			}
			if payload.Build.Phase != requestledger.PayloadBuildSealed ||
				!bytes.Equal(payload.Target, target) || !bytes.Equal(payload.Command, command) ||
				len(ledger.chunks) != 2 {
				t.Fatal("resumed payload differs from exact staged bytes")
			}
		})
	}
}

func TestDurableRequestDynamicPayloadReopensExactChunksAndRejectsReplacement(t *testing.T) {
	store, ledger, home, key, target, command := newDynamicPayloadFixture(t)
	payload, err := store.Stage(context.Background(), home, key, target, command)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(
		context.Background(), home, key, payload.Build.BuildDigest, payload.Step, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reopened.Bytes, payload.Bytes) ||
		!bytes.Equal(reopened.Target, target) || !bytes.Equal(reopened.Command, command) {
		t.Fatal("reopened payload changed exact bytes")
	}
	if _, err = store.Stage(context.Background(), home, key, target, append(bytes.Clone(command), 1)); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("replacement err=%v", err)
	}
	chunk := ledger.chunks[1]
	chunk.Data = bytes.Clone(chunk.Data)
	chunk.Data[0] ^= 0xff
	ledger.chunks[1] = chunk
	if _, err = store.Open(context.Background(), home, key, payload.Build.BuildDigest, payload.Step, 1); !errors.Is(err, ErrDurableRequestConflict) {
		t.Fatalf("corrupt reopen err=%v", err)
	}
}

func TestDurableRequestDynamicPayloadCleanupResumesUnknownOutcome(t *testing.T) {
	store, ledger, home, key, target, command := newDynamicPayloadFixture(t)
	payload, err := store.Stage(context.Background(), home, key, target, command)
	if err != nil {
		t.Fatal(err)
	}
	ledger.head.CleanupBuildDigest = payload.Build.BuildDigest
	ledger.head.CleanupChunkCount = payload.Build.ChunkCount
	ledger.head.CleanupTotalDataBytes = payload.Build.TotalBytes
	for ordinal, chunk := range ledger.chunks {
		raw, appendErr := requestledger.AppendPayloadChunk(nil, chunk)
		if appendErr != nil {
			t.Fatal(appendErr)
		}
		keyBytes := requestledger.AppendPayloadChunkKey(
			nil, home.Point, ledger.head.KeyDigest, payload.Build.ContentRoot, ordinal,
		)
		ledger.head.CleanupPayloadBytes += uint64(len(keyBytes) + len(raw))
	}
	buildRaw, err := requestledger.AppendPayloadBuild(nil, payload.Build)
	if err != nil {
		t.Fatal(err)
	}
	ledger.head.CleanupPayloadBytes += uint64(len(requestledger.AppendPayloadBuildKey(
		nil, home.Point, ledger.head.KeyDigest,
	)) + len(buildRaw))
	ledger.fault = requestledger.OperationCleanupPayload
	if _, err = store.Cleanup(context.Background(), home, key); !errors.Is(err, errDynamicPayloadFault) {
		t.Fatalf("first cleanup err=%v", err)
	}
	revision, err := store.Cleanup(context.Background(), home, key)
	if err != nil {
		t.Fatal(err)
	}
	if revision != ledger.head.Revision || ledger.head.CleanupBuildDigest != (requestledger.Digest{}) ||
		ledger.build != (requestledger.PayloadBuildRecord{}) || len(ledger.chunks) != 0 {
		t.Fatal("payload cleanup did not converge after unknown outcome")
	}
}
