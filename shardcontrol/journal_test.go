package shardcontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type journalTestActions struct{ calls int }

func sameResponse(left, right Response) bool {
	return left.Code == right.Code && left.Operation == right.Operation &&
		left.Step == right.Step && left.ResultDigest == right.ResultDigest &&
		bytes.Equal(left.Payload, right.Payload)
}

type failOnceActions struct{ failed bool }

func (actions *failOnceActions) ExecuteAction(
	ctx context.Context, peer rafttransport.PeerIdentity, request Request,
) (Response, error) {
	if !actions.failed {
		actions.failed = true
		return Response{}, errors.New("crash after durable intent")
	}
	return new(journalTestActions).ExecuteAction(ctx, peer, request)
}

func (actions *journalTestActions) ExecuteAction(
	_ context.Context, _ rafttransport.PeerIdentity, request Request,
) (Response, error) {
	actions.calls++
	payload := []byte(`{"settled":true}`)
	digest := sha256.Sum256(payload)
	return Response{Code: ResultAccepted, Operation: request.Operation, Step: request.Step,
		ResultDigest: digest, Payload: payload}, nil
}

func TestJournalExecutorDurableExactReplayAndConflict(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.journal")
	limits := JournalLimits{MaxRecords: 8, MaxFileBytes: 8 << 20}
	firstActions := new(journalTestActions)
	journal, err := OpenJournalExecutor(path, limits, firstActions)
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest()
	first, err := journal.ExecuteControl(context.Background(), rafttransport.PeerIdentity{}, request)
	if err != nil || firstActions.calls != 1 {
		t.Fatalf("first=%+v calls=%d err=%v", first, firstActions.calls, err)
	}
	replayed, err := journal.ExecuteControl(context.Background(), rafttransport.PeerIdentity{}, request)
	if err != nil || !sameResponse(replayed, first) || firstActions.calls != 1 {
		t.Fatalf("replay=%+v calls=%d err=%v", replayed, firstActions.calls, err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}

	restartActions := new(journalTestActions)
	restarted, err := OpenJournalExecutor(path, limits, restartActions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	afterRestart, err := restarted.ExecuteControl(context.Background(), rafttransport.PeerIdentity{}, request)
	if err != nil || !sameResponse(afterRestart, first) || restartActions.calls != 0 {
		t.Fatalf("restart replay=%+v calls=%d err=%v", afterRestart, restartActions.calls, err)
	}
	conflict := request
	conflict.Payload = []byte(`{"cut":12,"sealed":true}`)
	if _, err = restarted.ExecuteControl(context.Background(), rafttransport.PeerIdentity{}, conflict); !errors.Is(err, ErrJournalConflict) {
		t.Fatalf("conflicting replay = %v", err)
	}
}

func TestJournalExecutorRecoversTornTailAndEnforcesBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.journal")
	limits := JournalLimits{MaxRecords: 1, MaxFileBytes: 8 << 20}
	actions := new(journalTestActions)
	journal, err := OpenJournalExecutor(path, limits, actions)
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest()
	if _, err = journal.ExecuteControl(context.Background(), rafttransport.PeerIdentity{}, request); err != nil {
		t.Fatal(err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenJournalExecutor(path, limits, new(journalTestActions))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	second := request
	second.Operation[0]++
	second.Step[0]++
	if _, err = restarted.ExecuteControl(context.Background(), rafttransport.PeerIdentity{}, second); !errors.Is(err, ErrJournalBound) {
		t.Fatalf("record bound = %v", err)
	}
}

func TestJournalExecutorResumesDurableIntentAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control.journal")
	limits := JournalLimits{MaxRecords: 2, MaxFileBytes: 8 << 20}
	actions := new(failOnceActions)
	journal, err := OpenJournalExecutor(path, limits, actions)
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest()
	if _, err = journal.ExecuteControl(context.Background(), rafttransport.PeerIdentity{}, request); err == nil {
		t.Fatal("action failure was hidden")
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenJournalExecutor(path, limits, actions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	if response, executeErr := restarted.ExecuteControl(
		context.Background(), rafttransport.PeerIdentity{}, request,
	); executeErr != nil || response.Code != ResultAccepted {
		t.Fatalf("resumed response=%+v err=%v", response, executeErr)
	}
}
