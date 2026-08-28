package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/store/durable"
)

type postgresWriteServiceStub struct {
	writes, replays, acks       int
	identity                    durableExecBatchIdentity
	queries                     []gateway.Query
	unknown, ackUnknown, refuse bool
}

type postgresLeaderChangeStub struct{ postgresWriteServiceStub }

type postgresAckLeaderChangeStub struct{ postgresWriteServiceStub }

func (s *postgresAckLeaderChangeStub) AckExecBatch(ctx context.Context, authority serviceauthz.Authority, ack durableExecBatchAckWireRequest) (durableExecBatchAckWireResponse, error) {
	if s.acks < 2 {
		s.acks++
		return durableExecBatchAckWireResponse{}, gateway.ErrReplicatedLeader
	}
	return s.postgresWriteServiceStub.AckExecBatch(ctx, authority, ack)
}

func TestPostgreSQLWriteRecoversPreviousACKBeforeNextInsert(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	s := &postgresAckLeaderChangeStub{}
	w, err := openPostgresDurableWriter(filepath.Join(t.TempDir(), "writes"), authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, sql := range []string{"INSERT INTO employees (id) VALUES ('1')", "INSERT INTO employees (id) VALUES ('2')"} {
		if r, err := w.Write(t.Context(), authority, gateway.Query{SQL: sql}); err != nil || r == nil || r.RowsAffected != 2 {
			t.Fatalf("insert=%+v err=%v", r, err)
		}
	}
	if s.writes != 2 || s.replays != 0 || s.acks != 4 || w.record.Query != nil || w.record.Sequence != 3 {
		t.Fatalf("ACK recovery repeated execution: writes=%d replays=%d acks=%d record=%+v", s.writes, s.replays, s.acks, w.record)
	}
}

func (s *postgresLeaderChangeStub) ExecBatch(ctx context.Context, authority serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (durableExecBatchExecuteResult, error) {
	_, _ = s.postgresWriteServiceStub.ExecBatch(ctx, authority, id, q)
	return durableExecBatchExecuteResult{}, gateway.ErrReplicatedLeader
}

func TestPostgreSQLWriteLeaderChangeResolvesSameIdentity(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	s := &postgresLeaderChangeStub{}
	w, err := openPostgresDurableWriter(filepath.Join(t.TempDir(), "writes"), authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	r, err := w.Write(t.Context(), authority, gateway.Query{SQL: "INSERT INTO employees (id) VALUES ('1')"})
	if err != nil || r == nil || r.RowsAffected != 2 || s.writes != 1 || s.replays != 1 || s.acks != 1 || w.record.Query != nil {
		t.Fatalf("leader change did not resolve exact write: result=%+v err=%v writes=%d replays=%d acks=%d", r, err, s.writes, s.replays, s.acks)
	}
}

func (s *postgresWriteServiceStub) OpenIssuer(_ context.Context, _ serviceauthz.Authority, open gateway.ReplicatedIssuerOpen) (gateway.ReplicatedIssuerLaneGrant, error) {
	return gateway.ReplicatedIssuerLaneGrant{Installation: open.Installation, Epoch: open.Epoch, LaneOrdinal: open.LaneOrdinal, GrantDigest: replication.Digest{9}}, nil
}
func (s *postgresWriteServiceStub) result() durableExecBatchExecuteResult {
	return durableExecBatchExecuteResult{Result: &gateway.Result{Kind: shardservice.ResponseCompletion, RowsAffected: 2}, Ack: durableExecBatchAckWireRequest{
		Identity: durableExecBatchAckIdentity{RequestID: s.identity.RequestID, Reference: s.identity.Reference, IssuerSequence: s.identity.IssuerSequence, RequestDigest: replication.Digest{3}}, TerminalRevision: 1, ResultDigest: replication.Digest{4}, AckToken: requestledger.AckToken{5},
	}}
}
func (s *postgresWriteServiceStub) ExecBatch(_ context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (durableExecBatchExecuteResult, error) {
	s.writes++
	s.identity, s.queries = id, q
	if s.refuse {
		return durableExecBatchExecuteResult{}, gateway.ErrDurableSQLNotAdmitted
	}
	if s.unknown {
		return durableExecBatchExecuteResult{}, gateway.ErrDurableRequestUnresolved
	}
	return s.result(), nil
}
func (s *postgresWriteServiceStub) ReplayBatch(_ context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, q []gateway.Query) (durableExecBatchExecuteResult, bool, error) {
	s.replays++
	if id != s.identity || !reflect.DeepEqual(q, s.queries) {
		return durableExecBatchExecuteResult{}, false, errors.New("retry changed request")
	}
	return s.result(), true, nil
}
func (s *postgresWriteServiceStub) AckExecBatch(_ context.Context, _ serviceauthz.Authority, ack durableExecBatchAckWireRequest) (durableExecBatchAckWireResponse, error) {
	s.acks++
	if s.ackUnknown {
		return durableExecBatchAckWireResponse{}, gateway.ErrDurableRequestUnresolved
	}
	if ack != s.result().Ack {
		return durableExecBatchAckWireResponse{}, errors.New("ACK changed")
	}
	return durableExecBatchAckWireResponse{durableExecBatchAckWireRequest: ack}, nil
}

func TestPostgreSQLWriteJournalRestartResolvesExactUnknownRequest(t *testing.T) {
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 1
	path := filepath.Join(t.TempDir(), "writes")
	s := &postgresWriteServiceStub{unknown: true}
	w, err := openPostgresDurableWriter(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	q := gateway.Query{SQL: "INSERT INTO documents (id,value) VALUES (?,?)", Params: []shardservice.Param{shardservice.StringParam("a"), shardservice.StringParam("b")}, Class: gateway.ClassBatch}
	if _, err = w.Write(t.Context(), authority, q); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("unknown=%v", err)
	}
	id := s.identity
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = openPostgresDurableWriter(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	result, err := w.resolve(t.Context(), false)
	if err != nil || result.RowsAffected != 2 || s.writes != 1 || s.replays != 1 || s.identity != id || w.record.Sequence != 2 || w.record.Query != nil {
		t.Fatalf("recovery result=%+v err=%v writes=%d replay=%d record=%+v", result, err, s.writes, s.replays, w.record)
	}
}

func TestPostgreSQLWriteJournalRetainsACKWithoutReexecuting(t *testing.T) {
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 2
	path := filepath.Join(t.TempDir(), "writes")
	s := &postgresWriteServiceStub{ackUnknown: true}
	w, err := openPostgresDurableWriter(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	result, err := w.Write(t.Context(), authority, gateway.Query{SQL: "DELETE FROM documents WHERE id='a'"})
	if err != nil || result.RowsAffected != 2 || w.record.Ack == nil {
		t.Fatalf("known success lost: %+v %v", result, err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = openPostgresDurableWriter(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	s.ackUnknown = false
	if _, err = w.resolve(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if s.writes != 1 || s.replays != 0 || s.acks != 2 || w.record.Query != nil {
		t.Fatalf("ACK replay dispatched SQL: %+v", s)
	}
}

func TestPostgreSQLWriteJournalRefusalDoesNotConsumeSequence(t *testing.T) {
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 3
	s := &postgresWriteServiceStub{refuse: true}
	w, err := openPostgresDurableWriter(filepath.Join(t.TempDir(), "writes"), authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Write(t.Context(), authority, gateway.Query{SQL: "bad"})
	if !errors.Is(err, gateway.ErrDurableSQLNotAdmitted) || w.record.Sequence != 1 || w.record.Query != nil {
		t.Fatalf("refusal leaked: %v %+v", err, w.record)
	}
	first := s.identity
	s.refuse = false
	if _, err = w.Write(t.Context(), authority, gateway.Query{SQL: "good"}); err != nil {
		t.Fatal(err)
	}
	if s.identity.RequestID == first.RequestID || s.identity.IssuerSequence != first.IssuerSequence {
		t.Fatal("wrong refusal retry identity")
	}
}

func TestPostgreSQLWriteJournalRejectsConcurrentOwnerAndCorruption(t *testing.T) {
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 4
	path := filepath.Join(t.TempDir(), "writes")
	s := &postgresWriteServiceStub{}
	w, err := openPostgresDurableWriter(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := openPostgresDurableWriter(path, authority, s); err == nil {
		other.Close()
		t.Fatal("second owner accepted")
	}
	w.Close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 1
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if other, err := openPostgresDurableWriter(path, authority, s); err == nil {
		other.Close()
		t.Fatal("corrupt authority replaced")
	}
}
