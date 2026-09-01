package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

type postgresWriteServiceStub struct {
	writes, replays, acks       int
	identity                    durableExecBatchIdentity
	queries                     []gateway.Query
	unknown, ackUnknown, refuse bool
	direct                      bool
}

type postgresLeaderChangeStub struct{ postgresWriteServiceStub }

type postgresAckLeaderChangeStub struct{ postgresWriteServiceStub }

func TestPostgreSQLWriteJournalUntypedV1Golden(t *testing.T) {
	installation := replication.ID128{3}
	reference := gateway.ReplicatedIssuerReference{
		Installation: installation,
		Epoch:        1,
		GrantDigest:  replication.Digest{5},
	}
	identity := durableExecBatchIdentity{
		RequestID:      replication.ID128{6},
		Reference:      reference,
		IssuerSequence: 4,
	}
	query := gateway.Query{
		SQL:    "DELETE FROM employees WHERE id=?",
		Params: []shardservice.Param{shardservice.StringParam("a")},
		Class:  gateway.ClassBatch,
	}
	record := postgresWriteRecord{
		Version:      postgresWriteJournalVersionUntyped,
		Table:        "employees",
		Authority:    serviceauthz.Authority{Node: [16]byte{1}, Generation: 2},
		Installation: installation,
		Sequence:     4,
		Reference:    reference,
		Identity:     identity,
		Query:        &query,
	}
	raw, err := vibejson.Marshal(&record)
	if err != nil {
		t.Fatal(err)
	}
	const golden = `{"Version":1,"Table":"employees","Authority":{"Node":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Generation":2},"Installation":[3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Sequence":4,"Reference":{"Installation":[3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Epoch":1,"LaneOrdinal":0,"GrantDigest":[5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"Identity":{"RequestID":[6,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Reference":{"Installation":[3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Epoch":1,"LaneOrdinal":0,"GrantDigest":[5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"IssuerSequence":4},"Query":{"SQL":"DELETE FROM employees WHERE id=?","Params":[{"Kind":4,"Bool":false,"Bytes":"YQ=="}],"Class":1},"Ack":null}`
	if !bytes.Equal(raw, []byte(golden)) {
		t.Fatalf("update only for an intentional V1 format change:\n%s", raw)
	}
	emptyTypesQuery := query
	emptyTypesQuery.ParamTypes = []driver.ParamType{}
	emptyTypesRecord := record
	emptyTypesRecord.Query = &emptyTypesQuery
	emptyTypesRaw, err := vibejson.Marshal(&emptyTypesRecord)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(emptyTypesRaw, raw) {
		t.Fatalf("empty type metadata changed V1 bytes:\n%s", emptyTypesRaw)
	}
	path := filepath.Join(t.TempDir(), "writes")
	writePostgresJournalFixture(t, path, raw)
	service := &postgresWriteServiceStub{
		identity: identity,
		queries:  []gateway.Query{query},
	}
	writer, err := openPostgresDurableWriter(path, record.Authority, service, record.Table)
	if err != nil {
		t.Fatalf("open deployed V1 journal: %v", err)
	}
	defer writer.Close()
	result, err := writer.resolve(t.Context(), false)
	if err != nil || result == nil || result.RowsAffected != 2 ||
		service.replays != 1 || service.writes != 0 || writer.record.Query != nil ||
		writer.record.Version != postgresWriteJournalVersionUntyped {
		t.Fatalf("recover deployed V1 journal: result=%+v err=%v service=%+v record=%+v", result, err, service, writer.record)
	}
}

func writePostgresJournalFixture(t testing.TB, path string, payload []byte) {
	t.Helper()
	digest := sha256.Sum256(payload)
	journal := make([]byte, 0, sha256.Size+len(payload))
	journal = append(journal, digest[:]...)
	journal = append(journal, payload...)
	if err := os.WriteFile(path, journal, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestPostgreSQLWriteJournalTypedRestartPreservesDomain(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{7}, Generation: 1}
	path := filepath.Join(t.TempDir(), "writes")
	service := &postgresWriteServiceStub{unknown: true}
	writer, err := openPostgresDurableWriter(path, authority, service)
	if err != nil {
		t.Fatal(err)
	}
	employeeID := []byte("employee-1")
	query := gateway.Query{
		SQL: "UPDATE employees SET active=? WHERE id=?",
		Params: []shardservice.Param{
			shardservice.NullParam(),
			shardservice.StringBytesParam(employeeID),
		},
		ParamTypes: []driver.ParamType{driver.ParamTypeBool, driver.ParamTypeText},
		Class:      gateway.ClassBatch,
	}
	if _, err = writer.Write(t.Context(), authority, query); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("stage typed unknown: %v", err)
	}
	if writer.record.Version != postgresWriteJournalVersionTyped ||
		writer.record.Query == nil || !reflect.DeepEqual(writer.record.Query.ParamTypes, query.ParamTypes) {
		t.Fatalf("typed request did not select V2: %+v", writer.record)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= sha256.Size ||
		!bytes.Contains(raw[sha256.Size:], []byte(`"Version":2`)) ||
		!bytes.Contains(raw[sha256.Size:], []byte(`"ParamTypes":"AQI="`)) {
		t.Fatalf("typed metadata absent from V2 journal: %s", raw[sha256.Size:])
	}
	// The journal owns the PostgreSQL bind buffers and the metadata vector.
	employeeID[0] = 'X'
	query.ParamTypes[0] = driver.ParamTypeText
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	service.unknown = false
	writer, err = openPostgresDurableWriter(path, authority, service)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if writer.record.Query == nil ||
		writer.record.Query.ParamTypes[0] != driver.ParamTypeBool ||
		string(writer.record.Query.Params[1].Bytes) != "employee-1" {
		t.Fatalf("caller mutation reached journal: %+v", writer.record.Query)
	}
	result, err := writer.resolve(t.Context(), false)
	if err != nil || result == nil || result.RowsAffected != 2 ||
		service.replays != 1 || service.writes != 1 ||
		writer.record.Query != nil || writer.record.Version != postgresWriteJournalVersionUntyped {
		t.Fatalf("typed recovery: result=%+v err=%v service=%+v record=%+v", result, err, service, writer.record)
	}
}

func TestPostgreSQLWriteJournalRejectsUnsafeTypedVersions(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{8}, Generation: 1}
	installation := replication.ID128{9}
	reference := gateway.ReplicatedIssuerReference{
		Installation: installation,
		Epoch:        1,
		GrantDigest:  replication.Digest{10},
	}
	valid := func() postgresWriteRecord {
		return postgresWriteRecord{
			Version:      postgresWriteJournalVersionTyped,
			Authority:    authority,
			Installation: installation,
			Sequence:     1,
			Reference:    reference,
			Identity: durableExecBatchIdentity{
				RequestID:      replication.ID128{11},
				Reference:      reference,
				IssuerSequence: 1,
			},
			Query: &gateway.Query{
				SQL: "UPDATE employees SET active=? WHERE id=?",
				Params: []shardservice.Param{
					shardservice.NullParam(),
					shardservice.StringParam("employee-1"),
				},
				ParamTypes: []driver.ParamType{driver.ParamTypeBool, driver.ParamTypeText},
				Class:      gateway.ClassBatch,
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(*postgresWriteRecord)
	}{
		{"v1_with_types", func(record *postgresWriteRecord) {
			record.Version = postgresWriteJournalVersionUntyped
		}},
		{"v2_without_types", func(record *postgresWriteRecord) {
			record.Query.ParamTypes = nil
		}},
		{"all_unspecified", func(record *postgresWriteRecord) {
			record.Query.ParamTypes = []driver.ParamType{
				driver.ParamTypeUnspecified, driver.ParamTypeUnspecified,
			}
		}},
		{"count_mismatch", func(record *postgresWriteRecord) {
			record.Query.ParamTypes = record.Query.ParamTypes[:1]
		}},
		{"invalid_enum", func(record *postgresWriteRecord) {
			record.Query.ParamTypes[0] = driver.ParamTypeInvalid
		}},
		{"typed_document", func(record *postgresWriteRecord) {
			record.Query.Params[0] = shardservice.DocumentParam(`{"active":true}`)
		}},
		{"invalid_payload", func(record *postgresWriteRecord) {
			record.Query.Params[1] = shardservice.StringBytesParam([]byte{0xff})
		}},
		{"future_version", func(record *postgresWriteRecord) {
			record.Version = postgresWriteJournalVersionTyped + 1
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := valid()
			test.mutate(&record)
			payload, err := vibejson.Marshal(&record)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "writes")
			writePostgresJournalFixture(t, path, payload)
			writer, err := openPostgresDurableWriter(path, authority, &postgresWriteServiceStub{})
			if err == nil {
				_ = writer.Close()
				t.Fatal("accepted unsafe typed journal")
			}
			if !errors.Is(err, errInvalidDurableRequestAdapter) {
				t.Fatalf("wrong rejection: %v", err)
			}
		})
	}
}

func TestPostgreSQLWriteUnknownRecoversWithoutReopen(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	s := &postgresWriteServiceStub{unknown: true}
	w, err := openPostgresDurableWriter(filepath.Join(t.TempDir(), "writes"), authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	_, err = w.Write(t.Context(), authority, gateway.Query{SQL: "INSERT INTO employees (id) VALUES ('1')"})
	id := s.identity
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || !errors.Is(err, gateway.ErrDurableRequestUnresolved) ||
		!strings.Contains(err.Error(), fmt.Sprintf("%x", id.RequestID)) ||
		!strings.Contains(err.Error(), "automatic recovery") || strings.Contains(err.Error(), "reopen required") {
		t.Fatalf("incorrect recovery instruction or lost cause: %v", err)
	}
	// The same writer, not a reopened storage handle or client connection,
	// resolves exactly the retained request before accepting another write.
	s.unknown = false
	result, err := w.Write(t.Context(), authority, gateway.Query{SQL: "INSERT INTO employees (id) VALUES ('2')"})
	if err != nil || result == nil || s.replays != 1 || s.writes != 2 || s.acks != 2 ||
		w.record.Query != nil || w.record.Sequence != 3 || s.identity.RequestID == id.RequestID {
		t.Fatalf("same-writer recovery: result=%+v err=%v writes=%d replays=%d acks=%d", result, err, s.writes, s.replays, s.acks)
	}
}

func TestPostgreSQLWriteOutcomeMessageDistinguishesStorageRecovery(t *testing.T) {
	root := errors.New("journal sync failed")
	w := &postgresDurableWriter{poison: root}
	err := w.outcomeError(replication.ID128{7}, root, false)
	if !errors.Is(err, root) || !errors.Is(err, durable.ErrCommitOutcomeUnknown) ||
		!strings.Contains(err.Error(), "server-side storage recovery") || strings.Contains(err.Error(), "automatic recovery") {
		t.Fatalf("poisoned outbox misreported: %v", err)
	}
}

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
	if s.direct {
		return durableExecBatchExecuteResult{
			Result: &gateway.Result{Kind: shardservice.ResponseCompletion, RowsAffected: 2},
			Direct: true,
		}
	}
	return durableExecBatchExecuteResult{Result: &gateway.Result{Kind: shardservice.ResponseCompletion, RowsAffected: 2}, Ack: durableExecBatchAckWireRequest{
		Identity: durableExecBatchAckIdentity{RequestID: s.identity.RequestID, Reference: s.identity.Reference, IssuerSequence: s.identity.IssuerSequence, RequestDigest: replication.Digest{3}}, TerminalRevision: 1, ResultDigest: replication.Digest{4}, AckToken: requestledger.AckToken{5},
	}}
}

func TestPostgreSQLDirectWriteAdvancesOutboxWithoutAckRoundTrip(t *testing.T) {
	authority := serviceauthz.Authority{Generation: 1}
	authority.Node[0] = 5
	path := filepath.Join(t.TempDir(), "writes")
	s := &postgresWriteServiceStub{direct: true}
	w, err := openPostgresDurableWriter(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	result, err := w.Write(t.Context(), authority, gateway.Query{SQL: "DELETE FROM documents WHERE id=?"})
	if err != nil || result.RowsAffected != 2 || s.writes != 1 || s.replays != 0 || s.acks != 0 ||
		w.record.Sequence != 2 || w.record.Query != nil || w.record.Ack != nil ||
		w.record.Version != postgresWriteJournalVersionUntyped {
		t.Fatalf("direct write result=%+v err=%v service=%+v record=%+v", result, err, s, w.record)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = openPostgresDurableWriter(path, authority, s)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if w.record.Sequence != 2 || w.record.Query != nil || w.record.Ack != nil {
		t.Fatalf("reopened direct outbox=%+v", w.record)
	}
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
	result, err := w.Write(t.Context(), authority, gateway.Query{
		SQL:        "DELETE FROM documents WHERE id=?",
		Params:     []shardservice.Param{shardservice.StringParam("a")},
		ParamTypes: []driver.ParamType{driver.ParamTypeText},
	})
	if err != nil || result.RowsAffected != 2 || w.record.Ack == nil ||
		w.record.Version != postgresWriteJournalVersionTyped {
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
	if w.record.Version != postgresWriteJournalVersionTyped ||
		w.record.Query == nil || w.record.Query.ParamTypes[0] != driver.ParamTypeText {
		t.Fatalf("typed ACK journal lost metadata: %+v", w.record)
	}
	s.ackUnknown = false
	if _, err = w.resolve(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if s.writes != 1 || s.replays != 0 || s.acks != 2 || w.record.Query != nil ||
		w.record.Version != postgresWriteJournalVersionUntyped {
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
