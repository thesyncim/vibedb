package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/store/durable"
)

type modeOutcome struct {
	mode   gateway.DurableSQLExecutionMode
	sql    string
	result durableExecBatchExecuteResult
}
type modeWriteService struct {
	postgresWriteServiceStub
	lanes         map[replication.ID128]uint64
	outcomes      map[durableExecBatchIdentity]modeOutcome
	lostReply     bool
	refusals      int
	replayed      []durableExecBatchIdentity
	beforeRefusal func()
}

func (s *modeWriteService) ExecBatchMode(_ context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, qs []gateway.Query, mode gateway.DurableSQLExecutionMode) (durableExecBatchExecuteResult, error) {
	if len(qs) != 1 {
		return durableExecBatchExecuteResult{}, errInvalidDurableRequestAdapter
	}
	if mode == gateway.DurableSQLDirectOnly && qs[0].SQL == "UPDATE docs SET n=n+1 WHERE id='a'" {
		s.refusals++
		if s.beforeRefusal != nil {
			s.beforeRefusal()
		}
		return durableExecBatchExecuteResult{}, errors.Join(gateway.ErrDurableSQLNotAdmitted, gateway.ErrDurableSQLDirectIneligible)
	}
	if s.lanes == nil {
		s.lanes = make(map[replication.ID128]uint64)
		s.outcomes = make(map[durableExecBatchIdentity]modeOutcome)
	}
	if _, ok := s.outcomes[id]; ok {
		return durableExecBatchExecuteResult{}, errors.New("unexpected second execution; recovery should replay")
	}
	if id.IssuerSequence != s.lanes[id.Reference.Installation]+1 {
		return durableExecBatchExecuteResult{}, errors.New("issuer sequence gap")
	}
	s.lanes[id.Reference.Installation] = id.IssuerSequence
	s.identity, s.direct = id, mode == gateway.DurableSQLDirectOnly
	result := s.result()
	s.outcomes[id] = modeOutcome{mode, qs[0].SQL, result}
	s.writes++
	if s.lostReply {
		return durableExecBatchExecuteResult{}, gateway.ErrDurableRequestUnresolved
	}
	return result, nil
}
func (s *modeWriteService) ReplayBatchMode(_ context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, qs []gateway.Query, mode gateway.DurableSQLExecutionMode) (durableExecBatchExecuteResult, bool, error) {
	outcome, found := s.outcomes[id]
	if !found {
		return durableExecBatchExecuteResult{}, false, nil
	}
	if outcome.mode != mode || len(qs) != 1 || outcome.sql != qs[0].SQL {
		return durableExecBatchExecuteResult{}, true, errors.New("recovery changed mode or statement")
	}
	s.replayed = append(s.replayed, id)
	return outcome.result, true, nil
}

func TestPostgreSQLModesAlternateAcrossRestart(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	service := &modeWriteService{}
	path := filepath.Join(t.TempDir(), "outbox")
	for _, sql := range []string{"INSERT INTO docs VALUES ('a',0)", "UPDATE docs SET n=n+1 WHERE id='a'", "DELETE FROM docs WHERE id='b'", "UPDATE docs SET n=n+1 WHERE id='a'"} {
		writer, err := openPostgresDurableWriter(path, authority, service)
		if err != nil {
			t.Fatal(err)
		}
		result, err := writer.Write(t.Context(), authority, gateway.Query{SQL: sql})
		if err != nil || result == nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if writer.record.Version != 3 || writer.record.Query != nil {
			t.Fatalf("record=%+v", writer.record)
		}
		if err = writer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writer, err := openPostgresDurableWriter(path, authority, service)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if writer.record.Sequence != 3 || writer.record.Coordinated == nil || writer.record.Coordinated.Sequence != 3 ||
		writer.record.Coordinated.Installation == writer.record.Installation || service.writes != 4 || service.refusals != 2 {
		t.Fatalf("sequence domains not independent: record=%+v service=%+v", writer.record, service)
	}
}

func TestPostgreSQLModesLostReplyRecoversSameIdentity(t *testing.T) {
	for _, sql := range []string{"INSERT INTO docs VALUES ('a',0)", "UPDATE docs SET n=n+1 WHERE id='a'"} {
		t.Run(sql, func(t *testing.T) {
			authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
			service := &modeWriteService{lostReply: true}
			path := filepath.Join(t.TempDir(), "outbox")
			writer, err := openPostgresDurableWriter(path, authority, service)
			if err != nil {
				t.Fatal(err)
			}
			_, err = writer.Write(t.Context(), authority, gateway.Query{SQL: sql})
			if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || writer.record.Query == nil {
				t.Fatalf("lost reply: %v", err)
			}
			identity, mode := writer.record.Identity, writer.record.Mode
			if err = writer.Close(); err != nil {
				t.Fatal(err)
			}
			service.lostReply = false
			writer, err = openPostgresDurableWriter(path, authority, service)
			if err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			if writer.record.Identity != identity || writer.record.Mode != mode {
				t.Fatal("journal changed recovery identity")
			}
			if _, err = writer.resolve(t.Context(), false); err != nil {
				t.Fatal(err)
			}
			if service.writes != 1 || len(service.replayed) != 1 || service.replayed[0] != identity || writer.record.Query != nil {
				t.Fatalf("re-executed or lost recovery: %+v", service)
			}
		})
	}
}

func TestPostgreSQLModeVersionAndIssuerFences(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	service := &modeWriteService{}
	writer, err := openPostgresDurableWriter(filepath.Join(t.TempDir(), "outbox"), authority, service)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	_, err = writer.Write(t.Context(), authority, gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
	if err != nil {
		t.Fatal(err)
	}
	record := writer.record
	for _, mutate := range []func(*postgresWriteRecord){
		func(r *postgresWriteRecord) { r.Version = 1 },
		func(r *postgresWriteRecord) { r.Mode = 99 },
		func(r *postgresWriteRecord) { r.Coordinated = nil },
		func(r *postgresWriteRecord) {
			lane := *r.Coordinated
			lane.Installation = r.Installation
			r.Coordinated = &lane
		},
		func(r *postgresWriteRecord) { lane := *r.Coordinated; lane.Sequence = 0; r.Coordinated = &lane },
		func(r *postgresWriteRecord) {
			lane := *r.Coordinated
			lane.Reference.Installation = replication.ID128{99}
			r.Coordinated = &lane
		},
	} {
		corrupt := record
		mutate(&corrupt)
		if validPostgresWriteJournalVersion(&corrupt) {
			t.Fatalf("accepted corrupt mode journal: %+v", corrupt)
		}
	}
}

func TestPostgreSQLModeRefusalJournalFailureCannotSwitchLanes(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	path := filepath.Join(t.TempDir(), "outbox")
	service := &modeWriteService{beforeRefusal: func() {
		if err := os.Mkdir(path+".pending", 0700); err != nil {
			t.Fatal(err)
		}
	}}
	writer, err := openPostgresDurableWriter(path, authority, service)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	_, err = writer.Write(t.Context(), authority, gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
	if err == nil || writer.poison == nil || service.writes != 0 || writer.record.Coordinated != nil {
		t.Fatalf("switched lanes after journal failure: %v %+v", err, writer.record)
	}
}

func TestPostgreSQLModeRetainedACKAdvancesOnlyCoordinatedLane(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	service := &modeWriteService{}
	service.ackUnknown = true
	path := filepath.Join(t.TempDir(), "outbox")
	writer, err := openPostgresDurableWriter(path, authority, service)
	if err != nil {
		t.Fatal(err)
	}
	result, err := writer.Write(t.Context(), authority, gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
	if err != nil || result == nil || writer.record.Ack == nil {
		t.Fatalf("lost retained ACK: %v", err)
	}
	id := writer.record.Identity
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	service.ackUnknown = false
	writer, err = openPostgresDurableWriter(path, authority, service)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if writer.record.Identity != id {
		t.Fatal("ACK recovery changed identity")
	}
	if _, err = writer.resolve(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if service.writes != 1 || writer.record.Sequence != 1 || writer.record.Coordinated.Sequence != 2 || writer.record.Ack != nil {
		t.Fatalf("ACK advanced wrong domain: %+v", writer.record)
	}
}
