package gatewayruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

type preparedWriteService struct {
	postgresWriteServiceStub
	t                  *testing.T
	path               string
	prepared, executed int
	recipe             []byte
}

func (s *preparedWriteService) ExecBatchMode(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query, gateway.DurableSQLExecutionMode) (durableExecBatchExecuteResult, error) {
	s.t.Fatal("prepared outbox fell back to SQL execution")
	return durableExecBatchExecuteResult{}, nil
}
func (s *preparedWriteService) ReplayBatchMode(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query, gateway.DurableSQLExecutionMode) (durableExecBatchExecuteResult, bool, error) {
	s.t.Fatal("prepared outbox replanned on recovery")
	return durableExecBatchExecuteResult{}, false, nil
}
func (s *preparedWriteService) PrepareDirectBatch(_ context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, _ []gateway.Query) (*gateway.DurableSQLDirectPlan, error) {
	s.prepared++
	return &gateway.DurableSQLDirectPlan{Key: requestledger.RequestKey{Request: requestledger.RequestID(id.RequestID), IssuerSequence: id.IssuerSequence}, RequestDigest: replication.Digest{7}, CatalogGeneration: 1, Participant: gateway.ReplicatedTransactionParticipant{Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{Kind: replication.MutationPutDigestEqual, Key: []byte("a"), Value: []byte(`{"id":"a","n":42}`), ExpectedValueLength: 17, ExpectedValueDigest: replication.Digest{9}}}}}}}, nil
}
func (s *preparedWriteService) ExecutePreparedDirectBatch(_ context.Context, _ serviceauthz.Authority, id durableExecBatchIdentity, _ []gateway.Query, plan *gateway.DurableSQLDirectPlan) (durableExecBatchExecuteResult, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		s.t.Fatal(err)
	}
	var onDisk postgresWriteRecord
	if err = vibejson.Unmarshal(raw[sha256.Size:], &onDisk); err != nil {
		s.t.Fatal(err)
	}
	encoded, err := vibejson.Marshal(plan)
	if err != nil {
		s.t.Fatal(err)
	}
	persisted, err := vibejson.Marshal(onDisk.DirectPlan)
	if err != nil {
		s.t.Fatal(err)
	}
	if onDisk.Version != 5 || onDisk.Identity != id || !bytes.Equal(encoded, persisted) {
		s.t.Fatal("executed before exact recipe was journaled")
	}
	if s.executed == 0 {
		s.recipe = bytes.Clone(encoded)
	} else if !bytes.Equal(s.recipe, encoded) {
		s.t.Fatal("recovery changed recipe")
	}
	s.executed++
	s.identity = id
	s.direct = true
	if s.unknown {
		return durableExecBatchExecuteResult{}, gateway.ErrDurableRequestUnresolved
	}
	return s.result(), nil
}

func TestPostgreSQLPreparedWriteRestartsWithoutReevaluation(t *testing.T) {
	authority := serviceauthz.Authority{Node: [16]byte{1}, Generation: 1}
	service := &preparedWriteService{t: t, path: filepath.Join(t.TempDir(), "outbox")}
	service.unknown = true
	writer, err := openPostgresDurableWriter(service.path, authority, service)
	if err != nil {
		t.Fatal(err)
	}
	_, err = writer.Write(t.Context(), authority, gateway.Query{SQL: "UPDATE docs SET n=n+1 WHERE id='a'"})
	if !errors.Is(err, durable.ErrCommitOutcomeUnknown) || writer.record.DirectPlan == nil {
		t.Fatalf("unknown update: %v", err)
	}
	corrupt := writer.record
	corrupt.Version = 3
	if validPostgresWriteJournalVersion(&corrupt) {
		t.Fatal("recipe journal accepted old version")
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	writer, err = openPostgresDurableWriter(service.path, authority, service)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	service.unknown = false
	if _, err = writer.resolve(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if service.prepared != 1 || service.executed != 2 || writer.record.DirectPlan != nil || writer.record.Query != nil || writer.record.Sequence != 2 {
		t.Fatalf("recovery state=%+v record=%+v", service, writer.record)
	}
}
