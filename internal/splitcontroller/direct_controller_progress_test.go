package splitcontroller

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
)

type directProgressTestDirectory struct {
	*testControllerCatalog
	readCount uint8
	postRead  func(gateway.ReplicatedOperationRecord) (gateway.ReplicatedOperationRecord, error)
}

func (directory *directProgressTestDirectory) ReadOperation(
	ctx context.Context, id [32]byte,
) (gateway.ReplicatedOperationRecord, error) {
	directory.readCount++
	record, err := directory.testControllerCatalog.ReadOperation(ctx, id)
	if err != nil || directory.readCount != 2 || directory.postRead == nil {
		return record, err
	}
	return directory.postRead(record)
}

func TestDirectOperationProgressUsesMonotonicValidatedRecordState(t *testing.T) {
	_, catalog, _, _, _ := newDirectControllerLoopFixture(t)
	before := catalog.record
	progressed, err := directOperationProgress(before.ID, before, before, nil)
	if err != nil || progressed {
		t.Fatalf("identical operation progressed=%t err=%v", progressed, err)
	}

	for _, waitCase := range []struct {
		name string
		kind ActionKind
	}{
		{name: "source leader", kind: ActionAwaitSourceLeader},
		{name: "child ready", kind: ActionAwaitChildReady},
		{name: "catalog drain", kind: ActionAwaitCatalogDrain},
	} {
		t.Run(waitCase.name, func(t *testing.T) {
			wait := before
			wait.State, wait.Revision = gateway.ReplicatedOperationRunning, 2
			wait.Cursor = replicatedActionCursor(Action{Kind: waitCase.kind, Child: 1, CatalogGeneration: 20})
			wait.Proof = replicatedActionProof(wait.ID, wait.Cursor)
			progressed, progressErr := directOperationProgress(wait.ID, wait, wait, nil)
			if progressErr != nil || progressed {
				t.Fatalf("stable wait progressed=%t err=%v", progressed, progressErr)
			}
		})
	}

	settled := before
	settled.State, settled.Revision = gateway.ReplicatedOperationRunning, 2
	settled.Cursor = replicatedActionCursor(Action{Kind: ActionStartCapture})
	settled.Proof = replicatedActionProof(settled.ID, settled.Cursor)
	settled.Execution = []byte("exact settled action wave")
	settled.ExecutionRevision, settled.ExecutionSettled = settled.Revision, true
	progressed, err = directOperationProgress(settled.ID, settled, settled, nil)
	if err != nil || progressed {
		t.Fatalf("identical settled non-wait replay progressed=%t err=%v", progressed, err)
	}

	advanced := before
	advanced.Revision++
	progressed, err = directOperationProgress(before.ID, before, advanced, nil)
	if err != nil || !progressed {
		t.Fatalf("revision advance progressed=%t err=%v", progressed, err)
	}
	advancedGenerationBackwards := before
	advancedGenerationBackwards.CatalogGeneration--
	advancedGenerationBackwards.Revision++
	if progressed, err = directOperationProgress(before.ID, before, advancedGenerationBackwards, nil); progressed ||
		!errors.Is(err, ErrReplicatedExecution) {
		t.Fatalf("revision advance with backwards catalog generation progressed=%t err=%v", progressed, err)
	}
	progressed, err = directOperationProgress(before.ID, before,
		gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing)
	if err != nil || !progressed {
		t.Fatalf("present-to-missing deletion progressed=%t err=%v", progressed, err)
	}
	invalidBefore := before
	invalidBefore.Revision = 0
	if progressed, err = directOperationProgress(before.ID, invalidBefore,
		gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing); progressed ||
		!errors.Is(err, ErrReplicatedExecution) {
		t.Fatalf("missing readback hid invalid pre-read progressed=%t err=%v", progressed, err)
	}

	readErr := errors.New("catalog readback failed")
	if progressed, err = directOperationProgress(before.ID, before,
		gateway.ReplicatedOperationRecord{}, readErr); progressed || !errors.Is(err, readErr) {
		t.Fatalf("read error progressed=%t err=%v", progressed, err)
	}

	for name, mutate := range map[string]func(*gateway.ReplicatedOperationRecord){
		"invalid record":               func(record *gateway.ReplicatedOperationRecord) { record.Revision = 0 },
		"backwards revision":           func(record *gateway.ReplicatedOperationRecord) { record.Revision-- },
		"same revision changed cursor": func(record *gateway.ReplicatedOperationRecord) { record.Cursor[1]++ },
		"changed id":                   func(record *gateway.ReplicatedOperationRecord) { record.ID[0]++ },
		"changed kind": func(record *gateway.ReplicatedOperationRecord) {
			record.Kind = gateway.ReplicatedOperationMove
		},
		"changed intent": func(record *gateway.ReplicatedOperationRecord) {
			record.Intent = append([]byte(nil), record.Intent...)
			record.Intent[len(record.Intent)-1] ^= 1
			record.IntentDigest = sha256.Sum256(record.Intent)
		},
	} {
		t.Run(name, func(t *testing.T) {
			current := before
			if name == "backwards revision" || name == "same revision changed cursor" {
				current.Revision = 2
				current.State = gateway.ReplicatedOperationRunning
			}
			after := current
			mutate(&after)
			if progressed, progressErr := directOperationProgress(current.ID, current, after, nil); progressed ||
				!errors.Is(progressErr, ErrReplicatedExecution) {
				t.Fatalf("corrupt/inconsistent readback progressed=%t err=%v", progressed, progressErr)
			}
		})
	}
}

func TestDirectControllerPassCountsProgressOnlyFromReadback(t *testing.T) {
	for name, postRead := range map[string]func(gateway.ReplicatedOperationRecord) (gateway.ReplicatedOperationRecord, error){
		"authoritative advance": func(record gateway.ReplicatedOperationRecord) (gateway.ReplicatedOperationRecord, error) {
			return record, nil
		},
		"present to missing": func(gateway.ReplicatedOperationRecord) (gateway.ReplicatedOperationRecord, error) {
			return gateway.ReplicatedOperationRecord{}, gateway.ErrReplicatedOperationMissing
		},
		"post-read failure": func(gateway.ReplicatedOperationRecord) (gateway.ReplicatedOperationRecord, error) {
			return gateway.ReplicatedOperationRecord{}, errors.New("injected post-read failure")
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, catalog, observer, router, _ := newDirectControllerLoopFixture(t)
			directory := &directProgressTestDirectory{
				testControllerCatalog: catalog,
				postRead: func(record gateway.ReplicatedOperationRecord) (gateway.ReplicatedOperationRecord, error) {
					if name == "authoritative advance" {
						// The service has published Planned→Running before this read.
						if record.State != gateway.ReplicatedOperationRunning || record.Revision != 2 {
							t.Fatalf("service record=%+v, want Running revision 2", record)
						}
					}
					return postRead(record)
				},
			}
			controller, err := NewControllerService(catalog, observer, router)
			if err != nil {
				t.Fatal(err)
			}
			controller.gateway = noOpGatewayAwaitExecutor{}
			pass, err := RunDirectControllerPass(t.Context(), directory, controller)
			switch name {
			case "authoritative advance":
				if err != nil || pass.Triggered != 1 || pass.Progressed != 1 || directory.readCount != 2 {
					t.Fatalf("pass=%+v post_reads=%d err=%v", pass, directory.readCount-1, err)
				}
			case "present to missing":
				if err != nil || pass.Triggered != 1 || pass.Progressed != 1 || directory.readCount != 2 {
					t.Fatalf("pass=%+v post_reads=%d err=%v", pass, directory.readCount-1, err)
				}
			case "post-read failure":
				if err == nil || pass.Triggered != 1 || pass.Progressed != 0 || directory.readCount != 2 {
					t.Fatalf("pass=%+v post_reads=%d err=%v", pass, directory.readCount-1, err)
				}
			}
		})
	}

	t.Run("cancellation during readback", func(t *testing.T) {
		_, catalog, observer, router, _ := newDirectControllerLoopFixture(t)
		directory := &directProgressTestDirectory{testControllerCatalog: catalog}
		controller, err := NewControllerService(catalog, observer, router)
		if err != nil {
			t.Fatal(err)
		}
		controller.gateway = noOpGatewayAwaitExecutor{}
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		cause := errors.New("cancelled during authoritative readback")
		directory.postRead = func(record gateway.ReplicatedOperationRecord) (gateway.ReplicatedOperationRecord, error) {
			cancel(cause)
			return record, nil
		}
		pass, err := RunDirectControllerPass(ctx, directory, controller)
		if pass.Triggered != 1 || pass.Progressed != 0 || directory.readCount != 2 || !errors.Is(err, cause) {
			t.Fatalf("pass=%+v reads=%d err=%v", pass, directory.readCount, err)
		}
	})
}
