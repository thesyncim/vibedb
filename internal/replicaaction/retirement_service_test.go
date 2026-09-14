package replicaaction

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

type retirementBoundaryOwner struct {
	fakeOwner
	validate func(context.Context, raftservice.ReplicaRetirementRequest) error
	retire   func(context.Context, raftservice.ReplicaRetirementRequest) error
}

func (*fakeOwner) ValidateReplicaRetirement(context.Context, raftservice.ReplicaRetirementRequest) error {
	return nil
}

func (owner *retirementBoundaryOwner) ValidateReplicaRetirement(ctx context.Context, request raftservice.ReplicaRetirementRequest) error {
	return owner.validate(ctx, request)
}

func (owner *retirementBoundaryOwner) RetireReplicaSource(ctx context.Context, request raftservice.ReplicaRetirementRequest) error {
	return owner.retire(ctx, request)
}

func retirementServiceRequest(t *testing.T, address string) Request {
	t.Helper()
	request := actionFixture(t, SourceRetirement)
	var err error
	request.Command, err = EncodeRetirementLocators([]RetirementLocator{{Member: request.TargetMember, Address: address}})
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func requireRetirementJournalState(t *testing.T, journal *FileJournal, request Request, state State, revision uint64) {
	t.Helper()
	record, err := journal.ReadReplicaAction(t.Context(), request.Operation, request.Kind)
	if err != nil || record.State != state || record.Revision != revision || journal.pending != nil {
		t.Fatalf("journal state=%d revision=%d pending=%v err=%v; want %d/%d", record.State, record.Revision,
			journal.pending != nil, err, state, revision)
	}
}

func TestRetirementServiceDurablyAuthorizesBeforeCloseAndCleanup(t *testing.T) {
	journal, err := OpenFileJournal(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	request := retirementServiceRequest(t, "127.0.0.1:1234")
	var events []string
	owner := &retirementBoundaryOwner{
		validate: func(_ context.Context, retirement raftservice.ReplicaRetirementRequest) error {
			requireRetirementJournalState(t, journal, request, Running, 1)
			if retirement.Authorized || retirement.Proof == nil {
				t.Fatalf("validation bypassed fresh proof: %+v", retirement)
			}
			events = append(events, "validate")
			return nil
		},
		retire: func(_ context.Context, retirement raftservice.ReplicaRetirementRequest) error {
			requireRetirementJournalState(t, journal, request, RetirementAuthorized, 2)
			if !retirement.Authorized {
				t.Fatal("close lacked durable authorization")
			}
			events = append(events, "close")
			return nil
		},
	}
	service := testService(t, journal, owner)
	service.retirementProof = func(_ context.Context, observed Request) (raftservice.ReplicaRetirementProof, error) {
		requireRetirementJournalState(t, journal, request, Running, 1)
		if !equalRequest(observed, request) {
			t.Fatal("proof fetched for a different retirement")
		}
		events = append(events, "proof")
		// The fake owner controls proof validation; raftservice tests validate the
		// proof's contents. Here the real journal enforces publication ordering.
		return raftservice.ReplicaRetirementProof{}, nil
	}
	service.retired = func(context.Context, Request) error {
		requireRetirementJournalState(t, journal, request, RetirementAuthorized, 2)
		events = append(events, "cleanup")
		return nil
	}
	record, err := service.Execute(t.Context(), request)
	if err != nil || record.State != Complete || record.Revision != 3 {
		t.Fatalf("retirement=%+v err=%v", record, err)
	}
	requireRetirementJournalState(t, journal, request, Complete, 3)
	if !slices.Equal(events, []string{"proof", "validate", "close", "cleanup"}) {
		t.Fatalf("retirement event order=%v", events)
	}
	retry := retirementServiceRequest(t, "127.0.0.1:4321")
	if record, err := service.Execute(t.Context(), retry); err != nil || record.State != Complete {
		t.Fatalf("completed retirement with changed locator: %+v %v", record, err)
	}
	if len(events) != 4 {
		t.Fatalf("completed retirement repeated effects: %v", events)
	}
}

func TestRetirementServiceDoesNotCrossUncertainJournalBoundaries(t *testing.T) {
	for _, boundary := range []string{"authorization", "completion"} {
		t.Run(boundary, func(t *testing.T) {
			journal, err := OpenFileJournal(t.TempDir(), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer journal.Close()
			request := retirementServiceRequest(t, "127.0.0.1:1234")
			fault := errors.New("injected directory sync failure")
			failSync := func(*os.Root) error { return fault }
			proofs, validations, closes, cleanups := 0, 0, 0, 0
			owner := &retirementBoundaryOwner{
				validate: func(context.Context, raftservice.ReplicaRetirementRequest) error {
					validations++
					if boundary == "authorization" {
						journal.syncRoot = failSync
					}
					return nil
				},
				retire: func(_ context.Context, retirement raftservice.ReplicaRetirementRequest) error {
					requireRetirementJournalState(t, journal, request, RetirementAuthorized, 2)
					if !retirement.Authorized {
						t.Fatal("retirement reached close without authorization")
					}
					closes++
					return nil
				},
			}
			service := testService(t, journal, owner)
			service.retirementProof = func(context.Context, Request) (raftservice.ReplicaRetirementProof, error) {
				proofs++
				return raftservice.ReplicaRetirementProof{}, nil
			}
			service.retired = func(context.Context, Request) error {
				cleanups++
				if boundary == "completion" {
					journal.syncRoot = failSync
				}
				return nil
			}
			if _, err := service.Execute(t.Context(), request); !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, fault) {
				t.Fatalf("uncertain %s publication=%v", boundary, err)
			}
			wantEffects, wantDurableState := 0, Running
			if boundary == "completion" {
				wantEffects, wantDurableState = 1, RetirementAuthorized
			}
			if closes != wantEffects || cleanups != wantEffects || proofs != 1 || validations != 1 {
				t.Fatalf("effects after failed %s: proof=%d validation=%d close=%d cleanup=%d", boundary,
					proofs, validations, closes, cleanups)
			}
			key := replicaActionJournalKey(request.Operation, request.Kind)
			if journal.records[key].State != wantDurableState {
				t.Fatalf("unsynced %s entered durable map: %+v", boundary, journal.records[key])
			}
			journal.syncRoot = syncReplicaActionRoot
			service.retirementProof = func(context.Context, Request) (raftservice.ReplicaRetirementProof, error) {
				t.Fatal("authorized retry fetched proof again")
				return raftservice.ReplicaRetirementProof{}, ErrControl
			}
			owner.validate = func(context.Context, raftservice.ReplicaRetirementRequest) error {
				t.Fatal("authorized retry validated proof again")
				return ErrControl
			}
			record, err := service.Execute(t.Context(), retirementServiceRequest(t, "127.0.0.1:4321"))
			if err != nil || record.State != Complete || record.Revision != 3 || closes != 1 || cleanups != 1 {
				t.Fatalf("settled retry=%+v close=%d cleanup=%d err=%v", record, closes, cleanups, err)
			}
		})
	}
}

func TestRetirementServiceRunningMissingSourceIsNotCompletionProof(t *testing.T) {
	journal, err := OpenFileJournal(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	request := actionFixture(t, SourceRetirement)
	owner := &retirementBoundaryOwner{
		validate: func(_ context.Context, retirement raftservice.ReplicaRetirementRequest) error {
			if retirement.Authorized {
				t.Fatal("Running request acquired authorization without proof")
			}
			return multiraft.ErrGroupNotFound
		},
		retire: func(context.Context, raftservice.ReplicaRetirementRequest) error {
			t.Fatal("missing source with only Running journal reached close")
			return multiraft.ErrGroupNotFound
		},
	}
	service := testService(t, journal, owner)
	service.retired = func(context.Context, Request) error {
		t.Fatal("missing source with only Running journal reached cleanup")
		return nil
	}
	if _, err := service.Execute(t.Context(), request); !errors.Is(err, multiraft.ErrGroupNotFound) {
		t.Fatalf("unproven missing source completed: %v", err)
	}
	requireRetirementJournalState(t, journal, request, Running, 1)
	retirements, err := journal.SourceRetirements(t.Context())
	if err != nil || len(retirements) != 0 {
		t.Fatalf("Running request became tombstone: %+v %v", retirements, err)
	}
}

func TestRetirementServiceLegacyRequestAuthorizesBeforeClose(t *testing.T) {
	journal, err := OpenFileJournal(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	request := actionFixture(t, SourceRetirement)
	owner := &retirementBoundaryOwner{
		validate: func(_ context.Context, retirement raftservice.ReplicaRetirementRequest) error {
			requireRetirementJournalState(t, journal, request, Running, 1)
			if retirement.Proof != nil || retirement.Authorized {
				t.Fatalf("legacy request bypassed local final-membership validation: %+v", retirement)
			}
			return nil
		},
		retire: func(_ context.Context, retirement raftservice.ReplicaRetirementRequest) error {
			requireRetirementJournalState(t, journal, request, RetirementAuthorized, 2)
			if !retirement.Authorized {
				t.Fatal("legacy close lacked durable authorization")
			}
			return nil
		},
	}
	service := testService(t, journal, owner)
	service.retirementProof = func(context.Context, Request) (raftservice.ReplicaRetirementProof, error) {
		t.Fatal("legacy request attempted discovery without locators")
		return raftservice.ReplicaRetirementProof{}, ErrControl
	}
	service.retired = func(context.Context, Request) error {
		requireRetirementJournalState(t, journal, request, RetirementAuthorized, 2)
		return nil
	}
	record, err := service.Execute(t.Context(), request)
	if err != nil || record.State != Complete || record.Revision != 3 {
		t.Fatalf("legacy retirement=%+v err=%v", record, err)
	}
}

func TestRetirementServiceRetriesCleanupAfterAuthorizedRestartWithoutProof(t *testing.T) {
	path := t.TempDir()
	journal, err := OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = journal.Close() }()
	request := retirementServiceRequest(t, "127.0.0.1:1234")
	owner := &retirementBoundaryOwner{
		validate: func(context.Context, raftservice.ReplicaRetirementRequest) error { return nil },
		retire: func(_ context.Context, retirement raftservice.ReplicaRetirementRequest) error {
			requireRetirementJournalState(t, journal, request, RetirementAuthorized, 2)
			return nil
		},
	}
	service := testService(t, journal, owner)
	service.retirementProof = func(context.Context, Request) (raftservice.ReplicaRetirementProof, error) {
		return raftservice.ReplicaRetirementProof{}, nil
	}
	fault := errors.New("cleanup interrupted after source close")
	service.retired = func(context.Context, Request) error { return fault }
	if _, err := service.Execute(t.Context(), request); !errors.Is(err, fault) {
		t.Fatalf("cleanup failure=%v", err)
	}
	requireRetirementJournalState(t, journal, request, RetirementAuthorized, 2)
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	journal, err = OpenFileJournal(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	owner = &retirementBoundaryOwner{
		validate: func(context.Context, raftservice.ReplicaRetirementRequest) error {
			t.Fatal("recovered authorization needed fresh proof validation")
			return ErrControl
		},
		retire: func(_ context.Context, retirement raftservice.ReplicaRetirementRequest) error {
			if !retirement.Authorized || retirement.Proof != nil {
				t.Fatalf("recovered close did not use retained authorization: %+v", retirement)
			}
			return multiraft.ErrGroupNotFound
		},
	}
	service = testService(t, journal, owner)
	// An unavailable proof source must not block the already authorized retry.
	service.retirementProof = nil
	cleanups := 0
	service.retired = func(context.Context, Request) error {
		requireRetirementJournalState(t, journal, request, RetirementAuthorized, 2)
		cleanups++
		return nil
	}
	retry := retirementServiceRequest(t, "127.0.0.1:4321")
	record, err := service.Execute(t.Context(), retry)
	if err != nil || record.State != Complete || record.Revision != 3 || cleanups != 1 {
		t.Fatalf("recovered cleanup=%+v count=%d err=%v", record, cleanups, err)
	}
	requireRetirementJournalState(t, journal, request, Complete, 3)
}
