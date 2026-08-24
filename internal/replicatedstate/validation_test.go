package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
	"google.golang.org/protobuf/proto"
)

var testMutationValidationDigest = sha256.Sum256([]byte("validator"))

type mutationValidatorFuncs struct {
	put    func(key, value []byte) MutationValidation
	delete func(key, current []byte, found bool) MutationValidation
}

func (validator mutationValidatorFuncs) ValidatePut(key, value []byte) MutationValidation {
	if validator.put == nil {
		return MutationValidationAccept
	}
	return validator.put(key, value)
}

func (validator mutationValidatorFuncs) ValidateDelete(
	key, current []byte,
	found bool,
) MutationValidation {
	if validator.delete == nil {
		return MutationValidationAccept
	}
	return validator.delete(key, current, found)
}

type observedMutationKeys struct {
	mu         sync.Mutex
	calls      [][][]byte
	updateErrs []error
}

func (observed *observedMutationKeys) callback(keys AttemptedMutationKeys, updateErr error) {
	call := make([][]byte, keys.Len())
	for index := range call {
		call[index] = bytes.Clone(keys.Key(index))
	}
	observed.mu.Lock()
	observed.calls = append(observed.calls, call)
	observed.updateErrs = append(observed.updateErrs, updateErr)
	observed.mu.Unlock()
}

func (observed *observedMutationKeys) reset() {
	observed.mu.Lock()
	observed.calls = nil
	observed.updateErrs = nil
	observed.mu.Unlock()
}

func (observed *observedMutationKeys) snapshot() [][][]byte {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	result := make([][][]byte, len(observed.calls))
	for index := range observed.calls {
		result[index] = make([][]byte, len(observed.calls[index]))
		for key := range observed.calls[index] {
			result[index][key] = bytes.Clone(observed.calls[index][key])
		}
	}
	return result
}

func (observed *observedMutationKeys) errorSnapshot() []error {
	observed.mu.Lock()
	defer observed.mu.Unlock()
	return append([]error(nil), observed.updateErrs...)
}

func newValidatedMachineFixture(
	t testing.TB,
	validator MutationValidator,
	observer MutationAttemptObserver,
) machineFixture {
	t.Helper()
	fixture := newMachineFixture(t)
	target := fixture.user
	target.Validation = ValidationDeterministicMutation
	target.ValidationDigest = testMutationValidationDigest
	target.Validator = validator
	target.ObserveMutationAttempt = observer
	machine, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: target}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("Open validated machine: %v", err)
	}
	fixture.machine = machine
	fixture.user = target
	return fixture
}

func completionResultCode(t testing.TB, machine *Machine, command []byte) uint32 {
	code, _ := completionResult(t, machine, command)
	return code
}

func completionResult(t testing.TB, machine *Machine, command []byte) (uint32, uint16) {
	t.Helper()
	lookup, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatalf("LookupCompletion: %v", err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil {
		t.Fatalf("OpenCompletion: %v", err)
	}
	return completion.ResultCode, completion.ResultFormat
}

func TestCollectionTargetValidationProfiles(t *testing.T) {
	fixture := newMachineFixture(t)
	accepting := mutationValidatorFuncs{}
	tests := []struct {
		name   string
		mutate func(*CollectionTarget)
		want   error
	}{
		{
			name: "opaque profile on JSON collection",
			mutate: func(target *CollectionTarget) {
				target.Validation = ValidationOpaqueBinary
				target.ValidationDigest = [32]byte{}
				target.Validator = nil
			},
			want: ErrInvalidCollection,
		},
		{
			name: "schema free digest",
			mutate: func(target *CollectionTarget) {
				target.Validation = ValidationOpaqueBinary
				target.Validator = nil
				target.ValidationDigest = testMutationValidationDigest
			},
			want: ErrInvalidCollection,
		},
		{
			name: "schema free validator",
			mutate: func(target *CollectionTarget) {
				target.Validation = ValidationOpaqueBinary
				target.ValidationDigest = [32]byte{}
				target.Validator = accepting
			},
			want: ErrInvalidCollection,
		},
		{
			name:   "validated",
			mutate: func(*CollectionTarget) {},
		},
		{
			name: "validated zero digest",
			mutate: func(target *CollectionTarget) {
				target.ValidationDigest = [32]byte{}
			},
			want: ErrInvalidCollection,
		},
		{
			name: "validated nil validator",
			mutate: func(target *CollectionTarget) {
				target.Validator = nil
			},
			want: ErrInvalidCollection,
		},
		{
			name: "unknown",
			mutate: func(target *CollectionTarget) {
				target.Validation = 255
			},
			want: ErrInvalidCollection,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := fixture.user
			test.mutate(&target)
			err := target.validate()
			if !errors.Is(err, test.want) || test.want == nil && err != nil {
				t.Fatalf("validate error = %v, want %v", err, test.want)
			}
		})
	}
	schemaFreeUser := fixture.user
	schemaFreeUser.Validation = ValidationOpaqueBinary
	schemaFreeUser.ValidationDigest = [32]byte{}
	schemaFreeUser.Validator = nil
	if _, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: schemaFreeUser}, fixture.log, fixture.machine.options,
	); !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("schema-free user Open error = %v, want ErrInvalidCollection", err)
	}
	if err := fixture.system.validate(); err != nil {
		t.Fatalf("opaque system target validation: %v", err)
	}

	for _, mutateSystem := range []func(*CollectionTarget){
		func(system *CollectionTarget) {
			system.ObserveMutationAttempt = func(AttemptedMutationKeys, error) {}
		},
		func(system *CollectionTarget) {
			system.Validation = ValidationDeterministicMutation
			system.ValidationDigest = testMutationValidationDigest
			system.Validator = accepting
		},
	} {
		system := fixture.system
		mutateSystem(&system)
		_, err := Open(
			fixture.binding, fixture.bootstrap, system,
			UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
		)
		if !errors.Is(err, ErrInvalidCollection) {
			t.Fatalf("non-schema-free system Open error = %v, want ErrInvalidCollection", err)
		}
	}
}

func TestValidatedMutationPlanningUsesCollapsedSnapshotAwareFinals(t *testing.T) {
	type validationCall struct {
		kind    replication.MutationKind
		key     string
		value   string
		current string
		found   bool
	}
	var calls []validationCall
	validator := mutationValidatorFuncs{
		put: func(key, value []byte) MutationValidation {
			calls = append(calls, validationCall{
				kind: replication.MutationPut, key: string(key), value: string(value),
			})
			return MutationValidationAccept
		},
		delete: func(key, current []byte, found bool) MutationValidation {
			calls = append(calls, validationCall{
				kind: replication.MutationDelete, key: string(key), current: string(current), found: found,
			})
			return MutationValidationAccept
		},
	}
	observed := new(observedMutationKeys)
	fixture := newValidatedMachineFixture(t, validator, observed.callback)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	seed := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("same"), Value: []byte(`{"n":1}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("gone"), Value: []byte(`{"n":3}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), seed); err != nil {
		t.Fatal(err)
	}
	calls = nil
	observed.reset()

	command := testCommand(fixture.binding, 2,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("put"), Value: []byte(`{"n":0}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("same"), Value: []byte(`{"n":1}`)},
		replication.Mutation{Kind: replication.MutationDelete, Key: []byte("missing")},
		replication.Mutation{Kind: replication.MutationDelete, Key: []byte("gone")},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("put"), Value: []byte(`{"n":2}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(4), command); err != nil {
		t.Fatal(err)
	}
	wantCalls := []validationCall{
		{kind: replication.MutationPut, key: "put", value: `{"n":2}`},
		{kind: replication.MutationPut, key: "same", value: `{"n":1}`},
		{kind: replication.MutationDelete, key: "missing", found: false},
		{kind: replication.MutationDelete, key: "gone", current: `{"n":3}`, found: true},
	}
	if !slices.Equal(calls, wantCalls) {
		t.Fatalf("validator calls = %#v, want %#v", calls, wantCalls)
	}
	wantObserved := [][][]byte{{[]byte("gone"), []byte("put")}}
	if got := observed.snapshot(); !equalObservedMutationKeys(got, wantObserved) {
		t.Fatalf("observed keys = %q, want %q", got, wantObserved)
	}
}

func equalObservedMutationKeys(a, b [][][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for index := range a {
		if len(a[index]) != len(b[index]) {
			return false
		}
		for key := range a[index] {
			if !bytes.Equal(a[index][key], b[index][key]) {
				return false
			}
		}
	}
	return true
}

func TestValidatedMutationResultMappingAndPrestageObserver(t *testing.T) {
	tests := []struct {
		name       string
		validation MutationValidation
		wantCode   uint32
		wantFormat uint16
		wantError  error
	}{
		{name: "invalid",
			validation: MutationValidationInvalid, wantCode: ResultInvalidDocument,
			wantFormat: ResultFormatMutation},
		{name: "target bound",
			validation: MutationValidationTargetBound, wantCode: ResultTargetBound,
			wantFormat: ResultFormatMutation},
		{name: "wrong shard",
			validation: MutationValidationWrongShard, wantCode: ResultWrongShard,
			wantFormat: ResultFormatMutation},
		{name: "unknown",
			validation: 0, wantError: ErrInvalidCollection},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observed := new(observedMutationKeys)
			fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{
				put: func([]byte, []byte) MutationValidation { return test.validation },
			}, observed.callback)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			open := commandValue(fixture.binding, 1)
			applySessionOpen(t, fixture.machine, 2, open)
			command := testCommand(fixture.binding, 1, replication.Mutation{
				Kind: replication.MutationPut, Key: []byte("bad"), Value: []byte(`{"n":1}`),
			})
			_, err := fixture.machine.ApplyNormal(normalMeta(3), command)
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("ApplyNormal error = %v, want %v", err, test.wantError)
				}
			} else {
				if err != nil {
					t.Fatalf("ApplyNormal: %v", err)
				}
				if code, format := completionResult(t, fixture.machine, command); code != test.wantCode || format != test.wantFormat {
					t.Fatalf("completion result = code %d format %d, want code %d format %d",
						code, format, test.wantCode, test.wantFormat)
				}
			}
			if got := observed.snapshot(); len(got) != 0 {
				t.Fatalf("pre-stage rejection observed keys: %q", got)
			}
			if fixture.user.Collection.Len() != 0 {
				t.Fatal("rejected mutation changed user collection")
			}
		})
	}
}

func TestValidatedMutationRejectsMalformedJSONBeforeCustomValidator(t *testing.T) {
	validatorCalls := 0
	observed := new(observedMutationKeys)
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{
		put: func([]byte, []byte) MutationValidation {
			validatorCalls++
			return MutationValidationAccept
		},
	}, observed.callback)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("bad"), Value: []byte(`{"n":`),
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	if got := completionResultCode(t, fixture.machine, command); got != ResultInvalidDocument {
		t.Fatalf("completion result = %d, want %d", got, ResultInvalidDocument)
	}
	if validatorCalls != 0 {
		t.Fatalf("custom validator calls = %d, want zero", validatorCalls)
	}
	if got := observed.snapshot(); len(got) != 0 {
		t.Fatalf("malformed JSON observed keys: %q", got)
	}
}

func TestValidatedOpenScansRowsOnceAndBindsApplyContract(t *testing.T) {
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, nil)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("a"), Value: []byte(`{"n":1}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("b"), Value: []byte(`{"n":2}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}

	var scanned []string
	reopenTarget := fixture.user
	reopenTarget.Validator = mutationValidatorFuncs{
		put: func(key, _ []byte) MutationValidation {
			scanned = append(scanned, string(key))
			return MutationValidationAccept
		},
	}
	reopened, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: reopenTarget}, fixture.log, fixture.machine.options,
	)
	if err != nil {
		t.Fatalf("matching reopen: %v", err)
	}
	gotPublication, wantPublication := reopened.Published(), fixture.machine.Published()
	if gotPublication.Applied != wantPublication.Applied ||
		gotPublication.DataChainDigest != wantPublication.DataChainDigest ||
		gotPublication.ReplicaSetVersion != wantPublication.ReplicaSetVersion ||
		!proto.Equal(gotPublication.ConfState, wantPublication.ConfState) ||
		!slices.Equal(scanned, []string{"a", "b"}) {
		t.Fatalf("reopen publication=%+v scanned=%q", gotPublication, scanned)
	}

	rejecting := reopenTarget
	rejecting.Validator = mutationValidatorFuncs{
		put: func([]byte, []byte) MutationValidation { return MutationValidationInvalid },
	}
	if _, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: rejecting}, fixture.log, fixture.machine.options,
	); !errors.Is(err, ErrSchemaProfile) {
		t.Fatalf("rejecting reopen error = %v, want ErrSchemaProfile", err)
	}

	wrongDigest := reopenTarget
	wrongDigest.ValidationDigest[0] ^= 0xff
	if _, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: wrongDigest}, fixture.log, fixture.machine.options,
	); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("wrong digest reopen error = %v, want ErrStateCorrupt", err)
	}

	wrongCompletionBound := fixture.machine.options
	wrongCompletionBound.MaxSessions++
	if _, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: reopenTarget}, fixture.log, wrongCompletionBound,
	); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("wrong completion bound reopen error = %v, want ErrStateCorrupt", err)
	}
}

func TestValidatedOpenAndExplicitAuditRouteEveryExtantRow(t *testing.T) {
	validator := mutationValidatorFuncs{
		put: func(key, _ []byte) MutationValidation {
			if bytes.Equal(key, []byte("outside")) {
				return MutationValidationWrongShard
			}
			return MutationValidationAccept
		},
	}
	fixture := newValidatedMachineFixture(t, validator, nil)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("inside"), Value: []byte(`{"n":1}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.user.Collection.Put([]byte("outside"), []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(
		fixture.binding, fixture.bootstrap, fixture.system,
		UserCollection{Name: "docs", Target: fixture.user}, fixture.log, fixture.machine.options,
	); !errors.Is(err, ErrSchemaProfile) {
		t.Fatalf("wrong-shard reopen = %v, want ErrSchemaProfile", err)
	}
	snapshot, err := fixture.machine.Snapshot("docs")
	if err != nil {
		t.Fatalf("cheap coherent snapshot: %v", err)
	}
	defer snapshot.Close()
	if _, err := snapshot.CanonicalImageDigest(); !errors.Is(err, ErrSchemaProfile) {
		t.Fatalf("wrong-shard image audit = %v, want ErrSchemaProfile", err)
	}
}

func TestCompletionResultGrammar(t *testing.T) {
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, nil)
	for _, code := range []uint32{
		ResultApplied, ResultTargetBound, ResultWrongShard, ResultSessionRetired,
		ResultSessionOpened, ResultSessionRenewed, ResultSessionRevoked,
	} {
		if err := fixture.machine.validateCompletionResult(replication.CompletionView{
			ResultFormat: ResultFormatMutation, ResultCode: code,
		}); err != nil {
			t.Fatalf("rejected result code %d: %v", code, err)
		}
	}
	for _, completion := range []replication.CompletionView{
		{ResultFormat: ResultFormatMutation + 1, ResultCode: ResultApplied},
		{ResultFormat: ResultFormatMutation, ResultCode: 0},
		{ResultFormat: ResultFormatMutation, ResultCode: ResultSessionRevoked + 1},
	} {
		if err := fixture.machine.validateCompletionResult(completion); !errors.Is(err, ErrCompletionCorrupt) {
			t.Fatalf("accepted invalid completion grammar %+v: %v", completion, err)
		}
	}
}

func TestMutationAttemptObserverCoversDecisionSyncOutcomeUnknown(t *testing.T) {
	observed := new(observedMutationKeys)
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, observed.callback)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	restore := durable.InstallTxnMarkerSyncFaultForFacadeTest()
	defer restore()
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("a"), Value: []byte(`{"n":1}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("b"), Value: []byte(`{"n":2}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("ApplyNormal error = %v, want ErrCommitOutcomeUnknown", err)
	}
	want := [][][]byte{{[]byte("a"), []byte("b")}}
	if got := observed.snapshot(); !equalObservedMutationKeys(got, want) {
		t.Fatalf("outcome-unknown observed keys = %q, want %q", got, want)
	}
	if got := observed.errorSnapshot(); len(got) != 1 ||
		!errors.Is(got[0], durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("outcome-unknown observer errors = %v", got)
	}
}

func TestMutationAttemptObserverCoversDefiniteTransactionSetupFailure(t *testing.T) {
	observed := new(observedMutationKeys)
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, observed.callback)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	beforeGeneration := fixture.user.Collection.Generation()
	storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{
		Phase: storeio.TxnMarkerFaultCreateHeaderWrite,
	})
	defer storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("a"), Value: []byte(`{"n":1}`),
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err == nil {
		t.Fatal("ApplyNormal unexpectedly succeeded")
	} else if errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("definite setup failure classified outcome-unknown: %v", err)
	}
	if !storeio.TxnMarkerCreateFaulted() {
		t.Fatal("transaction-marker setup fault did not fire")
	}
	want := [][][]byte{{[]byte("a")}}
	if got := observed.snapshot(); !equalObservedMutationKeys(got, want) {
		t.Fatalf("definite-failure attempted keys = %q, want %q", got, want)
	}
	if got := observed.errorSnapshot(); len(got) != 1 || got[0] == nil ||
		errors.Is(got[0], durable.ErrCommitOutcomeUnknown) ||
		!errors.Is(got[0], storeio.ErrFaultInjected) {
		t.Fatalf("definite-failure observer errors = %v", got)
	}
	if got := fixture.user.Collection.Generation(); got != beforeGeneration {
		t.Fatalf("user generation = %d, want unchanged %d", got, beforeGeneration)
	}
	if err := fixture.user.Collection.PersistenceError(); err != nil {
		t.Fatalf("user persistence error = %v, want nil for definite setup failure", err)
	}
}

func TestMutationAttemptObserverIsSynchronousWithApply(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, func(keys AttemptedMutationKeys, updateErr error) {
		if keys.Len() != 1 || !bytes.Equal(keys.Key(0), []byte("a")) {
			panic("unexpected attempted mutation keys")
		}
		if updateErr != nil {
			panic("unexpected mutation update error")
		}
		entered <- struct{}{}
		<-release
	})
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("a"), Value: []byte(`{"n":1}`),
	})
	done := make(chan error, 1)
	go func() {
		_, err := fixture.machine.ApplyNormal(normalMeta(3), command)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("observer was not invoked")
	}
	select {
	case err := <-done:
		t.Fatalf("ApplyNormal returned before observer: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ApplyNormal did not return after observer")
	}
}

func TestCanonicalImageDigestGolden(t *testing.T) {
	overlay := []finalMutation{
		{key: []byte("b"), value: []byte(`{"n":2}`)},
		{key: []byte("z"), delete: true},
		{key: []byte("a"), value: []byte(`{"n":1}`)},
	}
	if _, err := canonicalImageDigest(
		"docs", ValidationOpaqueBinary, [32]byte{}, nil, nil, overlay,
	); !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("schema-free image digest error = %v, want ErrInvalidCollection", err)
	}
	validated, err := canonicalImageDigest(
		"docs", ValidationDeterministicMutation, testMutationValidationDigest,
		mutationValidatorFuncs{}, nil, overlay,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertDigestHex(t, "validated", validated, "83def7d1e73f8a492bf9513203bb76e20fa931b8e8d1a268815df3c85f0f3b16")
}

func assertDigestHex(t testing.TB, name string, got [32]byte, wantHex string) {
	t.Helper()
	want, err := hex.DecodeString(wantHex)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got[:], want) {
		t.Fatalf("%s digest = %x, want %s", name, got, wantHex)
	}
}

func FuzzCanonicalImageDigestValidationProfile(f *testing.F) {
	f.Add("docs", []byte("a"), []byte(`{"n":1}`), false)
	f.Add("", []byte{}, []byte{}, true)
	f.Fuzz(func(t *testing.T, name string, key, value []byte, deleteMutation bool) {
		if len(name) > 128 || len(key) > 256 || len(value) > 1024 {
			t.Skip()
		}
		overlay := []finalMutation{{
			key: bytes.Clone(key), value: bytes.Clone(value), delete: deleteMutation,
		}}
		beforeKey, beforeValue := bytes.Clone(overlay[0].key), bytes.Clone(overlay[0].value)
		first, err := canonicalImageDigest(
			name, ValidationDeterministicMutation, testMutationValidationDigest,
			mutationValidatorFuncs{}, nil, overlay,
		)
		if err != nil {
			t.Fatal(err)
		}
		second, err := canonicalImageDigest(
			name, ValidationDeterministicMutation, testMutationValidationDigest,
			mutationValidatorFuncs{}, nil, overlay,
		)
		if err != nil {
			t.Fatal(err)
		}
		if first != second {
			t.Fatalf("nondeterministic digest: %x != %x", first, second)
		}
		if !bytes.Equal(overlay[0].key, beforeKey) || !bytes.Equal(overlay[0].value, beforeValue) {
			t.Fatal("canonical image digest mutated its overlay")
		}
	})
}
