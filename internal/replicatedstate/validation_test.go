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

var testMutationValidationDigest = sha256.Sum256([]byte("validator-v1"))

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
	mu    sync.Mutex
	calls [][][]byte
}

func (observed *observedMutationKeys) callback(keys AttemptedMutationKeys) {
	call := make([][]byte, keys.Len())
	for index := range call {
		call[index] = bytes.Clone(keys.Key(index))
	}
	observed.mu.Lock()
	observed.calls = append(observed.calls, call)
	observed.mu.Unlock()
}

func (observed *observedMutationKeys) reset() {
	observed.mu.Lock()
	observed.calls = nil
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

func newValidatedMachineFixture(
	t testing.TB,
	validator MutationValidator,
	observer MutationAttemptObserver,
) machineFixture {
	t.Helper()
	fixture := newMachineFixture(t)
	target := fixture.user
	target.Validation = ValidationDeterministicMutationV1
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
	t.Helper()
	lookup, err := machine.LookupCompletion(command)
	if err != nil {
		t.Fatalf("LookupCompletion: %v", err)
	}
	completion, err := replication.OpenCompletionV1(lookup.Bytes)
	if err != nil {
		t.Fatalf("OpenCompletionV1: %v", err)
	}
	return completion.ResultCode
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
			name:   "legacy",
			mutate: func(*CollectionTarget) {},
		},
		{
			name: "legacy digest",
			mutate: func(target *CollectionTarget) {
				target.ValidationDigest = testMutationValidationDigest
			},
			want: ErrInvalidCollection,
		},
		{
			name: "legacy validator",
			mutate: func(target *CollectionTarget) {
				target.Validator = accepting
			},
			want: ErrInvalidCollection,
		},
		{
			name: "validated",
			mutate: func(target *CollectionTarget) {
				target.Validation = ValidationDeterministicMutationV1
				target.ValidationDigest = testMutationValidationDigest
				target.Validator = accepting
			},
		},
		{
			name: "validated zero digest",
			mutate: func(target *CollectionTarget) {
				target.Validation = ValidationDeterministicMutationV1
				target.Validator = accepting
			},
			want: ErrInvalidCollection,
		},
		{
			name: "validated nil validator",
			mutate: func(target *CollectionTarget) {
				target.Validation = ValidationDeterministicMutationV1
				target.ValidationDigest = testMutationValidationDigest
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

	for _, mutateSystem := range []func(*CollectionTarget){
		func(system *CollectionTarget) {
			system.ObserveMutationAttempt = func(AttemptedMutationKeys) {}
		},
		func(system *CollectionTarget) {
			system.Validation = ValidationDeterministicMutationV1
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
			t.Fatalf("non-legacy system Open error = %v, want ErrInvalidCollection", err)
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
	seed := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("same"), Value: []byte(`{"n":1}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("gone"), Value: []byte(`{"n":3}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), seed); err != nil {
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
	if _, err := fixture.machine.ApplyNormal(normalMeta(3), command); err != nil {
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
	wantObserved := [][][]byte{{[]byte("put"), []byte("gone")}}
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
		wantError  error
	}{
		{name: "invalid", validation: MutationValidationInvalid, wantCode: ResultInvalidDocument},
		{name: "target bound", validation: MutationValidationTargetBound, wantCode: ResultTargetBound},
		{name: "unknown", validation: 0, wantError: ErrInvalidCollection},
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
			command := testCommand(fixture.binding, 1, replication.Mutation{
				Kind: replication.MutationPut, Key: []byte("bad"), Value: []byte(`{"n":1}`),
			})
			_, err := fixture.machine.ApplyNormal(normalMeta(2), command)
			if test.wantError != nil {
				if !errors.Is(err, test.wantError) {
					t.Fatalf("ApplyNormal error = %v, want %v", err, test.wantError)
				}
			} else {
				if err != nil {
					t.Fatalf("ApplyNormal: %v", err)
				}
				if got := completionResultCode(t, fixture.machine, command); got != test.wantCode {
					t.Fatalf("completion result = %d, want %d", got, test.wantCode)
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
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("bad"), Value: []byte(`{"n":`),
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), command); err != nil {
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

func TestValidatedOpenScansRowsAndBindsDigest(t *testing.T) {
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, nil)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("a"), Value: []byte(`{"n":1}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("b"), Value: []byte(`{"n":2}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), command); err != nil {
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
		gotPublication.LogicalDigest != wantPublication.LogicalDigest ||
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
}

func TestMutationAttemptObserverCoversDecisionSyncOutcomeUnknown(t *testing.T) {
	observed := new(observedMutationKeys)
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, observed.callback)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	restore := durable.InstallTxnMarkerSyncFaultForFacadeTest()
	defer restore()
	command := testCommand(fixture.binding, 1,
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("a"), Value: []byte(`{"n":1}`)},
		replication.Mutation{Kind: replication.MutationPut, Key: []byte("b"), Value: []byte(`{"n":2}`)},
	)
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), command); !errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		t.Fatalf("ApplyNormal error = %v, want ErrCommitOutcomeUnknown", err)
	}
	want := [][][]byte{{[]byte("a"), []byte("b")}}
	if got := observed.snapshot(); !equalObservedMutationKeys(got, want) {
		t.Fatalf("outcome-unknown observed keys = %q, want %q", got, want)
	}
}

func TestMutationAttemptObserverCoversDefiniteTransactionSetupFailure(t *testing.T) {
	observed := new(observedMutationKeys)
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, observed.callback)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	beforeGeneration := fixture.user.Collection.Generation()
	storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{
		Phase: storeio.TxnMarkerFaultCreateHeaderWrite,
	})
	defer storeio.ProgramTxnMarkerCreateFault(storeio.TxnMarkerFaultPlan{})
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("a"), Value: []byte(`{"n":1}`),
	})
	if _, err := fixture.machine.ApplyNormal(normalMeta(2), command); err == nil {
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
	fixture := newValidatedMachineFixture(t, mutationValidatorFuncs{}, func(keys AttemptedMutationKeys) {
		if keys.Len() != 1 || !bytes.Equal(keys.Key(0), []byte("a")) {
			panic("unexpected attempted mutation keys")
		}
		entered <- struct{}{}
		<-release
	})
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	command := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("a"), Value: []byte(`{"n":1}`),
	})
	done := make(chan error, 1)
	go func() {
		_, err := fixture.machine.ApplyNormal(normalMeta(2), command)
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

func TestLogicalDigestValidationProfileGoldens(t *testing.T) {
	overlay := []finalMutation{
		{key: []byte("b"), value: []byte(`{"n":2}`)},
		{key: []byte("z"), delete: true},
		{key: []byte("a"), value: []byte(`{"n":1}`)},
	}
	legacy, err := logicalDigestV1(
		"docs", ValidationSchemaFreeJSONV1, [32]byte{}, nil, overlay,
	)
	if err != nil {
		t.Fatal(err)
	}
	validated, err := logicalDigestV1(
		"docs", ValidationDeterministicMutationV1, testMutationValidationDigest, nil, overlay,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertDigestHex(t, "legacy", legacy, "8ba4f8aa91e17d78aacddad9003f3f8e40f39ee0534d7d8414ee9a37bf4fa786")
	assertDigestHex(t, "validated", validated, "3370965d8c0b979ebf62a0366d9e55278b1f815ecfc1f4d321d0d3695f1495ec")
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

func FuzzLogicalDigestValidationProfile(f *testing.F) {
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
		first, err := logicalDigestV1(
			name, ValidationDeterministicMutationV1, testMutationValidationDigest, nil, overlay,
		)
		if err != nil {
			t.Fatal(err)
		}
		second, err := logicalDigestV1(
			name, ValidationDeterministicMutationV1, testMutationValidationDigest, nil, overlay,
		)
		if err != nil {
			t.Fatal(err)
		}
		if first != second {
			t.Fatalf("nondeterministic digest: %x != %x", first, second)
		}
		if !bytes.Equal(overlay[0].key, beforeKey) || !bytes.Equal(overlay[0].value, beforeValue) {
			t.Fatal("logical digest mutated its overlay")
		}
	})
}
