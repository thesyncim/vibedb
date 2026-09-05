package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/routegate"
)

var (
	dataChainDigestTestSink [32]byte
	dataChainDigestTestErr  error
)

func TestDataChainTransitionDigestDeterministicAndHistorySensitive(t *testing.T) {
	previous := sha256.Sum256([]byte("previous publication"))
	contract := sha256.Sum256([]byte("apply contract"))
	changes := []finalMutation{
		{key: []byte("a"), value: []byte(`{"n":1}`)},
		{
			key: []byte("b"), before: []byte(`{"n":1}`), value: []byte(`{"n":2}`),
			beforeFound: true,
		},
		{key: []byte("c"), before: []byte(`{"n":3}`), beforeFound: true, delete: true},
	}

	baseline := mustDataChainTransitionDigest(t, nil, previous, contract, changes)
	workspace := newDataChainHasher()
	if got := mustDataChainTransitionDigest(t, workspace, previous, contract, changes); got != baseline {
		t.Fatalf("reusable-workspace digest = %x, want %x", got, baseline)
	}
	if got := mustDataChainTransitionDigest(t, workspace, previous, contract, changes); got != baseline {
		t.Fatalf("reused-workspace digest = %x, want %x", got, baseline)
	}

	tests := []struct {
		name     string
		previous [32]byte
		contract [32]byte
		changes  []finalMutation
	}{
		{
			name: "previous publication", previous: sha256.Sum256([]byte("another publication")),
			contract: contract, changes: cloneFinalMutations(changes),
		},
		{
			name: "apply contract", previous: previous,
			contract: sha256.Sum256([]byte("another contract")), changes: cloneFinalMutations(changes),
		},
		{
			name: "key", previous: previous, contract: contract,
			changes: mutateFinalMutations(changes, func(changes []finalMutation) {
				changes[1].key = []byte("bb")
			}),
		},
		{
			name: "before value", previous: previous, contract: contract,
			changes: mutateFinalMutations(changes, func(changes []finalMutation) {
				changes[1].before = []byte(`{"n":0}`)
			}),
		},
		{
			name: "before existence", previous: previous, contract: contract,
			changes: mutateFinalMutations(changes, func(changes []finalMutation) {
				changes[0].before = []byte(`{"n":0}`)
				changes[0].beforeFound = true
			}),
		},
		{
			name: "after value", previous: previous, contract: contract,
			changes: mutateFinalMutations(changes, func(changes []finalMutation) {
				changes[1].value = []byte(`{"n":4}`)
			}),
		},
		{
			name: "delete marker", previous: previous, contract: contract,
			changes: mutateFinalMutations(changes, func(changes []finalMutation) {
				changes[2].delete = false
				changes[2].value = []byte(`{"n":4}`)
			}),
		},
		{
			name: "change set", previous: previous, contract: contract,
			changes: cloneFinalMutations(changes[:2]),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := mustDataChainTransitionDigest(t, newDataChainHasher(), test.previous, test.contract, test.changes)
			if got == baseline {
				t.Fatalf("digest did not bind %s", test.name)
			}
		})
	}
}

func TestDataChainTransitionDigestStoredBeforeDescriptorsMatchRawValues(t *testing.T) {
	previous := sha256.Sum256([]byte("previous publication"))
	contract := sha256.Sum256([]byte("apply contract"))
	raw := []finalMutation{
		{key: []byte("a"), value: []byte(`{"n":1}`)},
		{
			key: []byte("b"), before: []byte(`{"n":1}`), value: []byte(`{"n":2}`),
			beforeFound: true,
		},
		{key: []byte("c"), before: []byte(`{"n":3}`), beforeFound: true, delete: true},
	}
	want := mustDataChainTransitionDigest(t, newDataChainHasher(), previous, contract, raw)
	described := cloneFinalMutations(raw)
	descriptors := make([]mutationValueDescriptor, len(described))
	for index := range described {
		described[index].described = true
		described[index].descriptorIndex = uint16(index)
		if described[index].beforeFound {
			descriptors[index].beforeLength = uint64(len(described[index].before))
			descriptors[index].beforeDigest = sha256.Sum256(described[index].before)
			described[index].before = nil
		}
		if !described[index].delete {
			descriptors[index].afterLength = uint64(len(described[index].value))
			descriptors[index].afterDigest = sha256.Sum256(described[index].value)
		}
	}
	if got := mustDataChainTransitionDigestWithDescriptors(
		t, newDataChainHasher(), previous, contract, described, descriptors,
	); got != want {
		t.Fatalf("descriptor digest = %x, want raw-value digest %x", got, want)
	}
	descriptors[1].beforeLength++
	if got := mustDataChainTransitionDigestWithDescriptors(
		t, newDataChainHasher(), previous, contract, described, descriptors,
	); got == want {
		t.Fatal("descriptor digest did not bind before-value length")
	}
	descriptors[1].beforeLength--
	descriptors[1].beforeDigest[0] ^= 1
	if got := mustDataChainTransitionDigestWithDescriptors(
		t, newDataChainHasher(), previous, contract, described, descriptors,
	); got == want {
		t.Fatal("descriptor digest did not bind before-value SHA-256")
	}
	descriptors[1].beforeDigest[0] ^= 1
	descriptors[1].afterDigest[0] ^= 1
	if got := mustDataChainTransitionDigestWithDescriptors(
		t, newDataChainHasher(), previous, contract, described, descriptors,
	); got == want {
		t.Fatal("descriptor digest did not bind after-value SHA-256")
	}
}

func TestDataChainSeedDigestBindsContractAndCanonicalImage(t *testing.T) {
	contract := sha256.Sum256([]byte("apply contract"))
	image := sha256.Sum256([]byte("canonical image"))
	baseline, err := dataChainSeedDigest(contract, image)
	if err != nil || baseline == ([32]byte{}) || baseline == image {
		t.Fatalf("seed digest = %x, %v", baseline, err)
	}
	if again, err := dataChainSeedDigest(contract, image); err != nil || again != baseline {
		t.Fatalf("repeated seed digest = %x, %v; want %x", again, err, baseline)
	}
	if other, err := dataChainSeedDigest(sha256.Sum256([]byte("other contract")), image); err != nil || other == baseline {
		t.Fatalf("contract-independent seed = %x, %v", other, err)
	}
	if other, err := dataChainSeedDigest(contract, sha256.Sum256([]byte("other image"))); err != nil || other == baseline {
		t.Fatalf("image-independent seed = %x, %v", other, err)
	}
	if _, err := dataChainSeedDigest([32]byte{}, image); !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("zero-contract seed error = %v", err)
	}
	if _, err := dataChainSeedDigest(contract, [32]byte{}); !errors.Is(err, ErrInvalidCollection) {
		t.Fatalf("zero-image seed error = %v", err)
	}
}

func TestReplicatedDigestGoldenVectors(t *testing.T) {
	validationDigest := sha256.Sum256([]byte("validation profile"))
	target := CollectionTarget{
		Validation:       ValidationDeterministicMutation,
		ValidationDigest: validationDigest,
		Limits: CollectionLimits{
			MaxKeyBytes: 256, MaxDocumentBytes: 1 << 20,
			MaxDistinctMutations: 64, MaxBatchBytes: 8 << 20,
		},
	}
	relations := []relationCollection{{
		id: 1, kind: RelationJSON, name: "docs", target: target,
	}}
	contract, err := bundleApplyContractDigest(
		relationManifestDigest(1, relations), relations, 1024, 8,
		0, 0, RequestLedgerRange{}, 59,
	)
	if err != nil {
		t.Fatal(err)
	}
	derived := deriveBundleContractForTest(relationManifestDigest(1, relations), 1024, 8, 59)
	if derived != contract {
		t.Fatalf("contract implementation diverged from independent semantic frame: got=%x derived=%x",
			contract, derived)
	}
	seed, err := dataChainSeedDigest(contract, sha256.Sum256([]byte("canonical image")))
	if err != nil {
		t.Fatal(err)
	}
	transition, err := dataChainTransitionDigest(nil, seed, contract, []finalMutation{
		{key: []byte("a"), value: []byte(`{"n":1}`)},
		{
			key: []byte("b"), before: []byte(`{"n":1}`), value: []byte(`{"n":2}`),
			beforeFound: true,
		},
		{key: []byte("c"), before: []byte(`{"n":3}`), beforeFound: true, delete: true},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertDigestHex(t, "apply contract", contract,
		"469dedc6a745c6764b1b69b944cb5661230e116187878610fdf2e88895c6cd3a")
	assertDigestHex(t, "data-chain seed", seed,
		"1bc7b779235ab5578166d0343c9c9ead27bed8534b1466a2f67627024b45d5ba")
	assertDigestHex(t, "data-chain transition", transition,
		"32e8a23f2c06ede9e774fca0141292ad70410234567f6628f252b72feab56190")
}

func deriveBundleContractForTest(manifest [sha256.Size]byte, maxSessions uint64,
	retryWindow uint16, routeGateMaxRecords uint64) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("vibedb/replicated-state/apply-contract\x00"))
	_, _ = h.Write(manifest[:])
	base := sha256.Sum256([]byte(deterministicApplySemantics))
	bundle := sha256.Sum256([]byte(deterministicBundleApplySemantics))
	_, _ = h.Write(base[:])
	_, _ = h.Write(bundle[:])
	var grammar [4 + 37*4]byte
	binary.LittleEndian.PutUint16(grammar[:2], ResultFormatMutation)
	binary.LittleEndian.PutUint16(grammar[2:4], ResultFormatRouteGate)
	for index, code := range [...]uint32{
		ResultApplied, ResultStaleFence, ResultUnknownRelation, ResultInvalidDocument,
		ResultTargetBound, ResultWrongShard, ResultSessionRetired, ResultSessionOpened,
		ResultSessionRenewed, ResultSessionRevoked, ResultIndexConflict, ResultIntentBusy,
		MaxDistinctMutations,
		uint32(replication.MutationPut), uint32(replication.MutationDelete),
		uint32(replication.MutationPutAbsentOrEqual),
		uint32(replication.MutationDeleteDigestEqual),
		uint32(replication.MutationPutDigestEqual), uint32(replication.MutationPutAbsent),
		uint32(replication.MutationPutPresent), uint32(replication.MutationPutIfAbsent), uint32(replication.MutationPutConflict), replication.MutationDigestCompareBytes,
		ResultRouteGate, uint32(replication.CommandRouteGate),
		routegate.CommandBytes, routegate.OutcomeBytes,
		routegate.HeadBytes, routegate.StoredPinBytes,
		uint32(routegate.OperationAcquireShared), uint32(routegate.OperationReleaseShared),
		uint32(routegate.OperationBeginExclusive), uint32(routegate.OperationReleaseExclusive),
		uint32(routegate.OperationCompactReleased), uint32(routegate.ReasonExhausted),
		uint32(routegate.PinReleased), uint32(routegate.DrainReleased),
	} {
		binary.LittleEndian.PutUint32(grammar[4+index*4:], code)
	}
	_, _ = h.Write(grammar[:])
	var limits [26]byte
	binary.LittleEndian.PutUint64(limits[:8], maxSessions)
	binary.LittleEndian.PutUint16(limits[8:10], retryWindow)
	binary.LittleEndian.PutUint64(limits[10:], MaxSessionRetryWindow)
	binary.LittleEndian.PutUint64(limits[18:], routeGateMaxRecords)
	_, _ = h.Write(limits[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

func TestDataChainTransitionDigestRejectsNonCanonicalTransitions(t *testing.T) {
	previous := sha256.Sum256([]byte("previous publication"))
	contract := sha256.Sum256([]byte("apply contract"))
	valid := []finalMutation{
		{key: []byte("a"), value: []byte(`{"n":1}`)},
		{key: []byte("b"), before: []byte(`{"n":1}`), beforeFound: true, delete: true},
	}
	tests := []struct {
		name        string
		previous    [32]byte
		contract    [32]byte
		changes     []finalMutation
		descriptors []mutationValueDescriptor
	}{
		{name: "zero previous", contract: contract, changes: cloneFinalMutations(valid)},
		{name: "zero contract", previous: previous, changes: cloneFinalMutations(valid)},
		{name: "empty changes", previous: previous, contract: contract},
		{
			name: "empty key", previous: previous, contract: contract,
			changes: []finalMutation{{value: []byte(`{"n":1}`)}},
		},
		{
			name: "delete absent row", previous: previous, contract: contract,
			changes: []finalMutation{{key: []byte("a"), delete: true}},
		},
		{
			name: "missing before descriptor", previous: previous, contract: contract,
			changes: []finalMutation{{
				key: []byte("a"), beforeFound: true, delete: true,
			}},
		},
		{
			name: "descriptor for absent before", previous: previous, contract: contract,
			changes: []finalMutation{{
				key: []byte("a"), value: []byte(`{"n":1}`), described: true,
			}},
			descriptors: []mutationValueDescriptor{{beforeLength: 1}},
		},
		{
			name: "duplicate key", previous: previous, contract: contract,
			changes: []finalMutation{
				{key: []byte("a"), value: []byte(`{"n":1}`)},
				{key: []byte("a"), value: []byte(`{"n":2}`)},
			},
		},
		{
			name: "descriptor for deleted after", previous: previous, contract: contract,
			changes: []finalMutation{{
				key: []byte("a"), beforeFound: true, delete: true, described: true,
			}},
			descriptors: []mutationValueDescriptor{{beforeLength: 7, afterLength: 1}},
		},
		{
			name: "unsorted keys", previous: previous, contract: contract,
			changes: []finalMutation{
				{key: []byte("b"), value: []byte(`{"n":2}`)},
				{key: []byte("a"), value: []byte(`{"n":1}`)},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := dataChainTransitionDigest(
				newDataChainHasher(), test.previous, test.contract, test.changes,
				test.descriptors,
			)
			if !errors.Is(err, ErrInvalidCollection) {
				t.Fatalf("error = %v, want ErrInvalidCollection", err)
			}
		})
	}
}

func TestDataChainTransitionDigestReusableWorkspaceAllocations(t *testing.T) {
	previous := sha256.Sum256([]byte("previous publication"))
	contract := sha256.Sum256([]byte("apply contract"))
	changes := []finalMutation{{
		key: []byte("k"), before: []byte(`{"n":1}`), value: []byte(`{"n":2}`),
		beforeFound: true,
	}}
	workspace := newDataChainHasher()
	if allocations := testing.AllocsPerRun(1000, func() {
		dataChainDigestTestSink, dataChainDigestTestErr = dataChainTransitionDigest(
			workspace, previous, contract, changes, nil,
		)
	}); allocations != 0 {
		t.Fatalf("dataChainTransitionDigest allocations = %v, want 0", allocations)
	}
	if dataChainDigestTestErr != nil {
		t.Fatal(dataChainDigestTestErr)
	}
}

func BenchmarkDataChainTransitionDigestOneMutation(b *testing.B) {
	previous := sha256.Sum256([]byte("previous publication"))
	contract := sha256.Sum256([]byte("apply contract"))
	changes := []finalMutation{{
		key: []byte("k"), before: []byte(`{"n":1}`), value: []byte(`{"n":2}`),
		beforeFound: true,
	}}
	workspace := newDataChainHasher()
	b.ReportAllocs()
	b.SetBytes(int64(len(changes[0].key) + len(changes[0].before) + len(changes[0].value)))
	b.ResetTimer()
	for b.Loop() {
		dataChainDigestTestSink, dataChainDigestTestErr = dataChainTransitionDigest(
			workspace, previous, contract, changes, nil,
		)
	}
	if dataChainDigestTestErr != nil {
		b.Fatal(dataChainDigestTestErr)
	}
}

func BenchmarkMachineAdmitPointUpdate(b *testing.B) {
	for _, rows := range []int{2, 256, 4096} {
		b.Run(fmt.Sprintf("rows=%d", rows), func(b *testing.B) {
			fixture := newMachineFixture(b)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				b.Fatal(err)
			}
			open := commandValue(fixture.binding, 1)
			applySessionOpen(b, fixture.machine, 2, open)
			sequence := uint64(1)
			applied := uint64(3)
			for first := 0; first < rows; first += MaxDistinctMutations {
				last := min(first+MaxDistinctMutations, rows)
				mutations := make([]replication.Mutation, 0, last-first)
				for index := first; index < last; index++ {
					mutations = append(mutations, replication.Mutation{
						Kind: replication.MutationPut, Key: dataChainRowKey(index),
						Value: []byte(`{"seed":true}`),
					})
				}
				command := testCommand(fixture.binding, sequence, mutations...)
				if _, err := fixture.machine.ApplyNormal(normalMeta(applied), command); err != nil {
					b.Fatalf("seed rows [%d,%d): %v", first, last, err)
				}
				sequence++
				applied++
			}
			command := testCommand(fixture.binding, sequence, replication.Mutation{
				Kind: replication.MutationPut, Key: dataChainRowKey(rows / 2),
				Value: []byte(`{"seed":false}`),
			})
			b.ReportAllocs()
			b.SetBytes(int64(len(command)))
			b.ResetTimer()
			for b.Loop() {
				if err := fixture.machine.AdmitCommand(command); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type dataChainValidationCall struct {
	kind    replication.MutationKind
	key     []byte
	value   []byte
	current []byte
	found   bool
}

type dataChainValidationTrace struct {
	calls []dataChainValidationCall
}

func (trace *dataChainValidationTrace) ValidatePut(key, value []byte) MutationValidation {
	trace.calls = append(trace.calls, dataChainValidationCall{
		kind: replication.MutationPut, key: bytes.Clone(key), value: bytes.Clone(value),
	})
	return MutationValidationAccept
}

func (trace *dataChainValidationTrace) ValidateDelete(
	key, current []byte,
	found bool,
) MutationValidation {
	trace.calls = append(trace.calls, dataChainValidationCall{
		kind: replication.MutationDelete, key: bytes.Clone(key),
		current: bytes.Clone(current), found: found,
	})
	return MutationValidationAccept
}

func (trace *dataChainValidationTrace) reset() {
	trace.calls = trace.calls[:0]
}

func TestAdmitAndApplyValidateOnlyCommandKeys(t *testing.T) {
	for _, rows := range []int{2, 256} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			trace := new(dataChainValidationTrace)
			fixture := newValidatedMachineFixture(t, trace, nil)
			if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
				t.Fatal(err)
			}
			open := commandValue(fixture.binding, 1)
			applySessionOpen(t, fixture.machine, 2, open)

			sequence := uint64(1)
			applied := uint64(3)
			for first := 0; first < rows; first += MaxDistinctMutations {
				last := min(first+MaxDistinctMutations, rows)
				mutations := make([]replication.Mutation, 0, last-first)
				for index := first; index < last; index++ {
					mutations = append(mutations, replication.Mutation{
						Kind: replication.MutationPut, Key: dataChainRowKey(index),
						Value: []byte(`{"seed":true}`),
					})
				}
				command := testCommand(fixture.binding, sequence, mutations...)
				if _, err := fixture.machine.ApplyNormal(normalMeta(applied), command); err != nil {
					t.Fatalf("seed rows [%d,%d): %v", first, last, err)
				}
				sequence++
				applied++
			}

			command := testCommand(fixture.binding, sequence,
				replication.Mutation{
					Kind: replication.MutationPut, Key: dataChainRowKey(0),
					Value: []byte(`{"seed":false}`),
				},
				replication.Mutation{Kind: replication.MutationDelete, Key: dataChainRowKey(1)},
			)
			want := []dataChainValidationCall{
				{
					kind: replication.MutationPut, key: dataChainRowKey(0),
					value: []byte(`{"seed":false}`),
				},
				{
					kind: replication.MutationDelete, key: dataChainRowKey(1),
					current: []byte(`{"seed":true}`), found: true,
				},
			}

			trace.reset()
			if err := fixture.machine.AdmitCommand(command); err != nil {
				t.Fatalf("AdmitCommand: %v", err)
			}
			assertDataChainValidationCalls(t, "AdmitCommand", trace.calls, want)

			trace.reset()
			if _, err := fixture.machine.ApplyNormal(normalMeta(applied), command); err != nil {
				t.Fatalf("ApplyNormal: %v", err)
			}
			assertDataChainValidationCalls(t, "ApplyNormal", trace.calls, want)
		})
	}
}

func TestDataChainPreservedWithoutEffectiveRowChanges(t *testing.T) {
	fixture := newMachineFixture(t)
	if _, err := fixture.machine.InstallSnapshot(fixture.bootstrap); err != nil {
		t.Fatal(err)
	}
	open := commandValue(fixture.binding, 1)
	applySessionOpen(t, fixture.machine, 2, open)
	put := testCommand(fixture.binding, 1, replication.Mutation{
		Kind: replication.MutationPut, Key: []byte("present"), Value: []byte(`{"n":1}`),
	})
	publication, err := fixture.machine.ApplyNormal(normalMeta(3), put)
	if err != nil {
		t.Fatal(err)
	}
	want := publication.DataChainDigest

	publication, err = fixture.machine.ApplyNormal(normalMeta(3), put)
	if err != nil || publication.DataChainDigest != want {
		t.Fatalf("exact replay chain = %x, %v; want %x", publication.DataChainDigest, err, want)
	}
	commands := [][]byte{
		testCommand(fixture.binding, 2, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("present"), Value: []byte(`{"n":1}`),
		}),
		testCommand(fixture.binding, 3, replication.Mutation{
			Kind: replication.MutationDelete, Key: []byte("absent"),
		}),
		testCommand(fixture.binding, 4, replication.Mutation{
			Kind: replication.MutationPut, Key: []byte("invalid"), Value: []byte(`{"n":`),
		}),
	}
	for index, command := range commands {
		publication, err = fixture.machine.ApplyNormal(normalMeta(uint64(index+4)), command)
		if err != nil || publication.DataChainDigest != want {
			t.Fatalf("non-changing command %d chain = %x, %v; want %x",
				index, publication.DataChainDigest, err, want)
		}
	}
}

func mustDataChainTransitionDigest(
	t testing.TB,
	workspace *dataChainHasher,
	previous, contract [32]byte,
	changes []finalMutation,
) [32]byte {
	t.Helper()
	digest, err := dataChainTransitionDigest(workspace, previous, contract, changes, nil)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func mustDataChainTransitionDigestWithDescriptors(
	t testing.TB,
	workspace *dataChainHasher,
	previous, contract [32]byte,
	changes []finalMutation,
	descriptors []mutationValueDescriptor,
) [32]byte {
	t.Helper()
	digest, err := dataChainTransitionDigest(
		workspace, previous, contract, changes, descriptors,
	)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func cloneFinalMutations(changes []finalMutation) []finalMutation {
	cloned := make([]finalMutation, len(changes))
	for index := range changes {
		cloned[index] = changes[index]
		cloned[index].key = bytes.Clone(changes[index].key)
		cloned[index].value = bytes.Clone(changes[index].value)
		cloned[index].before = bytes.Clone(changes[index].before)
	}
	return cloned
}

func mutateFinalMutations(
	changes []finalMutation,
	mutate func([]finalMutation),
) []finalMutation {
	cloned := cloneFinalMutations(changes)
	mutate(cloned)
	return cloned
}

func dataChainRowKey(index int) []byte {
	return fmt.Appendf(nil, "row-%06d", index)
}

func assertDataChainValidationCalls(
	t testing.TB,
	operation string,
	got, want []dataChainValidationCall,
) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s validator calls = %#v, want %#v", operation, got, want)
	}
	for index := range want {
		if got[index].kind != want[index].kind ||
			!bytes.Equal(got[index].key, want[index].key) ||
			!bytes.Equal(got[index].value, want[index].value) ||
			!bytes.Equal(got[index].current, want[index].current) ||
			got[index].found != want[index].found {
			t.Fatalf("%s validator call %d = %#v, want %#v", operation, index, got[index], want[index])
		}
	}
}
