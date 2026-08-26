package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"runtime"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

type durableLogicalStreamPages struct {
	pages [][]byte
	gets  uint64
	bytes uint64
}

func (pages *durableLogicalStreamPages) Put(ordinal uint32, raw []byte) error {
	if pages == nil || ordinal != uint32(len(pages.pages)) {
		return ErrDurableRequestConflict
	}
	pages.pages = append(pages.pages, bytes.Clone(raw))
	pages.bytes += uint64(len(raw))
	return nil
}

func (pages *durableLogicalStreamPages) Get(ordinal uint32) ([]byte, error) {
	if pages == nil || int(ordinal) >= len(pages.pages) {
		return nil, ErrDurableRequestConflict
	}
	pages.gets++
	return pages.pages[ordinal], nil
}

func durableLogicalStreamFixture(
	t testing.TB,
	count int,
	valueBytes int,
) (DurableRequestLedgerKey, DurableRequestLogicalProgram) {
	t.Helper()
	tenant := []byte("logical-stream-tenant")
	requestID := replication.ID128{0x31, byte(count), byte(count >> 8)}
	requestDigest := replication.Digest{0x41, byte(count), byte(count >> 8)}
	key := DurableRequestLedgerKey{
		RequestKey: requestledger.RequestKey{
			Scope:        requestledger.ScopeAuthenticated,
			Principal:    requestledger.PrincipalID{0x11},
			Request:      requestledger.RequestID(requestID),
			TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		},
		Digest: requestDigest,
	}
	keyDigest, err := requestledger.KeyDigest(key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte{0x5a}, max(1, valueBytes))
	participants := make([]DurableRequestLogicalParticipant, count)
	for index := range participants {
		shard := distribution.ShardID(fmt.Sprintf("shard-%08d", index))
		participant := &participants[index]
		*participant = DurableRequestLogicalParticipant{
			Distribution: distribution.DistributionName("orders"), Shard: shard,
			RangeIdentity: replication.Digest{0x51, byte(index), byte(index >> 8)},
			Group: raftmember.GroupKey{
				ClusterID: [16]byte{0x61}, ClusterIncarnation: [16]byte{0x62},
				TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{0x63, byte(index)},
				GroupID: [16]byte{0x64, byte(index), byte(index >> 8)},
			},
			SchemaGeneration:       7,
			RelationManifestDigest: replication.Digest{0x71, byte(index)},
			LineageDigest:          replication.Digest{0x72, byte(index)},
			ForwardingRuleDigest:   replication.Digest{0x73, byte(index)},
			BucketBits:             8,
			IntentScopes:           []distributedtxn.IntentScope{{Start: uint32(index % 128), End: uint32(index%128 + 1)}},
			Batches: []replication.RelationMutationBatch{{
				Relation: 1,
				Mutations: []replication.Mutation{{
					Kind: replication.MutationPut, Key: []byte{0x81, byte(index)}, Value: value,
				}},
			}},
		}
	}
	program := DurableRequestLogicalProgram{
		Identity: ReplicatedTransactionIdentity{
			CatalogGeneration: 7, RecoveryDeadline: 9_000_000_000,
			CoordinatorOrdinal: uint32(count / 2),
		},
		Contract: DurableRequestExecutionContract{
			ApplyContractDigest:          replication.Digest{1},
			InitialStateDigest:           replication.Digest{2},
			CommitTerminalStateDigest:    replication.Digest{3},
			AbortTerminalStateDigest:     replication.Digest{4},
			TerminalSummaryDigest:        replication.Digest{10},
			PinEpoch:                     1,
			PinDigest:                    replication.Digest{6},
			RouteSchemaCertificateDigest: replication.Digest{7},
			RetirementWitnessDigest:      replication.Digest{9},
			CommitTransitionTag:          1, AbortTransitionTag: 2,
			CommitFinalWaveCount: 1, AbortFinalWaveCount: 1,
			MaxPendingWaveBytes: 1024, MaxContinuationBytes: 1024,
			MaxTerminalBytes: 1024, PlanningLeaseExpiryIndex: 100,
			PlanningLeaseGeneration: 1,
		},
		Tenant: tenant, KeyDigest: replication.Digest(keyDigest),
		RequestID: requestID, RequestDigest: requestDigest, Participants: participants,
	}
	program, err = SealDurableRequestLogicalProgram(program)
	if err != nil {
		t.Fatal(err)
	}
	return key, program
}

func durableLogicalStreamBuild(
	t testing.TB,
	key DurableRequestLedgerKey,
	program DurableRequestLogicalProgram,
) (durableRequestPlanMeasurement, *durableLogicalStreamPages) {
	t.Helper()
	measurement, err := measureDurableRequestPlan(key, program)
	if err != nil {
		t.Fatal(err)
	}
	pages := new(durableLogicalStreamPages)
	if err := streamDurableRequestPlan(measurement, program, pages); err != nil {
		t.Fatal(err)
	}
	if pages.bytes != measurement.PlanBytes || len(pages.pages) != int(measurement.PhysicalPages) {
		t.Fatalf("stored bytes/pages=%d/%d want=%d/%d", pages.bytes, len(pages.pages), measurement.PlanBytes, measurement.PhysicalPages)
	}
	return measurement, pages
}

func TestDurableRequestLogicalPlanStreamRoundTripWide(t *testing.T) {
	t.Parallel()
	for _, count := range []int{1, 64, 65, 4097} {
		count := count
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			key, program := durableLogicalStreamFixture(t, count, 3)
			measurement, pages := durableLogicalStreamBuild(t, key, program)
			source := durableRequestPlanPageSource(pages)
			if len(measurement.Inline) != 0 {
				source = nil
			}
			reader, err := openDurableRequestRecipeStream(key, measurement.descriptor(), source)
			if err != nil {
				t.Fatal(err)
			}
			if reader.Identity != program.Identity || reader.Contract != program.Contract ||
				reader.KeyDigest != program.KeyDigest || reader.RequestID != program.RequestID ||
				reader.RequestDigest != program.RequestDigest ||
				!bytes.Equal(reader.Tenant, program.Tenant) || reader.ParticipantCount != uint64(count) {
				t.Fatal("decoded fixed logical program differs")
			}
			visited := 0
			for reader.Next() {
				current := reader.Current()
				want := program.Participants[visited]
				if current.Distribution != want.Distribution || current.Shard != want.Shard ||
					current.RangeIdentity != want.RangeIdentity || current.Group != want.Group ||
					current.SchemaGeneration != want.SchemaGeneration ||
					current.MutationDigest != want.MutationDigest ||
					len(current.IntentScopes) != len(want.IntentScopes) ||
					len(current.Batches) != 1 || len(current.Batches[0].Mutations) != 1 ||
					!bytes.Equal(current.Batches[0].Mutations[0].Value, want.Batches[0].Mutations[0].Value) {
					t.Fatalf("participant %d differs", visited)
				}
				visited++
			}
			if reader.Err() != nil || !reader.Complete() || visited != count {
				t.Fatalf("visited=%d complete=%v err=%v", visited, reader.Complete(), reader.Err())
			}
		})
	}
}

func TestDurableRequestLogicalPlanFragmentationAndMaxScopes(t *testing.T) {
	t.Parallel()
	key, program := durableLogicalStreamFixture(t, 2, requestledger.MaxPlanPageBytes)
	scopes := make([]distributedtxn.IntentScope, distributedtxn.MaxIntentScopes)
	for index := range scopes {
		scopes[index] = distributedtxn.IntentScope{Start: uint32(index * 2), End: uint32(index*2 + 1)}
	}
	program.Participants[0].BucketBits = 16
	program.Participants[0].IntentScopes = scopes
	var err error
	program, err = SealDurableRequestLogicalProgram(program)
	if err != nil {
		t.Fatal(err)
	}
	measurement, pages := durableLogicalStreamBuild(t, key, program)
	if measurement.PhysicalPages < 3 {
		t.Fatalf("page-boundary fixture used only %d pages", measurement.PhysicalPages)
	}
	reader, err := openDurableRequestRecipeStream(key, measurement.descriptor(), pages)
	if err != nil || !reader.Next() || len(reader.Current().IntentScopes) != distributedtxn.MaxIntentScopes ||
		len(reader.Current().Batches[0].Mutations[0].Value) != requestledger.MaxPlanPageBytes {
		t.Fatalf("fragmented participant failed: err=%v", err)
	}
}

func TestDurableRequestLogicalPlanRejectsProgramPerturbation(t *testing.T) {
	t.Parallel()
	key, program := durableLogicalStreamFixture(t, 3, 8)
	measurement, _ := durableLogicalStreamBuild(t, key, program)
	for _, mutate := range []func(*DurableRequestLogicalProgram){
		func(value *DurableRequestLogicalProgram) { value.Tenant[0] ^= 1 },
		func(value *DurableRequestLogicalProgram) { value.RequestDigest[0] ^= 1 },
		func(value *DurableRequestLogicalProgram) { value.Identity.RecoveryDeadline++ },
		func(value *DurableRequestLogicalProgram) { value.Identity.RetryHome[0] ^= 1 },
		func(value *DurableRequestLogicalProgram) { value.Contract.TerminalSummaryDigest[0] ^= 1 },
		func(value *DurableRequestLogicalProgram) { value.Contract.PlanBuildID[0] ^= 1 },
		func(value *DurableRequestLogicalProgram) { value.Contract.PlanningLeaseGeneration++ },
		func(value *DurableRequestLogicalProgram) { value.Participants[0].SchemaGeneration++ },
		func(value *DurableRequestLogicalProgram) { value.Participants[0].BucketBits++ },
		func(value *DurableRequestLogicalProgram) { value.Participants[0].IntentScopes[0].End++ },
	} {
		changed := cloneDurableLogicalStreamProgram(program)
		mutate(&changed)
		if err := streamDurableRequestPlan(measurement, changed, new(durableLogicalStreamPages)); !errors.Is(err, ErrDurableRequestConflict) {
			t.Fatalf("perturbed sealed program error=%v", err)
		}
	}
	reordered := cloneDurableLogicalStreamProgram(program)
	reordered.Participants[0], reordered.Participants[1] = reordered.Participants[1], reordered.Participants[0]
	if _, err := measureDurableRequestPlan(key, reordered); err == nil {
		t.Fatal("noncanonical participant order accepted")
	}
	duplicate := cloneDurableLogicalStreamProgram(program)
	duplicate.Participants[1] = duplicate.Participants[0]
	if _, err := measureDurableRequestPlan(key, duplicate); err == nil {
		t.Fatal("duplicate logical participant accepted")
	}
}

func cloneDurableLogicalStreamProgram(program DurableRequestLogicalProgram) DurableRequestLogicalProgram {
	cloned := program
	cloned.Tenant = bytes.Clone(program.Tenant)
	cloned.Participants = append([]DurableRequestLogicalParticipant(nil), program.Participants...)
	for index := range cloned.Participants {
		cloned.Participants[index].IntentScopes = append([]distributedtxn.IntentScope(nil), program.Participants[index].IntentScopes...)
		cloned.Participants[index].Batches = append([]replication.RelationMutationBatch(nil), program.Participants[index].Batches...)
	}
	return cloned
}

func TestDurableRequestLogicalPlanRejectsPageMutationAfterValidation(t *testing.T) {
	t.Parallel()
	key, program := durableLogicalStreamFixture(t, 3, requestledger.MaxPlanPageBytes)
	measurement, pages := durableLogicalStreamBuild(t, key, program)
	reader, err := openDurableRequestRecipeStream(key, measurement.descriptor(), pages)
	if err != nil || len(pages.pages) < 2 {
		t.Fatalf("open err=%v pages=%d", err, len(pages.pages))
	}
	pages.pages[1][0] ^= 1
	for reader.Next() {
	}
	if !errors.Is(reader.Err(), ErrDurableRequestConflict) || reader.Complete() {
		t.Fatalf("changed lazy page accepted: complete=%v err=%v", reader.Complete(), reader.Err())
	}
}

func TestDurableRequestLogicalPlanSizeBounds(t *testing.T) {
	t.Parallel()
	maximum := min(uint64(MaxDurableRequestRecipeBytes),
		uint64(requestledger.MaxPlanBytes-durableRequestPlanHeaderBytes-durableRequestPlanTrailerBytes))
	for _, recipeBytes := range []uint64{maximum - 1, maximum} {
		planBytes, pages, err := durableRequestPlanSizes(recipeBytes)
		if err != nil || planBytes != recipeBytes+durableRequestPlanHeaderBytes+durableRequestPlanTrailerBytes || pages == 0 {
			t.Fatalf("recipe bytes=%d plan=%d pages=%d err=%v", recipeBytes, planBytes, pages, err)
		}
	}
	if _, _, err := durableRequestPlanSizes(maximum + 1); !errors.Is(err, ErrDurableRequestBound) {
		t.Fatalf("maximum+1 error=%v", err)
	}
}

func TestDurableRequestLogicalPlanSemanticMalformedRefusal(t *testing.T) {
	t.Parallel()
	key, program := durableLogicalStreamFixture(t, 1, 8)
	measurement, _ := durableLogicalStreamBuild(t, key, program)
	if len(measurement.Inline) == 0 {
		t.Fatal("semantic corruption fixture unexpectedly paged")
	}
	tenantBytes := len(program.Tenant)
	frameAt := durableRequestLogicalRecipeHeaderBytes + tenantBytes
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"header_reserved", func(recipe []byte) { recipe[26] = 1 }},
		{"participant_count", func(recipe []byte) { binary.LittleEndian.PutUint64(recipe[16:24], 2) }},
		{"frame_reserved", func(recipe []byte) { recipe[frameAt+21] = 1 }},
		{"frame_length", func(recipe []byte) { binary.LittleEndian.PutUint32(recipe[frameAt:frameAt+4], 1) }},
		{"relation_count", func(recipe []byte) { binary.LittleEndian.PutUint16(recipe[frameAt+10:frameAt+12], 0) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := measurement.Inline
			recipe := bytes.Clone(raw[durableRequestPlanHeaderBytes : len(raw)-durableRequestPlanTrailerBytes])
			test.mutate(recipe)
			binary.LittleEndian.PutUint32(
				recipe[len(recipe)-durableRequestRecipeTrailerBytes:],
				crc32.Checksum(recipe[:len(recipe)-durableRequestRecipeTrailerBytes], durableRequestLogicalRecipeCRC),
			)
			plan, err := requestledger.AppendPlan(nil, recipe)
			if err != nil {
				t.Fatal(err)
			}
			keyDigest, err := requestledger.KeyDigest(key.RequestKey)
			if err != nil {
				t.Fatal(err)
			}
			root, err := requestledger.PlanRoot(keyDigest, plan)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := DurableRequestPlanDescriptor{
				TotalBytes: uint64(len(plan)), Root: replication.Digest(root), Inline: plan,
			}
			if _, err := openDurableRequestRecipeStream(key, descriptor, nil); err == nil {
				t.Fatal("malformed but reauthenticated recipe accepted")
			}
		})
	}
}

func TestDurableRequestLogicalReplayPeakIsCardinalityBounded(t *testing.T) {
	t.Parallel()
	for _, count := range []int{64, 65, 4097} {
		key, program := durableLogicalStreamFixture(t, count, 4)
		measurement, pages := durableLogicalStreamBuild(t, key, program)
		reader, err := openDurableRequestRecipeStream(key, measurement.descriptor(), pages)
		if err != nil {
			t.Fatal(err)
		}
		got := reader.BufferedBytes()
		if got > durableRequestReaderMaxLiveBytes {
			t.Fatalf("buffer bytes=%d exceeds exact reader admission bound %d", got, durableRequestReaderMaxLiveBytes)
		}
	}
}

func TestDurableRequestLogicalPlan4097AllocationsAreFixed(t *testing.T) {
	key, program := durableLogicalStreamFixture(t, 4097, 4)
	measurement, err := measureDurableRequestPlan(key, program)
	if err != nil {
		t.Fatal(err)
	}
	if got := testing.AllocsPerRun(3, func() {
		measured, measureErr := measureDurableRequestPlan(key, program)
		if measureErr != nil || measured.Root != measurement.Root {
			panic("logical plan measurement changed")
		}
	}); got > 32 {
		t.Fatalf("4097-participant measure allocations=%v, want fixed <=32", got)
	}
	if got := testing.AllocsPerRun(3, func() {
		if streamErr := streamDurableRequestPlan(measurement, program, durableLogicalDiscardSink{}); streamErr != nil {
			panic(streamErr)
		}
	}); got > 32 {
		t.Fatalf("4097-participant stream allocations=%v, want fixed <=32", got)
	}
	_, pages := durableLogicalStreamBuild(t, key, program)
	if got := testing.AllocsPerRun(3, func() {
		reader, openErr := openDurableRequestRecipeStream(key, measurement.descriptor(), pages)
		if openErr != nil {
			panic(openErr)
		}
		visited := 0
		for reader.Next() {
			visited++
		}
		if reader.Err() != nil || !reader.Complete() || visited != 4097 {
			panic("logical plan replay changed")
		}
	}); got > 40 {
		t.Fatalf("4097-participant replay allocations=%v, want fixed <=40", got)
	}
}

func TestDurableRequestLogicalContractGolden(t *testing.T) {
	_, program := durableLogicalStreamFixture(t, 1, 4)
	for _, golden := range []struct {
		name string
		got  replication.Digest
		want string
	}{
		{"transaction_manifest", program.Contract.TransactionManifestDigest, "51e7388c9e85977a405006ea2d4b44fe374a9f8336f534d63bb2fdfd33082eb3"},
		{"protocol_program", program.Contract.ProtocolProgramDigest, "eb55f88e21584451b8f4b4acc994b05bea9bf148e61da133eefe4c1ea0f73f95"},
		{"terminal_contract", program.Contract.TerminalContractDigest, "a7fae65f0ad8a8b60a7e6aec51c39b02f1c68712a77e005915f0a7a116937e10"},
		{"retry_home", program.Contract.RetryHomeDerivationDigest, "f8d18eb29f23cf552f4f74ab86fdf1717bcb18c8599c08692941e2dd2163f7a3"},
		{"plan_build", program.Contract.PlanBuildID, "cd61dffcc1b2288c743473b7fac0ac8fcbdf2bffb64d214ef6a58cf857a24325"},
	} {
		if got := fmt.Sprintf("%x", golden.got); got != golden.want {
			t.Fatalf("%s digest=%s want=%s", golden.name, got, golden.want)
		}
	}
}

type durableLogicalDiscardSink struct{}

func (durableLogicalDiscardSink) Put(uint32, []byte) error { return nil }

func BenchmarkDurableRequestLogicalPlanMeasure4097(b *testing.B) {
	key, program := durableLogicalStreamFixture(b, 4097, 4)
	measurement, err := measureDurableRequestPlan(key, program)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(measurement.PlanBytes))
	b.ReportMetric(float64(measurement.PlanBytes), "encoded_B/op")
	b.ResetTimer()
	for range b.N {
		measured, err := measureDurableRequestPlan(key, program)
		if err != nil || measured.PlanBytes != measurement.PlanBytes || measured.Root != measurement.Root {
			b.Fatalf("measure bytes/root changed: err=%v", err)
		}
		runtime.KeepAlive(measured)
	}
}

func BenchmarkDurableRequestLogicalProgramValidate4097(b *testing.B) {
	_, program := durableLogicalStreamFixture(b, 4097, 4)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !validDurableRequestLogicalProgram(program) {
			b.Fatal("sealed program became invalid")
		}
	}
}

func BenchmarkDurableRequestLogicalPlanStream4097(b *testing.B) {
	key, program := durableLogicalStreamFixture(b, 4097, 4)
	measurement, err := measureDurableRequestPlan(key, program)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(measurement.PlanBytes))
	b.ReportMetric(float64(measurement.PlanBytes), "encoded_B/op")
	b.ReportMetric(float64(measurement.PhysicalPages), "pages/op")
	b.ResetTimer()
	for range b.N {
		if err := streamDurableRequestPlan(measurement, program, durableLogicalDiscardSink{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDurableRequestLogicalPlanReplay4097(b *testing.B) {
	key, program := durableLogicalStreamFixture(b, 4097, 4)
	measurement, pages := durableLogicalStreamBuild(b, key, program)
	b.ReportAllocs()
	b.SetBytes(int64(measurement.PlanBytes))
	b.ReportMetric(float64(durableRequestReaderMaxLiveBytes), "admission_bound_B")
	b.ResetTimer()
	for range b.N {
		reader, err := openDurableRequestRecipeStream(key, measurement.descriptor(), pages)
		if err != nil {
			b.Fatal(err)
		}
		visited := 0
		for reader.Next() {
			visited++
		}
		if reader.Err() != nil || !reader.Complete() || visited != 4097 {
			b.Fatalf("replay visited=%d complete=%v err=%v", visited, reader.Complete(), reader.Err())
		}
		runtime.KeepAlive(reader)
	}
}

func BenchmarkDurableRequestLogicalPlanLifecycle4097(b *testing.B) {
	key, program := durableLogicalStreamFixture(b, 4097, 4)
	baseline, err := measureDurableRequestPlan(key, program)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(baseline.PlanBytes))
	b.ReportMetric(float64(baseline.PlanBytes), "encoded_B/op")
	b.ResetTimer()
	for range b.N {
		measurement, err := measureDurableRequestPlan(key, program)
		if err != nil {
			b.Fatal(err)
		}
		pages := new(durableLogicalStreamPages)
		if err := streamDurableRequestPlan(measurement, program, pages); err != nil {
			b.Fatal(err)
		}
		reader, err := openDurableRequestRecipeStream(key, measurement.descriptor(), pages)
		if err != nil {
			b.Fatal(err)
		}
		for reader.Next() {
		}
		if reader.Err() != nil || !reader.Complete() {
			b.Fatalf("lifecycle replay complete=%v err=%v", reader.Complete(), reader.Err())
		}
		runtime.KeepAlive(reader)
	}
}
