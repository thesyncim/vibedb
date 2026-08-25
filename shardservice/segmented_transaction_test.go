package shardservice

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func buildShardManifest(t testing.TB, id distributedtxn.ID, count int) ([]byte, [][]byte) {
	t.Helper()
	owner := testOwner()
	arena := make([]byte, distributedtxn.ManifestSegmentBytes)
	var pages [][]byte
	builder, err := distributedtxn.NewManifestBuilder(arena, func(segment distributedtxn.ManifestSegment) error {
		pages = append(pages, bytes.Clone(segment.Raw))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < count; i++ {
		shard := []byte(fmt.Sprintf("s%06d", i))
		if i == 0 {
			shard = []byte(owner.Shard)
		}
		digest := distributedtxn.Digest{1}
		digest[1] = byte(i)
		digest[2] = byte(i >> 8)
		err = builder.Append(distributedtxn.ParticipantRef{
			Distribution: []byte(owner.Distribution), Shard: shard,
			RoutingVersion:       uint64(owner.RoutingVersion),
			AllocationGeneration: uint64(owner.AllocationGeneration),
			OwnershipEpoch:       uint64(owner.Epoch), MutationDigest: digest,
			State: distributedtxn.ParticipantStaged,
		})
		if err != nil {
			t.Fatalf("append participant %d: %v", i, err)
		}
	}
	descriptor, err := builder.Seal()
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := distributedtxn.AppendManifestCoordinator(nil,
		distributedtxn.ManifestCoordinatorRecord{
			ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
			CatalogGeneration: 7, Manifest: descriptor,
		})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, pages
}

func TestSegmentedTransactionWireCanonicalAndMalformed(t *testing.T) {
	id := testTransactionID(201)
	coordinator, pages := buildShardManifest(t, id, 2200)
	if len(pages) < 2 {
		t.Fatalf("manifest pages = %d, want multiple", len(pages))
	}
	requests := []*ShardRequest{
		ownedRequest(""), ownedRequest(""), ownedRequest(""),
	}
	requests[0].Transaction = TransactionRequest{
		Operation: TransactionStageManifestCoordinator, Record: coordinator,
		ManifestSegment: pages[0],
	}
	missingFirstPage := *requests[0]
	missingFirstPage.Transaction.ManifestSegment = nil
	if err := EncodeRequest(&bytes.Buffer{}, &missingFirstPage); err == nil {
		t.Fatal("segmented coordinator begin encoded without page zero")
	}
	requests[1].Transaction = TransactionRequest{
		Operation: TransactionStageManifestSegment, ID: id, ManifestSegment: pages[1],
	}
	requests[2].ExecutionMode = ExecutionReadOnly
	requests[2].Transaction = TransactionRequest{
		Operation: TransactionReadManifestSegment, ID: id, SegmentIndex: 1,
	}
	for _, request := range requests {
		var first, second bytes.Buffer
		if err := EncodeRequest(&first, request); err != nil {
			t.Fatalf("EncodeRequest(%d): %v", request.Transaction.Operation, err)
		}
		decoded, err := DecodeRequest(bytes.NewReader(first.Bytes()))
		if err != nil {
			t.Fatalf("DecodeRequest(%d): %v", request.Transaction.Operation, err)
		}
		if err := EncodeRequest(&second, decoded); err != nil {
			t.Fatalf("re-EncodeRequest(%d): %v", request.Transaction.Operation, err)
		}
		if !bytes.Equal(first.Bytes(), second.Bytes()) {
			t.Fatalf("operation %d did not round-trip canonically", request.Transaction.Operation)
		}
	}

	page, err := inspectTransactionManifestSegment(pages[1])
	if err != nil {
		t.Fatal(err)
	}
	reply := CompletionResponse(0)
	reply.Transaction = TransactionReply{
		Role: TransactionRoleCoordinator, ID: id, Revision: 1,
		CoordinatorState: distributedtxn.CoordinatorStaging,
		RecordKind:       TransactionRecordManifestSegment,
		SegmentIndex:     page.index, Record: pages[1],
	}
	var first, second bytes.Buffer
	if err := EncodeResponse(&first, reply); err != nil {
		t.Fatal(err)
	}
	decodedReply, err := DecodeResponse(bytes.NewReader(first.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if err := EncodeResponse(&second, decodedReply); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) || !decodedReply.Transaction.Equal(reply.Transaction) {
		t.Fatal("manifest page reply did not round-trip canonically")
	}

	corrupt := bytes.Clone(pages[0])
	corrupt[len(corrupt)-1] ^= 1
	bad := ownedRequest("")
	bad.Transaction = TransactionRequest{
		Operation: TransactionStageManifestSegment, ID: id, ManifestSegment: corrupt,
	}
	if err := EncodeRequest(&bytes.Buffer{}, bad); err == nil {
		t.Fatal("corrupt manifest segment encoded")
	}
	bad.Transaction.ManifestSegment = pages[1]
	bad.Transaction.SegmentIndex = 1
	if err := EncodeRequest(&bytes.Buffer{}, bad); err == nil {
		t.Fatal("segment stage with redundant index encoded")
	}
	bad.Transaction = TransactionRequest{
		Operation: TransactionReadManifestSegment, ID: id, Record: pages[0],
	}
	if err := EncodeRequest(&bytes.Buffer{}, bad); err == nil {
		t.Fatal("segment read with payload encoded")
	}
	reply.Transaction.SegmentIndex++
	if err := EncodeResponse(&bytes.Buffer{}, reply); err == nil {
		t.Fatal("page reply with mismatched authenticated index encoded")
	}
}

func TestEncodeSegmentedTransactionRequestDoesNotMutateReusableCaller(t *testing.T) {
	id := testTransactionID(203)
	coordinator, pages := buildShardManifest(t, id, 10)
	request := ownedRequest("")
	request.Transaction = TransactionRequest{
		Operation: TransactionStageManifestCoordinator, Record: coordinator,
		ManifestSegment: pages[0],
	}
	want := request.Transaction
	const workers = 8
	var wait sync.WaitGroup
	errors := make(chan error, workers)
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range 25 {
				var encoded bytes.Buffer
				if err := EncodeRequest(&encoded, request); err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	if request.Transaction.Operation != want.Operation || request.Transaction.ID != want.ID ||
		request.Transaction.Revision != want.Revision || request.Transaction.SegmentIndex != want.SegmentIndex ||
		!bytes.Equal(request.Transaction.Record, want.Record) ||
		!bytes.Equal(request.Transaction.ManifestSegment, want.ManifestSegment) ||
		request.Transaction.manifestMeta.valid {
		t.Fatalf("EncodeRequest mutated caller transaction: %+v", request.Transaction)
	}
}

func TestSegmentedTransactionLifecycleIdempotencySealReadAndRetire(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)
	id := testTransactionID(202)
	coordinator, pages := buildShardManifest(t, id, 2200)

	begin := ownedRequest("")
	begin.Transaction = TransactionRequest{
		Operation: TransactionStageManifestCoordinator, Record: coordinator,
		ManifestSegment: pages[0],
	}
	staged := exec(t, conn, begin)
	if staged.Transaction.Role != TransactionRoleCoordinator ||
		staged.Transaction.ID != id || staged.Transaction.Revision != 1 ||
		staged.Transaction.CoordinatorState != distributedtxn.CoordinatorStaging {
		t.Fatalf("begin = %+v", staged)
	}
	if retried := exec(t, conn, begin); !retried.Transaction.Equal(staged.Transaction) {
		t.Fatalf("begin retry = %+v, want %+v", retried, staged)
	}

	commit := ownedRequest("")
	commit.Transaction = TransactionRequest{
		Operation: TransactionCommitCoordinator, ID: id, Revision: 1,
	}
	if response := roundTrip(t, conn, commit); response.Kind != ResponseError ||
		response.ErrorKind != ErrorTransactionConflict {
		t.Fatalf("incomplete manifest commit = %+v", response)
	}

	stagePage := ownedRequest("")
	stagePage.Transaction = TransactionRequest{
		Operation: TransactionStageManifestSegment, ID: id, ManifestSegment: pages[0],
	}
	if err := EncodeRequest(&bytes.Buffer{}, stagePage); err == nil {
		t.Fatal("page-zero encoded outside atomic coordinator begin")
	}
	stagePage.Transaction.ManifestSegment = pages[2]
	if response := roundTrip(t, conn, stagePage); response.Kind != ResponseError ||
		response.ErrorKind != ErrorTransactionConflict {
		t.Fatalf("out-of-order page = %+v", response)
	}
	for index := 1; index < len(pages); index++ {
		stagePage.Transaction.ManifestSegment = pages[index]
		first := exec(t, conn, stagePage)
		second := exec(t, conn, stagePage)
		if !second.Transaction.Equal(first.Transaction) {
			t.Fatalf("page %d retry = %+v, want %+v", index, second, first)
		}
	}

	for index := range pages {
		read := ownedRequest("")
		read.ExecutionMode = ExecutionReadOnly
		read.Transaction = TransactionRequest{
			Operation: TransactionReadManifestSegment, ID: id, SegmentIndex: uint32(index),
		}
		response := exec(t, conn, read)
		if response.Transaction.RecordKind != TransactionRecordManifestSegment ||
			response.Transaction.SegmentIndex != uint32(index) ||
			!bytes.Equal(response.Transaction.Record, pages[index]) {
			t.Fatalf("read page %d = %+v", index, response.Transaction)
		}
	}

	committed := exec(t, conn, commit)
	if committed.Transaction.CoordinatorState != distributedtxn.CoordinatorCommitted ||
		committed.Transaction.Revision != 2 {
		t.Fatalf("commit = %+v", committed)
	}
	if retried := exec(t, conn, commit); !retried.Transaction.Equal(committed.Transaction) {
		t.Fatalf("commit retry = %+v, want %+v", retried, committed)
	}
	if delayedBegin := exec(t, conn, begin); !delayedBegin.Transaction.Equal(committed.Transaction) {
		t.Fatalf("delayed begin retry = %+v, want %+v", delayedBegin, committed)
	}

	lookup := ownedRequest("")
	lookup.ExecutionMode = ExecutionReadOnly
	lookup.Transaction = TransactionRequest{Operation: TransactionLookupCoordinator, ID: id}
	observed := exec(t, conn, lookup)
	if observed.Transaction.RecordKind != TransactionRecordManifestCoordinator ||
		!bytes.Equal(observed.Transaction.Record, coordinator) {
		t.Fatalf("lookup = %+v", observed.Transaction)
	}
	scan := ownedRequest("")
	scan.ExecutionMode = ExecutionReadOnly
	scan.Transaction = TransactionRequest{Operation: TransactionScanCoordinator}
	if scanned := exec(t, conn, scan); scanned.Transaction.RecordKind != TransactionRecordManifestCoordinator ||
		!bytes.Equal(scanned.Transaction.Record, coordinator) {
		t.Fatalf("scan = %+v", scanned.Transaction)
	}

	retire := ownedRequest("")
	retire.Transaction = TransactionRequest{
		Operation: TransactionRetireCoordinator, ID: id, Revision: 2,
	}
	retired := exec(t, conn, retire)
	if retired.Transaction.CoordinatorState != distributedtxn.CoordinatorRetired ||
		retired.Transaction.Revision != 3 {
		t.Fatalf("retire = %+v", retired)
	}
	stagePage.Transaction.ManifestSegment = pages[len(pages)-1]
	if response := roundTrip(t, conn, stagePage); response.Kind != ResponseError ||
		response.ErrorKind != ErrorTransactionConflict {
		t.Fatalf("page stage after retire = %+v", response)
	}
}

func TestSegmentedTransactionCapabilitiesAreSealed(t *testing.T) {
	checks := []struct {
		op   TransactionOperation
		want serviceauthz.Capability
	}{
		{TransactionStageManifestCoordinator, serviceauthz.CapabilityDataWrite},
		{TransactionStageManifestSegment, serviceauthz.CapabilityDataWrite},
		{TransactionReadManifestSegment, serviceauthz.CapabilityDataRead},
	}
	for _, check := range checks {
		request := &ShardRequest{Transaction: TransactionRequest{Operation: check.op}}
		got, ok := sealedRequestCapability(request)
		if !ok || got != check.want {
			t.Fatalf("operation %d capability = %x, %t; want %x", check.op, got, ok, check.want)
		}
	}
}

func maximalExpansionManifestPage(t testing.TB) []byte {
	t.Helper()
	arena := make([]byte, distributedtxn.ManifestSegmentBytes)
	var raw []byte
	builder, err := distributedtxn.NewManifestBuilder(arena, func(segment distributedtxn.ManifestSegment) error {
		if raw == nil {
			raw = bytes.Clone(segment.Raw)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	distribution := bytes.Repeat([]byte{'a'}, distributedtxn.MaxShardIdentityBytes)
	for i := 0; i < 900; i++ {
		shard := bytes.Repeat([]byte{'b'}, distributedtxn.MaxShardIdentityBytes-4)
		shard = strconv.AppendInt(shard, int64(1000+i), 10)
		digest := distributedtxn.Digest{1, byte(i), byte(i >> 8)}
		if err := builder.Append(distributedtxn.ParticipantRef{
			Distribution: distribution, Shard: shard,
			RoutingVersion: 1, AllocationGeneration: 1, OwnershipEpoch: 1,
			MutationDigest: digest, State: distributedtxn.ParticipantStaged,
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if _, err := builder.Seal(); err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		t.Fatal("builder did not emit a page")
	}
	return raw
}

func TestTransactionManifestScratchAcceptsMaximalPrefixExpansion(t *testing.T) {
	raw := maximalExpansionManifestPage(t)
	meta, err := inspectTransactionManifestSegment(raw)
	if err != nil {
		t.Fatalf("inspect highly compressed page: %v", err)
	}
	if meta.participantCount == 0 ||
		int(meta.participantCount)*distributedtxn.MaxShardIdentityBytes*2 <= distributedtxn.ManifestSegmentBytes {
		t.Fatalf("page expansion is not above encoded page: participants=%d", meta.participantCount)
	}
	scratch := new(transactionManifestScratch)
	wantIdentityBytes := distributedtxn.MaxManifestPageParticipants *
		distributedtxn.MaxShardIdentityBytes * 2
	if len(scratch.identities) != wantIdentityBytes {
		t.Fatalf("identity scratch bytes=%d, want exact one-page maximum %d",
			len(scratch.identities), wantIdentityBytes)
	}
	_, _ = inspectTransactionManifestSegment(raw) // warm the pool
	if allocations := testing.AllocsPerRun(100, func() {
		if _, inspectErr := inspectTransactionManifestSegment(raw); inspectErr != nil {
			panic(inspectErr)
		}
	}); allocations != 0 {
		t.Fatalf("warmed one-page inspection allocations=%v", allocations)
	}
}

func BenchmarkInspectTransactionManifestSegmentMaxExpansion(b *testing.B) {
	raw := maximalExpansionManifestPage(b)
	if _, err := inspectTransactionManifestSegment(raw); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for range b.N {
		if _, err := inspectTransactionManifestSegment(raw); err != nil {
			b.Fatal(err)
		}
	}
}
