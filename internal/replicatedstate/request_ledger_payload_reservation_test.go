package replicatedstate

import (
	"bytes"
	"math"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestRequestLedgerDynamicReservationSurvivesAdvanceAndCleanup(t *testing.T) {
	head, _ := requestLedgerStateTestHead(t, true)
	head.MaxActivePayloadBytes, head.MaxActivePayloadChunks = 2*requestledger.MaxPlanPageBytes, 2
	home, _ := requestledger.Home(head.Key)
	image := make(map[string][]byte)
	put := func(key []byte, raw []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		image[string(key)] = raw
	}
	setHead := func() {
		raw, err := requestledger.AppendHead(nil, head)
		put(requestledger.AppendHeadKey(nil, home, head.KeyDigest), raw, err)
	}
	var total uint64
	check := func(phase string) {
		t.Helper()
		setHead()
		keys := make([]string, 0, len(image))
		for key := range image {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		scanner := newRequestLedgerImageScanner(math.MaxUint64>>1, 1, RequestLedgerRange{Identity: requestledger.Digest{1}})
		for _, key := range keys {
			if err := scanner.observe([]byte(key), image[key]); err != nil {
				t.Fatalf("%s observe: %v", phase, err)
			}
		}
		if err := scanner.finishRequest(); err != nil {
			t.Fatalf("%s reopen accounting: %v", phase, err)
		}
		got := scanner.resident + scanner.reserved
		if total == 0 {
			total = got
		}
		if got != total {
			t.Fatalf("%s consumption=%d (resident=%d reserved=%d), initial=%d", phase, got, scanner.resident, scanner.reserved, total)
		}
	}
	check("initial")
	data := bytes.Repeat([]byte{0x53}, requestledger.MaxPlanPageBytes+17)
	acc, err := requestledger.NewPayloadRootAccumulator(head.KeyDigest, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	for offset := 0; offset < len(data); offset += requestledger.MaxPlanPageBytes {
		if err := acc.Append(data[offset:min(offset+requestledger.MaxPlanPageBytes, len(data))]); err != nil {
			t.Fatal(err)
		}
	}
	root, err := acc.Root()
	if err != nil {
		t.Fatal(err)
	}
	build, err := requestledger.NewPayloadBuild(head, root, uint64(len(data)), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	buildKey := requestledger.AppendPayloadBuildKey(nil, home, head.KeyDigest)
	setBuild := func() {
		raw, err := requestledger.AppendPayloadBuild(nil, build)
		put(buildKey, raw, err)
	}
	setBuild()
	check("build")
	for offset := 0; offset < len(data); {
		end := min(offset+requestledger.MaxPlanPageBytes, len(data))
		chunk, err := requestledger.NewPayloadChunk(build, data[offset:end])
		if err != nil {
			t.Fatal(err)
		}
		build, err = requestledger.AdvancePayloadBuild(build, chunk, build.Revision+1)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := requestledger.AppendPayloadChunk(nil, chunk)
		put(requestledger.AppendPayloadChunkKey(nil, home, head.KeyDigest, root, chunk.Ordinal), raw, err)
		setBuild()
		check("chunk")
		offset = end
	}
	build, err = requestledger.SealPayloadBuild(build, build.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	setBuild()
	check("sealed")
	acquiring, err := requestledger.NewRoutePinAcquiring(head, head.PinID, requestLedgerStateTestDigest("binding"), requestLedgerStateTestDigest("physical"), []byte("acquire"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, requestledger.RoutePinRecord{}, acquiring, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	route, err := requestledger.RecordVerifiedRoutePinAcquired(acquiring, acquiring.Revision+1, []byte("acquired"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, acquiring, route, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	setRoute := func() {
		raw, err := requestledger.AppendRoutePin(nil, route)
		put(requestledger.AppendRoutePinKey(nil, home, head.KeyDigest), raw, err)
	}
	setRoute()
	check("acquired")
	step := requestledger.StepRef{TargetSource: requestledger.PayloadSourceDynamic, CommandSource: requestledger.PayloadSourceDynamic,
		TargetLength: 1, CommandOffset: 1, CommandLength: 1, TargetDigest: requestLedgerStateTestDigest("target"), CommandDigest: requestLedgerStateTestDigest("command")}
	pending, err := requestledger.NewPendingWaveWithRoutePin(head, build, head.Revision+1, route, []requestledger.StepRef{step})
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.InstallPendingWave(head, pending, build, route)
	if err != nil {
		t.Fatal(err)
	}
	pendingKey := requestledger.AppendPendingKey(nil, home, head.KeyDigest)
	raw, err := requestledger.AppendPendingWave(nil, pending)
	put(pendingKey, raw, err)
	check("pending")
	continuation, err := requestledger.NewContinuation(head, pending, route, head.Revision+1, head.TerminalTransitionTag, []byte("terminal"), []byte("observation"))
	if err != nil {
		t.Fatal(err)
	}
	continuationRaw, err := requestledger.AppendContinuation(nil, continuation)
	if err != nil {
		t.Fatal(err)
	}
	commandRaw, err := requestledger.AppendCommand(nil, requestledger.Command{Operation: requestledger.OperationAdvance,
		ExpectedRevision: head.Revision, Revision: continuation.Revision, KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest,
		PlanRoot: head.PlanRoot, SubjectDigest: continuation.ContinuationDigest, ExpectedRangeIdentity: requestledger.Digest{1}, Home: home, Payload: continuationRaw})
	if err != nil {
		t.Fatal(err)
	}
	command, err := requestledger.OpenCommandInto(commandRaw, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := requestLedgerRows{home: home, head: head, headFound: true, headRaw: image[string(requestledger.AppendHeadKey(nil, home, head.KeyDigest))],
		pending: pending, pendingFound: true, pendingRaw: image[string(pendingKey)], routePin: route, routePinFound: true,
		routePinRaw: image[string(requestledger.AppendRoutePinKey(nil, home, head.KeyDigest))], payloadBuild: build, payloadBuildFound: true, payloadBuildRaw: image[string(buildKey)]}
	transition, err := planRequestLedgerAdvance(requestLedgerCommandPlan{}, command, rows)
	if err != nil || transition.completion.ResultCode != ResultApplied {
		t.Fatalf("actual dynamic advance planner: code=%d err=%v", transition.completion.ResultCode, err)
	}
	if transition.delta.residentBytes+transition.delta.reservedBytes != 0 {
		t.Fatalf("advance changed admitted consumption: %+v", transition.delta)
	}
	head, err = requestledger.AdvancePendingWithBuild(head, pending, continuation, build)
	if err != nil {
		t.Fatal(err)
	}
	delete(image, string(pendingKey))
	image[string(requestledger.AppendContinuationKey(nil, home, head.KeyDigest))] = continuationRaw
	check("advanced")
	prior := route
	route, err = requestledger.BeginRoutePinRelease(route, route.Revision+1, []byte("release"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.AdvanceHeadRoutePin(head, prior, route, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	setRoute()
	check("releasing")
	route, err = requestledger.RecordVerifiedRoutePinReleased(route, route.Revision+1, []byte("released"))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.MarkRoutePinReleased(head, route, head.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	setRoute()
	check("released")
	for head.CleanupBuildDigest != (requestledger.Digest{}) {
		maxRows := uint16(1)
		if head.CleanupNextChunk == 1 {
			maxRows = 2
		}
		request, err := requestledger.NewPayloadCleanupRequest(head, maxRows, math.MaxUint32)
		if err != nil {
			t.Fatal(err)
		}
		chunk, err := requestledger.PlanPayloadCleanup(head, request)
		if err != nil {
			t.Fatal(err)
		}
		head, err = requestledger.AdvancePayloadCleanup(head, request, chunk, head.Revision+1)
		if err != nil {
			t.Fatal(err)
		}
		for i := chunk.FirstOrdinal; i < chunk.FirstOrdinal+chunk.ChunkCount; i++ {
			delete(image, string(requestledger.AppendPayloadChunkKey(nil, home, head.KeyDigest, root, i)))
		}
		if chunk.Final {
			delete(image, string(buildKey))
		}
		check("cleanup")
	}
}
