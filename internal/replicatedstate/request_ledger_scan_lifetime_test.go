package replicatedstate

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/store/durable"
)

func requestLedgerBorrowedScanRows(t testing.TB) (requestledger.HeadRecord, []requestLedgerImageRow) {
	t.Helper()
	head, _ := requestLedgerStateTestHead(t, true)
	plan, err := requestledger.AppendPlan(nil, bytes.Repeat([]byte("inline recipe"), 400))
	if err != nil {
		t.Fatal(err)
	}
	head, err = requestledger.NewHeadWithContract(head.Key, head.RequestDigest, head.TerminalContractDigest, plan)
	if err != nil {
		t.Fatal(err)
	}
	head.MaxActivePayloadBytes, head.MaxActivePayloadChunks = requestledger.MaxPlanPageBytes, 1
	home, _ := requestledger.Home(head.Key)
	// The next overflow value fits the buffer grown for Head, so RangeRaw
	// really reuses that storage rather than allocating a larger backing array.
	data := bytes.Repeat([]byte{0xa5}, 4<<10)
	acc, err := requestledger.NewPayloadRootAccumulator(head.KeyDigest, uint64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := acc.Append(data); err != nil {
		t.Fatal(err)
	}
	root, err := acc.Root()
	if err != nil {
		t.Fatal(err)
	}
	build, err := requestledger.NewPayloadBuild(head, root, uint64(len(data)), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	chunk, err := requestledger.NewPayloadChunk(build, data)
	if err != nil {
		t.Fatal(err)
	}
	build, err = requestledger.AdvancePayloadBuild(build, chunk, build.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	headRaw, err := requestledger.AppendHead(nil, head)
	if err != nil {
		t.Fatal(err)
	}
	chunkRaw, err := requestledger.AppendPayloadChunk(nil, chunk)
	if err != nil {
		t.Fatal(err)
	}
	buildRaw, err := requestledger.AppendPayloadBuild(nil, build)
	if err != nil {
		t.Fatal(err)
	}
	rows := []requestLedgerImageRow{
		{requestledger.AppendHeadKey(nil, home, head.KeyDigest), headRaw},
		{requestledger.AppendPayloadChunkKey(nil, home, head.KeyDigest, root, chunk.Ordinal), chunkRaw},
		{requestledger.AppendPayloadBuildKey(nil, home, head.KeyDigest), buildRaw},
	}
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].key, rows[j].key) < 0 })
	return head, rows
}

func TestRequestLedgerScannerOwnsInlinePlanAcrossDurableSnapshotReopen(t *testing.T) {
	head, rows := requestLedgerBorrowedScanRows(t)
	file, err := os.Create(filepath.Join(t.TempDir(), "ledger-image"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := durable.Options{OpaqueValues: true}
	collection, err := durable.Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = collection.Close() }()
	for _, row := range rows {
		if _, err := collection.Put(row.key, row.value); err != nil {
			t.Fatal(err)
		}
	}
	for cycle := range 2 {
		if err := collection.Close(); err != nil {
			t.Fatal(err)
		}
		collection, err = durable.Open(file, options)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		scan := newRequestLedgerImageScanner(math.MaxUint64>>1, 1, RequestLedgerRange{Identity: requestledger.Digest{1}})
		var borrowedPlan []byte
		err = snapshot.RangeRaw(func(key, value []byte) error {
			if decoded, err := requestledger.OpenHead(value); err == nil {
				borrowedPlan = decoded.InlinePlan
			}
			return scan.observe(key, value)
		})
		if err == nil {
			err = scan.finishRequest()
		}
		if err != nil {
			_ = snapshot.Close()
			t.Fatalf("reopen %d authentic durable image: %v", cycle, err)
		}
		if bytes.Equal(borrowedPlan, head.InlinePlan) {
			t.Fatal("physical fixture did not reuse overflow scan storage")
		}
		if !bytes.Equal(scan.head.InlinePlan, head.InlinePlan) {
			t.Fatal("durable scan lost exact inline plan")
		}
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRequestLedgerScannerOwnsInlinePlanAcrossBorrowedRows(t *testing.T) {
	head, rows := requestLedgerBorrowedScanRows(t)
	scan := newRequestLedgerImageScanner(math.MaxUint64>>1, 1, RequestLedgerRange{Identity: requestledger.Digest{1}})
	var largest int
	for _, row := range rows {
		largest = max(largest, len(row.value))
	}
	frame := make([]byte, largest)
	var borrowedPlan []byte
	for _, row := range rows {
		copy(frame, row.value)
		value := frame[:len(row.value)]
		if view, err := requestledger.OpenHead(value); err == nil {
			borrowedPlan = view.InlinePlan
		}
		if err := scan.observe(row.key, value); err != nil {
			t.Fatal(err)
		}
	}
	if bytes.Equal(borrowedPlan, head.InlinePlan) {
		t.Fatal("fixture did not overwrite the borrowed inline plan")
	}
	if err := scan.finishRequest(); err != nil {
		t.Fatalf("valid reused-frame image: %v", err)
	}
	if !bytes.Equal(scan.head.InlinePlan, head.InlinePlan) {
		t.Fatal("scanner retained a borrowed inline plan")
	}
}

func TestRequestLedgerScannerReusesBoundedInlinePlanStorage(t *testing.T) {
	head, rows := requestLedgerBorrowedScanRows(t)
	framed, err := requestledger.AppendPlan(nil, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := requestledger.AppendPlan(nil, bytes.Repeat([]byte{2}, requestledger.MaxInlinePlanBytes-(len(framed)-1)))
	if err != nil || len(plan) != requestledger.MaxInlinePlanBytes {
		t.Fatalf("maximum inline plan: %d %v", len(plan), err)
	}
	large, err := requestledger.NewHeadWithContract(head.Key, head.RequestDigest, head.TerminalContractDigest, plan)
	if err != nil {
		t.Fatal(err)
	}
	largeRaw, err := requestledger.AppendHead(nil, large)
	if err != nil {
		t.Fatal(err)
	}
	scan := newRequestLedgerImageScanner(math.MaxUint64>>1, 1, RequestLedgerRange{Identity: requestledger.Digest{1}})
	if err := scan.observe(rows[0].key, largeRaw); err != nil {
		t.Fatal(err)
	}
	if len(scan.inlinePlan) != requestledger.MaxInlinePlanBytes || cap(scan.inlinePlan) != requestledger.MaxInlinePlanBytes {
		t.Fatalf("scratch exceeds exact inline bound: %d/%d", len(scan.inlinePlan), cap(scan.inlinePlan))
	}
	backing := &scan.inlinePlan[0]
	if allocations := testing.AllocsPerRun(100, func() {
		scan.resetRequest()
		for _, row := range rows {
			if err := scan.observe(row.key, row.value); err != nil {
				t.Fatal(err)
			}
		}
		if err := scan.finishRequest(); err != nil {
			t.Fatal(err)
		}
	}); allocations != 0 {
		t.Fatalf("warm per-request scanner allocations: %v", allocations)
	}
	if &scan.inlinePlan[0] != backing || cap(scan.inlinePlan) != requestledger.MaxInlinePlanBytes {
		t.Fatal("request reset discarded reusable inline storage")
	}
	bad := bytes.Clone(largeRaw)
	bad[len(bad)-1] ^= 1
	scan.resetRequest()
	if err := scan.observe(rows[0].key, bad); err == nil {
		t.Fatal("malformed head admitted before owning inline bytes")
	}
	if len(scan.inlinePlan) != 0 || cap(scan.inlinePlan) != requestledger.MaxInlinePlanBytes {
		t.Fatal("failed authentication changed inline scratch")
	}
}
