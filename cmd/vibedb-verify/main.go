// Command vibedb-verify is the offline verify and salvage tool for vibedb store
// files and database directories. It is read-only against its input and wants a
// quiescent file, directory, or a copy: it does not take the writer lock and
// does not apply the in-place materialization rollback that opening the store
// would, so a concurrent writer may retire and reuse the extents it reads.
// Database-directory verify likewise opens txn.vtm and collection journals only
// to scan; it never appends, syncs, recycles, or removes residue.
//
// Usage:
//
//	vibedb-verify verify  <store-file|database-dir>
//	vibedb-verify salvage <store-file> <output-file>
//	vibedb-verify repack  <store-file> <output-file>
//
// Output is one line per finding followed by a machine-parseable summary. verify
// exits non-zero on any violation; salvage and repack exit non-zero on failure
// to write their fresh store.
//
// repack reads a quiescent store through the ordinary snapshot/read path in
// lexical order and rewrites it clustered, restoring perfect scan locality and
// reclaiming the free space churn left behind. It is inline-primary only.
package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	txnMarkerFilename    = "txn.vtm"
	txnMarkerRegionStart = 2 * storeio.TxnMarkerHeaderSize
)

func main() {
	os.Exit(run(os.Args))
}

func run(args []string) int {
	if len(args) < 2 {
		usage()
		return 2
	}
	switch args[1] {
	case "verify":
		if len(args) != 3 {
			usage()
			return 2
		}
		return runVerify(args[2])
	case "salvage":
		if len(args) != 4 {
			usage()
			return 2
		}
		return runSalvage(args[2], args[3])
	case "repack":
		if len(args) != 4 {
			usage()
			return 2
		}
		return runRepack(args[2], args[3])
	default:
		usage()
		return 2
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  vibedb-verify verify  <store-file|database-dir>")
	fmt.Fprintln(os.Stderr, "  vibedb-verify salvage <store-file> <output-file>")
	fmt.Fprintln(os.Stderr, "  vibedb-verify repack  <store-file> <output-file>")
}

func runVerify(path string) int {
	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open=%q: %v\n", path, err)
		return 1
	}
	if info.IsDir() {
		return runVerifyDatabase(path)
	}
	return runVerifyStore(path)
}

func runVerifyStore(path string) int {
	file, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open=%q: %v\n", path, err)
		return 1
	}
	defer file.Close()

	report, err := durable.Verify(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error verify=%q: %v\n", path, err)
		return 1
	}
	for _, finding := range report.Findings {
		fmt.Printf(
			"finding kind=%s offset=%d logical=%d detail=%q\n",
			finding.Kind, finding.Offset, finding.LogicalID, finding.Detail,
		)
	}
	kinds := make([]string, 0, len(report.PageCounts))
	for kind := range report.PageCounts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		fmt.Printf("pages kind=%s count=%d\n", kind, report.PageCounts[kind])
	}
	fmt.Printf(
		"summary root_slot=%d generation=%d file_end=%d documents=%d free_extents=%d findings=%d\n",
		report.RootSlot, report.Generation, report.FileEnd,
		report.Documents, report.FreeExtents, len(report.Findings),
	)
	if report.OK() {
		fmt.Println("result ok")
		return 0
	}
	fmt.Printf("result fail findings=%d\n", len(report.Findings))
	return 1
}

// txnVerifyFinding is one machine-parseable pairing/integrity violation for a
// database directory. Kind values are stable diagnostics the tests pin.
type txnVerifyFinding struct {
	Kind    string
	Offset  int64
	Logical uint64
	Detail  string
}

type txnVerifyReport struct {
	TxnLog    string
	Decisions int
	Journals  int
	Findings  []txnVerifyFinding
}

func (r txnVerifyReport) OK() bool { return len(r.Findings) == 0 }

func runVerifyDatabase(dir string) int {
	report, err := verifyDatabaseTxn(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error verify=%q: %v\n", dir, err)
		return 1
	}
	for _, finding := range report.Findings {
		fmt.Printf(
			"finding kind=%s offset=%d logical=%d detail=%q\n",
			finding.Kind, finding.Offset, finding.Logical, finding.Detail,
		)
	}
	fmt.Printf(
		"summary txn_log=%s decisions=%d journals=%d findings=%d\n",
		report.TxnLog, report.Decisions, report.Journals, len(report.Findings),
	)
	if report.OK() {
		fmt.Println("result ok")
		return 0
	}
	fmt.Printf("result fail findings=%d\n", len(report.Findings))
	return 1
}

func verifyDatabaseTxn(dir string) (txnVerifyReport, error) {
	report := txnVerifyReport{TxnLog: "absent"}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return report, err
	}

	primaries := make(map[[16]byte]primaryIdentity)
	journals := make(map[[16]byte]journalIdentity)
	var conditionals []conditionalRecord

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		base := entry.Name()
		path := filepath.Join(dir, base)
		if _, ok := collectionname.Decode(base); ok {
			id, err := readPrimaryIdentity(path)
			if err != nil {
				report.Findings = append(report.Findings, txnVerifyFinding{
					Kind:   "primary_unreadable",
					Detail: fmt.Sprintf("%s: %v", base, err),
				})
				continue
			}
			primaries[id.StoreID] = id
			continue
		}
		if _, ok := collectionname.DecodeJournal(base); ok {
			id, conds, err := scanJournalPairing(path)
			if err != nil {
				report.Findings = append(report.Findings, txnVerifyFinding{
					Kind:   "journal_unreadable",
					Detail: fmt.Sprintf("%s: %v", base, err),
				})
				continue
			}
			journals[id.JournalID] = id
			report.Journals++
			conditionals = append(conditionals, conds...)
		}
	}

	markerPath := filepath.Join(dir, txnMarkerFilename)
	markerInfo, statErr := os.Stat(markerPath)
	if statErr != nil {
		if !os.IsNotExist(statErr) {
			return report, statErr
		}
		report.TxnLog = "absent"
		if len(conditionals) > 0 {
			for _, cond := range conditionals {
				report.Findings = append(report.Findings, txnVerifyFinding{
					Kind:    "in_doubt",
					Logical: cond.TxnID,
					Detail: fmt.Sprintf(
						"journal %x holds conditional txn=%d but decision log is absent",
						cond.JournalID, cond.TxnID,
					),
				})
			}
		}
		return report, nil
	}
	if !markerInfo.Mode().IsRegular() {
		report.Findings = append(report.Findings, txnVerifyFinding{
			Kind:   "txn_log_corrupt",
			Detail: "txn.vtm is not a regular file",
		})
		report.TxnLog = "unusable"
		return report, nil
	}

	marker, decisions, openErr := storeio.InspectTxnMarker(markerPath)
	if openErr != nil {
		if len(conditionals) > 0 {
			report.TxnLog = "unusable"
			report.Findings = append(report.Findings, txnVerifyFinding{
				Kind: "in_doubt",
				Detail: fmt.Sprintf(
					"decision log unusable (%v) while journals hold conditional records",
					openErr,
				),
			})
			return report, nil
		}
		if errors.Is(openErr, storeio.ErrTxnMarkerNoValidHeader) {
			// L2 mint residue: remintable when no journal holds conditionals.
			report.TxnLog = "absent"
			return report, nil
		}
		report.TxnLog = "unusable"
		report.Findings = append(report.Findings, txnVerifyFinding{
			Kind:   "txn_log_corrupt",
			Detail: openErr.Error(),
		})
		return report, nil
	}
	defer marker.Close()
	report.TxnLog = "present"

	if offset, torn := txnMarkerTornTail(markerPath, marker); torn {
		report.Findings = append(report.Findings, txnVerifyFinding{
			Kind:   "torn_decision",
			Offset: offset,
			Detail: "decision log record region ends in a torn append",
		})
	}

	markerID := decisions.MarkerID()
	epoch := decisions.Epoch()
	decisions.RangeDecisions(func(
		txnID uint64, participants []storeio.TxnParticipant,
	) bool {
		report.Decisions++
		for _, p := range participants {
			if decisions.Retired(p.StoreID) {
				continue
			}
			primary, hasPrimary := primaries[p.StoreID]
			journal, hasJournal := journals[p.JournalID]
			switch {
			case !hasPrimary:
				report.Findings = append(report.Findings, txnVerifyFinding{
					Kind:    "missing_participant",
					Logical: txnID,
					Detail: fmt.Sprintf(
						"decision txn=%d names missing store %x",
						txnID, p.StoreID,
					),
				})
			case primary.JournalID != p.JournalID:
				report.Findings = append(report.Findings, txnVerifyFinding{
					Kind:    "missing_participant",
					Logical: txnID,
					Detail: fmt.Sprintf(
						"decision txn=%d store %x journal identity mismatch",
						txnID, p.StoreID,
					),
				})
			case !hasJournal:
				report.Findings = append(report.Findings, txnVerifyFinding{
					Kind:    "missing_participant",
					Logical: txnID,
					Detail: fmt.Sprintf(
						"decision txn=%d names missing journal %x for store %x",
						txnID, p.JournalID, p.StoreID,
					),
				})
			case journal.StoreID != p.StoreID:
				report.Findings = append(report.Findings, txnVerifyFinding{
					Kind:    "missing_participant",
					Logical: txnID,
					Detail: fmt.Sprintf(
						"decision txn=%d journal %x store identity mismatch",
						txnID, p.JournalID,
					),
				})
			}
		}
		return true
	})

	for _, cond := range conditionals {
		if cond.MarkerID != markerID || cond.MarkerEpoch != epoch {
			report.Findings = append(report.Findings, txnVerifyFinding{
				Kind:    "epoch_mismatch",
				Logical: cond.TxnID,
				Detail: fmt.Sprintf(
					"journal %x conditional txn=%d marker/epoch (%x,%d) != log (%x,%d)",
					cond.JournalID, cond.TxnID,
					cond.MarkerID, cond.MarkerEpoch,
					markerID, epoch,
				),
			})
			continue
		}
		participants, ok := decisions.Lookup(markerID, epoch, cond.TxnID)
		if !ok {
			report.Findings = append(report.Findings, txnVerifyFinding{
				Kind:    "in_doubt",
				Logical: cond.TxnID,
				Detail: fmt.Sprintf(
					"journal %x holds same-epoch conditional txn=%d with no decision",
					cond.JournalID, cond.TxnID,
				),
			})
			continue
		}
		bound := false
		for _, p := range participants {
			if p.StoreID == cond.StoreID && p.JournalID == cond.JournalID {
				bound = true
				break
			}
		}
		if !bound {
			report.Findings = append(report.Findings, txnVerifyFinding{
				Kind:    "in_doubt",
				Logical: cond.TxnID,
				Detail: fmt.Sprintf(
					"journal %x holds conditional txn=%d not named by the decision",
					cond.JournalID, cond.TxnID,
				),
			})
		}
	}
	return report, nil
}

type primaryIdentity struct {
	StoreID   [16]byte
	JournalID [16]byte
	Path      string
}

type journalIdentity struct {
	StoreID   [16]byte
	JournalID [16]byte
	Path      string
}

type conditionalRecord struct {
	StoreID     [16]byte
	JournalID   [16]byte
	MarkerID    [16]byte
	MarkerEpoch uint64
	TxnID       uint64
	Generation  uint64
}

func readPrimaryIdentity(path string) (primaryIdentity, error) {
	file, err := os.Open(path)
	if err != nil {
		return primaryIdentity{}, err
	}
	defer file.Close()
	bootstrap, err := storeio.DiscoverMutableInlineBootstrap(file)
	if err != nil {
		return primaryIdentity{}, err
	}
	scratch := make([]byte, bootstrap.MaxPageSize)
	_, root, _, _, err := storeio.RecoverInlineStateRootWithFallback(
		file, bootstrap.PageSize, scratch,
	)
	if err != nil {
		return primaryIdentity{}, err
	}
	return primaryIdentity{
		StoreID: root.StoreID, JournalID: root.JournalID, Path: path,
	}, nil
}

func scanJournalPairing(
	path string,
) (journalIdentity, []conditionalRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return journalIdentity{}, nil, err
	}
	defer file.Close()
	journal, err := storeio.InspectRecoveryJournal(file)
	if err != nil {
		return journalIdentity{}, nil, err
	}
	defer journal.Close()
	header := journal.Header()
	id := journalIdentity{
		StoreID: header.StoreID, JournalID: header.JournalID, Path: path,
	}
	if journal.Cursor() == 0 {
		return id, nil, nil
	}
	var conds []conditionalRecord
	err = journal.Replay(journal.BaseGeneration(), func(rec storeio.RecoveryRecord) error {
		if rec.Kind != storeio.RecoveryRecordKindConditionalBatch {
			return nil
		}
		conds = append(conds, conditionalRecord{
			StoreID:     header.StoreID,
			JournalID:   header.JournalID,
			MarkerID:    rec.Conditional.MarkerID,
			MarkerEpoch: rec.Conditional.MarkerEpoch,
			TxnID:       rec.Conditional.TxnID,
			Generation:  rec.Generation,
		})
		return nil
	})
	return id, conds, err
}

// txnMarkerTornTail reports whether the live record region contains non-zero
// bytes past the scanned cursor — the offline signature of a truncatable torn
// append that OpenTxnMarker leaves in place without mutating the file.
func txnMarkerTornTail(path string, marker *storeio.TxnMarkerInspection) (int64, bool) {
	if marker == nil {
		return 0, false
	}
	cursor := marker.Cursor()
	capacity := marker.Header().Capacity
	if cursor >= capacity {
		return 0, false
	}
	file, err := os.Open(path)
	if err != nil {
		return 0, false
	}
	defer file.Close()
	offset := int64(txnMarkerRegionStart) + int64(cursor)
	remain := capacity - cursor
	if remain > storeio.TxnMarkerMinSectorSize {
		remain = storeio.TxnMarkerMinSectorSize
	}
	buf := make([]byte, remain)
	n, err := file.ReadAt(buf, offset)
	if err != nil && n == 0 {
		return 0, false
	}
	for i := 0; i < n; i++ {
		if buf[i] != 0 {
			return offset, true
		}
	}
	return 0, false
}

func runSalvage(srcPath, outPath string) int {
	src, err := os.Open(srcPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open=%q: %v\n", srcPath, err)
		return 1
	}
	defer src.Close()

	out, err := os.OpenFile(outPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error create=%q: %v\n", outPath, err)
		return 1
	}
	defer out.Close()

	report, err := durable.Salvage(src, out, durable.Options{
		Backend: durable.BackendPortable,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error salvage=%q: %v\n", srcPath, err)
		return 1
	}
	fmt.Printf(
		"salvage leaves_scanned=%d buckets=%d documents=%d overflow_skipped=%d duplicate_skipped=%d output_file_end=%d\n",
		report.LeavesScanned, report.BucketsKept, report.Documents,
		report.OverflowSkipped, report.DuplicateSkipped, report.OutputFileEnd,
	)
	fmt.Println("result ok")
	return 0
}

func runRepack(srcPath, outPath string) int {
	// Repack opens the source through the ordinary read path, which needs write
	// access for its lock and any pending in-place rollback; the file itself is
	// otherwise left as it was.
	src, err := os.OpenFile(srcPath, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error open=%q: %v\n", srcPath, err)
		return 1
	}
	defer src.Close()

	out, err := os.OpenFile(outPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error create=%q: %v\n", outPath, err)
		return 1
	}
	defer out.Close()

	report, err := durable.Repack(src, out, durable.Options{
		Backend: durable.BackendPortable,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error repack=%q: %v\n", srcPath, err)
		return 1
	}
	fmt.Printf(
		"repack documents=%d source_file_end=%d output_file_end=%d\n",
		report.Documents, report.SourceFileEnd, report.OutputFileEnd,
	)
	fmt.Println("result ok")
	return 0
}
