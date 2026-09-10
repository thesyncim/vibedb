//go:build linux

package gatewayruntime

// This file owns the machine-readable acceptance contract for the online
// physical-node scale qualification.  The process fixture is intentionally
// kept separate: the fixture drives the shipped CLI and writes these facts,
// while this layer makes omissions or fabricated success impossible for both
// local runs and CI.

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	seamlessScaleEvidenceSchema  = "vibedb.seamless-scale-in-out"
	seamlessScaleEvidenceVersion = 1
	seamlessScalePhaseBaseline   = "baseline"
	seamlessScalePhaseDuring     = "during"
	seamlessScalePhaseAfter      = "after"
)

// seamlessScalePerformanceBounds are relative to the measured baseline.  A
// fixed additive floor keeps a very fast baseline from making scheduler and
// network jitter look like an unbounded regression.  All values are loaded
// from environment variables by the process qualification, so CI can tune
// the gate for its runner without weakening the conservation checks.
type seamlessScalePerformanceBounds struct {
	DuringP50PPM uint64
	DuringP95PPM uint64
	DuringP99PPM uint64
	AfterP50PPM  uint64
	AfterP95PPM  uint64
	AfterP99PPM  uint64
	LatencyFloor uint64
	DuringTPSPPM uint64
	AfterTPSPPM  uint64
	MaxPauseNS   uint64
	// Strict is false when a caller supplied any performance override. Such a
	// run is useful diagnostic evidence, but it cannot satisfy the CI
	// qualification: a relaxed bound must never silently become acceptance.
	Strict bool
}

func defaultSeamlessScalePerformanceBounds() seamlessScalePerformanceBounds {
	return seamlessScalePerformanceBounds{
		DuringP50PPM: 1_050_000,
		DuringP95PPM: 1_100_000,
		DuringP99PPM: 1_150_000,
		AfterP50PPM:  1_050_000,
		AfterP95PPM:  1_100_000,
		AfterP99PPM:  1_150_000,
		LatencyFloor: 100_000, // 100us additive scheduling floor
		DuringTPSPPM: 990_000,
		AfterTPSPPM:  990_000,
		MaxPauseNS:   100_000_000, // explicit 100ms continuity ceiling
		Strict:       true,
	}
}

func (bounds seamlessScalePerformanceBounds) valid() bool {
	return bounds.DuringP50PPM != 0 && bounds.DuringP50PPM <= 20_000_000 &&
		bounds.DuringP95PPM >= bounds.DuringP50PPM && bounds.DuringP95PPM <= 20_000_000 &&
		bounds.DuringP99PPM >= bounds.DuringP95PPM && bounds.DuringP99PPM <= 20_000_000 &&
		bounds.AfterP50PPM != 0 && bounds.AfterP50PPM <= 20_000_000 &&
		bounds.AfterP95PPM >= bounds.AfterP50PPM && bounds.AfterP95PPM <= 20_000_000 &&
		bounds.AfterP99PPM >= bounds.AfterP95PPM && bounds.AfterP99PPM <= 20_000_000 &&
		bounds.LatencyFloor <= 60_000_000_000 &&
		bounds.DuringTPSPPM != 0 && bounds.DuringTPSPPM <= 1_000_000 &&
		bounds.AfterTPSPPM != 0 && bounds.AfterTPSPPM <= 1_000_000 &&
		bounds.MaxPauseNS != 0 && bounds.MaxPauseNS <= 10*timeSecondNS
}

// Keep this a constant so bounds validation has no dependency on the clock or
// a duration import in the evidence-only package.
const timeSecondNS = uint64(1_000_000_000)

func loadSeamlessScalePerformanceBounds(environ func(string) string) (seamlessScalePerformanceBounds, error) {
	bounds := defaultSeamlessScalePerformanceBounds()
	overridden := false
	values := []struct {
		name   string
		dest   *uint64
		factor uint64
	}{
		{"VIBEDB_SCALE_DURING_P50_PPM", &bounds.DuringP50PPM, 1},
		{"VIBEDB_SCALE_DURING_P95_PPM", &bounds.DuringP95PPM, 1},
		{"VIBEDB_SCALE_DURING_P99_PPM", &bounds.DuringP99PPM, 1},
		{"VIBEDB_SCALE_AFTER_P50_PPM", &bounds.AfterP50PPM, 1},
		{"VIBEDB_SCALE_AFTER_P95_PPM", &bounds.AfterP95PPM, 1},
		{"VIBEDB_SCALE_AFTER_P99_PPM", &bounds.AfterP99PPM, 1},
		{"VIBEDB_SCALE_LATENCY_FLOOR_NS", &bounds.LatencyFloor, 1},
		{"VIBEDB_SCALE_DURING_THROUGHPUT_PPM", &bounds.DuringTPSPPM, 1},
		{"VIBEDB_SCALE_AFTER_THROUGHPUT_PPM", &bounds.AfterTPSPPM, 1},
		{"VIBEDB_SCALE_MAX_PAUSE_NS", &bounds.MaxPauseNS, 1},
	}
	for _, value := range values {
		raw := strings.TrimSpace(environ(value.name))
		if raw == "" {
			continue
		}
		overridden = true
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || parsed == 0 {
			return seamlessScalePerformanceBounds{}, fmt.Errorf("%s=%q: positive integer required", value.name, raw)
		}
		if value.factor != 0 && parsed > math.MaxUint64/value.factor {
			return seamlessScalePerformanceBounds{}, fmt.Errorf("%s overflows", value.name)
		}
		*value.dest = parsed * value.factor
	}
	if !bounds.valid() {
		return seamlessScalePerformanceBounds{}, errors.New("seamless scale performance bounds are invalid")
	}
	bounds.Strict = !overridden
	return bounds, nil
}

// seamlessScalePhaseEvidence is the complete result of one workload window.
// Requests is the number of attempted authenticated requests; Successes is
// the number with a valid response and Errors counts transport or protocol
// failures.  AcknowledgedWrites and VerifiedReads are independent counters so
// an implementation cannot claim that a failed write was later verified.
type seamlessScalePhaseEvidence struct {
	Phase                 string
	StartNS               uint64
	EndNS                 uint64
	DurationNS            uint64
	Scheduled             uint64
	Started               uint64
	Completed             uint64
	Requests              uint64
	Successes             uint64
	Errors                uint64
	Timeouts              uint64
	Missed                uint64
	Retries               uint64
	AcknowledgedWrites    uint64
	VerifiedReads         uint64
	P50NS                 uint64
	P95NS                 uint64
	P99NS                 uint64
	MaxPauseNS            uint64
	CompletionGapNS       uint64
	QueueLagP99NS         uint64
	OfferedRateMilli      uint64
	ThroughputMilli       uint64
	AcknowledgementDigest string
	VerificationDigest    string
}

func (phase seamlessScalePhaseEvidence) valid() bool {
	return phase.Phase == seamlessScalePhaseBaseline || phase.Phase == seamlessScalePhaseDuring ||
		phase.Phase == seamlessScalePhaseAfter
}

// seamlessScaleBudgetEvidence is populated from the node-wide migration
// budget.  Positive throttling is required in the qualification: an artifact
// that fits entirely in the initial burst would only test an unpaced fast path.
type seamlessScaleBudgetEvidence struct {
	ThrottledCalls uint64
	ThrottledBytes uint64
	PeakActive     uint64
	MaxActive      uint64
}

// seamlessScaleEvidence is serialized as bounded TSV by the real process
// fixture.  Its booleans are deliberately explicit and independently checked
// by CI instead of being inferred from a final catalog shape.
type seamlessScaleEvidence struct {
	Result                        string
	Phase                         string
	PhysicalBefore                uint64
	PhysicalPeak                  uint64
	PhysicalAfter                 uint64
	ApplicationGroupsMoved        uint64
	InternalGroupsMoved           uint64
	EmptyTargetAtEnrollment       bool
	ControllerRestarted           bool
	TargetRestarted               bool
	DuplicateOperationStable      bool
	PostRestartOperationRecovered bool
	SafeToStop                    bool
	NodeStopped                   bool
	AcknowledgedDataIntact        bool
	NoSkippedSuccess              bool
	Cycles                        uint64
	BaselineWindows               uint64
	DuringWindows                 uint64
	AfterWindows                  uint64
	SurvivorSessionStable         bool
	RetiringSessionBlocked        bool
	RetiringSessionReleased       bool
	RetiringReferencesAfter       uint64
	GroupInventoryBeforeDigest    string
	GroupInventoryAfterDigest     string
	Baseline                      seamlessScalePhaseEvidence
	During                        seamlessScalePhaseEvidence
	After                         seamlessScalePhaseEvidence
	Budget                        seamlessScaleBudgetEvidence
}

func (evidence seamlessScaleEvidence) valid(bounds seamlessScalePerformanceBounds) error {
	if !bounds.valid() {
		return errors.New("invalid performance bounds")
	}
	if !bounds.Strict {
		return errors.New("performance overrides produce diagnostic evidence, not qualification")
	}
	if evidence.Result != "pass" || evidence.Phase != "terminal_post_stop_verified" ||
		evidence.PhysicalBefore != 3 || evidence.PhysicalPeak != 4 || evidence.PhysicalAfter != 3 ||
		evidence.ApplicationGroupsMoved == 0 || evidence.InternalGroupsMoved == 0 ||
		!evidence.EmptyTargetAtEnrollment || !evidence.ControllerRestarted || !evidence.TargetRestarted ||
		!evidence.DuplicateOperationStable || !evidence.PostRestartOperationRecovered || !evidence.SafeToStop ||
		!evidence.NodeStopped || !evidence.AcknowledgedDataIntact || !evidence.NoSkippedSuccess ||
		evidence.Cycles < 3 || evidence.BaselineWindows < 5 || evidence.DuringWindows < 3 || evidence.AfterWindows < 3 ||
		!evidence.SurvivorSessionStable || !evidence.RetiringSessionBlocked || !evidence.RetiringSessionReleased ||
		evidence.RetiringReferencesAfter != 0 || !validEvidenceDigest(evidence.GroupInventoryBeforeDigest) ||
		!validEvidenceDigest(evidence.GroupInventoryAfterDigest) || evidence.GroupInventoryBeforeDigest == evidence.GroupInventoryAfterDigest {
		return errors.New("seamless scale topology or conservation evidence is incomplete")
	}
	for _, phase := range []seamlessScalePhaseEvidence{evidence.Baseline, evidence.During, evidence.After} {
		if !phase.valid() || phase.StartNS == 0 || phase.EndNS <= phase.StartNS || phase.DurationNS != phase.EndNS-phase.StartNS ||
			phase.DurationNS < 10*timeSecondNS || phase.Scheduled < 10_000 || phase.Started != phase.Scheduled ||
			phase.Completed != phase.Started || phase.Requests != phase.Scheduled || phase.Successes != phase.Completed ||
			phase.Errors != 0 || phase.Timeouts != 0 || phase.Missed != 0 || phase.OfferedRateMilli == 0 ||
			phase.AcknowledgedWrites == 0 || phase.VerifiedReads < phase.AcknowledgedWrites ||
			phase.P50NS == 0 || phase.P95NS < phase.P50NS || phase.P99NS < phase.P95NS ||
			phase.MaxPauseNS == 0 || phase.CompletionGapNS == 0 || phase.QueueLagP99NS == 0 ||
			phase.ThroughputMilli == 0 || !validEvidenceDigest(phase.AcknowledgementDigest) ||
			!validEvidenceDigest(phase.VerificationDigest) || phase.AcknowledgementDigest != phase.VerificationDigest {
			return fmt.Errorf("invalid %s workload evidence", phase.Phase)
		}
	}
	if evidence.Budget.ThrottledCalls == 0 || evidence.Budget.ThrottledBytes == 0 ||
		evidence.Budget.MaxActive == 0 || evidence.Budget.PeakActive == 0 ||
		evidence.Budget.PeakActive > evidence.Budget.MaxActive {
		return errors.New("migration budget did not provide positive throttling evidence")
	}
	if evidence.Baseline.MaxPauseNS > bounds.MaxPauseNS || evidence.During.MaxPauseNS > bounds.MaxPauseNS ||
		evidence.After.MaxPauseNS > bounds.MaxPauseNS {
		return errors.New("foreground pause exceeded configured maximum")
	}
	if !withinContinuityBound(evidence.During.CompletionGapNS, evidence.Baseline.CompletionGapNS, evidence.Baseline.OfferedRateMilli) ||
		!withinContinuityBound(evidence.After.CompletionGapNS, evidence.Baseline.CompletionGapNS, evidence.Baseline.OfferedRateMilli) {
		return errors.New("completion gap exceeded configured continuity bound")
	}
	if !withinRelativeBound(evidence.During.P50NS, evidence.Baseline.P50NS, bounds.DuringP50PPM, bounds.LatencyFloor) ||
		!withinRelativeBound(evidence.During.P95NS, evidence.Baseline.P95NS, bounds.DuringP95PPM, bounds.LatencyFloor) ||
		!withinRelativeBound(evidence.During.P99NS, evidence.Baseline.P99NS, bounds.DuringP99PPM, bounds.LatencyFloor) ||
		!withinRelativeBound(evidence.After.P50NS, evidence.Baseline.P50NS, bounds.AfterP50PPM, bounds.LatencyFloor) ||
		!withinRelativeBound(evidence.After.P95NS, evidence.Baseline.P95NS, bounds.AfterP95PPM, bounds.LatencyFloor) ||
		!withinRelativeBound(evidence.After.P99NS, evidence.Baseline.P99NS, bounds.AfterP99PPM, bounds.LatencyFloor) {
		return errors.New("latency exceeded configured relative bound")
	}
	if !throughputAtLeast(evidence.During.ThroughputMilli, evidence.Baseline.ThroughputMilli, bounds.DuringTPSPPM) ||
		!throughputAtLeast(evidence.After.ThroughputMilli, evidence.Baseline.ThroughputMilli, bounds.AfterTPSPPM) {
		return errors.New("throughput fell below configured relative bound")
	}
	return nil
}

func withinContinuityBound(value, baselineGap, offeredRateMilli uint64) bool {
	if value == 0 || baselineGap == 0 || offeredRateMilli == 0 {
		return false
	}
	// The schedule interval is 1e12/offeredRateMilli nanoseconds. Allow two
	// arrivals plus a 1ms clock/measurement floor, or 1.25x the matched
	// baseline completion gap, whichever is larger.
	interval := uint64(1_000_000_000_000 / offeredRateMilli)
	baselineLimit := baselineGap
	if baselineGap <= math.MaxUint64/5 {
		baselineLimit = baselineGap * 5 / 4
	}
	scheduleLimit := interval
	if interval <= math.MaxUint64/2 {
		scheduleLimit = interval * 2
	} else {
		scheduleLimit = math.MaxUint64
	}
	limit := baselineLimit
	if scheduleLimit > limit {
		limit = scheduleLimit
	}
	if limit > math.MaxUint64-1_000_000 {
		limit = math.MaxUint64
	} else {
		limit += 1_000_000
	}
	return value <= limit
}

func withinRelativeBound(value, baseline, multiplierPPM, floor uint64) bool {
	if baseline == 0 || multiplierPPM == 0 {
		return false
	}
	if baseline > (math.MaxUint64-floor)/multiplierPPM {
		return false // Refuse an unrepresentable bound instead of fabricating success.
	}
	return value <= baseline*multiplierPPM/1_000_000+floor
}

func throughputAtLeast(value, baseline, minimumPPM uint64) bool {
	if baseline == 0 || minimumPPM == 0 {
		return false
	}
	if baseline > math.MaxUint64/minimumPPM {
		return false // The required floor cannot be represented exactly.
	}
	return value >= baseline*minimumPPM/1_000_000
}

func boolBit(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func validEvidenceDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return strings.Trim(value, "0") != ""
}

func appendSeamlessScaleEvidence(raw []byte, evidence seamlessScaleEvidence) []byte {
	raw = fmt.Appendf(raw, "schema\t%s\t%d\nresult\t%s\nphase\t%s\n",
		seamlessScaleEvidenceSchema, seamlessScaleEvidenceVersion, evidence.Result, evidence.Phase)
	raw = fmt.Appendf(raw, "topology\tphysical_before\t%d\n", evidence.PhysicalBefore)
	raw = fmt.Appendf(raw, "topology\tphysical_peak\t%d\n", evidence.PhysicalPeak)
	raw = fmt.Appendf(raw, "topology\tphysical_after\t%d\n", evidence.PhysicalAfter)
	raw = fmt.Appendf(raw, "topology\tapplication_groups_moved\t%d\n", evidence.ApplicationGroupsMoved)
	raw = fmt.Appendf(raw, "topology\tinternal_groups_moved\t%d\n", evidence.InternalGroupsMoved)
	raw = fmt.Appendf(raw, "topology\tcycles\t%d\n", evidence.Cycles)
	raw = fmt.Appendf(raw, "topology\tbaseline_windows\t%d\n", evidence.BaselineWindows)
	raw = fmt.Appendf(raw, "topology\tduring_windows\t%d\n", evidence.DuringWindows)
	raw = fmt.Appendf(raw, "topology\tafter_windows\t%d\n", evidence.AfterWindows)
	raw = fmt.Appendf(raw, "topology\tretiring_references_after\t%d\n", evidence.RetiringReferencesAfter)
	raw = fmt.Appendf(raw, "topology\tgroup_inventory_before_digest\t%s\n", evidence.GroupInventoryBeforeDigest)
	raw = fmt.Appendf(raw, "topology\tgroup_inventory_after_digest\t%s\n", evidence.GroupInventoryAfterDigest)
	for _, marker := range []struct {
		name  string
		value bool
	}{
		{"empty_target_at_enrollment", evidence.EmptyTargetAtEnrollment},
		{"controller_restarted", evidence.ControllerRestarted},
		{"target_restarted", evidence.TargetRestarted},
		{"duplicate_operation_stable", evidence.DuplicateOperationStable},
		{"post_restart_operation_recovered", evidence.PostRestartOperationRecovered},
		{"safe_to_stop", evidence.SafeToStop},
		{"node_stopped", evidence.NodeStopped},
		{"acknowledged_data_intact", evidence.AcknowledgedDataIntact},
		{"no_skipped_success", evidence.NoSkippedSuccess},
		{"survivor_session_stable", evidence.SurvivorSessionStable},
		{"retiring_session_blocked", evidence.RetiringSessionBlocked},
		{"retiring_session_released", evidence.RetiringSessionReleased},
	} {
		raw = fmt.Appendf(raw, "marker\t%s\t%d\n", marker.name, boolBit(marker.value))
	}
	for _, phase := range []seamlessScalePhaseEvidence{evidence.Baseline, evidence.During, evidence.After} {
		for _, metric := range []struct {
			name  string
			value uint64
		}{
			{"start_ns", phase.StartNS}, {"end_ns", phase.EndNS}, {"duration_ns", phase.DurationNS},
			{"scheduled", phase.Scheduled}, {"started", phase.Started}, {"completed", phase.Completed},
			{"requests", phase.Requests}, {"successes", phase.Successes}, {"errors", phase.Errors},
			{"timeouts", phase.Timeouts}, {"missed", phase.Missed}, {"retries", phase.Retries},
			{"acknowledged_writes", phase.AcknowledgedWrites}, {"verified_reads", phase.VerifiedReads},
			{"p50_ns", phase.P50NS}, {"p95_ns", phase.P95NS}, {"p99_ns", phase.P99NS},
			{"max_pause_ns", phase.MaxPauseNS}, {"completion_gap_ns", phase.CompletionGapNS},
			{"queue_lag_p99_ns", phase.QueueLagP99NS}, {"offered_rate_milli", phase.OfferedRateMilli},
			{"throughput_milli", phase.ThroughputMilli},
		} {
			raw = fmt.Appendf(raw, "workload\t%s\t%s\t%d\n", phase.Phase, metric.name, metric.value)
		}
		raw = fmt.Appendf(raw, "workload\t%s\tacknowledgement_digest\t%s\n", phase.Phase, phase.AcknowledgementDigest)
		raw = fmt.Appendf(raw, "workload\t%s\tverification_digest\t%s\n", phase.Phase, phase.VerificationDigest)
	}
	raw = fmt.Appendf(raw, "budget\tthrottled_calls\t%d\nbudget\tthrottled_bytes\t%d\nbudget\tpeak_active\t%d\nbudget\tmax_active\t%d\n",
		evidence.Budget.ThrottledCalls, evidence.Budget.ThrottledBytes, evidence.Budget.PeakActive, evidence.Budget.MaxActive)
	return raw
}

// parseSeamlessScaleEvidence is intentionally strict about the top-level
// schema/result markers but ignores unknown rows. This keeps the acceptance
// parser forward-compatible while CI still refuses a missing required fact.
func parseSeamlessScaleEvidence(raw []byte) (seamlessScaleEvidence, error) {
	var evidence seamlessScaleEvidence
	seenSchema, seenResult, seenPhase := false, false, false
	seenWorkload := map[string]map[string]bool{}
	seenTopology := map[string]bool{}
	seenMarkers := map[string]bool{}
	seenBudget := map[string]bool{}
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	scanner.Buffer(make([]byte, 1024), 64<<10)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) < 2 {
			return seamlessScaleEvidence{}, errors.New("malformed seamless scale evidence row")
		}
		switch fields[0] {
		case "schema":
			if len(fields) != 3 || fields[1] != seamlessScaleEvidenceSchema || fields[2] != strconv.Itoa(seamlessScaleEvidenceVersion) || seenSchema {
				return seamlessScaleEvidence{}, errors.New("invalid seamless scale evidence schema")
			}
			seenSchema = true
		case "result":
			if len(fields) != 2 || seenResult {
				return seamlessScaleEvidence{}, errors.New("invalid seamless scale evidence result")
			}
			evidence.Result, seenResult = fields[1], true
		case "phase":
			if len(fields) != 2 || seenPhase {
				return seamlessScaleEvidence{}, errors.New("invalid seamless scale evidence phase")
			}
			evidence.Phase, seenPhase = fields[1], true
		case "topology":
			if len(fields) != 3 {
				return seamlessScaleEvidence{}, errors.New("invalid topology evidence row")
			}
			known := false
			switch fields[1] {
			case "physical_before", "physical_peak", "physical_after", "application_groups_moved", "internal_groups_moved",
				"cycles", "baseline_windows", "during_windows", "after_windows", "retiring_references_after",
				"group_inventory_before_digest", "group_inventory_after_digest":
				known = true
			}
			if known && seenTopology[fields[1]] {
				return seamlessScaleEvidence{}, errors.New("duplicate topology evidence row")
			}
			if strings.HasSuffix(fields[1], "_digest") {
				if !validEvidenceDigest(fields[2]) {
					return seamlessScaleEvidence{}, errors.New("invalid topology digest")
				}
				switch fields[1] {
				case "group_inventory_before_digest":
					evidence.GroupInventoryBeforeDigest = fields[2]
				case "group_inventory_after_digest":
					evidence.GroupInventoryAfterDigest = fields[2]
				}
				seenTopology[fields[1]] = true
				continue
			}
			value, err := parseEvidenceUint(fields[2])
			if err != nil {
				return seamlessScaleEvidence{}, err
			}
			switch fields[1] {
			case "physical_before":
				evidence.PhysicalBefore = value
				seenTopology[fields[1]] = true
			case "physical_peak":
				evidence.PhysicalPeak = value
				seenTopology[fields[1]] = true
			case "physical_after":
				evidence.PhysicalAfter = value
				seenTopology[fields[1]] = true
			case "application_groups_moved":
				evidence.ApplicationGroupsMoved = value
				seenTopology[fields[1]] = true
			case "internal_groups_moved":
				evidence.InternalGroupsMoved = value
				seenTopology[fields[1]] = true
			case "cycles":
				evidence.Cycles = value
				seenTopology[fields[1]] = true
			case "baseline_windows":
				evidence.BaselineWindows = value
				seenTopology[fields[1]] = true
			case "during_windows":
				evidence.DuringWindows = value
				seenTopology[fields[1]] = true
			case "after_windows":
				evidence.AfterWindows = value
				seenTopology[fields[1]] = true
			case "retiring_references_after":
				evidence.RetiringReferencesAfter = value
				seenTopology[fields[1]] = true
			}
		case "marker":
			if len(fields) != 3 {
				return seamlessScaleEvidence{}, errors.New("invalid marker evidence row")
			}
			value, err := parseEvidenceUint(fields[2])
			if err != nil || value > 1 {
				return seamlessScaleEvidence{}, errors.New("invalid marker value")
			}
			known := false
			switch fields[1] {
			case "empty_target_at_enrollment", "controller_restarted", "target_restarted", "duplicate_operation_stable", "post_restart_operation_recovered", "safe_to_stop", "node_stopped", "acknowledged_data_intact", "no_skipped_success", "survivor_session_stable", "retiring_session_blocked", "retiring_session_released":
				known = true
			}
			if known && seenMarkers[fields[1]] {
				return seamlessScaleEvidence{}, errors.New("duplicate marker evidence row")
			}
			marker := value == 1
			switch fields[1] {
			case "empty_target_at_enrollment":
				evidence.EmptyTargetAtEnrollment = marker
				seenMarkers[fields[1]] = true
			case "controller_restarted":
				evidence.ControllerRestarted = marker
				seenMarkers[fields[1]] = true
			case "target_restarted":
				evidence.TargetRestarted = marker
				seenMarkers[fields[1]] = true
			case "duplicate_operation_stable":
				evidence.DuplicateOperationStable = marker
				seenMarkers[fields[1]] = true
			case "post_restart_operation_recovered":
				evidence.PostRestartOperationRecovered = marker
				seenMarkers[fields[1]] = true
			case "safe_to_stop":
				evidence.SafeToStop = marker
				seenMarkers[fields[1]] = true
			case "node_stopped":
				evidence.NodeStopped = marker
				seenMarkers[fields[1]] = true
			case "acknowledged_data_intact":
				evidence.AcknowledgedDataIntact = marker
				seenMarkers[fields[1]] = true
			case "no_skipped_success":
				evidence.NoSkippedSuccess = marker
				seenMarkers[fields[1]] = true
			case "survivor_session_stable":
				evidence.SurvivorSessionStable = marker
				seenMarkers[fields[1]] = true
			case "retiring_session_blocked":
				evidence.RetiringSessionBlocked = marker
				seenMarkers[fields[1]] = true
			case "retiring_session_released":
				evidence.RetiringSessionReleased = marker
				seenMarkers[fields[1]] = true
			}
		case "workload":
			if len(fields) != 4 || !validSeamlessScalePhaseName(fields[1]) {
				return seamlessScaleEvidence{}, errors.New("invalid workload header")
			}
			validMetric := false
			switch fields[2] {
			case "start_ns", "end_ns", "duration_ns", "scheduled", "started", "completed", "requests", "successes", "errors", "timeouts", "missed", "retries", "acknowledged_writes", "verified_reads", "p50_ns", "p95_ns", "p99_ns", "max_pause_ns", "completion_gap_ns", "queue_lag_p99_ns", "offered_rate_milli", "throughput_milli":
				validMetric = true
			}
			if fields[2] == "acknowledgement_digest" || fields[2] == "verification_digest" {
				if !validEvidenceDigest(fields[3]) {
					return seamlessScaleEvidence{}, errors.New("invalid workload digest")
				}
				metrics := seenWorkload[fields[1]]
				if metrics == nil {
					metrics = make(map[string]bool)
					seenWorkload[fields[1]] = metrics
				}
				if metrics[fields[2]] {
					return seamlessScaleEvidence{}, errors.New("duplicate workload digest")
				}
				metrics[fields[2]] = true
				phase := newSeamlessScalePhase(&evidence, fields[1])
				if fields[2] == "acknowledgement_digest" {
					phase.AcknowledgementDigest = fields[3]
				} else {
					phase.VerificationDigest = fields[3]
				}
				continue
			}
			if !validMetric {
				return seamlessScaleEvidence{}, errors.New("unknown workload field")
			}
			value, err := parseEvidenceUint(fields[3])
			if err != nil {
				return seamlessScaleEvidence{}, err
			}
			metrics := seenWorkload[fields[1]]
			if metrics == nil {
				metrics = make(map[string]bool)
				seenWorkload[fields[1]] = metrics
			}
			if metrics[fields[2]] {
				return seamlessScaleEvidence{}, errors.New("duplicate workload metric")
			}
			metrics[fields[2]] = true
			phase := newSeamlessScalePhase(&evidence, fields[1])
			switch fields[2] {
			case "start_ns":
				phase.StartNS = value
			case "end_ns":
				phase.EndNS = value
			case "duration_ns":
				phase.DurationNS = value
			case "scheduled":
				phase.Scheduled = value
			case "started":
				phase.Started = value
			case "completed":
				phase.Completed = value
			case "requests":
				phase.Requests = value
			case "successes":
				phase.Successes = value
			case "errors":
				phase.Errors = value
			case "timeouts":
				phase.Timeouts = value
			case "missed":
				phase.Missed = value
			case "retries":
				phase.Retries = value
			case "acknowledged_writes":
				phase.AcknowledgedWrites = value
			case "verified_reads":
				phase.VerifiedReads = value
			case "p50_ns":
				phase.P50NS = value
			case "p95_ns":
				phase.P95NS = value
			case "p99_ns":
				phase.P99NS = value
			case "max_pause_ns":
				phase.MaxPauseNS = value
			case "completion_gap_ns":
				phase.CompletionGapNS = value
			case "queue_lag_p99_ns":
				phase.QueueLagP99NS = value
			case "offered_rate_milli":
				phase.OfferedRateMilli = value
			case "throughput_milli":
				phase.ThroughputMilli = value
			}
		case "budget":
			if len(fields) != 3 {
				return seamlessScaleEvidence{}, errors.New("invalid budget evidence row")
			}
			value, err := parseEvidenceUint(fields[2])
			if err != nil {
				return seamlessScaleEvidence{}, err
			}
			known := false
			switch fields[1] {
			case "throttled_calls", "throttled_bytes", "peak_active", "max_active":
				known = true
			}
			if known && seenBudget[fields[1]] {
				return seamlessScaleEvidence{}, errors.New("duplicate budget evidence row")
			}
			switch fields[1] {
			case "throttled_calls":
				evidence.Budget.ThrottledCalls = value
				seenBudget[fields[1]] = true
			case "throttled_bytes":
				evidence.Budget.ThrottledBytes = value
				seenBudget[fields[1]] = true
			case "peak_active":
				evidence.Budget.PeakActive = value
				seenBudget[fields[1]] = true
			case "max_active":
				evidence.Budget.MaxActive = value
				seenBudget[fields[1]] = true
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return seamlessScaleEvidence{}, err
	}
	if !seenSchema || !seenResult || !seenPhase {
		return seamlessScaleEvidence{}, errors.New("seamless scale evidence is missing required markers")
	}
	for _, name := range []string{seamlessScalePhaseBaseline, seamlessScalePhaseDuring, seamlessScalePhaseAfter} {
		if len(seenWorkload[name]) != 24 {
			return seamlessScaleEvidence{}, fmt.Errorf("workload %s is incomplete", name)
		}
	}
	return evidence, nil
}

func validSeamlessScalePhaseName(name string) bool {
	return name == seamlessScalePhaseBaseline || name == seamlessScalePhaseDuring || name == seamlessScalePhaseAfter
}

func newSeamlessScalePhase(evidence *seamlessScaleEvidence, name string) *seamlessScalePhaseEvidence {
	switch name {
	case seamlessScalePhaseBaseline:
		return &evidence.Baseline
	case seamlessScalePhaseDuring:
		return &evidence.During
	default:
		return &evidence.After
	}
}

func parseEvidenceUint(value string) (uint64, error) {
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid evidence integer %q", value)
	}
	return parsed, nil
}

func writeSeamlessScaleEvidence(path string, evidence seamlessScaleEvidence) error {
	if path == "" || !filepath.IsAbs(path) {
		return errors.New("seamless scale evidence path must be absolute")
	}
	raw := appendSeamlessScaleEvidence(nil, evidence)
	if len(raw) > 64<<10 {
		return errors.New("seamless scale evidence exceeds 64 KiB")
	}
	temporary := path + ".pending"
	if err := os.WriteFile(temporary, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return nil
}

func TestSeamlessScalePerformanceBoundsAreConfigurableAndRelative(t *testing.T) {
	bounds, err := loadSeamlessScalePerformanceBounds(func(name string) string {
		return map[string]string{
			"VIBEDB_SCALE_DURING_P99_PPM":       "7000000",
			"VIBEDB_SCALE_AFTER_THROUGHPUT_PPM": "750000",
			"VIBEDB_SCALE_MAX_PAUSE_NS":         "3000000000",
		}[name]
	})
	if err != nil || bounds.DuringP99PPM != 7_000_000 || bounds.AfterTPSPPM != 750_000 || bounds.MaxPauseNS != 3_000_000_000 {
		t.Fatalf("configured bounds=%+v err=%v", bounds, err)
	}
	if _, err := loadSeamlessScalePerformanceBounds(func(string) string { return "-1" }); err == nil {
		t.Fatal("negative bound accepted")
	}
	if withinRelativeBound(8_000_001, 1_000_000, 8_000_000, 0) {
		t.Fatal("latency above relative bound accepted")
	}
	if !withinRelativeBound(10_000_000, 1_000_000, 8_000_000, 2_000_000) {
		t.Fatal("latency additive floor was ignored")
	}
	if throughputAtLeast(499_999, 1_000_000, 500_000) {
		t.Fatal("throughput below relative floor accepted")
	}
}

func TestSeamlessScaleEvidenceRequiresPositivePacingAndConservation(t *testing.T) {
	bounds := defaultSeamlessScalePerformanceBounds()
	validPhase := func(name string) seamlessScalePhaseEvidence {
		return seamlessScalePhaseEvidence{Phase: name, StartNS: 1, EndNS: 10*timeSecondNS + 1, DurationNS: 10 * timeSecondNS,
			Scheduled: 10_000, Started: 10_000, Completed: 10_000, Requests: 10_000, Successes: 10_000,
			AcknowledgedWrites: 20,
			VerifiedReads:      20, P50NS: 1_000_000, P95NS: 2_000_000, P99NS: 3_000_000,
			MaxPauseNS: 4_000_000, CompletionGapNS: 4_000_000, QueueLagP99NS: 4_000_000,
			OfferedRateMilli: 1_000_000, ThroughputMilli: 1_000_000,
			AcknowledgementDigest: "1111111111111111111111111111111111111111111111111111111111111111",
			VerificationDigest:    "1111111111111111111111111111111111111111111111111111111111111111"}
	}
	evidence := seamlessScaleEvidence{
		Result: "pass", Phase: "terminal_post_stop_verified", PhysicalBefore: 3, PhysicalPeak: 4, PhysicalAfter: 3,
		ApplicationGroupsMoved: 2, InternalGroupsMoved: 1, EmptyTargetAtEnrollment: true, ControllerRestarted: true,
		TargetRestarted: true, DuplicateOperationStable: true, PostRestartOperationRecovered: true, SafeToStop: true,
		NodeStopped: true, AcknowledgedDataIntact: true, NoSkippedSuccess: true, Cycles: 3, BaselineWindows: 5,
		DuringWindows: 3, AfterWindows: 3, SurvivorSessionStable: true, RetiringSessionBlocked: true,
		RetiringSessionReleased: true, GroupInventoryBeforeDigest: "1111111111111111111111111111111111111111111111111111111111111111",
		GroupInventoryAfterDigest: "2222222222222222222222222222222222222222222222222222222222222222",
		Baseline:                  validPhase(seamlessScalePhaseBaseline), During: validPhase(seamlessScalePhaseDuring), After: validPhase(seamlessScalePhaseAfter),
		Budget: seamlessScaleBudgetEvidence{ThrottledCalls: 4, ThrottledBytes: 8 << 20, PeakActive: 2, MaxActive: 2},
	}
	if err := evidence.valid(bounds); err != nil {
		t.Fatal(err)
	}
	evidence.Budget.ThrottledCalls = 0
	if err := evidence.valid(bounds); err == nil {
		t.Fatal("evidence without positive throttling accepted")
	}
	evidence.Budget.ThrottledCalls = 4
	evidence.NoSkippedSuccess = false
	if err := evidence.valid(bounds); err == nil {
		t.Fatal("evidence with skipped success accepted")
	}
	evidence.NoSkippedSuccess = true
	evidence.During.AcknowledgedWrites++
	if err := evidence.valid(bounds); err == nil {
		t.Fatal("acknowledged write without verification accepted")
	}
}

func TestSeamlessScaleEvidenceRoundTripsCanonicalRows(t *testing.T) {
	evidence := seamlessScaleEvidence{
		Result: "pass", Phase: "terminal_post_stop_verified", PhysicalBefore: 3, PhysicalPeak: 4, PhysicalAfter: 3,
		ApplicationGroupsMoved: 1, InternalGroupsMoved: 1, EmptyTargetAtEnrollment: true, ControllerRestarted: true,
		TargetRestarted: true, DuplicateOperationStable: true, PostRestartOperationRecovered: true, SafeToStop: true,
		NodeStopped: true, AcknowledgedDataIntact: true, NoSkippedSuccess: true, Cycles: 3, BaselineWindows: 5,
		DuringWindows: 3, AfterWindows: 3, SurvivorSessionStable: true, RetiringSessionBlocked: true, RetiringSessionReleased: true,
		GroupInventoryBeforeDigest: "1111111111111111111111111111111111111111111111111111111111111111",
		GroupInventoryAfterDigest:  "2222222222222222222222222222222222222222222222222222222222222222",
		Baseline:                   strictSeamlessScaleRoundTripPhase(seamlessScalePhaseBaseline),
		During:                     strictSeamlessScaleRoundTripPhase(seamlessScalePhaseDuring),
		After:                      strictSeamlessScaleRoundTripPhase(seamlessScalePhaseAfter),
		Budget:                     seamlessScaleBudgetEvidence{ThrottledCalls: 1, ThrottledBytes: 1, PeakActive: 1, MaxActive: 1},
	}
	raw := appendSeamlessScaleEvidence(nil, evidence)
	parsed, err := parseSeamlessScaleEvidence(raw)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Result != evidence.Result || parsed.PhysicalPeak != 4 || parsed.During.P99NS != 3 ||
		!parsed.EmptyTargetAtEnrollment || parsed.Budget.ThrottledBytes != 1 {
		t.Fatalf("round trip=%+v", parsed)
	}
	if _, err := parseSeamlessScaleEvidence(append(raw, []byte("result\tpass\n")...)); err == nil {
		t.Fatal("duplicate result marker accepted")
	}
	for _, row := range []string{
		"topology\tphysical_peak\t4\n",
		"marker\tnode_stopped\t1\n",
		"budget\tmax_active\t1\n",
	} {
		if _, err := parseSeamlessScaleEvidence(append(raw, []byte(row)...)); err == nil {
			t.Fatalf("duplicate evidence row accepted: %q", row)
		}
	}
}

func strictSeamlessScaleRoundTripPhase(name string) seamlessScalePhaseEvidence {
	return seamlessScalePhaseEvidence{Phase: name, StartNS: 1, EndNS: 10*timeSecondNS + 1, DurationNS: 10 * timeSecondNS,
		Scheduled: 10_000, Started: 10_000, Completed: 10_000, Requests: 10_000, Successes: 10_000,
		AcknowledgedWrites: 1, VerifiedReads: 1, P50NS: 1, P95NS: 2, P99NS: 3, MaxPauseNS: 4,
		CompletionGapNS: 4, QueueLagP99NS: 4, OfferedRateMilli: 1, ThroughputMilli: 1,
		AcknowledgementDigest: "1111111111111111111111111111111111111111111111111111111111111111",
		VerificationDigest:    "1111111111111111111111111111111111111111111111111111111111111111"}
}
