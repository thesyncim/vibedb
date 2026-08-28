package gateway

import (
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// DurableRequestLedgerReadObservation describes one completed RF3 recovery
// read. ResponseBytes is the transferred wire value size, including values
// that subsequently fail canonical decoding. Retries is the transport's
// observed retry count. Failed distinguishes transport, validation, and decode
// failures from authoritative not-found results.
type DurableRequestLedgerReadObservation struct {
	Kind          replicatedstate.RequestLedgerReadKind
	Retries       uint64
	ResponseBytes uint64
	Failed        bool
}

// DurableRequestLedgerReadCollector receives one observation per ReadRow or
// ReadWaveCut call. It must be safe for concurrent use and must not retain
// references to call-owned state. Collection is opt-in so the default serving
// path pays no contended atomic increment.
type DurableRequestLedgerReadCollector interface {
	ObserveDurableRequestLedgerRead(DurableRequestLedgerReadObservation)
}

// DurableRequestLedgerReadSnapshot is a copyable view of one read kind's
// cumulative counters.
type DurableRequestLedgerReadSnapshot struct {
	Calls         uint64
	Retries       uint64
	ResponseBytes uint64
	Errors        uint64
}

type durableRequestLedgerReadCounters struct {
	calls         atomic.Uint64
	retries       atomic.Uint64
	responseBytes atomic.Uint64
	errors        atomic.Uint64
}

const durableRequestLedgerReadKindCount = 15

// DurableRequestLedgerReadMetrics is the optional lock-free collector shipped
// with the gateway. It uses a dense closed-kind index rather than allocating a
// sparse table for the synthetic 0xe0 and 0xf0 read kinds.
type DurableRequestLedgerReadMetrics struct {
	byKind [durableRequestLedgerReadKindCount]durableRequestLedgerReadCounters
}

func (metrics *DurableRequestLedgerReadMetrics) ObserveDurableRequestLedgerRead(
	observation DurableRequestLedgerReadObservation,
) {
	if metrics == nil {
		return
	}
	index, ok := durableRequestLedgerReadKindIndex(observation.Kind)
	if !ok {
		return
	}
	counters := &metrics.byKind[index]
	counters.calls.Add(1)
	counters.retries.Add(observation.Retries)
	counters.responseBytes.Add(observation.ResponseBytes)
	if observation.Failed {
		counters.errors.Add(1)
	}
}

// Snapshot returns a consistent-enough cumulative view for one read kind.
// Unknown kinds return the zero snapshot.
func (metrics *DurableRequestLedgerReadMetrics) Snapshot(
	kind replicatedstate.RequestLedgerReadKind,
) DurableRequestLedgerReadSnapshot {
	if metrics == nil {
		return DurableRequestLedgerReadSnapshot{}
	}
	index, ok := durableRequestLedgerReadKindIndex(kind)
	if !ok {
		return DurableRequestLedgerReadSnapshot{}
	}
	counters := &metrics.byKind[index]
	return DurableRequestLedgerReadSnapshot{
		Calls: counters.calls.Load(), Retries: counters.retries.Load(),
		ResponseBytes: counters.responseBytes.Load(), Errors: counters.errors.Load(),
	}
}

func durableRequestLedgerReadKindIndex(kind replicatedstate.RequestLedgerReadKind) (uint8, bool) {
	switch kind {
	case replicatedstate.RequestLedgerReadHead:
		return 0, true
	case replicatedstate.RequestLedgerReadPlanPage:
		return 1, true
	case replicatedstate.RequestLedgerReadPending:
		return 2, true
	case replicatedstate.RequestLedgerReadTerminal:
		return 3, true
	case replicatedstate.RequestLedgerReadAck:
		return 4, true
	case replicatedstate.RequestLedgerReadContinuation:
		return 5, true
	case replicatedstate.RequestLedgerReadPayloadChunk:
		return 6, true
	case replicatedstate.RequestLedgerReadPayloadBuild:
		return 7, true
	case replicatedstate.RequestLedgerReadRoutePin:
		return 8, true
	case replicatedstate.RequestLedgerReadPrepared:
		return 9, true
	case replicatedstate.RequestLedgerReadSchemaPin:
		return 10, true
	case replicatedstate.RequestLedgerReadWave:
		return 11, true
	case replicatedstate.RequestLedgerReadProgress:
		return 12, true
	case replicatedstate.RequestLedgerReadTerminalCut:
		return 13, true
	case replicatedstate.RequestLedgerReadIssuerStatus:
		return 14, true
	default:
		return 0, false
	}
}

// DurableRequestLedgerRF3Option configures optional RF3 lifecycle adapters.
type DurableRequestLedgerRF3Option func(*DurableRequestLedgerRF3)

// WithDurableRequestLedgerReadCollector enables per-kind recovery-read
// observations. Passing nil explicitly keeps collection disabled.
func WithDurableRequestLedgerReadCollector(
	collector DurableRequestLedgerReadCollector,
) DurableRequestLedgerRF3Option {
	return func(ledger *DurableRequestLedgerRF3) {
		ledger.readCollector = collector
	}
}
