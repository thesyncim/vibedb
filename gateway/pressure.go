package gateway

import (
	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/shardservice"
)

// PressureObservation is a borrowed, synchronous gateway admission sample.
// It contains no SQL, document, tenant identity, or wall-clock timestamp.
// AccessScopes are the already-planned canonical virtual-bucket intervals.
type PressureObservation struct {
	Source       autosplit.SourceIdentity
	AccessScopes []distributedtxn.IntentScope
	Write        bool
}

// PressureObserver receives routed demand after every topology and ownership
// fence has been constructed and immediately before dispatch. Implementations
// must copy anything they retain; calls may be concurrent.
type PressureObserver interface {
	ObservePressure(PressureObservation)
}

func (e *Executor) observePressureCalls(calls []shardCall) {
	if e == nil || e.pressure == nil {
		return
	}
	for index := range calls {
		e.observePressureCall(calls[index])
	}
}

func (e *Executor) observePressureCall(call shardCall) {
	if e == nil || e.pressure == nil || call.req == nil ||
		call.pressureSource == (autosplit.SourceIdentity{}) {
		return
	}
	e.pressure.ObservePressure(PressureObservation{Source: call.pressureSource,
		AccessScopes: call.req.AccessScopes,
		Write:        call.req.ExecutionMode == shardservice.ExecutionReadWrite,
	})
}
